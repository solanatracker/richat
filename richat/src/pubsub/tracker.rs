use {
    crate::{
        channel::{Messages, ParsedMessage},
        metrics,
        pubsub::{
            ClientId, SubscriptionId,
            notification::{
                ClientSender, RpcBlockUpdate, RpcNotification, RpcNotifications,
                RpcTransactionUpdate,
            },
            solana::{
                LogsSubscriptionKind, SubscribeConfig, SubscribeConfigHashId, SubscribeMethod,
            },
        },
    },
    ::metrics::{Gauge, gauge},
    foldhash::quality::RandomState,
    prost_types::Timestamp,
    rayon::{
        ThreadPoolBuilder,
        iter::{IntoParallelIterator, ParallelIterator},
    },
    richat_filter::message::{MessageSlot, MessageTransaction},
    richat_proto::{convert_from, geyser::SlotStatus},
    richat_shared::five8::{pubkey_encode, signature_encode},
    solana_account::ReadableAccount,
    solana_account_decoder::encode_ui_account,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    solana_nohash_hasher::{IntMap, IntSet},
    solana_pubkey::Pubkey,
    solana_rpc_client_api::response::{
        ProcessedSignatureResult, RpcKeyedAccount, RpcLogsResponse, RpcSignatureResult, SlotInfo,
        SlotTransactionStats, SlotUpdate,
    },
    solana_signature::Signature,
    solana_transaction_error::TransactionError,
    std::{
        collections::{BTreeMap, HashMap},
        hash::Hash,
        sync::{Arc, Weak},
        thread,
        time::Duration,
    },
    tokio::sync::oneshot,
};

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ClientRequest {
    /// Sent once by the websocket task before any other request of the client.
    Register {
        client_id: ClientId,
        sender: ClientSender,
    },
    Subscribe {
        client_id: ClientId,
        config: SubscribeConfig,
        tx: oneshot::Sender<(SubscriptionId, SubscribeMethod)>,
    },
    Unsubscribe {
        client_id: ClientId,
        subscription_id: SubscriptionId,
        tx: oneshot::Sender<bool>,
    },
    Remove {
        client_id: ClientId,
    },
}

#[derive(Debug)]
struct SubscriptionInfo {
    id: SubscriptionId,
    config_hash: SubscribeConfigHashId,
    config: SubscribeConfig,
    clients: IntSet<ClientId>,
}

#[derive(Debug)]
struct ClientInfo {
    sender: ClientSender,
    subscriptions: IntSet<SubscriptionId>,
}

/// Subscription ids by the key a message is matched on, so a message only visits subscriptions
/// that can match it instead of every subscription of the method.
#[derive(Debug, Default)]
struct SubscriptionsIndex {
    /// `accountSubscribe` by account pubkey
    account: HashMap<Pubkey, Vec<SubscriptionId>, RandomState>,
    /// `programSubscribe` by account owner
    program: HashMap<Pubkey, Vec<SubscriptionId>, RandomState>,
    /// `signatureSubscribe` by signature
    signature: HashMap<Signature, Vec<SubscriptionId>, RandomState>,
    /// `logsSubscribe` with `mentions` by mentioned pubkey
    logs_mentions: HashMap<Pubkey, Vec<SubscriptionId>, RandomState>,
    /// `logsSubscribe` with `all` / `allWithVotes`
    logs_all: IntSet<SubscriptionId>,
    /// methods without a key (slot, slotsUpdates, root, block, transaction)
    unkeyed: [IntSet<SubscriptionId>; SubscribeMethod::COUNT],
}

impl SubscriptionsIndex {
    fn bucket_insert<K: Eq + Hash>(
        map: &mut HashMap<K, Vec<SubscriptionId>, RandomState>,
        key: K,
        id: SubscriptionId,
    ) {
        map.entry(key).or_default().push(id);
    }

    fn bucket_remove<K: Eq + Hash>(
        map: &mut HashMap<K, Vec<SubscriptionId>, RandomState>,
        key: &K,
        id: SubscriptionId,
    ) {
        let ids = map
            .get_mut(key)
            .expect("subscriptions index inconsistent, remove #1");
        let index = ids
            .iter()
            .position(|value| *value == id)
            .expect("subscriptions index inconsistent, remove #2");
        ids.swap_remove(index);
        if ids.is_empty() {
            map.remove(key);
        }
    }

    fn insert(&mut self, id: SubscriptionId, config: &SubscribeConfig) {
        match config {
            SubscribeConfig::Account { pubkey, .. } => {
                Self::bucket_insert(&mut self.account, *pubkey, id)
            }
            SubscribeConfig::Program { pubkey, .. } => {
                Self::bucket_insert(&mut self.program, *pubkey, id)
            }
            SubscribeConfig::Signature { signature, .. } => {
                Self::bucket_insert(&mut self.signature, *signature, id)
            }
            SubscribeConfig::Logs {
                kind: LogsSubscriptionKind::Single(pubkey),
                ..
            } => Self::bucket_insert(&mut self.logs_mentions, *pubkey, id),
            SubscribeConfig::Logs { .. } => {
                self.logs_all.insert(id);
            }
            config => {
                self.unkeyed[config.method() as usize].insert(id);
            }
        }
    }

    fn remove(&mut self, id: SubscriptionId, config: &SubscribeConfig) {
        match config {
            SubscribeConfig::Account { pubkey, .. } => {
                Self::bucket_remove(&mut self.account, pubkey, id)
            }
            SubscribeConfig::Program { pubkey, .. } => {
                Self::bucket_remove(&mut self.program, pubkey, id)
            }
            SubscribeConfig::Signature { signature, .. } => {
                Self::bucket_remove(&mut self.signature, signature, id)
            }
            SubscribeConfig::Logs {
                kind: LogsSubscriptionKind::Single(pubkey),
                ..
            } => Self::bucket_remove(&mut self.logs_mentions, pubkey, id),
            SubscribeConfig::Logs { .. } => {
                assert!(
                    self.logs_all.remove(&id),
                    "subscriptions index inconsistent, remove #3"
                );
            }
            config => {
                assert!(
                    self.unkeyed[config.method() as usize].remove(&id),
                    "subscriptions index inconsistent, remove #4"
                );
            }
        }
    }

    /// Calls `f` for every subscription that can match the message, cost is O(candidates).
    fn candidates(
        &self,
        message: &ParsedMessage,
        mut f: impl FnMut(SubscribeMethod, SubscriptionId),
    ) {
        fn emit<'a>(
            f: &mut impl FnMut(SubscribeMethod, SubscriptionId),
            method: SubscribeMethod,
            ids: impl IntoIterator<Item = &'a SubscriptionId>,
        ) {
            for id in ids {
                f(method, *id);
            }
        }

        match message {
            ParsedMessage::Slot(_) => {
                for method in [
                    SubscribeMethod::Slot,
                    SubscribeMethod::SlotsUpdates,
                    SubscribeMethod::Root,
                ] {
                    emit(&mut f, method, &self.unkeyed[method as usize]);
                }
            }
            ParsedMessage::Account(message) => {
                if let Some(ids) = self.account.get(message.pubkey()) {
                    emit(&mut f, SubscribeMethod::Account, ids);
                }
                if let Some(ids) = self.program.get(message.owner()) {
                    emit(&mut f, SubscribeMethod::Program, ids);
                }
            }
            ParsedMessage::Transaction(message) => {
                emit(&mut f, SubscribeMethod::Logs, &self.logs_all);
                // iterate the smaller side; a subscription mentions one pubkey, so it is
                // visited at most once
                let keys = message.account_keys();
                if self.logs_mentions.len() < keys.len() {
                    for (pubkey, ids) in &self.logs_mentions {
                        if keys.contains(pubkey) {
                            emit(&mut f, SubscribeMethod::Logs, ids);
                        }
                    }
                } else {
                    for pubkey in keys {
                        if let Some(ids) = self.logs_mentions.get(pubkey) {
                            emit(&mut f, SubscribeMethod::Logs, ids);
                        }
                    }
                }
                if !self.signature.is_empty()
                    && let Some(ids) = self.signature.get(&message.signature())
                {
                    emit(&mut f, SubscribeMethod::Signature, ids);
                }
                let method = SubscribeMethod::Transaction;
                emit(&mut f, method, &self.unkeyed[method as usize]);
            }
            ParsedMessage::Block(_) => {
                let method = SubscribeMethod::Block;
                emit(&mut f, method, &self.unkeyed[method as usize]);
            }
            ParsedMessage::Entry(_)
            | ParsedMessage::BlockMeta(_)
            | ParsedMessage::BlockFooter(_)
            | ParsedMessage::EntryUpdateParent(_) => {}
        }
    }
}

const fn commitment_index(commitment: CommitmentLevel) -> usize {
    match commitment {
        CommitmentLevel::Processed => 0,
        CommitmentLevel::Confirmed => 1,
        CommitmentLevel::Finalized => 2,
    }
}

type Job<'a> = (
    SubscribeMethod,
    &'a ParsedMessage,
    &'a SubscriptionInfo,
    Option<SlotTransactionStats>,
);

#[derive(Debug, Default)]
struct Subscriptions {
    subscription_id: SubscriptionId,
    subscriptions_by_config: IntMap<SubscribeConfigHashId, SubscriptionId>,
    subscriptions: IntMap<SubscriptionId, SubscriptionInfo>,
    clients: IntMap<ClientId, ClientInfo>,
    index: [SubscriptionsIndex; 3],
}

impl Subscriptions {
    fn update(
        &mut self,
        request: ClientRequest,
        signatures: &mut CachedSignatures,
        notifications: &mut RpcNotifications,
    ) {
        match request {
            ClientRequest::Register { client_id, sender } => {
                self.remove_client(client_id);
                self.clients.insert(
                    client_id,
                    ClientInfo {
                        sender,
                        subscriptions: IntSet::default(),
                    },
                );
            }
            ClientRequest::Subscribe {
                client_id,
                config,
                tx,
            } => {
                let method = config.method();
                let subscription_id = self.subscribe(client_id, config, signatures, notifications);
                let _ = tx.send((subscription_id, method));
            }
            ClientRequest::Unsubscribe {
                client_id,
                subscription_id,
                tx,
            } => {
                let removed = self.unsubscribe(client_id, subscription_id);
                let _ = tx.send(removed);
            }
            ClientRequest::Remove { client_id } => self.remove_client(client_id),
        }
    }

    const fn next_subscription_id(&mut self) -> SubscriptionId {
        let subscription_id = self.subscription_id;
        self.subscription_id += 1;
        subscription_id
    }

    fn subscribe(
        &mut self,
        client_id: ClientId,
        config: SubscribeConfig,
        signatures: &mut CachedSignatures,
        notifications: &mut RpcNotifications,
    ) -> SubscriptionId {
        let Some(client) = self.clients.get_mut(&client_id) else {
            // client was dropped as lagged and is closing, nothing would be delivered
            return self.next_subscription_id();
        };

        let config_hash = config.get_hash_id();
        if let Some(subscription_id) = self.subscriptions_by_config.get(&config_hash).copied() {
            client.subscriptions.insert(subscription_id);
            self.subscriptions
                .get_mut(&subscription_id)
                .expect("subscriptions storage inconsistent, subscribe #1")
                .clients
                .insert(client_id);
            return subscription_id;
        }

        let subscription_id = self.next_subscription_id();
        let client = self
            .clients
            .get_mut(&client_id)
            .expect("subscriptions storage inconsistent, subscribe #2");

        if let SubscribeConfig::Signature {
            signature,
            commitment,
        } = &config
            && let Some((slot, err)) = signatures.get(signature, commitment.commitment)
        {
            let json = RpcNotification::serialize_with_context(
                "signatureNotification",
                subscription_id,
                slot,
                RpcSignatureResult::ProcessedSignature(ProcessedSignatureResult {
                    err: err.map(Into::into),
                }),
            );
            let json = notifications.push(json);
            if !client.sender.send(RpcNotification {
                subscription_id,
                method: SubscribeMethod::Signature,
                is_final: true,
                json,
            }) {
                self.remove_client(client_id);
            }
            return subscription_id;
        }

        client.subscriptions.insert(subscription_id);
        self.subscriptions_by_config
            .insert(config_hash, subscription_id);
        self.index[commitment_index(config.commitment())].insert(subscription_id, &config);
        self.subscriptions.insert(
            subscription_id,
            SubscriptionInfo {
                id: subscription_id,
                config_hash,
                config,
                clients: IntSet::from_iter([client_id]),
            },
        );
        subscription_id
    }

    fn unsubscribe(&mut self, client_id: ClientId, subscription_id: SubscriptionId) -> bool {
        if self
            .clients
            .get_mut(&client_id)
            .is_some_and(|client| client.subscriptions.remove(&subscription_id))
        {
            self.remove_client_subscription(subscription_id, client_id);
            true
        } else {
            false
        }
    }

    fn remove_client(&mut self, client_id: ClientId) {
        if let Some(client) = self.clients.remove(&client_id) {
            for subscription_id in client.subscriptions {
                self.remove_client_subscription(subscription_id, client_id);
            }
        }
    }

    fn remove_client_subscription(&mut self, subscription_id: SubscriptionId, client_id: ClientId) {
        let subscription = self
            .subscriptions
            .get_mut(&subscription_id)
            .expect("subscriptions storage inconsistent, remove client subscription #1");
        assert!(
            subscription.clients.remove(&client_id),
            "subscriptions storage inconsistent, remove client subscription #2"
        );

        // drop subscription if no clients left
        if subscription.clients.is_empty() {
            self.drop_subscription(subscription_id);
        }
    }

    /// Remove subscription from storage and index, returns clients that were subscribed.
    fn drop_subscription(&mut self, subscription_id: SubscriptionId) -> IntSet<ClientId> {
        let subscription = self
            .subscriptions
            .remove(&subscription_id)
            .expect("subscriptions storage inconsistent, drop subscription #1");
        assert!(
            self.subscriptions_by_config
                .remove(&subscription.config_hash)
                .is_some(),
            "subscriptions storage inconsistent, drop subscription #2"
        );
        self.index[commitment_index(subscription.config.commitment())]
            .remove(subscription_id, &subscription.config);
        subscription.clients
    }

    fn remove_subscription(&mut self, subscription_id: SubscriptionId) {
        for client_id in self.drop_subscription(subscription_id) {
            assert!(
                self.clients
                    .get_mut(&client_id)
                    .expect("subscriptions storage inconsistent, remove subscription #1")
                    .subscriptions
                    .remove(&subscription_id),
                "subscriptions storage inconsistent, remove subscription #2"
            );
        }
    }

    fn collect_jobs<'a>(
        &'a self,
        commitment: CommitmentLevel,
        message: &'a ParsedMessage,
        jobs: &mut Vec<Job<'a>>,
    ) {
        self.index[commitment_index(commitment)].candidates(message, |method, subscription_id| {
            let subscription = self
                .subscriptions
                .get(&subscription_id)
                .expect("subscriptions storage inconsistent, collect jobs");
            jobs.push((method, message, subscription, None));
        });
    }

    fn collect_slots_updates_jobs<'a>(
        &'a self,
        message: &'a ParsedMessage,
        stats: SlotTransactionStats,
        jobs: &mut Vec<Job<'a>>,
    ) {
        let index = &self.index[commitment_index(CommitmentLevel::Processed)];
        for subscription_id in &index.unkeyed[SubscribeMethod::SlotsUpdates as usize] {
            let subscription = self
                .subscriptions
                .get(subscription_id)
                .expect("subscriptions storage inconsistent, collect slots updates jobs");
            jobs.push((
                SubscribeMethod::SlotsUpdates,
                message,
                subscription,
                Some(stats),
            ));
        }
    }

    /// Store the encoded notification once and share it with every client of the subscription.
    fn deliver(
        &mut self,
        notifications: &mut RpcNotifications,
        subscription_id: SubscriptionId,
        method: SubscribeMethod,
        is_final: bool,
        json: Arc<String>,
    ) {
        // final subscription already notified in the same tick (same signature in two slots)
        let Some(subscription) = self.subscriptions.get(&subscription_id) else {
            return;
        };

        let json = notifications.push(json);
        let mut lagged = vec![];
        for client_id in subscription.clients.iter() {
            let client = self
                .clients
                .get(client_id)
                .expect("subscriptions storage inconsistent, deliver");
            if !client.sender.send(RpcNotification {
                subscription_id,
                method,
                is_final,
                json: Weak::clone(&json),
            }) {
                lagged.push(*client_id);
            }
        }

        if is_final {
            self.remove_subscription(subscription_id);
        }
        for client_id in lagged {
            self.remove_client(client_id);
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub fn subscriptions_worker(
    messages: Messages,
    clients_rx: kanal::AsyncReceiver<ClientRequest>,
    workers_count: usize,
    workers_affinity: Option<Vec<usize>>,
    max_clients_request_per_tick: usize,
    max_messages_per_commitment_per_tick: usize,
    mut notifications: RpcNotifications,
    signatures_cache_max: usize,
    signatures_cache_slots_max: usize,
) -> anyhow::Result<()> {
    // Subscriptions storage
    let mut subscriptions = Subscriptions::default();

    // Messages head
    let receiver = messages.to_receiver();
    let mut head_processed = messages.get_current_tail(CommitmentLevel::Processed);
    let mut head_confirmed = messages.get_current_tail(CommitmentLevel::Processed);
    let mut head_finalized = messages.get_current_tail(CommitmentLevel::Processed);

    // Subscriptions filters pool
    let workers = ThreadPoolBuilder::new()
        .num_threads(workers_count)
        .spawn_handler(move |thread| {
            let workers_affinity = workers_affinity.clone();
            thread::Builder::new()
                .name(format!("richatPSubWrk{:02}", thread.index()))
                .spawn(move || {
                    if let Some(cpus) = workers_affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    thread.run()
                })?;
            Ok(())
        })
        .build()?;

    // Signatures cache
    let mut signatures = CachedSignatures::new(signatures_cache_max, signatures_cache_slots_max);

    // main loop
    let mut slot_finalized = 0;
    let mut slots_stats = BTreeMap::<Slot, SlotTransactionStatsItem>::new();
    let mut messages_extra = vec![];
    loop {
        messages_extra.clear();

        // Update subscriptions from clients
        for _ in 0..max_clients_request_per_tick {
            match clients_rx.try_recv() {
                Ok(Some(request)) => {
                    subscriptions.update(request, &mut signatures, &mut notifications)
                }
                Ok(None) => break,
                Err(_) => return Ok(()), // means shutdown
            };
        }

        // Collect messages from channels
        let mut jobs = Vec::with_capacity(max_messages_per_commitment_per_tick * 3 * 3);
        let mut messages_processed = Vec::with_capacity(max_messages_per_commitment_per_tick);
        let mut messages_confirmed = Vec::with_capacity(max_messages_per_commitment_per_tick);
        let mut messages_finalized = Vec::with_capacity(max_messages_per_commitment_per_tick);
        for (commitment, head, messages) in [
            (
                CommitmentLevel::Processed,
                &mut head_processed,
                &mut messages_processed,
            ),
            (
                CommitmentLevel::Confirmed,
                &mut head_confirmed,
                &mut messages_confirmed,
            ),
            (
                CommitmentLevel::Finalized,
                &mut head_finalized,
                &mut messages_finalized,
            ),
        ] {
            while messages.len() < max_messages_per_commitment_per_tick {
                if let Some(message) = receiver.try_recv(commitment, *head)? {
                    *head += 1;
                    // ignore Slot messages for any commitment except processed
                    if commitment != CommitmentLevel::Processed
                        && let ParsedMessage::Slot(_) = &message
                    {
                        continue;
                    }
                    messages.push(message);
                } else {
                    break;
                }
            }
            for message in messages.iter() {
                if commitment == CommitmentLevel::Processed
                    && let Some((message, stats)) = match &message {
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotProcessed =>
                        {
                            gauge!(metrics::PUBSUB_SLOT, "commitment" => "processed")
                                .set(message.slot() as f64);
                            slots_stats
                                .entry(message.slot())
                                .or_default()
                                .add_created_at(message.slot(), message.created_at().into())
                        }
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotDead =>
                        {
                            signatures.dead_slot(message.slot());
                            None
                        }
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotConfirmed =>
                        {
                            gauge!(metrics::PUBSUB_SLOT, "commitment" => "confirmed")
                                .set(message.slot() as f64);
                            signatures.set_confirmed(message.slot());
                            None
                        }
                        ParsedMessage::Slot(message)
                            if message.status() == SlotStatus::SlotFinalized =>
                        {
                            gauge!(metrics::PUBSUB_SLOT, "commitment" => "finalized")
                                .set(message.slot() as f64);
                            signatures.set_finalized(message.slot());
                            slot_finalized = message.slot();
                            loop {
                                match slots_stats.keys().next().copied() {
                                    Some(slot_min) if slot_min <= slot_finalized => {
                                        slots_stats.remove(&slot_min);
                                    }
                                    _ => break,
                                }
                            }
                            None
                        }
                        ParsedMessage::Transaction(message) => {
                            signatures.add_signature(message);
                            slots_stats
                                .entry(message.slot())
                                .or_default()
                                .add_tx(message.failed())
                        }
                        ParsedMessage::Entry(message) => slots_stats
                            .entry(message.slot())
                            .or_default()
                            .add_entry(message.executed_transaction_count()),
                        ParsedMessage::BlockMeta(message) => {
                            slots_stats.entry(message.slot()).or_default().add_meta(
                                message.executed_transaction_count(),
                                message.entries_count(),
                            )
                        }
                        _ => None,
                    }
                {
                    messages_extra.push((message, stats));
                }

                subscriptions.collect_jobs(commitment, message, &mut jobs);
            }
        }
        for (message, stats) in messages_extra.iter() {
            subscriptions.collect_slots_updates_jobs(message, *stats, &mut jobs);
        }

        if jobs.is_empty() {
            thread::sleep(Duration::from_micros(1));
            continue;
        }

        // Filter messages
        let new_notifications = workers.install(|| {
            jobs.into_par_iter()
                .filter_map(|job| filter_job(job, slot_finalized))
                .collect::<Vec<_>>()
        });

        for (subscription_id, method, is_final, json) in new_notifications {
            subscriptions.deliver(&mut notifications, subscription_id, method, is_final, json);
        }
    }
}

/// Match the message against the subscription and encode the notification.
fn filter_job(
    (method, message, subscription, extra_info): Job<'_>,
    slot_finalized: Slot,
) -> Option<(SubscriptionId, SubscribeMethod, bool, Arc<String>)> {
    match (method, message) {
        (SubscribeMethod::Account, ParsedMessage::Account(message)) => {
            if let Some((encoding, data_slice)) = subscription.config.filter_account(message) {
                let json = RpcNotification::serialize_with_context(
                    "accountNotification",
                    subscription.id,
                    message.slot(),
                    encode_ui_account(
                        message.pubkey(),
                        message.as_ref(),
                        encoding,
                        None,
                        data_slice,
                    ),
                );
                return Some((subscription.id, method, false, json));
            }
        }
        (SubscribeMethod::Program, ParsedMessage::Account(message)) => {
            if let Some((encoding, data_slice)) = subscription.config.filter_program(message) {
                let json = RpcNotification::serialize_with_context(
                    "programNotification",
                    subscription.id,
                    message.slot(),
                    &RpcKeyedAccount {
                        pubkey: pubkey_encode(&(*message.pubkey()).to_bytes()), // TODO: use `.as_bytes()` from 2.2
                        account: encode_ui_account(
                            message.pubkey(),
                            message.as_ref(),
                            encoding,
                            None,
                            data_slice,
                        ),
                    },
                );
                return Some((subscription.id, method, false, json));
            }
        }
        (SubscribeMethod::Logs, ParsedMessage::Transaction(message)) => {
            if let Some((err, logs)) = subscription.config.filter_logs(message) {
                let json = RpcNotification::serialize_with_context(
                    "logsNotification",
                    subscription.id,
                    message.slot(),
                    &RpcLogsResponse {
                        signature: signature_encode(message.signature().as_array()),
                        err: err.map(Into::into),
                        logs,
                    },
                );
                return Some((subscription.id, method, false, json));
            }
        }
        (SubscribeMethod::Signature, ParsedMessage::Transaction(message)) => {
            if let Some(err) = subscription.config.filter_signature(message) {
                let json = RpcNotification::serialize_with_context(
                    "signatureNotification",
                    subscription.id,
                    message.slot(),
                    RpcSignatureResult::ProcessedSignature(ProcessedSignatureResult {
                        err: err.map(Into::into),
                    }),
                );
                return Some((subscription.id, method, true, json));
            }
        }
        (SubscribeMethod::Slot, ParsedMessage::Slot(message)) => {
            if message.status() == SlotStatus::SlotCreatedBank {
                let json = RpcNotification::serialize(
                    "slotNotification",
                    subscription.id,
                    SlotInfo {
                        slot: message.slot(),
                        parent: message.parent().unwrap_or_default(),
                        root: slot_finalized,
                    },
                );
                return Some((subscription.id, method, false, json));
            }
        }
        (SubscribeMethod::SlotsUpdates, ParsedMessage::Slot(message)) => {
            let json = RpcNotification::serialize(
                "slotsUpdatesNotification",
                subscription.id,
                match message.status() {
                    SlotStatus::SlotFirstShredReceived => SlotUpdate::FirstShredReceived {
                        slot: message.slot(),
                        timestamp: message.created_at().as_millis(),
                    },
                    SlotStatus::SlotCompleted => SlotUpdate::Completed {
                        slot: message.slot(),
                        timestamp: message.created_at().as_millis(),
                    },
                    SlotStatus::SlotCreatedBank => SlotUpdate::CreatedBank {
                        slot: message.slot(),
                        parent: message.parent().unwrap_or_default(),
                        timestamp: message.created_at().as_millis(),
                    },
                    SlotStatus::SlotProcessed => SlotUpdate::Frozen {
                        slot: message.slot(),
                        timestamp: message.created_at().as_millis(),
                        stats: extra_info?,
                    },
                    SlotStatus::SlotDead => SlotUpdate::Dead {
                        slot: message.slot(),
                        timestamp: message.created_at().as_millis(),
                        err: message
                            .dead_error()
                            .map(|de| de.to_owned())
                            .unwrap_or_default(),
                    },
                    SlotStatus::SlotConfirmed => SlotUpdate::OptimisticConfirmation {
                        slot: message.slot(),
                        timestamp: message.created_at().as_millis(),
                    },
                    SlotStatus::SlotFinalized => SlotUpdate::Root {
                        slot: message.slot(),
                        timestamp: message.created_at().as_millis(),
                    },
                },
            );
            return Some((subscription.id, method, false, json));
        }
        (SubscribeMethod::Block, ParsedMessage::Block(message)) => {
            if let Some((encoding, options)) = subscription.config.filter_block(message) {
                let json = RpcNotification::serialize_with_context(
                    "blockNotification",
                    subscription.id,
                    message.slot(),
                    RpcBlockUpdate::new(message, encoding, options),
                );
                return Some((subscription.id, method, false, json));
            }
        }
        (SubscribeMethod::Root, ParsedMessage::Slot(message)) => {
            if message.status() == SlotStatus::SlotFinalized {
                let json =
                    RpcNotification::serialize("rootNotification", subscription.id, message.slot());
                return Some((subscription.id, method, false, json));
            }
        }
        (SubscribeMethod::Transaction, ParsedMessage::Transaction(message)) => {
            if let Some((
                encoding,
                transaction_details,
                show_rewards,
                max_supported_transaction_version,
            )) = subscription.config.filter_transaction(message)
            {
                let json = RpcNotification::serialize_with_context(
                    "transactionNotification",
                    subscription.id,
                    message.slot(),
                    RpcTransactionUpdate::new(
                        message,
                        encoding,
                        transaction_details,
                        show_rewards,
                        max_supported_transaction_version,
                    ),
                );
                return Some((subscription.id, method, false, json));
            }
        }
        _ => {}
    };
    None
}

#[derive(Debug)]
struct CachedSignature {
    slot: Slot,
    err: Option<TransactionError>,
}

#[derive(Debug)]
struct CachedSignatures {
    signatures: HashMap<Signature, CachedSignature, RandomState>,
    signatures_max: usize,
    slots: BTreeMap<Slot, Vec<Signature>>,
    slots_max: usize,
    slot_confirmed: Slot,
    slot_finalized: Slot,
    gauge_total: Gauge,
}

impl CachedSignatures {
    fn new(signatures_max: usize, slots_max: usize) -> Self {
        Self {
            signatures: HashMap::with_capacity_and_hasher(signatures_max, RandomState::default()),
            signatures_max,
            slots: BTreeMap::new(),
            slots_max,
            slot_confirmed: 0,
            slot_finalized: 0,
            gauge_total: gauge!(metrics::PUBSUB_CACHED_SIGNATURES_TOTAL),
        }
    }

    fn get(
        &self,
        signature: &Signature,
        commitment: CommitmentLevel,
    ) -> Option<(Slot, Option<TransactionError>)> {
        if let Some(cached) = self.signatures.get(signature) {
            let valid_cache = match commitment {
                CommitmentLevel::Processed => true,
                CommitmentLevel::Confirmed if self.slot_confirmed >= cached.slot => true,
                CommitmentLevel::Finalized if self.slot_finalized >= cached.slot => true,
                _ => false,
            };
            if valid_cache {
                return Some((cached.slot, cached.err.clone()));
            }
        }
        None
    }

    fn add_signature(&mut self, message: &MessageTransaction) {
        if let Ok(err) = convert_from::create_tx_error(message.error().as_ref()) {
            while self.signatures.len() >= self.signatures_max || self.slots.len() >= self.slots_max
            {
                let Some((_slot, signatures)) = self.slots.pop_first() else {
                    break;
                };
                for signature in signatures {
                    self.signatures.remove(&signature);
                }
            }

            self.signatures.insert(
                message.signature(),
                CachedSignature {
                    slot: message.slot(),
                    err,
                },
            );
            self.slots
                .entry(message.slot())
                .or_default()
                .push(message.signature());

            self.gauge_total.set(self.signatures.len() as f64);
        }
    }

    fn dead_slot(&mut self, slot: Slot) {
        for signature in self.slots.remove(&slot).unwrap_or_default() {
            self.signatures.remove(&signature);
        }
        self.gauge_total.set(self.signatures.len() as f64);
    }

    const fn set_confirmed(&mut self, slot: Slot) {
        self.slot_confirmed = slot;
    }

    const fn set_finalized(&mut self, slot: Slot) {
        self.slot_finalized = slot;
    }
}

type SlotTransactionStatsItemResult = Option<(ParsedMessage, SlotTransactionStats)>;

#[derive(Debug, Default)]
struct SlotTransactionStatsItem {
    slot: Slot,
    created_at: Timestamp,

    meta: bool,
    num_transactions: u64,
    num_entries: u64,

    num_tx_success: u64,
    num_tx_failed: u64,

    num_entries_received: u64,
    max_tx_per_entry: u64,
}

impl SlotTransactionStatsItem {
    fn add_created_at(
        &mut self,
        slot: Slot,
        created_at: Timestamp,
    ) -> SlotTransactionStatsItemResult {
        self.slot = slot;
        self.created_at = created_at;
        self.try_create()
    }

    fn add_tx(&mut self, failed: bool) -> SlotTransactionStatsItemResult {
        if failed {
            self.num_tx_failed += 1;
        } else {
            self.num_tx_success += 1;
        }
        self.try_create()
    }

    fn add_entry(&mut self, executed_transaction_count: u64) -> SlotTransactionStatsItemResult {
        self.num_entries_received += 1;
        self.max_tx_per_entry = self.max_tx_per_entry.max(executed_transaction_count);
        self.try_create()
    }

    fn add_meta(
        &mut self,
        num_transactions: u64,
        num_entries: u64,
    ) -> SlotTransactionStatsItemResult {
        self.meta = true;
        self.num_transactions = num_transactions;
        self.num_entries = num_entries;
        self.try_create()
    }

    fn try_create(&self) -> SlotTransactionStatsItemResult {
        if self.slot != 0
            && self.meta
            && self.num_transactions == self.num_tx_success + self.num_tx_failed
            && self.num_entries == self.num_entries_received
        {
            let message = MessageSlot::Prost {
                slot: self.slot,
                parent: None,
                status: SlotStatus::SlotProcessed,
                dead_error: None,
                bank_id: None,
                created_at: self.created_at,
                size: 0,
            };
            let stats = SlotTransactionStats {
                num_transaction_entries: self.num_entries,
                num_successful_transactions: self.num_tx_success,
                num_failed_transactions: self.num_tx_failed,
                max_transactions_per_entry: self.max_tx_per_entry,
            };
            return Some((ParsedMessage::Slot(Arc::new(message)), stats));
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::pubsub::notification::ClientNotification,
        richat_filter::message::{MessageAccount, MessageTransaction},
        richat_proto::{
            geyser::{SubscribeUpdateAccountInfo, SubscribeUpdateTransactionInfo},
            solana::storage::confirmed_block::TransactionStatusMeta,
        },
        serde_json::{Value, json},
        solana_account_decoder::UiAccountEncoding,
        solana_commitment_config::CommitmentConfig,
        solana_rpc_client_api::filter::RpcFilterType,
        tokio::sync::mpsc,
    };

    const PROCESSED: CommitmentLevel = CommitmentLevel::Processed;
    const CONFIRMED: CommitmentLevel = CommitmentLevel::Confirmed;
    const FINALIZED: CommitmentLevel = CommitmentLevel::Finalized;

    const fn pubkey(n: u8) -> Pubkey {
        Pubkey::new_from_array([n; 32])
    }

    fn signature(n: u8) -> Signature {
        Signature::from([n; 64])
    }

    const fn commitment(commitment: CommitmentLevel) -> CommitmentConfig {
        CommitmentConfig { commitment }
    }

    fn account_config(n: u8, level: CommitmentLevel) -> SubscribeConfig {
        SubscribeConfig::Account {
            pubkey: pubkey(n),
            encoding: UiAccountEncoding::Base64,
            data_slice: None,
            commitment: commitment(level),
        }
    }

    fn program_config(n: u8, data_size: Option<u64>, level: CommitmentLevel) -> SubscribeConfig {
        SubscribeConfig::Program {
            pubkey: pubkey(n),
            filters: data_size.map(RpcFilterType::DataSize).into_iter().collect(),
            encoding: UiAccountEncoding::Base64,
            data_slice: None,
            commitment: commitment(level),
        }
    }

    fn logs_config(kind: LogsSubscriptionKind, level: CommitmentLevel) -> SubscribeConfig {
        SubscribeConfig::Logs {
            kind,
            commitment: commitment(level),
        }
    }

    fn signature_config(n: u8, level: CommitmentLevel) -> SubscribeConfig {
        SubscribeConfig::Signature {
            signature: signature(n),
            commitment: commitment(level),
        }
    }

    fn account_message(key: u8, owner: u8, data: Vec<u8>, slot: Slot) -> ParsedMessage {
        ParsedMessage::Account(Arc::new(MessageAccount::Prost {
            pubkey: pubkey(key),
            owner: pubkey(owner),
            account: SubscribeUpdateAccountInfo {
                pubkey: pubkey(key).to_bytes().to_vec(),
                lamports: 42,
                owner: pubkey(owner).to_bytes().to_vec(),
                data,
                ..Default::default()
            },
            slot,
            is_startup: false,
            bank_id: None,
            created_at: Timestamp::default(),
            size: 0,
        }))
    }

    fn transaction_message(sig: u8, keys: &[u8], is_vote: bool, slot: Slot) -> ParsedMessage {
        ParsedMessage::Transaction(Arc::new(MessageTransaction::Prost {
            error: None,
            account_keys: keys.iter().map(|n| pubkey(*n)).collect(),
            transaction: SubscribeUpdateTransactionInfo {
                signature: signature(sig).as_ref().to_vec(),
                is_vote,
                meta: Some(TransactionStatusMeta {
                    log_messages: vec![format!("log {sig}")],
                    ..Default::default()
                }),
                ..Default::default()
            },
            slot,
            bank_id: 0,
            created_at: Timestamp::default(),
            size: 0,
        }))
    }

    fn slot_message(slot: Slot, status: SlotStatus) -> ParsedMessage {
        ParsedMessage::Slot(Arc::new(MessageSlot::Prost {
            slot,
            parent: Some(slot - 1),
            status,
            dead_error: None,
            bank_id: None,
            created_at: Timestamp::default(),
            size: 0,
        }))
    }

    #[derive(Debug, PartialEq)]
    struct Received {
        subscription_id: SubscriptionId,
        is_final: bool,
        method: String,
        slot: Value,
    }

    struct Harness {
        subscriptions: Subscriptions,
        signatures: CachedSignatures,
        notifications: RpcNotifications,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                subscriptions: Subscriptions::default(),
                signatures: CachedSignatures::new(100, 100),
                notifications: RpcNotifications::new(1024, 1024 * 1024),
            }
        }

        fn register(&mut self, client_id: ClientId) -> mpsc::Receiver<ClientNotification> {
            self.register_with_capacity(client_id, 1024)
        }

        fn register_with_capacity(
            &mut self,
            client_id: ClientId,
            capacity: usize,
        ) -> mpsc::Receiver<ClientNotification> {
            let (sender, rx) = ClientSender::channel(capacity);
            self.update(ClientRequest::Register { client_id, sender });
            rx
        }

        fn update(&mut self, request: ClientRequest) {
            self.subscriptions
                .update(request, &mut self.signatures, &mut self.notifications);
        }

        fn subscribe(&mut self, client_id: ClientId, config: SubscribeConfig) -> SubscriptionId {
            let (tx, mut rx) = oneshot::channel();
            self.update(ClientRequest::Subscribe {
                client_id,
                config,
                tx,
            });
            rx.try_recv().unwrap().0
        }

        fn unsubscribe(&mut self, client_id: ClientId, subscription_id: SubscriptionId) -> bool {
            let (tx, mut rx) = oneshot::channel();
            self.update(ClientRequest::Unsubscribe {
                client_id,
                subscription_id,
                tx,
            });
            rx.try_recv().unwrap()
        }

        /// Same steps as `subscriptions_worker`, without the thread pool.
        fn process(&mut self, commitment: CommitmentLevel, message: &ParsedMessage) {
            let mut jobs = vec![];
            self.subscriptions
                .collect_jobs(commitment, message, &mut jobs);
            let new_notifications = jobs
                .into_iter()
                .filter_map(|job| filter_job(job, 0))
                .collect::<Vec<_>>();
            for (subscription_id, method, is_final, json) in new_notifications {
                self.subscriptions.deliver(
                    &mut self.notifications,
                    subscription_id,
                    method,
                    is_final,
                    json,
                );
            }
        }

        /// Notifications produced by the old algorithm: every subscription of the commitment
        /// and of a method the message can match.
        fn scan_all(&self, commitment: CommitmentLevel, message: &ParsedMessage) -> Vec<Value> {
            let methods: &[SubscribeMethod] = match message {
                ParsedMessage::Slot(_) => &[
                    SubscribeMethod::Slot,
                    SubscribeMethod::SlotsUpdates,
                    SubscribeMethod::Root,
                ],
                ParsedMessage::Account(_) => &[SubscribeMethod::Account, SubscribeMethod::Program],
                ParsedMessage::Transaction(_) => &[
                    SubscribeMethod::Logs,
                    SubscribeMethod::Signature,
                    SubscribeMethod::Transaction,
                ],
                ParsedMessage::Block(_) => &[SubscribeMethod::Block],
                _ => &[],
            };
            let mut values = self
                .subscriptions
                .subscriptions
                .values()
                .filter(|subscription| subscription.config.commitment() == commitment)
                .filter(|subscription| methods.contains(&subscription.config.method()))
                .filter_map(|subscription| {
                    filter_job(
                        (subscription.config.method(), message, subscription, None),
                        0,
                    )
                })
                .map(|(_, _, _, json)| serde_json::from_str(&json).unwrap())
                .collect::<Vec<_>>();
            values.sort_by_key(|value: &Value| value.to_string());
            values
        }

        fn indexed(&self, commitment: CommitmentLevel, message: &ParsedMessage) -> Vec<Value> {
            let mut jobs = vec![];
            self.subscriptions
                .collect_jobs(commitment, message, &mut jobs);
            let mut values = jobs
                .into_iter()
                .filter_map(|job| filter_job(job, 0))
                .map(|(_, _, _, json)| serde_json::from_str(&json).unwrap())
                .collect::<Vec<_>>();
            values.sort_by_key(|value: &Value| value.to_string());
            values
        }

        fn assert_empty(&self) {
            assert!(self.subscriptions.subscriptions.is_empty());
            assert!(self.subscriptions.subscriptions_by_config.is_empty());
            for index in &self.subscriptions.index {
                assert!(index.account.is_empty());
                assert!(index.program.is_empty());
                assert!(index.signature.is_empty());
                assert!(index.logs_mentions.is_empty());
                assert!(index.logs_all.is_empty());
                assert!(index.unkeyed.iter().all(|ids| ids.is_empty()));
            }
        }
    }

    fn drain(rx: &mut mpsc::Receiver<ClientNotification>) -> Vec<Received> {
        let mut received = vec![];
        while let Ok(notification) = rx.try_recv() {
            let ClientNotification::Notification(notification) = notification else {
                panic!("unexpected lagged");
            };
            let json = notification.json.upgrade().expect("notification is alive");
            let value: Value = serde_json::from_str(&json).unwrap();
            assert_eq!(
                value["params"]["subscription"],
                json!(notification.subscription_id)
            );
            received.push(Received {
                subscription_id: notification.subscription_id,
                is_final: notification.is_final,
                method: value["method"].as_str().unwrap().to_owned(),
                slot: value["params"]["result"]["context"]["slot"].clone(),
            });
        }
        received
    }

    fn received(subscription_id: SubscriptionId, method: &str, slot: Slot) -> Received {
        Received {
            subscription_id,
            is_final: false,
            method: method.to_owned(),
            slot: json!(slot),
        }
    }

    #[test]
    fn test_subscribe_new_config_inserts_client_id() {
        let mut h = Harness::new();
        let _rx = h.register(1);
        let subscription_id = h.subscribe(1, SubscribeConfig::Slot);
        let info = h.subscriptions.subscriptions.get(&subscription_id).unwrap();
        assert!(info.clients.contains(&1));
    }

    #[test]
    fn test_remove_client_after_new_subscription_does_not_panic() {
        let mut h = Harness::new();
        let _rx = h.register(1);
        h.subscribe(1, SubscribeConfig::Slot);
        h.update(ClientRequest::Remove { client_id: 1 });
        assert!(h.subscriptions.clients.is_empty());
        h.assert_empty();
    }

    #[test]
    fn test_account_subscribe_by_pubkey_shared_encoding() {
        let mut h = Harness::new();
        let mut rx1 = h.register(1);
        let mut rx2 = h.register(2);
        let mut rx3 = h.register(3);

        let id = h.subscribe(1, account_config(1, PROCESSED));
        assert_eq!(h.subscribe(2, account_config(1, PROCESSED)), id);
        let id_other = h.subscribe(3, account_config(2, PROCESSED));
        assert_ne!(id, id_other);

        h.process(PROCESSED, &account_message(1, 9, vec![1, 2, 3], 10));

        // one payload, shared by both clients of the subscription
        let (Ok(ClientNotification::Notification(n1)), Ok(ClientNotification::Notification(n2))) =
            (rx1.try_recv(), rx2.try_recv())
        else {
            panic!("expected notifications");
        };
        assert!(Weak::ptr_eq(&n1.json, &n2.json));
        assert_eq!(n1.subscription_id, id);
        assert!(drain(&mut rx3).is_empty());

        h.process(PROCESSED, &account_message(2, 9, vec![], 11));
        assert!(drain(&mut rx1).is_empty());
        assert_eq!(
            drain(&mut rx3),
            vec![received(id_other, "accountNotification", 11)]
        );

        // unsubscribe one client, the other keeps the subscription
        assert!(h.unsubscribe(1, id));
        assert!(!h.unsubscribe(1, id));
        h.process(PROCESSED, &account_message(1, 9, vec![], 12));
        assert!(drain(&mut rx1).is_empty());
        assert_eq!(
            drain(&mut rx2),
            vec![received(id, "accountNotification", 12)]
        );

        assert!(h.unsubscribe(2, id));
        assert!(h.unsubscribe(3, id_other));
        h.assert_empty();
    }

    #[test]
    fn test_commitment_levels() {
        let mut h = Harness::new();
        let mut rx = h.register(1);
        let processed = h.subscribe(1, account_config(1, PROCESSED));
        let confirmed = h.subscribe(1, account_config(1, CONFIRMED));
        let finalized = h.subscribe(1, account_config(1, FINALIZED));
        let logs_confirmed = h.subscribe(1, logs_config(LogsSubscriptionKind::All, CONFIRMED));

        let message = account_message(1, 9, vec![], 10);
        for (level, id) in [
            (PROCESSED, processed),
            (CONFIRMED, confirmed),
            (FINALIZED, finalized),
        ] {
            h.process(level, &message);
            assert_eq!(
                drain(&mut rx),
                vec![received(id, "accountNotification", 10)]
            );
        }

        let message = transaction_message(1, &[1], false, 10);
        h.process(PROCESSED, &message);
        h.process(FINALIZED, &message);
        assert!(drain(&mut rx).is_empty());
        h.process(CONFIRMED, &message);
        assert_eq!(
            drain(&mut rx),
            vec![received(logs_confirmed, "logsNotification", 10)]
        );

        h.update(ClientRequest::Remove { client_id: 1 });
        h.assert_empty();
    }

    #[test]
    fn test_program_subscribe_by_owner() {
        let mut h = Harness::new();
        let mut rx = h.register(1);
        let any = h.subscribe(1, program_config(5, None, PROCESSED));
        let size3 = h.subscribe(1, program_config(5, Some(3), PROCESSED));
        let other_owner = h.subscribe(1, program_config(6, None, PROCESSED));

        h.process(PROCESSED, &account_message(1, 5, vec![1, 2, 3], 10));
        let mut got = drain(&mut rx);
        got.sort_by_key(|r| r.subscription_id);
        assert_eq!(
            got,
            vec![
                received(any, "programNotification", 10),
                received(size3, "programNotification", 10),
            ]
        );

        // filters still apply among subscriptions of the owner
        h.process(PROCESSED, &account_message(1, 5, vec![1], 11));
        assert_eq!(
            drain(&mut rx),
            vec![received(any, "programNotification", 11)]
        );

        // account pubkey equal to the owner of another subscription doesn't match
        h.process(PROCESSED, &account_message(6, 7, vec![], 12));
        assert!(drain(&mut rx).is_empty());
        h.process(PROCESSED, &account_message(7, 6, vec![], 13));
        assert_eq!(
            drain(&mut rx),
            vec![received(other_owner, "programNotification", 13)]
        );

        assert!(h.unsubscribe(1, any));
        h.process(PROCESSED, &account_message(1, 5, vec![1], 14));
        assert!(drain(&mut rx).is_empty());

        assert!(h.unsubscribe(1, size3));
        assert!(h.unsubscribe(1, other_owner));
        h.assert_empty();
    }

    #[test]
    fn test_signature_subscribe() {
        let mut h = Harness::new();
        let mut rx1 = h.register(1);
        let mut rx2 = h.register(2);
        let id = h.subscribe(1, signature_config(1, CONFIRMED));
        assert_eq!(h.subscribe(2, signature_config(1, CONFIRMED)), id);
        let unsubscribed = h.subscribe(1, signature_config(2, CONFIRMED));
        assert!(h.unsubscribe(1, unsubscribed));

        h.process(CONFIRMED, &transaction_message(3, &[], false, 10));
        h.process(PROCESSED, &transaction_message(1, &[], false, 10));
        h.process(CONFIRMED, &transaction_message(2, &[], false, 10));
        assert!(drain(&mut rx1).is_empty());

        h.process(CONFIRMED, &transaction_message(1, &[], false, 11));
        let expected = Received {
            subscription_id: id,
            is_final: true,
            method: "signatureNotification".to_owned(),
            slot: json!(11),
        };
        assert_eq!(drain(&mut rx1), vec![expected]);
        assert_eq!(drain(&mut rx2).len(), 1);

        // final: the subscription is gone everywhere
        h.assert_empty();
        assert!(!h.unsubscribe(1, id));
        h.process(CONFIRMED, &transaction_message(1, &[], false, 12));
        assert!(drain(&mut rx1).is_empty());
    }

    #[test]
    fn test_signature_subscribe_cached() {
        let mut h = Harness::new();
        let mut rx = h.register(1);
        let ParsedMessage::Transaction(tx) = transaction_message(1, &[], false, 10) else {
            unreachable!()
        };
        h.signatures.add_signature(&tx);

        // not confirmed yet: regular subscription
        let confirmed = h.subscribe(1, signature_config(1, CONFIRMED));
        assert!(drain(&mut rx).is_empty());
        assert!(h.subscriptions.subscriptions.contains_key(&confirmed));

        // processed: notified on subscribe, nothing stored
        let processed = h.subscribe(1, signature_config(1, PROCESSED));
        let got = drain(&mut rx);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].subscription_id, processed);
        assert!(got[0].is_final);
        assert!(!h.subscriptions.subscriptions.contains_key(&processed));

        // an existing subscription is joined as is
        h.signatures.set_confirmed(10);
        let mut rx2 = h.register(2);
        assert_eq!(h.subscribe(2, signature_config(1, CONFIRMED)), confirmed);
        assert!(drain(&mut rx2).is_empty());
        assert!(h.unsubscribe(1, confirmed));
        assert!(h.unsubscribe(2, confirmed));
        h.assert_empty();

        // without one, the now confirmed signature is notified on subscribe
        let id = h.subscribe(2, signature_config(1, CONFIRMED));
        let got = drain(&mut rx2);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].subscription_id, id);
        assert!(got[0].is_final);
        h.assert_empty();

        // a client that isn't registered (dropped as lagged) gets an id, nothing is stored
        h.subscribe(3, signature_config(5, CONFIRMED));
        h.assert_empty();
    }

    #[test]
    fn test_logs_subscribe() {
        let mut h = Harness::new();
        let mut rx = h.register(1);
        let all = h.subscribe(1, logs_config(LogsSubscriptionKind::All, PROCESSED));
        let votes = h.subscribe(
            1,
            logs_config(LogsSubscriptionKind::AllWithVotes, PROCESSED),
        );
        let mention = h.subscribe(
            1,
            logs_config(LogsSubscriptionKind::Single(pubkey(7)), PROCESSED),
        );

        let ids = |rx: &mut mpsc::Receiver<ClientNotification>| {
            let mut ids = drain(rx)
                .into_iter()
                .map(|r| r.subscription_id)
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        };

        h.process(PROCESSED, &transaction_message(1, &[1, 2], false, 10));
        assert_eq!(ids(&mut rx), vec![all, votes]);
        h.process(PROCESSED, &transaction_message(1, &[1, 7], false, 10));
        assert_eq!(ids(&mut rx), vec![all, votes, mention]);
        h.process(PROCESSED, &transaction_message(1, &[7], true, 10));
        assert_eq!(ids(&mut rx), vec![votes, mention]);

        // more mention subscriptions than keys in the transaction, the other lookup side
        let mut mentions = vec![mention];
        for n in 10..20 {
            mentions.push(h.subscribe(
                1,
                logs_config(LogsSubscriptionKind::Single(pubkey(n)), PROCESSED),
            ));
        }
        h.process(PROCESSED, &transaction_message(1, &[7, 12], true, 10));
        assert_eq!(ids(&mut rx), vec![votes, mention, mentions[3]]);
        let keys = (0..40).collect::<Vec<u8>>();
        h.process(PROCESSED, &transaction_message(1, &keys, true, 10));
        let mut expected = vec![votes];
        expected.extend(&mentions);
        assert_eq!(ids(&mut rx), expected);

        assert!(h.unsubscribe(1, mention));
        h.process(PROCESSED, &transaction_message(1, &[7], false, 10));
        assert_eq!(ids(&mut rx), vec![all, votes]);

        h.update(ClientRequest::Remove { client_id: 1 });
        h.assert_empty();
    }

    #[test]
    fn test_slot_root_slots_updates() {
        let mut h = Harness::new();
        let mut rx = h.register(1);
        let slot = h.subscribe(1, SubscribeConfig::Slot);
        let root = h.subscribe(1, SubscribeConfig::Root);
        let updates = h.subscribe(1, SubscribeConfig::SlotsUpdates);

        let methods = |rx: &mut mpsc::Receiver<ClientNotification>| {
            let mut got = drain(rx)
                .into_iter()
                .map(|r| (r.subscription_id, r.method))
                .collect::<Vec<_>>();
            got.sort_unstable();
            got
        };

        h.process(PROCESSED, &slot_message(10, SlotStatus::SlotCreatedBank));
        assert_eq!(
            methods(&mut rx),
            vec![
                (slot, "slotNotification".to_owned()),
                (updates, "slotsUpdatesNotification".to_owned()),
            ]
        );
        h.process(PROCESSED, &slot_message(10, SlotStatus::SlotFinalized));
        assert_eq!(
            methods(&mut rx),
            vec![
                (root, "rootNotification".to_owned()),
                (updates, "slotsUpdatesNotification".to_owned()),
            ]
        );

        assert!(h.unsubscribe(1, slot));
        assert!(h.unsubscribe(1, root));
        assert!(h.unsubscribe(1, updates));
        h.assert_empty();
    }

    #[test]
    fn test_lagged_client_is_dropped_without_blocking() {
        let mut h = Harness::new();
        let mut slow = h.register_with_capacity(1, 2);
        let mut fast = h.register(2);
        let id = h.subscribe(1, account_config(1, PROCESSED));
        h.subscribe(2, account_config(1, PROCESSED));
        h.subscribe(1, account_config(2, PROCESSED));

        for slot in 0..5 {
            h.process(PROCESSED, &account_message(1, 9, vec![], slot));
        }

        assert!(matches!(
            slow.try_recv(),
            Ok(ClientNotification::Notification(_))
        ));
        assert!(matches!(
            slow.try_recv(),
            Ok(ClientNotification::Notification(_))
        ));
        assert!(matches!(slow.try_recv(), Ok(ClientNotification::Lagged)));
        // the tracker dropped its sender
        assert!(matches!(
            slow.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        assert_eq!(drain(&mut fast).len(), 5);

        // lagged client and its own subscriptions are removed, shared ones stay
        assert!(!h.subscriptions.clients.contains_key(&1));
        assert_eq!(h.subscriptions.subscriptions.len(), 1);
        assert!(h.subscriptions.subscriptions[&id].clients.contains(&2));

        // later requests of the closing client are harmless
        h.subscribe(1, account_config(3, PROCESSED));
        h.update(ClientRequest::Remove { client_id: 1 });
        h.update(ClientRequest::Remove { client_id: 2 });
        h.assert_empty();
    }

    #[test]
    fn test_index_matches_full_scan() {
        let mut h = Harness::new();
        let _rx = h.register(1);
        let levels = [PROCESSED, CONFIRMED, FINALIZED];
        for (i, level) in levels.into_iter().enumerate() {
            let n = i as u8;
            for key in 0..4 {
                h.subscribe(1, account_config(key + n, level));
                h.subscribe(1, program_config(key, Some(u64::from(n)), level));
                h.subscribe(1, program_config(key + n, None, level));
                h.subscribe(1, signature_config(key + n, level));
                h.subscribe(
                    1,
                    logs_config(LogsSubscriptionKind::Single(pubkey(key + n)), level),
                );
            }
            h.subscribe(1, logs_config(LogsSubscriptionKind::All, level));
        }
        h.subscribe(1, SubscribeConfig::Slot);
        h.subscribe(1, SubscribeConfig::Root);
        h.subscribe(1, SubscribeConfig::SlotsUpdates);

        let mut messages = vec![];
        for key in 0..6 {
            for owner in 0..6 {
                messages.push(account_message(
                    key,
                    owner,
                    vec![0; usize::from(owner % 3)],
                    1,
                ));
            }
            messages.push(transaction_message(key, &[key, key + 1], key % 2 == 0, 1));
            messages.push(transaction_message(key, &[], false, 1));
        }
        messages.push(slot_message(5, SlotStatus::SlotCreatedBank));
        messages.push(slot_message(5, SlotStatus::SlotFinalized));

        let mut total = 0;
        for level in levels {
            for message in &messages {
                let indexed = h.indexed(level, message);
                assert_eq!(indexed, h.scan_all(level, message), "{message:?}");
                total += indexed.len();
            }
        }
        assert!(total > 100, "{total}");
    }
}
