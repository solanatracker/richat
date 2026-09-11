use {
    crate::{
        config::ConfigChannelInner, grpc::server::SubscribeClient, metrics, storage::Storage,
        util::SpawnedThreads,
    },
    ::metrics::{Gauge, counter, gauge},
    foldhash::quality::RandomState,
    futures::stream::{Stream, StreamExt},
    richat_filter::{
        filter::FilteredUpdate,
        message::{
            Message, MessageAccount, MessageBlock, MessageBlockMeta, MessageEntry,
            MessageParserEncoding, MessageRef, MessageSlot, MessageTransaction,
        },
    },
    richat_proto::{geyser::SlotStatus, richat::RichatFilter},
    richat_shared::{
        mutex_lock,
        transports::{RecvError, RecvItem, RecvStream, Subscribe, SubscribeError},
    },
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    solana_nohash_hasher::IntMap,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::{
            BTreeMap, HashMap, HashSet, btree_map::Entry as BTreeMapEntry,
            hash_map::Entry as HashMapEntry,
        },
        fmt,
        hash::{BuildHasher, Hash, Hasher},
        path::PathBuf,
        pin::Pin,
        sync::{
            Arc, Mutex, MutexGuard,
            atomic::{AtomicU64, Ordering},
        },
        task::{Context, Poll, Waker},
        time::Duration,
    },
    tokio_util::sync::CancellationToken,
    tracing::debug,
};

#[derive(Debug, Clone)]
pub struct GlobalReplayFromSlot {
    inner: Arc<Mutex<GlobalReplayFromSlotInner>>,
    epoch: Arc<AtomicU64>,
}

#[derive(Debug)]
struct GlobalReplayFromSlotInner {
    value: Option<Slot>,
    sources_replay_failed: HashSet<&'static str>,
    sources_total: usize,
    epoch: u64,
}

impl GlobalReplayFromSlot {
    pub(crate) fn new(value: Option<Slot>, sources_total: usize) -> Self {
        Self {
            epoch: Arc::new(AtomicU64::new(0)),
            inner: Arc::new(Mutex::new(GlobalReplayFromSlotInner {
                value,
                sources_replay_failed: HashSet::new(),
                sources_total,
                epoch: 0,
            })),
        }
    }

    pub fn load(&self) -> Option<Slot> {
        mutex_lock(&self.inner).value
    }

    pub fn store(&self, slot: Slot) {
        let mut locked = mutex_lock(&self.inner);
        if locked.value.is_none_or(|value| slot > value) {
            locked.sources_replay_failed.clear();
        }
        locked.value = Some(locked.value.unwrap_or(slot).max(slot));
    }

    pub fn snapshot(&self) -> (Option<Slot>, u64) {
        let locked = mutex_lock(&self.inner);
        (locked.value, locked.epoch)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    pub fn fallback_to_live(
        &self,
        source_name: &'static str,
        requested: Option<Slot>,
        epoch: u64,
    ) -> bool {
        let mut locked = mutex_lock(&self.inner);
        if requested.is_none() || locked.value != requested || locked.epoch != epoch {
            return false;
        }
        locked.sources_replay_failed.insert(source_name);
        if locked.sources_replay_failed.len() < locked.sources_total {
            return false;
        }
        locked.value = None;
        locked.epoch += 1;
        self.epoch.store(locked.epoch, Ordering::Release);
        locked.sources_replay_failed.clear();
        true
    }

    /// Update sources_total and clear failed sources set.
    pub fn update_sources(&self, new_total: usize) {
        let mut locked = mutex_lock(&self.inner);
        locked.sources_total = new_total;
        locked.sources_replay_failed.clear();
    }
}

#[derive(Debug, Clone)]
pub enum ParsedMessage {
    Slot(Arc<MessageSlot>),
    Account(Arc<MessageAccount>),
    Transaction(Arc<MessageTransaction>),
    Entry(Arc<MessageEntry>),
    BlockMeta(Arc<MessageBlockMeta>),
    Block(Arc<MessageBlock>),
}

impl From<Message> for ParsedMessage {
    fn from(message: Message) -> Self {
        match message {
            Message::Slot(msg) => Self::Slot(Arc::new(msg)),
            Message::Account(msg) => Self::Account(Arc::new(msg)),
            Message::Transaction(msg) => Self::Transaction(Arc::new(msg)),
            Message::Entry(msg) => Self::Entry(Arc::new(msg)),
            Message::BlockMeta(msg) => Self::BlockMeta(Arc::new(msg)),
            Message::Block(msg) => Self::Block(Arc::new(msg)),
        }
    }
}

impl<'a> From<&'a ParsedMessage> for MessageRef<'a> {
    fn from(message: &'a ParsedMessage) -> Self {
        match message {
            ParsedMessage::Slot(msg) => Self::Slot(msg.as_ref()),
            ParsedMessage::Account(msg) => Self::Account(msg.as_ref()),
            ParsedMessage::Transaction(msg) => Self::Transaction(msg.as_ref()),
            ParsedMessage::Entry(msg) => Self::Entry(msg.as_ref()),
            ParsedMessage::BlockMeta(msg) => Self::BlockMeta(msg.as_ref()),
            ParsedMessage::Block(msg) => Self::Block(msg.as_ref()),
        }
    }
}

impl ParsedMessage {
    pub const fn as_str_type(&self) -> &'static str {
        match self {
            Self::Slot(_msg) => "slot",
            Self::Account(_msg) => "account",
            Self::Transaction(_msg) => "transaction",
            Self::Entry(_msg) => "entry",
            Self::BlockMeta(_msg) => "blockmeta",
            Self::Block(_msg) => "block",
        }
    }

    pub fn slot(&self) -> Slot {
        match self {
            Self::Slot(msg) => msg.slot(),
            Self::Account(msg) => msg.slot(),
            Self::Transaction(msg) => msg.slot(),
            Self::Entry(msg) => msg.slot(),
            Self::BlockMeta(msg) => msg.slot(),
            Self::Block(msg) => msg.slot(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Slot(msg) => msg.size(),
            Self::Account(msg) => msg.size(),
            Self::Transaction(msg) => msg.size(),
            Self::Entry(msg) => msg.size(),
            Self::BlockMeta(msg) => msg.size(),
            Self::Block(msg) => msg.size(),
        }
    }

    fn get_account(&self) -> Option<Arc<MessageAccount>> {
        if let Self::Account(msg) = self {
            Some(Arc::clone(msg))
        } else {
            None
        }
    }

    fn get_transaction(&self) -> Option<Arc<MessageTransaction>> {
        if let Self::Transaction(msg) = self {
            Some(Arc::clone(msg))
        } else {
            None
        }
    }

    fn get_entry(&self) -> Option<Arc<MessageEntry>> {
        if let Self::Entry(msg) = self {
            Some(Arc::clone(msg))
        } else {
            None
        }
    }

    fn get_id<H: Hasher>(&self, mut state: H) -> u64 {
        match self {
            ParsedMessage::Slot(msg) => {
                state.write_u8(0);
                state.write_u64(msg.slot());
                msg.status().hash(&mut state);
            }
            ParsedMessage::Account(msg) => {
                state.write_u8(1);
                state.write_u64(msg.slot());
                state.write(msg.pubkey().as_ref());
                state.write_u64(msg.write_version());
                // signature doesn't exist for block reward and system account updates
                if let Some(signature) = msg.txn_signature() {
                    state.write(signature);
                }
            }
            ParsedMessage::Transaction(msg) => {
                state.write_u8(2);
                state.write_u64(msg.slot());
                state.write(msg.signature_ref());
            }
            ParsedMessage::Entry(msg) => {
                state.write_u8(3);
                state.write_u64(msg.slot());
                state.write_u64(msg.index());
            }
            ParsedMessage::BlockMeta(msg) => {
                state.write_u8(4);
                state.write_u64(msg.slot());
            }
            ParsedMessage::Block(msg) => {
                state.write_u8(5);
                state.write_u64(msg.slot());
            }
        }
        state.finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum IndexLocation {
    Unknown,
    Storage(u64),
    Memory(u64),
}

#[derive(Debug, Clone)]
pub struct Messages {
    shared_processed: Arc<SharedChannel>,
    shared_confirmed: Option<Arc<SharedChannel>>,
    shared_finalized: Option<Arc<SharedChannel>>,
    max_messages: usize,
    max_bytes: usize,
    parser: MessageParserEncoding,
    storage: Option<Storage>,
    storage_max_slots: usize,
    replay_info: Option<Arc<Mutex<BTreeMap<Slot, ReplayInfo>>>>,
}

impl Messages {
    pub fn new(
        parser: MessageParserEncoding,
        config: ConfigChannelInner,
        richat: bool,
        grpc: bool,
        pubsub: bool,
        shutdown: CancellationToken,
    ) -> anyhow::Result<(Self, SpawnedThreads)> {
        let storage_max_slots = config
            .storage
            .as_ref()
            .map(|config| config.max_slots)
            .unwrap_or_default();
        let (storage, threads) = match config
            .storage
            .map(|config| Storage::open(config, parser, shutdown))
            .transpose()?
        {
            Some((storage, threads)) => (Some(storage), threads),
            None => (None, vec![]),
        };

        let max_messages = config.max_messages.next_power_of_two();
        set_optional_slot_gauge(metrics::CHANNEL_MEMORY_FIRST_SLOT, None);
        set_optional_slot_gauge(metrics::CHANNEL_MEMORY_LAST_SLOT, None);
        set_optional_slot_gauge(metrics::CHANNEL_STORAGE_FIRST_SLOT, None);
        set_optional_slot_gauge(metrics::CHANNEL_STORAGE_LAST_SLOT, None);
        let messages = Self {
            shared_processed: Arc::new(SharedChannel::new(max_messages, richat)),
            shared_confirmed: (grpc || pubsub)
                .then(|| Arc::new(SharedChannel::new(max_messages, richat))),
            shared_finalized: (grpc || pubsub)
                .then(|| Arc::new(SharedChannel::new(max_messages, richat))),
            max_messages,
            max_bytes: config.max_bytes,
            parser,
            storage,
            storage_max_slots,
            replay_info: None,
        };
        Ok((messages, threads))
    }

    pub fn to_sender(
        &mut self,
        sources_total: usize,
    ) -> anyhow::Result<(Sender, GlobalReplayFromSlot)> {
        let mut slot_finalized = 0;
        let hasher = RandomState::default();
        let mut replay_from_slot = None;
        let mut replay = BTreeMap::new();
        let mut index = 0;
        if let Some(storage) = &self.storage {
            index = storage.next_index();
            for shared in std::iter::once(&self.shared_processed)
                .chain(self.shared_confirmed.iter())
                .chain(self.shared_finalized.iter())
            {
                shared.replay_floor.store(index, Ordering::Relaxed);
            }
            let slots = storage.read_slots();

            for (slot, item) in slots.iter() {
                replay.insert(*slot, ReplayInfo::new(item.head));
            }
            gauge!(metrics::CHANNEL_STORAGE_SLOTS_TOTAL).set(replay.len() as f64);
            update_storage_slot_metrics(&replay);

            if let Some(finalized_slot) = slots
                .iter()
                .filter(|(_slot, value)| value.finalized)
                .map(|(slot, _value)| *slot)
                .max()
            {
                slot_finalized = finalized_slot;
                let replay_index = slots
                    .range((finalized_slot + 1)..)
                    .map(|(_, item)| item.head)
                    .min()
                    .unwrap_or(index);
                for chunk_result in storage.read_messages_from_index(replay_index, self.parser) {
                    let mut chunk = chunk_result?;
                    for result in &mut chunk {
                        let record = result?;
                        if record.commitment != CommitmentLevel::Processed {
                            continue;
                        }
                        let msg = record.message;
                        if msg.slot() <= finalized_slot {
                            continue;
                        }

                        let Some(replay) = replay.get_mut(&msg.slot()) else {
                            anyhow::bail!(
                                "failed to get replay info for existed message, slot#{}",
                                msg.slot()
                            );
                        };
                        let messages = replay.messages.get_or_insert_default();
                        messages.insert(msg.get_id(hasher.build_hasher()), record.payload_index);
                    }
                }
                replay_from_slot = Some(finalized_slot + 1);
            } else {
                // An initial capture can be durable before the first root arrives.
                replay_from_slot = slots.first_key_value().map(|(slot, _)| *slot);
                for chunk in storage.read_messages_from_index(
                    slots.values().map(|item| item.head).min().unwrap_or(index),
                    self.parser,
                ) {
                    for record in chunk? {
                        let record = record?;
                        if record.commitment == CommitmentLevel::Processed
                            && let Some(info) = replay.get_mut(&record.message.slot())
                        {
                            info.messages.get_or_insert_default().insert(
                                record.message.get_id(hasher.build_hasher()),
                                record.payload_index,
                            );
                        }
                    }
                }
            }
        }

        let replay = Arc::new(Mutex::new(replay));
        self.replay_info = Some(Arc::clone(&replay));

        let global_replay_from_slot = GlobalReplayFromSlot::new(replay_from_slot, sources_total);
        let sender = Sender {
            slots: BTreeMap::new(),
            dedup: BTreeMap::new(),
            processed: SenderShared::new(
                &self.shared_processed,
                self.max_messages,
                self.max_bytes,
                CommitmentLevel::Processed,
            ),
            confirmed: self.shared_confirmed.as_ref().map(|shared| {
                SenderShared::new(
                    shared,
                    self.max_messages,
                    self.max_bytes,
                    CommitmentLevel::Confirmed,
                )
            }),
            finalized: self.shared_finalized.as_ref().map(|shared| {
                SenderShared::new(
                    shared,
                    self.max_messages,
                    self.max_bytes,
                    CommitmentLevel::Finalized,
                )
            }),
            slot_finalized,
            global_replay_from_slot: global_replay_from_slot.clone(),
            storage: self.storage.clone(),
            storage_max_slots: self.storage_max_slots,
            hasher,
            replay,
            index,
        };
        Ok((sender, global_replay_from_slot))
    }

    pub fn to_receiver(&self) -> ReceiverSync {
        ReceiverSync {
            shared_processed: Arc::clone(&self.shared_processed),
            shared_confirmed: self.shared_confirmed.as_ref().map(Arc::clone),
            shared_finalized: self.shared_finalized.as_ref().map(Arc::clone),
        }
    }

    const fn get_shared(&self, commitment: CommitmentLevel) -> &Arc<SharedChannel> {
        match commitment {
            CommitmentLevel::Processed => &self.shared_processed,
            CommitmentLevel::Confirmed => {
                self.shared_confirmed.as_ref().expect("should be defined")
            }
            CommitmentLevel::Finalized => {
                self.shared_finalized.as_ref().expect("should be defined")
            }
        }
    }

    pub fn get_current_tail(&self, commitment: CommitmentLevel) -> u64 {
        self.get_shared(commitment).tail.load(Ordering::Relaxed)
    }

    pub fn recovery_epoch(&self, commitment: CommitmentLevel) -> u64 {
        self.get_shared(commitment).recovery_epoch()
    }

    pub fn get_current_tail_with_replay(
        &self,
        commitment: CommitmentLevel,
        replay_from_slot: Option<Slot>,
    ) -> Result<IndexLocation, String> {
        if let Some(replay_from_slot) = replay_from_slot {
            // Hold the replay map while choosing the cursor: sender publication and
            // retention use this same lock, so disk and memory describe one boundary.
            let replay = self.replay_info.as_deref().map(mutex_lock);
            {
                let slots = self.get_shared(commitment).slots_lock();
                if slots
                    .first_key_value()
                    .is_some_and(|(first, _)| replay_from_slot >= *first)
                    && let Some(index) = slots
                        .range(replay_from_slot..)
                        .map(|(_, obj)| obj.head)
                        .min()
                {
                    return Ok(IndexLocation::Memory(index));
                }
                let newest = replay
                    .as_ref()
                    .and_then(|replay| replay.last_key_value().map(|(slot, _)| *slot))
                    .or_else(|| slots.last_key_value().map(|(slot, _)| *slot));
                if newest.is_some_and(|newest| replay_from_slot > newest) {
                    return Ok(IndexLocation::Memory(self.get_current_tail(commitment) + 1));
                }
            }
            if let Some(replay) = replay.as_ref()
                && self.storage.is_some()
                && let Some(first) = replay.keys().next()
                && replay_from_slot < *first
            {
                return Err(format!(
                    "failed to get replay position for slot {replay_from_slot}; first available: {first}"
                ));
            }
            if let Some(replay) = replay.as_ref()
                && self.storage.is_some()
                && let Some(index) = replay
                    .range(replay_from_slot..)
                    .map(|(_, obj)| obj.head)
                    .min()
            {
                let storage = self.storage.as_ref().unwrap();
                if !storage.replay_enabled(commitment) {
                    return Err(format!(
                        "disk replay is not enabled for {commitment:?}; configure storage.commitments"
                    ));
                }
                if !storage.supports_commitment_replay(index, commitment) {
                    return Err(
                        "requested slot predates continuous disk history for this commitment"
                            .to_owned(),
                    );
                }
                return Ok(IndexLocation::Storage(index));
            }
            Err(format!(
                "failed to get replay position for slot {replay_from_slot}"
            ))
        } else {
            let index = self.get_shared(commitment).tail.load(Ordering::Relaxed) + 1;
            Ok(IndexLocation::Memory(index))
        }
    }

    pub fn get_first_available_slot(&self) -> Option<Slot> {
        let slot_replay = self
            .replay_info
            .as_deref()
            .map(mutex_lock)
            .and_then(|replay| replay.first_key_value().map(|(slot, _info)| *slot));

        let slot_processed = self
            .shared_processed
            .slots_lock()
            .first_key_value()
            .map(|(slot, _head)| *slot);

        match (slot_replay, slot_processed) {
            (Some(slot_replay), Some(slot_processed)) => Some(slot_replay.min(slot_processed)),
            _ => slot_replay.or(slot_processed),
        }
    }

    pub fn supports_block_replay(&self, head: IndexLocation) -> bool {
        match head {
            IndexLocation::Storage(index) => self
                .storage
                .as_ref()
                .is_some_and(|storage| storage.supports_block_replay(index)),
            _ => true,
        }
    }

    pub fn storage_disk_size_poll_config(&self) -> Option<(PathBuf, PathBuf, Duration)> {
        self.storage.as_ref().map(|s| s.disk_size_poll_config())
    }

    pub fn replay_from_storage(
        &self,
        client: SubscribeClient,
        metric_cpu_usage: Gauge,
        commitment: CommitmentLevel,
        generation: u64,
    ) -> Result<(), &'static str> {
        self.storage
            .as_ref()
            .ok_or("storage should exists to replay messages")?
            .replay(
                client,
                Arc::clone(self.get_shared(commitment)),
                metric_cpu_usage,
                generation,
            )
    }
}

impl Subscribe for Messages {
    fn subscribe(
        &self,
        replay_from_slot: Option<Slot>,
        filter: Option<RichatFilter>,
    ) -> Result<RecvStream, SubscribeError> {
        let head = if let Some(replay_from_slot) = replay_from_slot {
            let state = self.shared_processed.slots_lock();
            match state.get(&replay_from_slot) {
                Some(obj) => obj.head,
                None => {
                    return Err(match state.keys().min().copied() {
                        Some(first_available) => {
                            SubscribeError::SlotNotAvailable { first_available }
                        }
                        None => SubscribeError::NotInitialized,
                    });
                }
            }
        } else {
            self.shared_processed.tail.load(Ordering::Relaxed)
        };

        let filter = filter.unwrap_or_default();

        Ok(ReceiverAsync {
            shared: Arc::clone(&self.shared_processed),
            head,
            finished: false,
            enable_notifications_accounts: !filter.disable_accounts,
            enable_notifications_transactions: !filter.disable_transactions,
            enable_notifications_entries: !filter.disable_entries,
        }
        .boxed())
    }
}

#[derive(Debug)]
pub struct Sender {
    slots: BTreeMap<Slot, SlotInfo>,
    dedup: BTreeMap<Slot, DedupInfo>,
    processed: SenderShared,
    confirmed: Option<SenderShared>,
    finalized: Option<SenderShared>,
    slot_finalized: Slot,
    global_replay_from_slot: GlobalReplayFromSlot,
    storage: Option<Storage>,
    storage_max_slots: usize,
    index: u64,
    hasher: RandomState,
    replay: Arc<Mutex<BTreeMap<Slot, ReplayInfo>>>,
}

impl Sender {
    pub fn begin_live_epoch(&mut self) -> anyhow::Result<()> {
        let mut replay = mutex_lock(&self.replay);
        // Commit the discontinuity before accepting any data from the live source.
        // A restart must not recover the stale cursor from an earlier capture.
        if let Some(storage) = &self.storage {
            storage.begin_live_epoch(self.index)?;
        }
        for info in self.slots.values_mut() {
            info.failed = true; // These slots were abandoned deliberately.
        }
        self.slots.clear();
        self.dedup.clear();
        replay.clear();
        for sender in std::iter::once(&mut self.processed)
            .chain(self.confirmed.iter_mut())
            .chain(self.finalized.iter_mut())
        {
            sender.shared.slots_lock().clear();
            sender
                .shared
                .replay_floor
                .store(self.index, Ordering::Relaxed);
            sender.shared.recovery_epoch.fetch_add(1, Ordering::Relaxed);
        }
        update_storage_slot_metrics(&replay);
        update_memory_slot_metrics(&self.processed.shared.slots_lock());
        Ok(())
    }

    pub fn push(&mut self, dedup_required: bool, source_name: &'static str, message: Message) {
        let slot = message.slot();

        // early return, probably nothing can be received after finalized slot status?
        if slot <= self.slot_finalized {
            return;
        }

        // get or create slot info
        let mut messages = SmallVec::<[ParsedMessage; 4]>::new();
        if dedup_required {
            // dedup info
            let dedup = self.dedup.entry(slot).or_default();

            match message {
                Message::Slot(msg) => {
                    let index = msg.status() as i32 as usize;
                    if !dedup.slots[index] {
                        dedup.slots[index] = true;
                        messages.push(ParsedMessage::Slot(Arc::new(msg)));
                    }
                }
                Message::Account(mut msg) => {
                    let key = DedupInfoAccountTransactionKey::from(&msg);
                    if dedup.accounts_updates.insert(key) {
                        if let Some(signature) = key.signature {
                            match dedup.transactions.entry(signature) {
                                HashMapEntry::Occupied(mut entry) => match entry.get_mut() {
                                    DedupInfoTransactionIndex::Index(index) => {
                                        msg.update_write_version(*index as u64);
                                        messages.push(ParsedMessage::Account(Arc::new(msg)));
                                    }
                                    DedupInfoTransactionIndex::Accounts(vec) => {
                                        vec.push(msg);
                                    }
                                },
                                HashMapEntry::Vacant(entry) => {
                                    entry.insert(DedupInfoTransactionIndex::Accounts(vec![msg]));
                                }
                            }
                        } else {
                            let index = dedup.accounts_updates_phantom_index;
                            dedup.accounts_updates_phantom_index += 1;
                            msg.update_write_version(index);
                            messages.push(ParsedMessage::Account(Arc::new(msg)));
                        }
                    }
                }
                Message::Transaction(msg) => {
                    // we keep some space for account updates without signature
                    let index = msg.index() as usize + 1_000;
                    match dedup.transactions.entry(msg.signature()) {
                        HashMapEntry::Occupied(mut entry) => {
                            let entry = entry.get_mut();
                            if let DedupInfoTransactionIndex::Accounts(vec) = entry {
                                for mut msg in vec.drain(..) {
                                    msg.update_write_version(index as u64);
                                    messages.push(ParsedMessage::Account(Arc::new(msg)));
                                }

                                *entry = DedupInfoTransactionIndex::Index(index);
                                messages.push(ParsedMessage::Transaction(Arc::new(msg)));
                            }
                        }
                        HashMapEntry::Vacant(entry) => {
                            entry.insert(DedupInfoTransactionIndex::Index(index));
                            messages.push(ParsedMessage::Transaction(Arc::new(msg)));
                        }
                    }
                }
                Message::Entry(msg) => {
                    let index = msg.index() as usize;
                    if dedup.entries.len() <= index {
                        dedup.entries.resize(
                            index.next_power_of_two().max(dedup.entries.len() * 2),
                            false,
                        );
                    }
                    if !dedup.entries[index] {
                        dedup.entries[index] = true;
                        messages.push(ParsedMessage::Entry(Arc::new(msg)));
                    }
                }
                Message::BlockMeta(msg) => {
                    if !dedup.block_meta {
                        dedup.block_meta = true;
                        messages.push(ParsedMessage::BlockMeta(Arc::new(msg)));
                    }
                }
                Message::Block(_) => unreachable!(),
            };
        } else {
            messages.push(message.into());
        }

        if messages.is_empty() {
            return;
        }
        // push messages
        let mut replay_lock = mutex_lock(&self.replay);
        let (replay, replay_inserted) = match replay_lock.entry(slot) {
            BTreeMapEntry::Vacant(entry) => (entry.insert(ReplayInfo::new(self.index)), true),
            BTreeMapEntry::Occupied(entry) => (entry.into_mut(), false),
        };
        let mut clean_after_finalized = false;
        for message in messages {
            counter!(
                metrics::CHANNEL_EVENTS_RECEIVED,
                "source" => source_name,
                "type" => message.as_str_type()
            )
            .increment(1);

            let slot_info = self
                .slots
                .entry(slot)
                .or_insert_with(|| SlotInfo::new(slot, replay.head));
            let slot_index_head = slot_info.index;
            let block_message = slot_info.get_block_message(&message);

            for message in [Some(message), block_message].into_iter().flatten() {
                if let Some(messages) = &mut replay.messages {
                    let id = message.get_id(self.hasher.build_hasher());
                    if let Some(&index) = messages.get(&id) {
                        if let Some(storage) = &self.storage {
                            storage.restore_payload(index, message.clone());
                        }
                        continue;
                    }
                    messages.insert(id, self.index);
                }

                // update metrics, push messages to confirmed / finalized
                if let ParsedMessage::Slot(msg) = &message {
                    // update metrics
                    if let Some(commitment) = match msg.status() {
                        SlotStatus::SlotProcessed => Some("processed"),
                        SlotStatus::SlotConfirmed => Some("confirmed"),
                        SlotStatus::SlotFinalized => Some("finalized"),
                        _ => None,
                    } {
                        gauge!(metrics::CHANNEL_SLOT, "commitment" => commitment)
                            .set(msg.slot() as f64)
                    }
                    if msg.status() == SlotStatus::SlotProcessed {
                        let processed_slots_len = self.processed.shared.slots_lock().len();
                        debug!(
                            "new processed {slot} / {} messages / {} slots / {} bytes",
                            self.processed.tail + 1 - self.processed.head,
                            processed_slots_len,
                            self.processed.bytes_total
                        );

                        gauge!(metrics::CHANNEL_MESSAGES_TOTAL)
                            .set((self.processed.tail + 1 - self.processed.head) as f64);
                        gauge!(metrics::CHANNEL_SLOTS_TOTAL).set(processed_slots_len as f64);
                        gauge!(metrics::CHANNEL_BYTES_TOTAL).set(self.processed.bytes_total as f64);
                    }

                    // push slot message to confirmed / finalized
                    if let Some(shared) = self.confirmed.as_mut() {
                        shared.push_stored(
                            slot,
                            message.clone(),
                            &self.storage,
                            &mut self.index,
                            slot_index_head,
                        );
                    }
                    if let Some(shared) = self.finalized.as_mut() {
                        shared.push_stored(
                            slot,
                            message.clone(),
                            &self.storage,
                            &mut self.index,
                            slot_index_head,
                        );
                    }

                    // push messages to confirmed
                    if msg.status() == SlotStatus::SlotConfirmed
                        && let Some(shared) = self.confirmed.as_mut()
                        && let Some(slot_info) = self.slots.get(&slot)
                    {
                        for message in slot_info.get_messages_cloned() {
                            shared.push_stored(
                                slot,
                                message,
                                &self.storage,
                                &mut self.index,
                                slot_index_head,
                            );
                        }
                    }

                    // push messages to finalized
                    if msg.status() == SlotStatus::SlotFinalized {
                        clean_after_finalized = true;
                        self.slot_finalized = slot;
                        self.global_replay_from_slot.store(slot + 1);
                        if let Some(shared) = self.finalized.as_mut()
                            && let Some(mut slot_info) = self.slots.remove(&slot)
                        {
                            for message in slot_info.get_messages_owned() {
                                shared.push_stored(
                                    slot,
                                    message,
                                    &self.storage,
                                    &mut self.index,
                                    slot_index_head,
                                );
                            }
                        }
                    }
                } else {
                    // push to confirmed (if we received SlotStatus or message after it)
                    if self.slots.get(&slot).is_some_and(|info| info.confirmed)
                        && let Some(shared) = self.confirmed.as_mut()
                    {
                        shared.push_stored(
                            slot,
                            message.clone(),
                            &self.storage,
                            &mut self.index,
                            slot_index_head,
                        );
                    }
                }

                // Journal exactly the same events as each live commitment stream.
                self.processed.push_stored(
                    slot,
                    message,
                    &self.storage,
                    &mut self.index,
                    slot_index_head,
                );
            }
        }

        if clean_after_finalized {
            loop {
                match self.slots.keys().next().copied() {
                    Some(slot_min) if slot_min <= self.slot_finalized => {
                        self.slots.remove(&slot_min);
                    }
                    _ => break,
                }
            }
            loop {
                match self.dedup.keys().next().copied() {
                    Some(slot_min) if slot_min < self.slot_finalized => {
                        self.dedup.remove(&slot_min);
                    }
                    _ => break,
                }
            }
        }
        if self.storage.is_some() {
            let first_retained = replay_lock
                .last_key_value()
                .map(|(slot, _)| {
                    slot.saturating_sub(self.storage_max_slots.saturating_sub(1) as u64)
                })
                .unwrap_or_default();
            while replay_lock
                .first_key_value()
                .is_some_and(|(slot, _)| *slot < first_retained)
            {
                if let Some((slot, _replay)) = replay_lock.pop_first()
                    && let Some(storage) = &self.storage
                {
                    let until = replay_lock.values().map(|replay| replay.head).min();

                    storage.trim_messages(slot, until);
                }
            }
        }
        if self.storage.is_none() {
            replay_lock.clear();
        }
        if replay_inserted || clean_after_finalized {
            gauge!(metrics::CHANNEL_STORAGE_SLOTS_TOTAL).set(replay_lock.len() as f64);
            update_storage_slot_metrics(&replay_lock);
        }
        update_memory_slot_metrics(&self.processed.shared.slots_lock());

        if let Some(mut wakers) = self.processed.shared.wakers_lock() {
            for waker in wakers.drain(..) {
                waker.wake();
            }
        }
    }
}

#[derive(Debug)]
struct SenderShared {
    commitment: CommitmentLevel,
    shared: Arc<SharedChannel>,
    head: u64,
    tail: u64,
    bytes_total: usize,
    bytes_max: usize,
    incomplete_through: Option<Slot>,
}

impl SenderShared {
    fn new(
        shared: &Arc<SharedChannel>,
        max_messages: usize,
        max_bytes: usize,
        commitment: CommitmentLevel,
    ) -> Self {
        Self {
            commitment,
            shared: Arc::clone(shared),
            head: max_messages as u64 + 1,
            tail: max_messages as u64,
            bytes_total: 0,
            bytes_max: max_bytes,
            incomplete_through: None,
        }
    }

    fn push_stored(
        &mut self,
        slot: Slot,
        message: ParsedMessage,
        storage: &Option<Storage>,
        index: &mut u64,
        slot_head: u64,
    ) {
        // Processed records are also the restart/deduplication journal, even
        // when only higher commitments are exposed for disk replay.
        let replay_index = storage
            .as_ref()
            .filter(|storage| {
                self.commitment == CommitmentLevel::Processed
                    || storage.replay_enabled(self.commitment)
            })
            .map(|storage| {
                let current = *index;
                storage.push_message(
                    current == slot_head,
                    slot,
                    slot_head,
                    current,
                    message.clone(),
                    self.commitment,
                );
                *index += 1;
                current
            });
        self.push(slot, message, replay_index);
    }

    fn push(&mut self, slot: Slot, message: ParsedMessage, replay_index: Option<u64>) {
        let mut removed_max_slot = None;

        let mut slots_lock = self.shared.slots_lock();

        // bump current tail
        self.tail = self.tail.wrapping_add(1);

        // lock and update item
        self.bytes_total += message.size();
        let idx = self.shared.get_idx(self.tail);
        let mut item = self.shared.buffer_idx(idx);
        if let Some(message) = item.data.take() {
            self.head = self.head.wrapping_add(1);
            self.bytes_total -= message.size();
            removed_max_slot = Some(item.slot);
            if item.replay_index != u64::MAX {
                self.shared
                    .replay_floor
                    .store(item.replay_index + 1, Ordering::Relaxed);
            }
        }
        item.replay_index = replay_index.unwrap_or(u64::MAX);
        item.pos = self.tail;
        item.slot = slot;
        item.data = Some(message);
        drop(item);

        // drop messages by extra bytes
        while self.bytes_total >= self.bytes_max && self.head < self.tail {
            let idx = self.shared.get_idx(self.head);
            let mut item = self.shared.buffer_idx(idx);
            let Some(message) = item.data.take() else {
                panic!("nothing to remove to keep bytes under limit")
            };

            self.head = self.head.wrapping_add(1);
            self.bytes_total -= message.size();
            if item.replay_index != u64::MAX {
                self.shared
                    .replay_floor
                    .store(item.replay_index + 1, Ordering::Relaxed);
            }
            removed_max_slot = Some(match removed_max_slot {
                Some(slot) => item.slot.max(slot),
                None => item.slot,
            });
        }

        // store new position for receivers
        self.shared.head.store(self.head, Ordering::Relaxed);
        self.shared.tail.store(self.tail, Ordering::Relaxed);

        // update slot head info
        if self
            .incomplete_through
            .is_none_or(|incomplete| slot > incomplete)
        {
            slots_lock
                .entry(slot)
                .or_insert_with(|| SlotHead { head: self.tail });
        }

        // remove not-complete slots
        if let Some(remove_upto) = removed_max_slot {
            let remove_upto = self.incomplete_through.unwrap_or_default().max(remove_upto);
            self.incomplete_through = Some(remove_upto);
            loop {
                match slots_lock.first_key_value() {
                    Some((slot, _)) if *slot <= remove_upto => {
                        let slot = *slot;
                        slots_lock.remove(&slot);
                    }
                    _ => break,
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct ReceiverAsync {
    shared: Arc<SharedChannel>,
    head: u64,
    finished: bool,
    enable_notifications_accounts: bool,
    enable_notifications_transactions: bool,
    enable_notifications_entries: bool,
}

impl ReceiverAsync {
    fn recv_ref(&mut self, waker: &Waker) -> Result<Option<RecvItem>, RecvError> {
        let tail = self.shared.tail.load(Ordering::Relaxed);
        while self.head <= tail {
            let idx = self.shared.get_idx(self.head);
            let item = self.shared.buffer_idx(idx);
            if item.pos != self.head {
                return Err(RecvError::Lagged);
            }
            self.head = self.head.wrapping_add(1);

            let item = item.data.as_ref().ok_or(RecvError::Lagged)?;
            match item {
                ParsedMessage::Account(_) if !self.enable_notifications_accounts => continue,
                ParsedMessage::Transaction(_) if !self.enable_notifications_transactions => {
                    continue;
                }
                ParsedMessage::Entry(_) if !self.enable_notifications_entries => continue,
                ParsedMessage::Block(_) => continue,
                _ => {}
            }

            let data = FilteredUpdate {
                filters: SmallVec::new_const(),
                filtered_update: MessageRef::from(item).into(),
            }
            .encode_to_vec();
            return Ok(Some(Arc::new(data)));
        }

        if let Some(mut wakers) = self.shared.wakers_lock() {
            wakers.push(waker.clone());
        }
        Ok(None)
    }
}

impl Stream for ReceiverAsync {
    type Item = Result<RecvItem, RecvError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();
        if me.finished {
            return Poll::Ready(None);
        }

        match me.recv_ref(cx.waker()) {
            Ok(Some(value)) => Poll::Ready(Some(Ok(value))),
            Ok(None) => Poll::Pending,
            Err(error) => {
                me.finished = true;
                Poll::Ready(Some(Err(error)))
            }
        }
    }
}

#[derive(Debug)]
pub struct ReceiverSync {
    shared_processed: Arc<SharedChannel>,
    shared_confirmed: Option<Arc<SharedChannel>>,
    shared_finalized: Option<Arc<SharedChannel>>,
}

impl ReceiverSync {
    pub fn try_recv(
        &self,
        commitment: CommitmentLevel,
        head: u64,
    ) -> Result<Option<ParsedMessage>, RecvError> {
        let Some(shared) = (match commitment {
            CommitmentLevel::Processed => Some(&self.shared_processed),
            CommitmentLevel::Confirmed => self.shared_confirmed.as_ref(),
            CommitmentLevel::Finalized => self.shared_finalized.as_ref(),
        }) else {
            return Err(RecvError::Closed);
        };

        let tail = shared.tail.load(Ordering::Relaxed);
        if head <= tail {
            let idx = shared.get_idx(head);
            let item = shared.buffer_idx(idx);
            if item.pos != head {
                return Err(RecvError::Lagged);
            }

            return item.data.clone().ok_or(RecvError::Lagged).map(Some);
        }

        Ok(None)
    }
}

pub struct SharedChannel {
    head: AtomicU64,
    replay_floor: AtomicU64,
    recovery_epoch: AtomicU64,
    tail: AtomicU64,
    mask: u64,
    buffer: Box<[Mutex<Item>]>,
    slots: Mutex<BTreeMap<Slot, SlotHead>>,
    wakers: Option<Mutex<Vec<Waker>>>,
}

impl fmt::Debug for SharedChannel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").field("mask", &self.mask).finish()
    }
}

impl SharedChannel {
    pub(crate) fn new(max_messages: usize, richat: bool) -> Self {
        let mut buffer = Vec::with_capacity(max_messages);
        for i in 0..max_messages {
            buffer.push(Mutex::new(Item {
                replay_index: u64::MAX,
                pos: i as u64,
                slot: 0,
                data: None,
            }));
        }

        Self {
            head: AtomicU64::new(max_messages as u64 + 1),
            replay_floor: AtomicU64::new(0),
            recovery_epoch: AtomicU64::new(0),
            tail: AtomicU64::new(max_messages as u64),
            mask: (max_messages - 1) as u64,
            buffer: buffer.into_boxed_slice(),
            slots: Mutex::default(),
            wakers: richat.then_some(Mutex::default()),
        }
    }

    pub fn get_head_by_replay_index(&self, replay_index: u64) -> Option<u64> {
        // All stream records carry increasing journal indices. Protect the ring
        // against concurrent eviction while finding the first not-yet-replayed event.
        let _guard = self.slots_lock();
        if replay_index < self.replay_floor.load(Ordering::Relaxed) {
            return None;
        }
        let mut low = self.head.load(Ordering::Relaxed);
        let mut high = self.tail.load(Ordering::Relaxed) + 1;
        while low < high {
            let mid = low + (high - low) / 2;
            let item = self.buffer_idx(self.get_idx(mid));
            if item.replay_index < replay_index {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        Some(low)
    }

    pub fn recovery_epoch(&self) -> u64 {
        self.recovery_epoch.load(Ordering::Relaxed)
    }

    #[inline]
    const fn get_idx(&self, pos: u64) -> usize {
        (pos & self.mask) as usize
    }

    #[inline]
    fn buffer_idx(&self, idx: usize) -> MutexGuard<'_, Item> {
        mutex_lock(&self.buffer[idx])
    }

    #[inline]
    fn slots_lock(&self) -> MutexGuard<'_, BTreeMap<Slot, SlotHead>> {
        mutex_lock(&self.slots)
    }

    #[inline]
    fn wakers_lock(&self) -> Option<MutexGuard<'_, Vec<Waker>>> {
        self.wakers.as_ref().map(mutex_lock)
    }
}

fn update_memory_slot_metrics(slots: &BTreeMap<Slot, SlotHead>) {
    set_optional_slot_gauge(
        metrics::CHANNEL_MEMORY_FIRST_SLOT,
        slots.first_key_value().map(|(slot, _head)| *slot),
    );
    set_optional_slot_gauge(
        metrics::CHANNEL_MEMORY_LAST_SLOT,
        slots.last_key_value().map(|(slot, _head)| *slot),
    );
}

fn update_storage_slot_metrics(slots: &BTreeMap<Slot, ReplayInfo>) {
    set_optional_slot_gauge(
        metrics::CHANNEL_STORAGE_FIRST_SLOT,
        slots.first_key_value().map(|(slot, _info)| *slot),
    );
    set_optional_slot_gauge(
        metrics::CHANNEL_STORAGE_LAST_SLOT,
        slots.last_key_value().map(|(slot, _info)| *slot),
    );
}

fn set_optional_slot_gauge(metric: &'static str, slot: Option<Slot>) {
    gauge!(metric).set(optional_slot_gauge_value(slot));
}

fn optional_slot_gauge_value(slot: Option<Slot>) -> f64 {
    slot.map(|slot| slot as f64).unwrap_or(-1.0)
}

#[derive(Debug)]
struct Item {
    replay_index: u64,
    pos: u64,
    slot: Slot,
    data: Option<ParsedMessage>,
}

#[derive(Debug, Clone, Copy)]
struct SlotHead {
    head: u64,
}

#[derive(Debug, Default)]
struct SlotInfo {
    slot: Slot,
    block_created: bool,
    confirmed: bool,
    failed: bool,
    landed: bool,
    messages: Vec<Option<ParsedMessage>>,
    accounts_dedup: HashMap<Pubkey, (u64, usize), RandomState>,
    transactions_count: usize,
    entries_count: usize,
    block_meta: Option<Arc<MessageBlockMeta>>,
    index: u64,
}

impl Drop for SlotInfo {
    fn drop(&mut self) {
        if !self.block_created && !self.failed && self.landed {
            let mut reasons = vec![];
            if let Some(block_meta) = &self.block_meta {
                let executed_transaction_count = block_meta.executed_transaction_count() as usize;
                if executed_transaction_count != self.transactions_count {
                    reasons.push(metrics::BlockMessageFailedReason::MismatchTransactions {
                        actual: self.transactions_count,
                        expected: executed_transaction_count,
                    });
                }
                let entries_count = block_meta.entries_count() as usize;
                if entries_count != self.entries_count {
                    reasons.push(metrics::BlockMessageFailedReason::MismatchEntries {
                        actual: self.entries_count,
                        expected: entries_count,
                    });
                }
            } else {
                reasons.push(metrics::BlockMessageFailedReason::MissedBlockMeta);
            }

            metrics::block_message_failed_inc(self.slot, &reasons);
        }
    }
}

impl SlotInfo {
    fn new(slot: Slot, index: u64) -> Self {
        Self {
            slot,
            block_created: false,
            confirmed: false,
            failed: false,
            landed: false,
            messages: Vec::with_capacity(16_384),
            accounts_dedup: HashMap::default(),
            transactions_count: 0,
            entries_count: 0,
            block_meta: None,
            index,
        }
    }

    fn get_block_message(&mut self, message: &ParsedMessage) -> Option<ParsedMessage> {
        // mark as landed
        if let ParsedMessage::Slot(message) = message
            && matches!(
                message.status(),
                SlotStatus::SlotConfirmed | SlotStatus::SlotFinalized
            )
        {
            self.landed = true;
        }

        if let ParsedMessage::Slot(status) = message
            && status.status() == SlotStatus::SlotConfirmed
        {
            self.confirmed = true;
        }

        // report error if block already created
        if self.block_created {
            if !self.failed {
                self.failed = true;
                let mut reasons = vec![];
                match message {
                    ParsedMessage::Slot(_) => {}
                    ParsedMessage::Account(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraAccount);
                    }
                    ParsedMessage::Transaction(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraTransaction);
                    }
                    ParsedMessage::Entry(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraEntry);
                    }
                    ParsedMessage::BlockMeta(_) => {
                        reasons.push(metrics::BlockMessageFailedReason::ExtraBlockMeta);
                    }
                    ParsedMessage::Block(_) => {}
                }
                metrics::block_message_failed_inc(self.slot, &reasons);
            }
            return None;
        }

        // store message
        match message {
            ParsedMessage::Account(message) => {
                let idx_new = self.messages.len();
                let item = ParsedMessage::Account(Arc::clone(message));
                self.messages.push(Some(item));

                let pubkey = message.pubkey();
                let write_version = message.write_version();
                if let Some(entry) = self.accounts_dedup.get_mut(pubkey) {
                    if entry.0 < write_version {
                        self.messages[entry.1] = None;
                        *entry = (write_version, idx_new);
                    } else {
                        self.messages[idx_new] = None;
                    }
                } else {
                    self.accounts_dedup
                        .insert(*pubkey, (write_version, idx_new));
                }
            }
            ParsedMessage::Slot(_message) => {}
            ParsedMessage::Transaction(message) => {
                let item = ParsedMessage::Transaction(Arc::clone(message));
                self.messages.push(Some(item));
                self.transactions_count += 1;
            }
            ParsedMessage::Entry(message) => {
                let item = ParsedMessage::Entry(Arc::clone(message));
                self.messages.push(Some(item));
                self.entries_count += 1
            }
            ParsedMessage::BlockMeta(message) => {
                let item = ParsedMessage::BlockMeta(Arc::clone(message));
                self.messages.push(Some(item));
                self.block_meta = Some(Arc::clone(message));
            }
            ParsedMessage::Block(_message) => unreachable!(),
        }

        //  attempt to create Block
        if let Some(block_meta) = &self.block_meta
            && block_meta.executed_transaction_count() as usize == self.transactions_count
            && block_meta.entries_count() as usize == self.entries_count
        {
            self.block_created = true;

            let accounts = self
                .messages
                .iter()
                .filter_map(|item| item.as_ref().and_then(|item| item.get_account()))
                .collect();
            let transactions = self
                .messages
                .iter()
                .filter_map(|item| item.as_ref().and_then(|item| item.get_transaction()))
                .collect();
            let entries = self
                .messages
                .iter()
                .filter_map(|item| item.as_ref().and_then(|item| item.get_entry()))
                .collect();
            let block = ParsedMessage::Block(Arc::new(Message::unchecked_create_block(
                accounts,
                transactions,
                entries,
                Arc::clone(block_meta),
                block_meta.created_at(),
            )));
            self.messages.push(Some(block.clone()));

            return Some(block);
        }

        None
    }

    fn get_messages_cloned(&self) -> impl Iterator<Item = ParsedMessage> + '_ {
        self.messages
            .iter()
            .filter_map(|item| item.as_ref().cloned())
    }

    fn get_messages_owned(&mut self) -> impl Iterator<Item = ParsedMessage> + '_ {
        self.messages.drain(..).flatten()
    }
}

#[derive(Debug)]
struct ReplayInfo {
    head: u64,
    messages: Option<IntMap<u64, u64>>,
}

impl ReplayInfo {
    const fn new(head: u64) -> Self {
        Self {
            head,
            messages: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{optional_slot_gauge_value, update_storage_slot_metrics},
        std::collections::BTreeMap,
    };

    #[test]
    fn storage_slot_metrics_report_empty_as_negative_one() {
        update_storage_slot_metrics(&BTreeMap::new());

        assert_eq!(optional_slot_gauge_value(None), -1.0);
        assert_eq!(optional_slot_gauge_value(Some(42)), 42.0);
    }
}

#[derive(Debug)]
struct DedupInfo {
    slots: [bool; 7],
    accounts_updates: HashSet<DedupInfoAccountTransactionKey, RandomState>,
    accounts_updates_phantom_index: u64,
    transactions: HashMap<Signature, DedupInfoTransactionIndex, RandomState>,
    entries: Vec<bool>,
    block_meta: bool,
}

impl Default for DedupInfo {
    fn default() -> Self {
        Self {
            slots: [false; 7],
            accounts_updates: HashSet::with_capacity_and_hasher(8_192, RandomState::default()),
            accounts_updates_phantom_index: 0,
            transactions: HashMap::with_capacity_and_hasher(8_192, RandomState::default()),
            entries: std::iter::repeat_n(false, 256).collect(),
            block_meta: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct DedupInfoAccountTransactionKey {
    signature: Option<Signature>,
    pubkey: Pubkey,
}

impl From<&MessageAccount> for DedupInfoAccountTransactionKey {
    fn from(value: &MessageAccount) -> Self {
        Self {
            signature: value
                .txn_signature()
                .map(|sig| sig.try_into().expect("valid signature")),
            pubkey: *value.pubkey(),
        }
    }
}

#[derive(Debug)]
enum DedupInfoTransactionIndex {
    Index(usize),
    Accounts(Vec<MessageAccount>),
}

#[cfg(test)]
#[path = "channel_tests.rs"]
mod replay_tests;
