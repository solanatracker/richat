use {
    crate::{
        channel::GlobalReplayFromSlot,
        config::{
            ConfigChannelSource, ConfigChannelSourceGeneral, ConfigChannelSourceReconnect,
            ConfigGrpcClientSource,
        },
    },
    anyhow::Context as _,
    futures::{
        future::{BoxFuture, try_join_all},
        stream::{BoxStream, Stream, StreamExt},
    },
    maplit::hashmap,
    richat_client::{
        grpc::{ConfigGrpcClient, GrpcClientBuilderError},
        quic::{ConfigQuicClient, QuicConnectError},
    },
    richat_filter::message::{Message, MessageParseError, MessageParserEncoding},
    richat_proto::{
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SubscribeRequest,
            SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
            SubscribeRequestFilterEntry, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions,
        },
        richat::{GrpcSubscribeRequest, RichatFilter},
    },
    solana_clock::Slot,
    std::{
        collections::{HashMap, HashSet},
        fmt,
        pin::Pin,
        sync::{LazyLock, Mutex},
        task::{Context, Poll},
    },
    thiserror::Error,
    tokio::time::{Duration, sleep},
    tonic::Code,
    tracing::{error, info, warn},
};

#[derive(Debug, Error)]
enum ConnectError {
    #[error(transparent)]
    Quic(QuicConnectError),
    #[error(transparent)]
    Grpc(GrpcClientBuilderError),
}

#[derive(Debug, Error)]
enum SubscribeError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error(transparent)]
    Subscribe(#[from] richat_client::error::SubscribeError),
    #[error(transparent)]
    SubscribeGrpc(#[from] tonic::Status),
}

#[derive(Debug, Error)]
pub enum ReceiveError {
    #[error(transparent)]
    Receive(#[from] richat_client::error::ReceiveError),
    #[error(transparent)]
    Parse(#[from] MessageParseError),
    #[error("replay from the requested slot is not available from any source")]
    ReplayFailed,
    #[error(
        "upstream replay unavailable from slot {from_slot}; continuing live with a history gap"
    )]
    ReplayGap { from_slot: Slot },
    #[error("source stream closed")]
    SourceClosed,
    #[error("source subscription failed: {0}")]
    Subscribe(String),
}

#[derive(Debug, Clone)]
enum SubscriptionConfig {
    Quic {
        config: ConfigQuicClient,
    },
    Grpc {
        source: ConfigGrpcClientSource,
        config: ConfigGrpcClient,
    },
}

impl SubscriptionConfig {
    fn new(config: ConfigChannelSource) -> (Self, ConfigChannelSourceGeneral) {
        match config {
            ConfigChannelSource::Quic { general, config } => (Self::Quic { config }, general),
            ConfigChannelSource::Grpc {
                general,
                source,
                config,
            } => (Self::Grpc { source, config }, general),
        }
    }
}

impl SubscribeError {
    /// Returns `true` when the upstream rejected the replay slot because it
    /// is too old. Covers Quic, Richat-native gRPC, and Dragons Mouth error
    /// paths at subscribe time.
    fn is_replay_slot_not_available(&self) -> bool {
        match self {
            Self::Subscribe(richat_client::error::SubscribeError::ReplayFromSlotNotAvailable(
                _,
            )) => true,
            Self::SubscribeGrpc(status) => is_grpc_replay_rejected(status),
            _ => false,
        }
    }
}

/// Check whether a gRPC status indicates that the requested replay slot is
/// not available.
fn is_grpc_replay_rejected(status: &tonic::Status) -> bool {
    match status.code() {
        // richat plugin: first available slot: {first_available}
        // richat grpc: failed to get replay position for slot {replay_from_slot}
        Code::InvalidArgument => {
            let msg = status.message();
            msg.contains("first available slot")
                || msg.contains("failed to get replay position for slot")
        }
        // dragons mouth: broadcast from {from_slot} is not available, last available: {first_available}
        Code::Internal | Code::OutOfRange => {
            let msg = status.message();
            msg.contains("is not available, last available") || msg == "from_slot is not supported"
        }
        // laserstream
        Code::DataLoss => true,
        _ => false,
    }
}

#[derive(Debug)]
struct Backoff {
    current_interval: Duration,
    initial_interval: Duration,
    max_interval: Duration,
    multiplier: f64,
}

impl Backoff {
    const fn new(config: ConfigChannelSourceReconnect) -> Self {
        Self {
            current_interval: config.initial_interval,
            initial_interval: config.initial_interval,
            max_interval: config.max_interval,
            multiplier: config.multiplier,
        }
    }

    async fn sleep(&mut self) {
        sleep(self.current_interval).await;
        self.current_interval = self
            .current_interval
            .mul_f64(self.multiplier)
            .min(self.max_interval);
    }

    const fn reset(&mut self) {
        self.current_interval = self.initial_interval;
    }
}

type SubscribeFn = Box<
    dyn FnMut(
            Option<Slot>,
        ) -> BoxFuture<
            'static,
            Result<kanal::AsyncReceiver<SubscriptionMessage>, SubscribeError>,
        > + Send,
>;

type SubscriptionMessage = Result<(&'static str, Message), ReceiveError>;

pub type PreparedReloadResult = anyhow::Result<(Vec<&'static str>, Vec<Subscription>)>;

pub struct Subscription {
    name: &'static str,
    config: ConfigChannelSource,
    stream: BoxStream<'static, SubscriptionMessage>,
}

impl fmt::Debug for Subscription {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscription")
            .field("name", &self.name)
            .finish()
    }
}

impl Subscription {
    async fn new(
        source_config: ConfigChannelSource,
        global_replay_from_slot: GlobalReplayFromSlot,
    ) -> anyhow::Result<Self> {
        let (subscription_config, config) = SubscriptionConfig::new(source_config.clone());
        let name = Self::get_static_name(&config.name);
        let stream = Self::source_stream(
            name,
            config.reconnect.map(Backoff::new),
            global_replay_from_slot,
            Box::new(move |from_slot| {
                Box::pin(Self::subscribe(
                    name,
                    subscription_config.clone(),
                    config.disable_accounts,
                    config.parser,
                    config.channel_size,
                    from_slot,
                ))
            }),
        );

        Ok(Self {
            name,
            config: source_config,
            stream,
        })
    }

    fn source_stream(
        name: &'static str,
        backoff: Option<Backoff>,
        replay: GlobalReplayFromSlot,
        subscribe: SubscribeFn,
    ) -> BoxStream<'static, SubscriptionMessage> {
        struct State {
            backoff: Option<Backoff>,
            replay: GlobalReplayFromSlot,
            subscribe: SubscribeFn,
            stream: Option<kanal::AsyncReceiver<SubscriptionMessage>>,
            requested: Option<Slot>,
            epoch: u64,
            finished: bool,
        }
        let state = State {
            backoff,
            replay,
            subscribe,
            stream: None,
            requested: None,
            epoch: 0,
            finished: false,
        };
        futures::stream::unfold(state, move |mut state| async move {
            if state.finished { return None; }
            loop {
                if state.epoch != state.replay.epoch() {
                    state.stream = None;
                }
                let failure = if let Some(stream) = &state.stream {
                    let next = stream.recv().await;
                    // Another source may have advanced recovery while this receive was pending.
                    if state.epoch != state.replay.epoch() {
                        state.stream = None;
                        continue;
                    }
                    match next {
                        Ok(Ok(message)) => {
                            if let Some(backoff) = &mut state.backoff { backoff.reset(); }
                            return Some((Ok(message), state));
                        }
                        Ok(Err(error)) => error,
                        Err(_) => ReceiveError::SourceClosed,
                    }
                } else {
                    (state.requested, state.epoch) = state.replay.snapshot();
                    match (state.subscribe)(state.requested).await {
                        Ok(stream) => { state.stream = Some(stream); continue; }
                        Err(error) if error.is_replay_slot_not_available() => ReceiveError::ReplayFailed,
                        Err(error) => ReceiveError::Subscribe(error.to_string()),
                    }
                };
                state.stream = None;
                if matches!(failure, ReceiveError::SourceClosed) && state.backoff.is_none() {
                    return None;
                }
                if matches!(failure, ReceiveError::ReplayFailed) && let Some(from_slot) = state.requested {
                    if state.replay.fallback_to_live(name, state.requested, state.epoch) {
                        warn!(name, from_slot, "upstream history unavailable; skipping missing history and reconnecting live");
                        // Yield the boundary before opening the live source so the channel can
                        // durably invalidate replay across the gap before receiving new events.
                        return Some((Err(ReceiveError::ReplayGap { from_slot }), state));
                    }
                    if state.epoch != state.replay.epoch() || state.requested != state.replay.load() {
                        continue;
                    }
                    // Give the remaining sources a chance to recover the missing interval.
                    if state.backoff.is_none() {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                if let Some(backoff) = &mut state.backoff {
                    error!(name, error = ?failure, "failed to receive source; reconnecting");
                    backoff.sleep().await;
                } else {
                    state.finished = true;
                    return Some((Err(failure), state));
                }
            }
        }).boxed()
    }

    fn get_static_name(name: &str) -> &'static str {
        static NAMES: LazyLock<Mutex<HashSet<&'static str>>> =
            LazyLock::new(|| Mutex::new(HashSet::new()));
        let mut set = NAMES.lock().expect("poisoned");
        if let Some(&name) = set.get(name) {
            name
        } else {
            let name: &'static str = name.to_owned().leak();
            set.insert(name);
            name
        }
    }

    async fn subscribe(
        name: &'static str,
        config: SubscriptionConfig,
        disable_accounts: bool,
        parser: MessageParserEncoding,
        channel_size: usize,
        replay_from_slot: Option<Slot>,
    ) -> Result<kanal::AsyncReceiver<SubscriptionMessage>, SubscribeError> {
        let (tx, rx) = kanal::bounded_async(channel_size);

        let mut stream = match config {
            SubscriptionConfig::Quic { config } => {
                let connection = config.connect().await.map_err(ConnectError::Quic)?;
                let filter = Self::create_richat_filter(disable_accounts);
                match connection.subscribe(replay_from_slot, filter).await {
                    Ok(stream) => {
                        info!(name, version = stream.get_version(), "connected");
                        stream.boxed()
                    }
                    Err(richat_client::error::SubscribeError::ReplayFromSlotNotAvailable(_)) => {
                        let _ = tx.send(Err(ReceiveError::ReplayFailed)).await;
                        return Ok(rx);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            SubscriptionConfig::Grpc { source, config } => {
                let mut connection = config.connect().await.map_err(ConnectError::Grpc)?;
                match source {
                    ConfigGrpcClientSource::DragonsMouth => {
                        let version = connection
                            .get_version()
                            .await
                            .map_err(|error| ConnectError::Grpc(error.into()))?;
                        info!(
                            name,
                            version = version.version,
                            ?replay_from_slot,
                            "connected"
                        );
                        connection
                            .subscribe_dragons_mouth_once(Self::create_dragons_mouth_filter(
                                disable_accounts,
                                replay_from_slot,
                            ))
                            .await?
                            .boxed()
                    }
                    ConfigGrpcClientSource::Richat => connection
                        .subscribe_richat(GrpcSubscribeRequest {
                            replay_from_slot,
                            filter: Self::create_richat_filter(disable_accounts),
                        })
                        .await?
                        .boxed(),
                }
            }
        };
        info!(name, ?replay_from_slot, "subscribed");

        tokio::spawn(async move {
            let mut received_data = false;
            loop {
                let message = match stream.next().await {
                    Some(Ok(data)) => match Message::parse(data.into(), parser) {
                        Ok(message) => Ok((name, message)),
                        Err(MessageParseError::InvalidUpdateMessage("Ping" | "Richat")) => {
                            continue;
                        }
                        Err(error) => Err(error.into()),
                    },
                    Some(Err(error)) => {
                        if matches!(
                            &error,
                            richat_client::error::ReceiveError::Status(status) if is_grpc_replay_rejected(status)
                        ) {
                            error!(name, ?replay_from_slot, %error, "upstream rejected required replay history");
                            Err(ReceiveError::ReplayFailed)
                        } else {
                            Err(error.into())
                        }
                    }
                    None => break,
                };

                if !received_data && let Ok((_, message)) = &message {
                    received_data = true;
                    info!(
                        name,
                        slot = message.slot(),
                        ?replay_from_slot,
                        "receiving source data"
                    );
                }
                if tx.send(message).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    const fn create_richat_filter(disable_accounts: bool) -> Option<RichatFilter> {
        Some(RichatFilter {
            disable_accounts,
            disable_transactions: false,
            disable_entries: false,
            // Richat-only messages are not supported yet
            enable_deshred_transactions: false,
            enable_contact_info: false,
            enable_block_footers: false,
            enable_entry_update_parents: false,
        })
    }

    fn create_dragons_mouth_filter(
        disable_accounts: bool,
        from_slot: Option<Slot>,
    ) -> SubscribeRequest {
        SubscribeRequest {
            accounts: if disable_accounts {
                HashMap::new()
            } else {
                hashmap! { "".to_owned() => SubscribeRequestFilterAccounts::default() }
            },
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            } },
            transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() },
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
            entry: hashmap! { "".to_owned() => SubscribeRequestFilterEntry::default() },
            commitment: Some(CommitmentLevelProto::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot,
        }
    }
}

pub struct Subscriptions {
    global_replay_from_slot: GlobalReplayFromSlot,
    streams: Vec<Subscription>,
    last_polled: usize,
}

impl fmt::Debug for Subscriptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Subscriptions")
            .field("streams", &self.streams.len())
            .field("last_polled", &self.last_polled)
            .finish()
    }
}

impl Subscriptions {
    pub async fn new(
        sources: Vec<ConfigChannelSource>,
        global_replay_from_slot: GlobalReplayFromSlot,
    ) -> anyhow::Result<Self> {
        let streams = Self::create_subscriptions(sources, &global_replay_from_slot).await?;

        Ok(Self {
            global_replay_from_slot,
            streams,
            last_polled: 0,
        })
    }

    async fn create_subscriptions(
        sources: impl IntoIterator<Item = ConfigChannelSource>,
        global_replay_from_slot: &GlobalReplayFromSlot,
    ) -> anyhow::Result<Vec<Subscription>> {
        try_join_all(sources.into_iter().map(|config| {
            let global_replay_from_slot = global_replay_from_slot.clone();
            async move {
                Subscription::new(config, global_replay_from_slot)
                    .await
                    .context("failed to subscribe")
            }
        }))
        .await
    }

    pub fn get_last_polled_name(&self) -> &'static str {
        self.streams[self.last_polled].name
    }

    /// Prepare reload by computing changes and creating subscriptions.
    /// Returns a future that resolves to names to remove and new streams to add.
    pub fn prepare_reload(
        &self,
        new_sources: Vec<ConfigChannelSource>,
    ) -> impl Future<Output = PreparedReloadResult> + use<> {
        // collect streams for removal
        let to_remove: Vec<&'static str> = self
            .streams
            .iter()
            .filter(|stream| {
                let matching = new_sources
                    .iter()
                    .find(|new_source_config| stream.name == new_source_config.name());
                match matching {
                    None => true,
                    Some(new_source_config) if &stream.config != new_source_config => true,
                    Some(_) => false,
                }
            })
            .map(|stream| stream.name)
            .collect();

        // collect sources to add
        let sources_to_add: Vec<ConfigChannelSource> = new_sources
            .into_iter()
            .filter(|new_source_config| {
                let name = Subscription::get_static_name(new_source_config.name());
                match self.streams.iter().find(|s| s.name == name) {
                    Some(stream) => &stream.config != new_source_config,
                    None => true,
                }
            })
            .collect();

        let global_replay_from_slot = self.global_replay_from_slot.clone();
        async move {
            Self::create_subscriptions(sources_to_add, &global_replay_from_slot)
                .await
                .map(|new_streams| (to_remove, new_streams))
        }
    }

    /// Apply the result of `prepare_reload` to update subscriptions.
    pub fn apply_reload(&mut self, to_remove: Vec<&'static str>, new_streams: Vec<Subscription>) {
        for name in to_remove {
            info!(name, "removing subscription");
            self.streams.retain(|stream| stream.name != name);
        }

        for stream in new_streams {
            info!(name = stream.name, "adding subscription");
            self.streams.push(stream);
        }

        self.global_replay_from_slot
            .update_sources(self.streams.len());

        self.last_polled = 0;
    }
}

impl Stream for Subscriptions {
    type Item = SubscriptionMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.streams.is_empty() {
            return Poll::Ready(None);
        }

        let init_index = self.last_polled;
        loop {
            self.last_polled = (self.last_polled + 1) % self.streams.len();
            let index = self.last_polled;

            match self.streams[index].stream.poll_next_unpin(cx) {
                Poll::Ready(Some(value)) => return Poll::Ready(Some(value)),
                Poll::Ready(None) => {
                    return if self.streams[index].config.exclude_on_finish() {
                        self.last_polled = 0;
                        let removed = self.streams.remove(index);
                        warn!(name = removed.name, "source stream finished, removing");
                        self.global_replay_from_slot
                            .update_sources(self.streams.len());
                        self.poll_next(cx)
                    } else {
                        Poll::Ready(None)
                    };
                }
                Poll::Pending => {
                    if index == init_index {
                        return Poll::Pending;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn yellowstone_replay_expiry_is_terminal_with_old_and_new_status_codes() {
        for code in [Code::Internal, Code::OutOfRange] {
            let status = tonic::Status::new(
                code,
                "broadcast from 446021639 is not available, last available: 446122003",
            );
            // Streaming failures and subscribe-time failures use the same classifier.
            assert!(is_grpc_replay_rejected(&status));
            assert!(SubscribeError::SubscribeGrpc(status).is_replay_slot_not_available());
        }
    }

    #[test]
    fn transient_replay_failures_do_not_discard_required_history() {
        for (code, message) in [
            (Code::Internal, "failed to get replay response"),
            (Code::Internal, "failed to send from_slot request"),
            (Code::Unavailable, "server is shutting down try again later"),
            (Code::OutOfRange, "unrelated range error"),
            (Code::InvalidArgument, "invalid account filter"),
        ] {
            let status = tonic::Status::new(code, message);
            assert!(!is_grpc_replay_rejected(&status));
            assert!(!SubscribeError::SubscribeGrpc(status).is_replay_slot_not_available());
        }
    }
    #[tokio::test]
    async fn unavailable_recovery_reconnects_live_without_forwarding_old_slot() {
        use prost::Message as _;
        for subscribe_time in [false, true] {
            for reconnect in [false, true] {
                let replay = GlobalReplayFromSlot::new(Some(446021639), 1);
                let requests = std::sync::Arc::new(Mutex::new(Vec::new()));
                let observed = std::sync::Arc::clone(&requests);
                let mut source = Subscription::source_stream(
                    "test",
                    reconnect.then(|| {
                        Backoff::new(ConfigChannelSourceReconnect {
                            initial_interval: Duration::from_millis(1),
                            max_interval: Duration::from_millis(4),
                            multiplier: 2.0,
                        })
                    }),
                    replay.clone(),
                    Box::new(move |from_slot| {
                        observed.lock().unwrap().push(from_slot);
                        Box::pin(async move {
                            if from_slot.is_some() && subscribe_time {
                                return Err(SubscribeError::SubscribeGrpc(
                                    tonic::Status::out_of_range(
                                        "broadcast from 446021639 is not available, last available: 446122003",
                                    ),
                                ));
                            }
                            let (tx, rx) = kanal::unbounded_async();
                            let event = if from_slot.is_some() {
                                Err(ReceiveError::ReplayFailed)
                            } else {
                                let bytes = richat_proto::geyser::SubscribeUpdate {
                                    created_at: Some(prost_types::Timestamp::default()),
                                    update_oneof: Some(
                                        richat_proto::geyser::subscribe_update::UpdateOneof::Slot(
                                            richat_proto::geyser::SubscribeUpdateSlot {
                                                slot: 446122100,
                                                ..Default::default()
                                            },
                                        ),
                                    ),
                                    ..Default::default()
                                }
                                .encode_to_vec();
                                Ok((
                                    "test",
                                    Message::parse(bytes.into(), MessageParserEncoding::Prost)
                                        .unwrap(),
                                ))
                            };
                            tx.send(event).await.unwrap();
                            Ok(rx)
                        })
                    }),
                );
                assert!(matches!(
                    tokio::time::timeout(Duration::from_secs(1), source.next())
                        .await
                        .unwrap(),
                    Some(Err(ReceiveError::ReplayGap {
                        from_slot: 446021639
                    }))
                ));
                assert_eq!(replay.load(), None);
                assert_eq!(*requests.lock().unwrap(), [Some(446021639)]);
                let (_, message) = tokio::time::timeout(Duration::from_secs(1), source.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert_eq!(message.slot(), 446122100);
                assert_eq!(*requests.lock().unwrap(), [Some(446021639), None]);
            }
        }
    }

    #[test]
    fn fallback_waits_for_other_sources_and_ignores_stale_failures() {
        let replay = GlobalReplayFromSlot::new(Some(10), 2);
        assert!(!replay.fallback_to_live("a", Some(10), 0));
        assert_eq!(replay.load(), Some(10));
        replay.store(11);
        assert!(!replay.fallback_to_live("b", Some(10), 0));
        assert!(!replay.fallback_to_live("b", Some(11), 0));
        assert!(replay.fallback_to_live("a", Some(11), 0));
        assert_eq!(replay.snapshot(), (None, 1));
        replay.store(20);
        assert!(!replay.fallback_to_live("b", Some(11), 0));
        assert_eq!(replay.load(), Some(20));
    }
}
