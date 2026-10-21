use {
    crate::{
        channel::{IndexLocation, Messages, ParsedMessage, ReceiverSync},
        config::ConfigAppsWorkers,
        grpc::{block_meta::BlockMetaStorage, config::ConfigAppsGrpc},
        metrics::{self, GrpcSubscribeMessage},
        version::VERSION,
    },
    ::metrics::{Counter, Gauge, counter, gauge, histogram},
    crossbeam_queue::SegQueue,
    futures::{
        future::{FutureExt, TryFutureExt, ready, try_join_all},
        stream::Stream,
        task::AtomicWaker,
    },
    prost::{Message, bytes::Bytes},
    quanta::Instant,
    richat_filter::{
        config::{
            ConfigFilter, ConfigFilterAccounts, ConfigFilterSlots,
            ConfigLimits as ConfigFilterLimits,
        },
        encode_cache::{EncodeCache, SharedEncodedUpdate, encode_body},
        filter::{Filter, FilteredUpdate},
        message::MessageRef,
    },
    richat_metrics::{duration_to_seconds, subscription_id_label},
    richat_proto::{
        geyser::{
            CommitmentLevel as CommitmentLevelProto, GetBlockHeightRequest, GetBlockHeightResponse,
            GetLatestBlockhashRequest, GetLatestBlockhashResponse, GetSlotRequest, GetSlotResponse,
            GetVersionRequest, GetVersionResponse, IsBlockhashValidRequest,
            IsBlockhashValidResponse, PingRequest, PongResponse, SubscribeReplayInfoRequest,
            SubscribeReplayInfoResponse, SubscribeRequest, SubscribeUpdate, SubscribeUpdatePing,
            SubscribeUpdatePong, subscribe_update::UpdateOneof,
        },
        richat::SubscribeAccountsRequest,
    },
    richat_shared::{
        jsonrpc::helpers::X_SUBSCRIPTION_ID,
        mutex_lock,
        transports::{
            RecvError,
            grpc::{ConfigGrpcServer, SubscribeMessage as SubscribeMessageEncode},
        },
    },
    solana_clock::{MAX_PROCESSING_AGE, Slot},
    solana_commitment_config::CommitmentLevel,
    solana_pubkey::Pubkey,
    std::{
        borrow::Cow,
        collections::HashSet,
        fmt,
        future::Future,
        pin::Pin,
        sync::{
            Arc, Mutex, MutexGuard,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        thread::sleep,
        time::{Duration, SystemTime},
    },
    tokio_util::sync::CancellationToken,
    tonic::{
        Request, Response, Result as TonicResult, Status, Streaming,
        service::interceptor::InterceptorLayer,
    },
    tracing::{error, info, warn},
};

pub mod geyser_gen {
    #![allow(clippy::clone_on_ref_ptr)]
    #![allow(clippy::missing_const_for_fn)]

    include!(concat!(env!("OUT_DIR"), "/geyser.Geyser.rs"));
}

#[derive(Debug, Clone)]
pub struct GrpcServer {
    shutdown: CancellationToken,
    messages: Messages,
    block_meta: Option<Arc<BlockMetaStorage>>,
    filter_limits: Arc<ConfigFilterLimits>,
    ping_interval: Duration,
    subscribe_id: Arc<AtomicU64>,
    subscribe_clients: Arc<SegQueue<SubscribeClient>>,
    subscribe_messages_len_max: usize,
    subscribe_messages_replay_len_max: usize,
}

impl GrpcServer {
    pub fn spawn(
        config: ConfigAppsGrpc,
        messages: Messages,
        shutdown: CancellationToken,
    ) -> anyhow::Result<impl Future<Output = anyhow::Result<()>>> {
        // Create gRPC server
        let (incoming, server_builder) = config.server.create_server_builder()?;
        info!("start server at {}", config.server.endpoint);

        // BlockMeta thread & task
        let (block_meta, block_meta_jh, block_meta_task_jh) = if config.unary.enabled {
            let (meta, task_jh) = BlockMetaStorage::new(config.unary.requests_queue_size);

            let jh = ConfigAppsWorkers::run_once(
                0,
                "richatGrpcWrkBM".to_owned(),
                config.unary.affinity,
                {
                    let messages = messages.clone();
                    let meta = meta.clone();
                    let shutdown = shutdown.clone();
                    move |_index| Self::worker_block_meta(messages, meta, shutdown)
                },
                shutdown.clone(),
            )?;

            (Some(Arc::new(meta)), jh.boxed(), task_jh.boxed())
        } else {
            (None, ready(Ok(())).boxed(), ready(Ok(())).boxed())
        };

        // gRPC service
        let grpc_server = Self {
            shutdown: shutdown.clone(),
            messages,
            block_meta,
            filter_limits: Arc::new(config.filter_limits),
            ping_interval: config.stream.ping_interval,
            subscribe_id: Arc::new(AtomicU64::new(0)),
            subscribe_clients: Arc::new(SegQueue::new()),
            subscribe_messages_len_max: config.stream.messages_len_max,
            subscribe_messages_replay_len_max: config.stream.messages_replay_len_max,
        };

        let service = grpc_server.clone().create_service(&config.server);

        // Spawn workers pool
        let workers = config
            .workers
            .threads
            .run(
                |index| format!("richatGrpcWrk{index:02}"),
                {
                    let shutdown = shutdown.clone();
                    move |index| {
                        grpc_server.worker_messages(
                            index,
                            config.workers.messages_cached_max,
                            config.workers.encoded_cached_max,
                            config
                                .workers
                                .ticks_without_messages_max
                                .unwrap_or(usize::MAX),
                            config.stream.messages_max_per_tick,
                            shutdown,
                        )
                    }
                },
                shutdown.clone(),
            )
            .boxed();

        // Spawn server
        let server = tokio::spawn(async move {
            if let Err(error) = server_builder
                .layer(InterceptorLayer::new(move |request: Request<()>| {
                    if config.x_tokens.is_empty() {
                        Ok(request)
                    } else {
                        match request.metadata().get("x-token") {
                            Some(token) if config.x_tokens.contains(token.as_bytes()) => {
                                Ok(request)
                            }
                            _ => Err(Status::unauthenticated("No valid auth token")),
                        }
                    }
                }))
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, shutdown.cancelled())
                .await
            {
                error!("server error: {error:?}")
            } else {
                info!("gRPC server shutdown")
            }
        })
        .map_err(anyhow::Error::new)
        .boxed();

        // Wait spawned features
        Ok(try_join_all([block_meta_jh, block_meta_task_jh, workers, server]).map_ok(|_| ()))
    }

    pub(crate) fn create_service(
        self,
        config: &ConfigGrpcServer,
    ) -> geyser_gen::geyser_server::GeyserServer<Self> {
        let mut service = geyser_gen::geyser_server::GeyserServer::new(self)
            .max_decoding_message_size(config.max_decoding_message_size);
        for encoding in config.compression.accept.iter().copied() {
            service = service.accept_compressed(encoding);
        }
        for encoding in config.compression.send.iter().copied() {
            service = service.send_compressed(encoding);
        }
        service
    }

    /// Bounded metrics label, see [`subscription_id_label`]
    fn get_x_subscription_id<T>(request: &Request<T>) -> Arc<str> {
        subscription_id_label(
            request
                .metadata()
                .get(X_SUBSCRIPTION_ID)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default(),
        )
    }

    fn parse_commitment(commitment: Option<i32>) -> Result<CommitmentLevelProto, Status> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        CommitmentLevelProto::try_from(commitment)
            .map_err(|_error| {
                let msg = format!("failed to create CommitmentLevel from {commitment:?}");
                Status::unknown(msg)
            })
            .and_then(|commitment| {
                if matches!(
                    commitment,
                    CommitmentLevelProto::Processed
                        | CommitmentLevelProto::Confirmed
                        | CommitmentLevelProto::Finalized
                ) {
                    Ok(commitment)
                } else {
                    Err(Status::unknown(
                        "only Processed, Confirmed and Finalized are allowed",
                    ))
                }
            })
    }

    async fn with_block_meta<'a, T, F>(
        &'a self,
        f: impl FnOnce(&'a BlockMetaStorage) -> F,
    ) -> TonicResult<Response<T>>
    where
        F: Future<Output = TonicResult<T>> + 'a,
    {
        if let Some(storage) = &self.block_meta {
            f(storage).await.map(Response::new)
        } else {
            Err(Status::unimplemented("method disabled"))
        }
    }

    #[inline]
    fn push_client(&self, client: SubscribeClient) {
        self.subscribe_clients.push(client);
    }

    #[inline]
    fn pop_client(&self, prev_client: Option<SubscribeClient>) -> Option<SubscribeClient> {
        if let Some(client) = prev_client {
            self.subscribe_clients.push(client);
        }
        self.subscribe_clients.pop()
    }

    fn worker_block_meta(
        messages: Messages,
        block_meta: BlockMetaStorage,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let receiver = messages.to_receiver();
        let mut head = messages.get_current_tail(CommitmentLevel::Processed) + 1;

        const COUNTER_LIMIT: i32 = 10_000;
        let mut counter = 0;
        loop {
            counter += 1;
            if counter > COUNTER_LIMIT {
                counter = 0;
                if shutdown.is_cancelled() {
                    info!("gRPC block meta thread shutdown");
                    return Ok(());
                }
            }

            let Some(message) = receiver.try_recv(CommitmentLevel::Processed, head)? else {
                counter = COUNTER_LIMIT;
                sleep(Duration::from_micros(100));
                continue;
            };
            head += 1;

            if matches!(
                message,
                ParsedMessage::Slot(_)
                    | ParsedMessage::BlockMeta(_)
                    | ParsedMessage::EntryUpdateParent(_)
            ) {
                block_meta.push(message);
            }
        }
    }

    fn worker_messages(
        &self,
        index: usize,
        messages_cached_max: usize,
        encoded_cached_max: usize,
        ticks_without_messages_max: usize,
        messages_max_per_tick: usize,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let messages_cached_max = messages_cached_max.next_power_of_two();
        let mut messages_cache_processed =
            MessagesCache::new(messages_cached_max, encoded_cached_max);
        let mut messages_cache_confirmed =
            MessagesCache::new(messages_cached_max, encoded_cached_max);
        let mut messages_cache_finalized =
            MessagesCache::new(messages_cached_max, encoded_cached_max);

        let receiver = self.messages.to_receiver();
        let mut ticks_without_messages = 0;
        const SHUTDOWN_COUNTER_LIMIT: i32 = 50_000;
        let mut shutdown_counter = 0;
        let mut prev_client = None;
        loop {
            shutdown_counter += 1;
            if shutdown_counter > SHUTDOWN_COUNTER_LIMIT {
                shutdown_counter = 0;
                if shutdown.is_cancelled() {
                    while self.pop_client(None).is_some() {}
                    info!("gRPC worker#{index:02} shutdown");
                    return Ok(());
                }
            }

            // get client and state
            let Some(client) = self.pop_client(prev_client.take()) else {
                shutdown_counter += 9;
                sleep(Duration::from_micros(1));
                continue;
            };
            let mut state = client.state_lock();
            if state.finished {
                continue;
            }
            if state.replay_from_slot.is_some()
                && state.recovery_epoch != self.messages.recovery_epoch(state.commitment)
            {
                state.finished = true;
                client.push_error(Status::data_loss("upstream history gap interrupted replay"));
                continue;
            }
            let ts = Instant::now();

            // filter messages
            let mut head = match (state.filter.is_some(), state.head) {
                (true, IndexLocation::Memory(head)) => head,
                _ => {
                    drop(state);
                    prev_client = Some(client);
                    continue;
                }
            };

            let messages_cache = match state.commitment {
                CommitmentLevel::Processed => &mut messages_cache_processed,
                CommitmentLevel::Confirmed => &mut messages_cache_confirmed,
                CommitmentLevel::Finalized => &mut messages_cache_finalized,
            };
            let mut errored = false;
            let mut pushed = false;
            let mut messages_counter = 0;
            let mut messages_len = client.messages_len.load(Ordering::Relaxed);
            while messages_len <= client.messages_len_max
                && messages_counter < messages_max_per_tick
            {
                let (message, mut encoder) =
                    match messages_cache.try_recv(&receiver, state.commitment, head) {
                        Ok(Some(item)) => {
                            messages_counter += 1;
                            head += 1;
                            state.head = IndexLocation::Memory(head);
                            item
                        }
                        Ok(None) => break,
                        Err(RecvError::Lagged) => {
                            client.push_error(Status::data_loss("lagged"));
                            errored = true;
                            break;
                        }
                        Err(RecvError::Closed) => {
                            client.push_error(Status::data_loss("closed"));
                            errored = true;
                            break;
                        }
                    };

                if state
                    .replay_from_slot
                    .is_some_and(|slot| message.slot() < slot)
                {
                    continue;
                }
                let message_ref: MessageRef = message.as_ref().into();
                if let Some(filter) = state.filter.as_ref() {
                    for update in filter.get_updates_ref(message_ref, state.commitment) {
                        let data = match encoder.as_mut() {
                            Some(encoder) => encoder.encode(&update),
                            None => update.encode_to_vec().into(),
                        };
                        messages_len += data.len();
                        client.push_message((&update.filtered_update).into(), data);
                        pushed = true;
                    }
                }
            }
            if messages_counter == 0 {
                ticks_without_messages += 1;
            } else {
                state
                    .metric_cpu_usage
                    .increment(duration_to_seconds(ts.elapsed()));
                ticks_without_messages = 0;
            }

            // Observe the time from filter set -> first data message pushed
            // to the client. This is the subscribe handshake latency the
            // clients actually perceive. Taken-once: only the first push
            // after the filter was applied records the histogram.
            if pushed {
                state.observe_time_to_first_message();
            }
            drop(state);

            if pushed {
                client.wake();
            }

            if !errored {
                prev_client = Some(client);
            }

            // sleep a little if no new messages during some ticks
            if ticks_without_messages >= ticks_without_messages_max {
                ticks_without_messages = 0;
                if let Some(client) = prev_client.take() {
                    self.push_client(client);
                }
                sleep(Duration::from_micros(1));
            }
        }
    }

    fn subscribe2<T: 'static>(
        &self,
        request: Request<Streaming<T>>,
        method: &'static str,
        get_ping: impl Fn(&T) -> Option<i32> + Send + 'static,
        mut get_filter: impl FnMut(&ConfigFilterLimits, T) -> (Option<Slot>, Result<Filter, Status>)
        + Send
        + 'static,
    ) -> TonicResult<Response<ReceiverStream>> {
        let x_subscription_id = Self::get_x_subscription_id(&request);
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => method
        )
        .increment(1);

        let ts_subscribe_entry = Instant::now();
        let id = self.subscribe_id.fetch_add(1, Ordering::Relaxed);
        let client = SubscribeClient::new(
            id,
            self.subscribe_messages_len_max,
            self.subscribe_messages_replay_len_max,
            Arc::clone(&x_subscription_id),
        );
        self.push_client(client.clone());

        tokio::spawn({
            let shutdown = self.shutdown.clone();
            let ping_interval = self.ping_interval;
            let client = client.clone();
            let x_subscription_id = Arc::clone(&x_subscription_id);
            async move {
                let mut ts_latest = Instant::now();
                let mut slow_filter_warned = false;
                loop {
                    tokio::select! {
                        () = shutdown.cancelled() => {
                            tracing::error!("push error");
                            client.push_error(Status::internal("shutdown"));
                            break
                        }
                        () = tokio::time::sleep(Duration::from_millis(500)) => {
                            let state = client.state_lock();
                            if state.finished {
                                break
                            }
                            let filter_set = state.filter.is_some();
                            drop(state);

                            let ts = Instant::now();

                            // Warn once when a client has been connected for >3s
                            // without a filter. Matches observed client timeouts.
                            if !filter_set && !slow_filter_warned {
                                let elapsed_ms = ts.duration_since(ts_subscribe_entry).as_millis() as u64;
                                if elapsed_ms > 3_000 {
                                    slow_filter_warned = true;
                                    warn!(
                                        id,
                                        x_subscription_id = x_subscription_id.as_ref(),
                                        elapsed_ms,
                                        "subscribe: filter not set after 3s"
                                    );
                                }
                            }

                            if ts.duration_since(ts_latest) > ping_interval {
                                ts_latest = ts;
                                let data = SubscribeClientState::create_ping();
                                client.push_message(GrpcSubscribeMessage::Ping, data.into());
                                client.wake();
                            }
                        }
                    }
                }
            }
        });

        tokio::spawn({
            let mut stream = request.into_inner();
            let limits = Arc::clone(&self.filter_limits);
            let client = client.clone();
            let messages = self.messages.clone();
            async move {
                let mut filter_ever_set = false;
                let mut handshake_abandoned = false;
                loop {
                    let message = tokio::select! {
                        message = stream.message() => message,
                        // nothing sends responses anymore, dropping `stream` resets the
                        // HTTP/2 stream and releases the client
                        () = client.finished() => break,
                    };
                    match message {
                        Ok(Some(message)) => {
                            if let Some(id) = get_ping(&message) {
                                let data = SubscribeClientState::create_pong(id);
                                client.push_message(GrpcSubscribeMessage::Pong, data.into());
                                client.wake();
                                continue;
                            }

                            let (subscribe_from_slot, new_filter) = get_filter(&limits, message);
                            let mut state = client.state_lock();
                            let was_unset = state.filter.is_none();
                            if let Err(error) = new_filter.and_then(|filter| {
                                let commitment = filter.commitment().into();
                                let recovery_epoch = messages.recovery_epoch(commitment);
                                let new_head = if state.filter.is_none()
                                    || commitment != state.commitment
                                    || subscribe_from_slot.is_some()
                                {
                                    Some(
                                        messages
                                            .get_current_tail_with_replay(
                                                commitment,
                                                subscribe_from_slot,
                                            )
                                            .map_err(Status::invalid_argument)?,
                                    )
                                } else {
                                    None
                                };
                                if subscribe_from_slot.is_some()
                                    && recovery_epoch != messages.recovery_epoch(commitment)
                                {
                                    return Err(Status::data_loss(
                                        "upstream history gap interrupted replay",
                                    ));
                                }
                                if filter.contains_blocks()
                                    && !messages
                                        .supports_block_replay(new_head.unwrap_or(state.head))
                                {
                                    return Err(Status::invalid_argument(
                                        "requested slot predates block-aware disk storage",
                                    ));
                                }
                                if let Some(head) = new_head {
                                    let generation = state.replay_generation.wrapping_add(1);
                                    if matches!(head, IndexLocation::Storage(_)) {
                                        let metric_cpu_usage = gauge!(
                                            metrics::GRPC_SUBSCRIBE_REPLAY_DISK_SECONDS_TOTAL,
                                            "x_subscription_id" => Arc::clone(&x_subscription_id)
                                        );
                                        messages
                                            .replay_from_storage(
                                                client.clone(),
                                                metric_cpu_usage,
                                                commitment,
                                                generation,
                                            )
                                            .map_err(Status::resource_exhausted)?;
                                    }
                                    state.head = head;
                                    state.commitment = commitment;
                                    state.replay_generation = generation;
                                    state.replay_from_slot = subscribe_from_slot;
                                    state.recovery_epoch = recovery_epoch;
                                }
                                state.filter = Some(filter);
                                if was_unset {
                                    state.ts_filter_set = Some(Instant::now());
                                }
                                Ok::<(), Status>(())
                            }) {
                                warn!(id, %error, "failed to handle request");
                                state.finished = true;
                                drop(state);
                                client.push_error(error);
                            } else {
                                // Record filter-parse latency only on the initial
                                // transition unset -> set. Subsequent filter
                                // updates (commitment change, etc.) are not
                                // part of the handshake and would skew the
                                // histogram.
                                if was_unset {
                                    filter_ever_set = true;
                                    histogram!(
                                        metrics::GRPC_SUBSCRIBE_FILTER_PARSE_SECONDS,
                                        "x_subscription_id" => Arc::clone(&x_subscription_id)
                                    )
                                    .record(duration_to_seconds(ts_subscribe_entry.elapsed()));
                                }
                                drop(state);
                                info!(id, "set new filter");
                                continue;
                            }
                        }
                        Ok(None) => {
                            handshake_abandoned = !filter_ever_set;
                            info!(id, "tx stream finished");
                        }
                        Err(error) => warn!(id, %error, "error to receive new filter"),
                    };
                    break;
                }
                if handshake_abandoned {
                    counter!(
                        metrics::GRPC_SUBSCRIBE_HANDSHAKE_ABANDONED_TOTAL,
                        "x_subscription_id" => Arc::clone(&x_subscription_id)
                    )
                    .increment(1);
                }
                info!(id, "drop client tx stream");
            }
        });

        Ok(Response::new(ReceiverStream::new(client)))
    }
}

#[tonic::async_trait]
impl geyser_gen::geyser_server::Geyser for GrpcServer {
    type SubscribeStream = ReceiverStream;
    type SubscribeAccountsStream = ReceiverStream;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        self.subscribe2(
            request,
            "subscribe",
            |message| message.ping.map(|msg| msg.id),
            |limits, message| {
                let subscribe_from_slot = message.from_slot;
                let new_filter = ConfigFilter::try_from(message)
                    .map_err(|error| {
                        Status::invalid_argument(format!("failed to create filter: {error:?}"))
                    })
                    .and_then(|config| {
                        limits
                            .check_filter(&config)
                            .map(|()| Filter::new(&config))
                            .map_err(|error| {
                                Status::invalid_argument(format!(
                                    "failed to check filter: {error:?}"
                                ))
                            })
                    });
                (subscribe_from_slot, new_filter)
            },
        )
    }

    async fn subscribe_accounts(
        &self,
        request: Request<Streaming<SubscribeAccountsRequest>>,
    ) -> TonicResult<Response<Self::SubscribeStream>> {
        let mut pubkeys = HashSet::new();
        self.subscribe2(
            request,
            "subscribe_accounts",
            |message| message.ping,
            move |limits, message| {
                fn try_conv(pubkeys: Vec<Vec<u8>>) -> impl Iterator<Item = Result<Pubkey, String>> {
                    pubkeys.into_iter().map(|bytes| {
                        let slice: [u8; 32] = bytes
                            .try_into()
                            .map_err(|_| "invalid pubkey len".to_owned())?;
                        Ok(Pubkey::from(slice))
                    })
                }

                let new_filter = Ok(&mut pubkeys)
                    .and_then(|pubkeys| {
                        for item in try_conv(message.add) {
                            pubkeys.insert(item?);
                        }
                        for item in try_conv(message.remove) {
                            pubkeys.remove(&item?);
                        }
                        Ok(pubkeys)
                    })
                    .and_then(|pubkeys| {
                        let config = ConfigFilter {
                            slots: [(message.filter, ConfigFilterSlots::default())]
                                .into_iter()
                                .collect(),
                            accounts: [(
                                "".to_owned(),
                                ConfigFilterAccounts {
                                    account: pubkeys.iter().cloned().collect(),
                                    ..Default::default()
                                },
                            )]
                            .into_iter()
                            .collect(),
                            blocks_meta: ["".to_owned()].into_iter().collect(),
                            ..Default::default()
                        };
                        limits
                            .check_filter(&config)
                            .map(|()| Filter::new(&config))
                            .map_err(|error| format!("failed to check filter: {error:?}"))
                    })
                    .map_err(Status::invalid_argument);

                (message.from_slot, new_filter)
            },
        )
    }

    async fn subscribe_replay_info(
        &self,
        _request: Request<SubscribeReplayInfoRequest>,
    ) -> TonicResult<Response<SubscribeReplayInfoResponse>> {
        let response = SubscribeReplayInfoResponse {
            first_available: self.messages.get_first_available_slot(),
        };
        Ok(Response::new(response))
    }

    async fn ping(&self, request: Request<PingRequest>) -> TonicResult<Response<PongResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "ping"
        )
        .increment(1);

        let count = request.get_ref().count;
        let response = PongResponse { count };
        Ok(Response::new(response))
    }

    async fn get_latest_blockhash(
        &self,
        request: Request<GetLatestBlockhashRequest>,
    ) -> TonicResult<Response<GetLatestBlockhashResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_latest_blockhash"
        )
        .increment(1);

        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetLatestBlockhashResponse {
                slot: block.slot,
                blockhash: block.blockhash.as_ref().clone(),
                last_valid_block_height: block.block_height + MAX_PROCESSING_AGE as u64,
            })
        })
        .await
    }

    async fn get_block_height(
        &self,
        request: Request<GetBlockHeightRequest>,
    ) -> TonicResult<Response<GetBlockHeightResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_block_height"
        )
        .increment(1);

        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetBlockHeightResponse {
                block_height: block.block_height,
            })
        })
        .await
    }

    async fn get_slot(
        &self,
        request: Request<GetSlotRequest>,
    ) -> TonicResult<Response<GetSlotResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_slot"
        )
        .increment(1);

        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let block = storage.get_block(commitment).await?;
            Ok(GetSlotResponse { slot: block.slot })
        })
        .await
    }

    async fn is_blockhash_valid(
        &self,
        request: tonic::Request<IsBlockhashValidRequest>,
    ) -> TonicResult<Response<IsBlockhashValidResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "is_blockhash_valid"
        )
        .increment(1);

        let commitment = Self::parse_commitment(request.get_ref().commitment)?;
        self.with_block_meta(|storage| async move {
            let (valid, slot) = storage
                .is_blockhash_valid(request.into_inner().blockhash, commitment)
                .await?;
            Ok(IsBlockhashValidResponse { valid, slot })
        })
        .await
    }

    async fn get_version(
        &self,
        request: Request<GetVersionRequest>,
    ) -> TonicResult<Response<GetVersionResponse>> {
        counter!(
            metrics::GRPC_REQUESTS_TOTAL,
            "x_subscription_id" => Self::get_x_subscription_id(&request),
            "method" => "get_version"
        )
        .increment(1);

        Ok(Response::new(GetVersionResponse {
            version: VERSION.create_grpc_version_info().json(),
        }))
    }
}

type SubscribeMessage = Result<(GrpcSubscribeMessage, SharedEncodedUpdate), Status>;

/// Encoded `SubscribeUpdate` written to the gRPC stream
#[derive(Debug)]
pub struct SubscribeData(SharedEncodedUpdate);

impl SubscribeMessageEncode for SubscribeData {
    fn encode(self, buf: &mut tonic::codec::EncodeBuf<'_>) {
        self.0.encode(buf);
    }
}

#[derive(Debug, Clone)]
pub struct SubscribeClient {
    state: Arc<Mutex<SubscribeClientState>>,
    messages: Arc<SegQueue<SubscribeMessage>>,
    pub messages_len: Arc<AtomicUsize>,
    pub messages_len_max: usize,
    pub messages_replay_len_max: usize,
    waker: Arc<AtomicWaker>,
    x_subscription_id: Arc<str>,
}

impl SubscribeClient {
    pub(crate) fn new(
        id: u64,
        messages_len_max: usize,
        messages_replay_len_max: usize,
        x_subscription_id: Arc<str>,
    ) -> Self {
        let state = SubscribeClientState::new(id, Arc::clone(&x_subscription_id));
        Self {
            state: Arc::new(Mutex::new(state)),
            messages: Arc::new(SegQueue::new()),
            messages_len: Arc::new(AtomicUsize::new(0)),
            messages_len_max,
            messages_replay_len_max,
            waker: Arc::new(AtomicWaker::new()),
            x_subscription_id,
        }
    }

    #[cfg(test)]
    pub(crate) fn pop_for_test(&self) -> Option<Result<Vec<u8>, Status>> {
        self.messages.pop().map(|item| {
            item.map(|(_, data)| {
                self.messages_len.fetch_sub(data.len(), Ordering::Relaxed);
                data.to_vec()
            })
        })
    }

    #[inline]
    pub fn state_lock(&self) -> MutexGuard<'_, SubscribeClientState> {
        mutex_lock(&self.state)
    }

    pub fn push_message(&self, message: GrpcSubscribeMessage, data: SharedEncodedUpdate) {
        self.messages_len.fetch_add(data.len(), Ordering::Relaxed);
        self.messages.push(Ok((message, data)));
    }

    pub fn push_error(&self, error: Status) {
        while self.messages.pop().is_some() {}
        self.messages.push(Err(error));
        self.waker.wake();
    }

    /// Wake the stream's poll_next after pushing messages.
    pub fn wake(&self) {
        self.waker.wake();
    }

    /// Resolves once the client is finished: an error was sent or the response stream dropped
    async fn finished(&self) {
        while !self.state_lock().finished {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

#[derive(Debug)]
pub struct SubscribeClientState {
    pub finished: bool, // check in workers with acquired mutex
    id: u64,
    x_subscription_id: Arc<str>,
    pub commitment: CommitmentLevel,
    pub replay_generation: u64,
    pub recovery_epoch: u64,
    pub replay_from_slot: Option<Slot>,
    pub head: IndexLocation,
    pub filter: Option<Filter>,
    metric_cpu_usage: Gauge,
    /// Taken once, when the worker pushes the first data message to this
    /// client after a filter has been applied. Observes
    /// `grpc_subscribe_time_to_first_message_seconds`.
    pub ts_filter_set: Option<Instant>,
}

impl Drop for SubscribeClientState {
    fn drop(&mut self) {
        info!(
            id = self.id,
            x_subscription_id = self.x_subscription_id.as_ref(),
            "drop client state"
        );
        gauge!(metrics::GRPC_SUBSCRIBE_TOTAL, "x_subscription_id" => Arc::clone(&self.x_subscription_id))
            .decrement(1);
    }
}

impl SubscribeClientState {
    fn new(id: u64, x_subscription_id: Arc<str>) -> Self {
        info!(
            id,
            x_subscription_id = x_subscription_id.as_ref(),
            "new client"
        );
        gauge!(metrics::GRPC_SUBSCRIBE_TOTAL, "x_subscription_id" => Arc::clone(&x_subscription_id))
            .increment(1);

        let metric_cpu_usage = gauge!(
            metrics::GRPC_SUBSCRIBE_CPU_SECONDS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id)
        );

        Self {
            finished: false,
            id,
            x_subscription_id,
            commitment: CommitmentLevel::default(),
            replay_generation: 0,
            recovery_epoch: 0,
            replay_from_slot: None,
            head: IndexLocation::Unknown,
            filter: None,
            metric_cpu_usage,
            ts_filter_set: None,
        }
    }

    #[inline]
    fn serialize_ping_pong(oneof: UpdateOneof) -> Vec<u8> {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(oneof),
            created_at: Some(SystemTime::now().into()),
        }
        .encode_to_vec()
    }

    #[inline]
    fn create_ping() -> Vec<u8> {
        Self::serialize_ping_pong(UpdateOneof::Ping(SubscribeUpdatePing {}))
    }

    #[inline]
    fn create_pong(id: i32) -> Vec<u8> {
        Self::serialize_ping_pong(UpdateOneof::Pong(SubscribeUpdatePong { id }))
    }

    /// Observe the first data message pushed after a filter is applied.
    pub fn observe_time_to_first_message(&mut self) {
        if let Some(ts_filter_set) = self.ts_filter_set.take() {
            histogram!(
                metrics::GRPC_SUBSCRIBE_TIME_TO_FIRST_MESSAGE_SECONDS,
                "x_subscription_id" => Arc::clone(&self.x_subscription_id)
            )
            .record(duration_to_seconds(ts_filter_set.elapsed()));
        }
    }
}

#[derive(Debug)]
pub struct ReceiverStream {
    client: SubscribeClient,
    finished: bool,
    /// (messages, bytes) counters per message type, a registry lookup per message is expensive
    counters: [Option<(Counter, Counter)>; GrpcSubscribeMessage::COUNT],
}

impl ReceiverStream {
    const fn new(client: SubscribeClient) -> Self {
        Self {
            client,
            finished: false,
            counters: [const { None }; GrpcSubscribeMessage::COUNT],
        }
    }
}

impl Drop for ReceiverStream {
    fn drop(&mut self) {
        let mut state = self.client.state_lock();
        state.finished = true;
    }
}

impl Stream for ReceiverStream {
    type Item = TonicResult<SubscribeData>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        self.client.waker.register(cx.waker());

        if let Some(item) = self.client.messages.pop() {
            let item = match item {
                Ok((message, data)) => {
                    self.client
                        .messages_len
                        .fetch_sub(data.len(), Ordering::Relaxed);
                    let me = &mut *self;
                    let (count, bytes) = me.counters[message as usize].get_or_insert_with(|| {
                        (
                            counter!(
                                metrics::GRPC_SUBSCRIBE_MESSAGES_COUNT_TOTAL,
                                "x_subscription_id" => Arc::clone(&me.client.x_subscription_id),
                                "message" => message.as_str(),
                            ),
                            counter!(
                                metrics::GRPC_SUBSCRIBE_MESSAGES_BYTES_TOTAL,
                                "x_subscription_id" => Arc::clone(&me.client.x_subscription_id),
                                "message" => message.as_str(),
                            ),
                        )
                    });
                    count.increment(1);
                    bytes.increment(data.len() as u64);

                    Ok(SubscribeData(data))
                }
                Err(error) => Err(error),
            };

            self.finished = item.is_err();
            Poll::Ready(Some(item))
        } else {
            Poll::Pending
        }
    }
}

struct MessagesCache {
    head: u64,
    mask: u64,
    buffer: Box<[MessagesCacheItem]>,
    encoded: EncodedCache,
}

impl fmt::Debug for MessagesCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessagesCache")
            .field("head", &self.head)
            .field("mask", &self.mask)
            .field("encoded_bytes", &self.encoded.bytes)
            .finish()
    }
}

impl MessagesCache {
    fn new(max_messages: usize, encoded_bytes_max: usize) -> Self {
        let buffer = (0..max_messages)
            .map(|_| MessagesCacheItem {
                pos: u64::MAX,
                msg: None,
            })
            .collect::<Vec<_>>();

        Self {
            head: 0,
            mask: (max_messages - 1) as u64,
            buffer: buffer.into_boxed_slice(),
            encoded: EncodedCache {
                mask: (max_messages - 1) as u64,
                buffer: (0..max_messages).map(|_| EncodeCache::default()).collect(),
                tail: 0,
                bytes: 0,
                bytes_max: encoded_bytes_max,
            },
        }
    }

    #[inline]
    const fn get_idx(&self, pos: u64) -> usize {
        (pos & self.mask) as usize
    }

    /// Returns the message and, if it is cached, an encoder that shares
    /// encoded updates with other clients reading the same message
    fn try_recv(
        &mut self,
        receiver: &ReceiverSync,
        commitment: CommitmentLevel,
        head: u64,
    ) -> Result<Option<MessagesCacheRecv<'_>>, RecvError> {
        if head > self.head {
            self.head = head;
        }
        let inrange = head >= self.head - self.mask;

        // return if item cached
        let idx = self.get_idx(head);
        if !(inrange && self.buffer[idx].pos == head) {
            // try to get from the channel
            let Some(item) = receiver.try_recv(commitment, head)? else {
                return Ok(None);
            };

            if !inrange {
                return Ok(Some((Cow::Owned(item), None)));
            }

            // save item, drop encoded updates of the previous one
            self.encoded.clear(idx);
            self.buffer[idx] = MessagesCacheItem {
                pos: head,
                msg: Some(item),
            };
        }

        let Self {
            buffer, encoded, ..
        } = self;
        Ok(buffer[idx].msg.as_ref().map(|msg| {
            (
                Cow::Borrowed(msg),
                Some(SharedEncoder {
                    cache: encoded,
                    pos: head,
                }),
            )
        }))
    }
}

type MessagesCacheRecv<'a> = (Cow<'a, ParsedMessage>, Option<SharedEncoder<'a>>);

struct MessagesCacheItem {
    pos: u64,
    msg: Option<ParsedMessage>,
}

/// Encoded updates per cached message; bodies of the oldest messages are
/// dropped first once `bytes_max` is reached
struct EncodedCache {
    mask: u64,
    buffer: Box<[EncodeCache]>,
    /// No bodies are cached for positions below `tail`
    tail: u64,
    bytes: usize,
    bytes_max: usize,
}

impl EncodedCache {
    fn clear(&mut self, idx: usize) {
        self.bytes -= self.buffer[idx].encoded_bytes();
        self.buffer[idx].clear();
    }

    /// Drop bodies of messages before `pos` until `bytes_max` is respected
    fn evict(&mut self, pos: u64) -> bool {
        self.tail = self.tail.max(pos.saturating_sub(self.mask));
        while self.bytes >= self.bytes_max && self.tail < pos {
            self.clear((self.tail & self.mask) as usize);
            self.tail += 1;
        }
        self.bytes < self.bytes_max
    }
}

/// Encodes an update once per message and filter output shape
struct SharedEncoder<'a> {
    cache: &'a mut EncodedCache,
    pos: u64,
}

impl SharedEncoder<'_> {
    fn encode(&mut self, update: &FilteredUpdate<'_>) -> SharedEncodedUpdate {
        if self.pos < self.cache.tail {
            return update.encode_to_vec().into();
        }

        let idx = (self.pos & self.cache.mask) as usize;
        let cache = &mut self.cache.buffer[idx];
        if let Some(body) = cache.get(update) {
            return SharedEncodedUpdate::new(&update.filters, body.clone());
        }
        // first client of this shape encodes as usual
        if !cache.mark_seen(update) || !self.cache.evict(self.pos) {
            return update.encode_to_vec().into();
        }

        let body = Bytes::from(encode_body(update));
        if self.cache.buffer[idx].insert(update, body.clone()) {
            self.cache.bytes += body.len();
        }
        SharedEncodedUpdate::new(&update.filters, body)
    }
}

#[cfg(test)]
impl GrpcServer {
    pub(crate) fn test_service(
        messages: Messages,
        shutdown: CancellationToken,
    ) -> (Self, std::thread::JoinHandle<anyhow::Result<()>>) {
        let service = Self {
            shutdown: shutdown.clone(),
            messages,
            block_meta: None,
            filter_limits: Arc::new(ConfigFilterLimits::default()),
            ping_interval: Duration::from_secs(60),
            subscribe_id: Arc::new(AtomicU64::new(0)),
            subscribe_clients: Arc::new(SegQueue::new()),
            subscribe_messages_len_max: 4096,
            subscribe_messages_replay_len_max: 4096,
        };
        let worker = service.clone();
        let thread = std::thread::spawn(move || {
            worker.worker_messages(0, 128, 64 * 1024 * 1024, 100, 100, shutdown)
        });
        (service, thread)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{EncodeCache, EncodedCache, ReceiverStream, SharedEncoder, SubscribeClient},
        futures::FutureExt,
        maplit::hashmap,
        prost::Message as _,
        richat_filter::{
            config::{ConfigFilter, ConfigFilterSlots},
            encode_cache::encode_body,
            filter::Filter,
            message::{Message, MessageParserEncoding},
        },
        richat_proto::geyser::{
            SubscribeUpdate, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        },
        solana_commitment_config::CommitmentLevel,
        std::{borrow::Cow, sync::Arc, time::Duration},
    };

    #[tokio::test]
    async fn request_reader_stops_when_response_stream_is_dropped() {
        let client = SubscribeClient::new(1, 1024, 1024, Arc::from("test"));
        let response = ReceiverStream::new(client.clone());
        let mut finished = Box::pin(client.finished());
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert!((&mut finished).now_or_never().is_none());

        // the response ends (lagged, error) or the client goes away, the request stream can
        // stay open, the reader must not keep the client and its metrics alive
        drop(response);
        tokio::time::timeout(Duration::from_secs(2), finished)
            .await
            .expect("request reader is not released");
    }

    #[test]
    fn test_encoded_cache_evicts_oldest() {
        let data = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 42,
                ..Default::default()
            })),
            created_at: Some(Default::default()),
        }
        .encode_to_vec();
        let message =
            Message::parse(Cow::Owned(data), MessageParserEncoding::Limited).expect("valid");
        let filter = Filter::new(&ConfigFilter {
            slots: hashmap! { "client".to_owned() => ConfigFilterSlots::default() },
            ..Default::default()
        });
        let updates = filter.get_updates_ref((&message).into(), CommitmentLevel::Processed);
        let update = &updates[0];
        let expected = update.encode_to_vec();
        let body_len = encode_body(update).len();

        let mut cache = EncodedCache {
            mask: 7,
            buffer: (0..8).map(|_| EncodeCache::default()).collect(),
            tail: 0,
            bytes: 0,
            bytes_max: body_len * 2,
        };
        {
            let mut encode = |pos| {
                let encoded = SharedEncoder {
                    cache: &mut cache,
                    pos,
                }
                .encode(update);
                assert_eq!(encoded.to_vec(), expected);
            };

            // second client caches the body, two bodies fit
            for pos in [0, 0, 1, 1, 1] {
                encode(pos);
            }
            // third body drops the oldest
            encode(2);
            encode(2);
            // evicted position is not cached again
            encode(0);
            encode(0);
        }
        assert_eq!(cache.tail, 1);
        assert_eq!(cache.bytes, body_len * 2);
        assert_eq!(cache.buffer[0].encoded_bytes(), 0);
        assert_eq!(cache.buffer[1].encoded_bytes(), body_len);
        assert_eq!(cache.buffer[2].encoded_bytes(), body_len);
    }
}
