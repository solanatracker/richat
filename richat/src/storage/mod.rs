pub mod metadata;
mod reader;
pub mod segments;

use {
    crate::{
        channel::{IndexLocation, ParsedMessage, SharedChannel},
        config::ConfigStorage,
        grpc::server::SubscribeClient,
        metrics::GrpcSubscribeMessage,
        storage::{
            metadata::Metadata,
            reader::{ReplaySelection, ScannedRecord},
            segments::{COMMITMENT_RECORDS, DecompressedChunk, SegmentReader, WriterCommand},
        },
        util::SpawnedThreads,
    },
    ::metrics::Gauge,
    anyhow::Context,
    futures::future::try_join_all,
    quanta::Instant,
    richat_filter::message::MessageParserEncoding,
    richat_metrics::duration_to_seconds,
    richat_shared::mutex_lock,
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    std::{
        collections::{BTreeMap, VecDeque},
        path::PathBuf,
        sync::{Arc, Mutex, atomic::Ordering},
        thread,
        time::Duration,
    },
    tokio::task::spawn_blocking,
    tokio_util::sync::CancellationToken,
    tonic::Status,
};

/// Replay start metadata exposed to the rest of `richat` for each retained
/// slot.
#[derive(Debug, Clone, Copy)]
pub struct SlotIndexValue {
    pub finalized: bool,
    pub head: u64,
}

/// Public storage facade used by channel startup, replay bootstrap, and disk
/// replay workers.
#[derive(Debug, Clone)]
pub struct Storage {
    metadata: Metadata,
    commitment_mask: u8,
    write_tx: kanal::Sender<WriterCommand>,
    replay_queue: Arc<Mutex<ReplayQueue>>,
    metric_disk_size_poll_interval: Duration,
}

impl Storage {
    pub fn open(
        config: ConfigStorage,
        parser: MessageParserEncoding,
        shutdown: CancellationToken,
    ) -> anyhow::Result<(Self, SpawnedThreads)> {
        anyhow::ensure!(config.max_slots > 0, "storage.max_slots must be positive");
        anyhow::ensure!(
            !config.commitments.is_empty(),
            "storage.commitments must not be empty"
        );
        anyhow::ensure!(
            config.replay_threads > 0 && config.compressor_threads > 0,
            "storage worker counts must be positive"
        );
        anyhow::ensure!(
            config.replay_decode_per_tick > 0
                && config.chunk_target_size > 0
                && config.segment_target_size > 0,
            "storage decode and chunk/segment sizes must be positive"
        );
        let segments_path = config.segments_path();
        std::fs::create_dir_all(&segments_path)
            .with_context(|| format!("failed to create segments path: {segments_path:?}"))?;

        let metadata = Metadata::open(&config.metadata_path(), segments_path)?;
        let (write_tx, mut threads) = segments::spawn_write_pipeline(&config, metadata.clone())?;

        let storage = Self {
            commitment_mask: config.commitment_mask(),
            metadata,
            write_tx,
            replay_queue: Arc::new(Mutex::new(ReplayQueue::new(config.replay_inflight_max))),
            metric_disk_size_poll_interval: config.metric_disk_size_poll_interval,
        };

        for index in 0..config.replay_threads {
            let th_name = format!("richatStrgRep{index:02}");
            let storage = storage.clone();
            let affinity = config.replay_affinity.clone();
            let shutdown = shutdown.clone();
            let replay_decode_per_tick = config.replay_decode_per_tick;
            let jh = thread::Builder::new()
                .name(th_name.clone())
                .spawn(move || {
                    if let Some(cpus) = affinity {
                        affinity_linux::set_thread_affinity(cpus.into_iter())
                            .expect("failed to set affinity");
                    }
                    Self::spawn_replay(storage, parser, replay_decode_per_tick, shutdown)
                })?;
            threads.push((th_name, Some(jh)));
        }

        Ok((storage, threads))
    }

    fn spawn_replay(
        storage: Self,
        parser: MessageParserEncoding,
        messages_decode_per_tick: usize,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        let mut shutdown_ts = Instant::now();
        let mut prev_request = None;
        loop {
            let Some(mut req) = ReplayQueue::pop_next(&storage.replay_queue, prev_request.take())
            else {
                if shutdown.is_cancelled() {
                    break;
                }
                thread::sleep(Duration::from_millis(1));
                continue;
            };

            let ts = Instant::now();
            if ts.duration_since(shutdown_ts) > Duration::from_millis(100) {
                shutdown_ts = ts;
                if shutdown.is_cancelled() {
                    break;
                }
            }

            let mut state = req.client.state_lock();
            if state.finished
                || state.replay_generation != req.generation
                || !matches!(state.head, IndexLocation::Storage(_))
            {
                drop(state);
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            }
            if let Some(error) = req.state.read_error.take() {
                state.finished = true;
                req.client.push_error(error);
                drop(state);
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            }
            if state.recovery_epoch != req.messages.recovery_epoch() {
                state.finished = true;
                req.client
                    .push_error(Status::data_loss("upstream history gap interrupted replay"));
                drop(state);
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            }
            let IndexLocation::Storage(mut next_index) = state.head else {
                unreachable!()
            };
            let mut pushed = false;
            while req.client.messages_len.load(Ordering::Relaxed)
                < req.client.messages_replay_len_max
            {
                let Some(record) = req.state.messages.pop_front() else {
                    break;
                };
                if record.index != next_index {
                    req.state.read_error = Some(Status::data_loss("gap in disk replay journal"));
                    break;
                }
                next_index = record.index + 1;
                let Some(message) = record.message else {
                    continue;
                };
                let filter = state.filter.as_ref().expect("defined filter");
                let items = filter
                    .get_updates_ref((&message).into(), state.commitment)
                    .iter()
                    .map(|msg| ((&msg.filtered_update).into(), msg.encode_to_vec()))
                    .collect::<SmallVec<[(GrpcSubscribeMessage, Vec<u8>); 2]>>();
                for (message, data) in items {
                    req.client.push_message(message, data);
                    pushed = true;
                }
            }
            state.head = IndexLocation::Storage(next_index);
            if pushed {
                state.observe_time_to_first_message();
                req.client.wake();
            }
            if req.state.read_error.is_none()
                && let Some(head) = req.messages.get_head_by_replay_index(next_index)
            {
                state.head = IndexLocation::Memory(head);
                drop(state);
                req.metric_cpu_usage
                    .increment(duration_to_seconds(ts.elapsed()));
                ReplayQueue::drop_req(&storage.replay_queue);
                continue;
            }
            let read_more = req.state.messages.is_empty()
                && req.client.messages_len.load(Ordering::Relaxed)
                    < req.client.messages_replay_len_max;
            let selection = ReplaySelection {
                commitment: state.commitment,
                from_slot: state.replay_from_slot,
            };
            drop(state);

            // Keep the reader and the partially decoded chunk between turns. The
            // configured budget is a record budget, including nonmatching commitments.
            if read_more && req.state.read_error.is_none() {
                let reader = req
                    .state
                    .reader
                    .get_or_insert_with(|| storage.read_messages_from_index(next_index, parser));
                let mut decoded_bytes = 0usize;
                for _ in 0..messages_decode_per_tick {
                    loop {
                        if let Some(chunk) = req.state.chunk.as_mut()
                            && let Some(record) = chunk.next_selected(Some(selection))
                        {
                            match record {
                                Ok(record) => {
                                    decoded_bytes = decoded_bytes.saturating_add(
                                        record.message.as_ref().map_or(0, ParsedMessage::size),
                                    );
                                    req.state.messages.push_back(record);
                                }
                                Err(error) => {
                                    req.state.read_error =
                                        Some(Status::data_loss(error.to_string()))
                                }
                            }
                            break;
                        }
                        match reader.next() {
                            Some(Ok(chunk)) => req.state.chunk = Some(chunk),
                            Some(Err(error)) => {
                                req.state.read_error = Some(Status::data_loss(error.to_string()));
                                break;
                            }
                            None => break, // writer may still be flushing: retry next turn
                        }
                    }
                    if req.state.read_error.is_some()
                        || req.state.messages.is_empty()
                        || decoded_bytes >= req.client.messages_replay_len_max
                    {
                        break;
                    }
                }
            }
            req.metric_cpu_usage
                .increment(duration_to_seconds(ts.elapsed()));
            if req.state.messages.is_empty() || !read_more {
                // Defer only this request. Other runnable clients keep the worker.
                req.retry_at = Some(Instant::now() + Duration::from_millis(1));
            }
            prev_request = Some(req);
        }
        ReplayQueue::shutdown(&storage.replay_queue);
        Ok(())
    }

    pub fn push_message(
        &self,
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
        commitment: CommitmentLevel,
    ) {
        let _ = self.write_tx.send(WriterCommand::PushMessage {
            commitment,
            init,
            slot,
            head,
            index,
            message,
        });
    }

    pub fn restore_payload(&self, index: u64, message: ParsedMessage) {
        let _ = self
            .write_tx
            .send(WriterCommand::RestorePayload { index, message });
    }

    pub fn trim_messages(&self, slot: Slot, until: Option<u64>) {
        let _ = self
            .write_tx
            .send(WriterCommand::RemoveReplay { slot, until });
    }

    pub fn next_index(&self) -> u64 {
        let catalog = self.metadata.catalog();
        catalog
            .chunks
            .last()
            .map_or(0, |chunk| chunk.last_index + 1)
            .max(catalog.replay_floor)
    }

    pub fn begin_live_epoch(&self, index: u64) -> anyhow::Result<()> {
        self.metadata.set_replay_floor(index)
    }

    pub const fn commitment_bit(commitment: CommitmentLevel) -> u8 {
        match commitment {
            CommitmentLevel::Processed => 1,
            CommitmentLevel::Confirmed => 2,
            CommitmentLevel::Finalized => 4,
        }
    }

    pub const fn replay_enabled(&self, commitment: CommitmentLevel) -> bool {
        self.commitment_mask & Self::commitment_bit(commitment) != 0
    }

    pub fn supports_commitment_replay(&self, index: u64, commitment: CommitmentLevel) -> bool {
        self.metadata
            .catalog()
            .chunks
            .iter()
            .filter(|chunk| chunk.last_index >= index)
            .all(|chunk| {
                if chunk.compression & COMMITMENT_RECORDS == 0 {
                    commitment == CommitmentLevel::Processed
                } else {
                    (chunk.compression >> 2) & Self::commitment_bit(commitment) != 0
                }
            })
    }

    pub fn supports_block_replay(&self, index: u64) -> bool {
        self.metadata
            .catalog()
            .chunks
            .iter()
            .filter(|chunk| chunk.last_index >= index)
            .all(|chunk| chunk.compression & COMMITMENT_RECORDS != 0)
    }

    pub fn read_slots(&self) -> BTreeMap<Slot, SlotIndexValue> {
        let catalog = self.metadata.catalog();
        catalog
            .slots
            .iter()
            .filter(|(_, meta)| meta.first_index >= catalog.replay_floor)
            .map(|(slot, meta)| {
                (
                    *slot,
                    SlotIndexValue {
                        finalized: meta.finalized,
                        head: meta.first_index,
                    },
                )
            })
            .collect()
    }

    pub fn read_messages_from_index(
        &self,
        index: u64,
        parser: MessageParserEncoding,
    ) -> SegmentReader {
        SegmentReader::new(&self.metadata, index, parser)
    }

    pub fn replay(
        &self,
        client: SubscribeClient,
        messages: Arc<SharedChannel>,
        metric_cpu_usage: Gauge,
        generation: u64,
    ) -> Result<(), &'static str> {
        ReplayQueue::push_new(
            &self.replay_queue,
            ReplayRequest {
                retry_at: None,
                generation,
                state: ReplayState::default(),
                client,
                messages,
                metric_cpu_usage,
            },
        )
        .map_err(|()| "replay queue is full; try again later")
    }

    pub fn disk_size_poll_config(&self) -> (PathBuf, PathBuf, Duration) {
        (
            self.metadata.db_path().to_path_buf(),
            self.metadata.segments_path().to_path_buf(),
            self.metric_disk_size_poll_interval,
        )
    }
}

pub async fn poll_disk_size(
    metadata_path: PathBuf,
    segments_path: PathBuf,
    interval: Duration,
    shutdown: CancellationToken,
) {
    let mut ticker = tokio::time::interval(interval);
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let (meta_size, segs_size) = tokio::try_join!(
                    dir_size(metadata_path.clone()),
                    dir_size(segments_path.clone()),
                )
                .unwrap_or((0, 0));
                metrics::gauge!(crate::metrics::STORAGE_DISK_SIZE_BYTES)
                    .set((meta_size + segs_size) as f64);
            }
            () = shutdown.cancelled() => break,
        }
    }
}

async fn dir_size(path: PathBuf) -> std::io::Result<u64> {
    let entries = spawn_blocking({
        let path = path.clone();
        move || -> std::io::Result<Vec<_>> {
            std::fs::read_dir(&path)?
                .map(|entry| {
                    let entry = entry?;
                    let meta = entry.metadata()?;
                    Ok((entry.path(), meta))
                })
                .collect()
        }
    })
    .await
    .expect("read_dir panicked")?;

    let mut futs = Vec::with_capacity(entries.len());
    let mut total: u64 = 0;
    for (entry_path, meta) in entries {
        if meta.is_dir() {
            futs.push(dir_size(entry_path));
        } else {
            total += meta.len();
        }
    }

    for size in try_join_all(futs).await? {
        total += size;
    }

    Ok(total)
}

#[derive(Debug)]
struct ReplayRequest {
    retry_at: Option<Instant>,
    generation: u64,
    state: ReplayState,
    client: SubscribeClient,
    messages: Arc<SharedChannel>,
    metric_cpu_usage: Gauge,
}

#[derive(Debug, Default)]
struct ReplayState {
    reader: Option<SegmentReader>,
    chunk: Option<DecompressedChunk>,
    messages: VecDeque<ScannedRecord>,
    read_error: Option<Status>,
}

#[derive(Debug)]
struct ReplayQueue {
    capacity: usize,
    len: usize,
    requests: VecDeque<ReplayRequest>,
}

impl ReplayQueue {
    const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            len: 0,
            requests: VecDeque::new(),
        }
    }

    fn pop_next(queue: &Mutex<Self>, prev_request: Option<ReplayRequest>) -> Option<ReplayRequest> {
        let mut locked = mutex_lock(queue);
        if locked.len > 0
            && let Some(request) = prev_request
        {
            locked.requests.push_back(request);
        }
        let now = Instant::now();
        for _ in 0..locked.requests.len() {
            let mut request = locked.requests.pop_front().unwrap();
            if request.retry_at.is_none_or(|retry_at| retry_at <= now) {
                request.retry_at = None;
                return Some(request);
            }
            locked.requests.push_back(request);
        }
        None
    }

    fn push_new(queue: &Mutex<Self>, request: ReplayRequest) -> Result<(), ()> {
        let mut locked = mutex_lock(queue);
        if locked.len < locked.capacity {
            locked.len += 1;
            locked.requests.push_back(request);
            Ok(())
        } else {
            Err(())
        }
    }

    fn drop_req(queue: &Mutex<Self>) {
        let mut locked = mutex_lock(queue);
        locked.len = locked.len.saturating_sub(1);
    }

    fn shutdown(queue: &Mutex<Self>) {
        let mut locked = mutex_lock(queue);
        locked.capacity = 0;
        locked.len = 0;
        locked.requests.clear();
    }
}

#[cfg(test)]
mod replay_queue_tests {
    use super::*;

    fn request(generation: u64, retry_at: Option<Instant>) -> ReplayRequest {
        ReplayRequest {
            retry_at,
            generation,
            state: ReplayState::default(),
            client: SubscribeClient::new(generation, 256, 256, Arc::from("queue-test")),
            messages: Arc::new(SharedChannel::new(8, false)),
            metric_cpu_usage: Gauge::noop(),
        }
    }

    #[test]
    fn deferred_clients_never_prevent_a_ready_client_from_running() {
        let queue = Mutex::new(ReplayQueue::new(4));
        let later = Some(Instant::now() + Duration::from_secs(60));
        for (generation, retry) in [(1, later), (2, None), (3, later), (4, None)] {
            assert!(ReplayQueue::push_new(&queue, request(generation, retry)).is_ok());
        }
        let first = ReplayQueue::pop_next(&queue, None).unwrap();
        assert_eq!(first.generation, 2);
        let second = ReplayQueue::pop_next(&queue, Some(first)).unwrap();
        assert_eq!(second.generation, 4);
        let third = ReplayQueue::pop_next(&queue, Some(second)).unwrap();
        assert_eq!(third.generation, 2);
    }
}
