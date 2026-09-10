use {
    crate::{
        channel::ParsedMessage,
        config::ConfigStorage,
        metrics::{
            CHANNEL_STORAGE_WRITE_COLLECTOR_INDEX, CHANNEL_STORAGE_WRITE_COMPRESSOR_INDEX,
            CHANNEL_STORAGE_WRITE_INDEX, STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL,
            STORAGE_WRITE_APPEND_SECONDS_TOTAL, STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL,
            STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL, STORAGE_WRITE_COMMIT_SECONDS_TOTAL,
            STORAGE_WRITE_COMPRESS_SECONDS_TOTAL, STORAGE_WRITE_ROTATE_SECONDS_TOTAL,
            STORAGE_WRITE_SERIALIZE_SECONDS_TOTAL, STORAGE_WRITE_TRIM_SECONDS_TOTAL,
        },
        storage::metadata::{
            ChunkMeta, Metadata, MetadataChunkCommit, MetadataMirror, MetadataTrimCommit,
            RotationCommit, SegmentMeta, SlotMeta,
        },
        util::{SpawnedThread, SpawnedThreads},
    },
    ::metrics::{counter, gauge},
    anyhow::{Context, anyhow},
    prost::encoding::encode_varint,
    richat_filter::{
        filter::{FilteredUpdate, FilteredUpdateFilters},
        message::MessageRef,
    },
    richat_metrics::duration_to_seconds,
    richat_proto::geyser::SlotStatus,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    std::{
        any::Any,
        collections::{BTreeMap, HashMap, HashSet},
        fs::{File, OpenOptions},
        io::{Seek, SeekFrom, Write},
        path::PathBuf,
        sync::{Arc, Weak},
        thread,
        time::Instant,
    },
    zstd::bulk::Compressor,
};

/// Compression algorithm used for a chunk payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkCompression {
    None,
    Zstd(i32),
}

impl ChunkCompression {
    const TAG_NONE: u8 = 0;
    const TAG_ZSTD: u8 = 1;

    const fn tag(self) -> u8 {
        match self {
            Self::None => Self::TAG_NONE,
            Self::Zstd(_) => Self::TAG_ZSTD,
        }
    }

    pub(super) fn from_tag(tag: u8) -> anyhow::Result<Self> {
        match tag {
            Self::TAG_NONE => Ok(Self::None),
            Self::TAG_ZSTD => Ok(Self::Zstd(0)),
            other => anyhow::bail!("unsupported chunk compression tag: {other}"),
        }
    }
}

// High bit distinguishes commitment-aware records from the original processed-only format.
pub const COMMITMENT_RECORDS: u8 = 0x80;

// Reference-capable chunks keep one payload and append publication references.
pub const REFERENCE_RECORDS: u8 = 0x40;

pub use super::reader::{DecompressedChunk, ReplayRecord, SegmentReader};

#[derive(Debug)]
pub enum WriterCommand {
    RestorePayload {
        index: u64,
        message: ParsedMessage,
    },
    PushMessage {
        commitment: CommitmentLevel,
        init: bool,
        slot: Slot,
        head: u64,
        index: u64,
        message: ParsedMessage,
    },
    RemoveReplay {
        slot: Slot,
        until: Option<u64>,
    },
}

enum CollectorOutput {
    RawChunk {
        seq: u64,
        first_index: u64,
        last_index: u64,
        records: Vec<RawRecord>,
        pending_slots: Vec<PendingSlot>,
        pending_finalized: HashSet<Slot>,
    },
    Trim {
        seq: u64,
        slot: Slot,
        until: Option<u64>,
    },
}

enum RawPayload {
    Message(ParsedMessage),
    Reference(u64),
}

struct RawRecord {
    commitment: CommitmentLevel,
    slot: Slot,
    block: bool,
    finalized: bool,
    payload: RawPayload,
}

impl RawRecord {
    fn estimated_size(&self) -> usize {
        match &self.payload {
            RawPayload::Message(message) => message.size().saturating_add(16),
            RawPayload::Reference(_) => 32,
        }
    }
}

type PayloadIdentity = (u8, usize);
type PayloadAllocation = Weak<dyn Any + Send + Sync>;

fn payload_identity(message: &ParsedMessage) -> (PayloadIdentity, PayloadAllocation) {
    fn identity<T: Any + Send + Sync>(
        tag: u8,
        message: &Arc<T>,
    ) -> (PayloadIdentity, PayloadAllocation) {
        let allocation: Weak<T> = Arc::downgrade(message);
        ((tag, Arc::as_ptr(message) as usize), allocation)
    }
    match message {
        ParsedMessage::Slot(message) => identity(0, message),
        ParsedMessage::Account(message) => identity(1, message),
        ParsedMessage::Transaction(message) => identity(2, message),
        ParsedMessage::Entry(message) => identity(3, message),
        ParsedMessage::BlockMeta(message) => identity(4, message),
        ParsedMessage::Block(message) => identity(5, message),
    }
}

/// Weak allocations prevent pointer reuse without retaining account/block payloads.
/// Only unrooted slots need an identity map; trimming or rooting frees it.
#[derive(Default)]
struct PayloadReferences {
    slots: BTreeMap<
        Slot,
        HashMap<PayloadIdentity, (u64, PayloadAllocation), foldhash::fast::RandomState>,
    >,
}

impl PayloadReferences {
    fn record(
        &mut self,
        slot: Slot,
        index: u64,
        commitment: CommitmentLevel,
        message: ParsedMessage,
    ) -> RawRecord {
        let (identity, allocation) = payload_identity(&message);
        let block = matches!(&message, ParsedMessage::Block(_));
        let finalized = commitment == CommitmentLevel::Processed
            && matches!(&message, ParsedMessage::Slot(message) if message.status() == SlotStatus::SlotFinalized);
        let payload = match self.slots.entry(slot).or_default().entry(identity) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                RawPayload::Reference(entry.get().0)
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert((index, allocation));
                RawPayload::Message(message)
            }
        };
        if finalized {
            while self
                .slots
                .first_key_value()
                .is_some_and(|(old, _)| *old <= slot)
            {
                self.slots.pop_first();
            }
        }
        RawRecord {
            commitment,
            slot,
            block,
            finalized,
            payload,
        }
    }
}

#[derive(Debug)]
struct PendingSlot {
    slot: Slot,
    first_index: u64,
}

#[derive(Debug, Default)]
struct PendingChunkMeta {
    estimated_uncompressed_size: usize,
    record_count: u32,
    first_index: u64,
    last_index: u64,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

impl PendingChunkMeta {
    fn push(&mut self, init: bool, slot: Slot, head: u64, index: u64, record: &RawRecord) {
        if self.record_count == 0 {
            self.first_index = index;
        }
        self.last_index = index;
        self.estimated_uncompressed_size = self
            .estimated_uncompressed_size
            .saturating_add(record.estimated_size());

        if init {
            self.pending_slots.push(PendingSlot {
                slot,
                first_index: head,
            });
        }
        if record.finalized {
            self.pending_finalized.insert(slot);
        }

        self.record_count += 1;
    }

    const fn should_flush(&self, target_size: usize) -> bool {
        self.estimated_uncompressed_size >= target_size
    }

    const fn is_empty(&self) -> bool {
        self.record_count == 0
    }
}

fn spawn_collector(
    chunk_target_size: usize,
    serialize_affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<WriterCommand>,
    tx: kanal::Sender<CollectorOutput>,
) -> anyhow::Result<SpawnedThread> {
    let th_name = "richatStrgCol".to_owned();
    let jh = thread::Builder::new()
        .name(th_name.clone())
        .spawn(move || {
            if let Some(cpus) = serialize_affinity {
                affinity_linux::set_thread_affinity(cpus.into_iter())
                    .expect("failed to set affinity");
            }
            run_collector(chunk_target_size, rx, tx)
        })?;
    Ok((th_name, Some(jh)))
}

fn run_collector(
    chunk_target_size: usize,
    rx: kanal::Receiver<WriterCommand>,
    tx: kanal::Sender<CollectorOutput>,
) -> anyhow::Result<()> {
    let mut records: Vec<RawRecord> = Vec::new();
    let mut pending = PendingChunkMeta::default();
    let mut next_seq: u64 = 0;
    let mut references = PayloadReferences::default();

    let mut closed = false;
    while !closed {
        let mut should_flush = false;
        let mut trim = None;

        closed = match rx.recv_timeout(std::time::Duration::from_millis(100)) {
            Ok(command) => {
                match command {
                    WriterCommand::RestorePayload { index, message } => {
                        let (identity, allocation) = payload_identity(&message);
                        references
                            .slots
                            .entry(message.slot())
                            .or_default()
                            .insert(identity, (index, allocation));
                    }
                    WriterCommand::PushMessage {
                        commitment,
                        init,
                        slot,
                        head,
                        index,
                        message,
                    } => {
                        let record = references.record(slot, index, commitment, message);
                        pending.push(init, slot, head, index, &record);
                        records.push(record);
                        counter!(CHANNEL_STORAGE_WRITE_COLLECTOR_INDEX).absolute(index);

                        // Processed is published last for each source event. Keep
                        // its commitment fan-out in one durable chunk so a crash
                        // cannot recover half of a promotion and then duplicate it.
                        should_flush = commitment == CommitmentLevel::Processed
                            && pending.should_flush(chunk_target_size);
                    }
                    WriterCommand::RemoveReplay { slot, until } => {
                        references.slots.remove(&slot);
                        should_flush = true;
                        trim = Some((slot, until));
                    }
                }
                false
            }
            Err(kanal::ReceiveErrorTimeout::Timeout) => {
                should_flush = records
                    .last()
                    .is_some_and(|record| record.commitment == CommitmentLevel::Processed);
                false
            }
            Err(_) => true,
        };

        if (should_flush || closed) && !pending.is_empty() {
            anyhow::ensure!(
                records
                    .last()
                    .is_some_and(|record| record.commitment == CommitmentLevel::Processed),
                "incomplete commitment publication in storage collector"
            );
            let flushed = std::mem::take(&mut pending);

            tx.send(CollectorOutput::RawChunk {
                seq: next_seq,
                first_index: flushed.first_index,
                last_index: flushed.last_index,
                records: std::mem::take(&mut records),
                pending_slots: flushed.pending_slots,
                pending_finalized: flushed.pending_finalized,
            })
            .map_err(|_| anyhow!("collector output channel closed"))?;
            next_seq += 1;
        }

        if let Some((slot, until)) = trim {
            tx.send(CollectorOutput::Trim {
                seq: next_seq,
                slot,
                until,
            })
            .map_err(|_| anyhow!("collector output channel closed"))?;
            next_seq += 1;
        }
    }

    Ok(())
}

enum CompressorOutput {
    CompressedChunk {
        seq: u64,
        chunk: CompressedChunk,
    },
    Trim {
        seq: u64,
        slot: Slot,
        until: Option<u64>,
    },
}

impl CompressorOutput {
    const fn seq(&self) -> u64 {
        match self {
            Self::CompressedChunk { seq, .. } => *seq,
            Self::Trim { seq, .. } => *seq,
        }
    }
}

#[derive(Debug)]
struct CompressedChunk {
    compression: u8,
    first_index: u64,
    last_index: u64,
    payload: Vec<u8>,
    pending_slots: Vec<PendingSlot>,
    pending_finalized: HashSet<Slot>,
}

fn spawn_compressor_pool(
    commitment_mask: u8,
    threads: usize,
    chunk_compression: Option<ChunkCompression>,
    affinity: Option<Vec<usize>>,
    rx: kanal::Receiver<CollectorOutput>,
    tx: kanal::Sender<CompressorOutput>,
) -> anyhow::Result<Vec<SpawnedThread>> {
    let mut handles = Vec::with_capacity(threads);
    for index in 0..threads {
        let th_name = format!("richatStrgCmp{index:02}");
        let rx = rx.clone();
        let tx = tx.clone();
        let cpus = affinity.as_ref().map(|aff| {
            if threads == aff.len() {
                vec![aff[index]]
            } else {
                aff.clone()
            }
        });
        let jh = thread::Builder::new()
            .name(th_name.clone())
            .spawn(move || {
                if let Some(cpus) = cpus {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity");
                }
                run_compressor(index, commitment_mask, chunk_compression, rx, tx)
            })?;
        handles.push((th_name, Some(jh)));
    }
    Ok(handles)
}

fn run_compressor(
    thread_index: usize,
    commitment_mask: u8,
    chunk_compression: Option<ChunkCompression>,
    rx: kanal::Receiver<CollectorOutput>,
    tx: kanal::Sender<CompressorOutput>,
) -> anyhow::Result<()> {
    let mut compressor = match chunk_compression {
        Some(ChunkCompression::Zstd(level)) => {
            Some(Compressor::new(level).context("failed to create zstd compressor")?)
        }
        _ => None,
    };
    let compression_tag = chunk_compression.unwrap_or(ChunkCompression::None).tag()
        | COMMITMENT_RECORDS
        | REFERENCE_RECORDS
        | ((commitment_mask | 1) << 2);
    let thread_index_str: &'static str = thread_index.to_string().leak();

    // Per-thread serialization buffers
    let mut record_buf = Vec::with_capacity(11 * 1024 * 1024);
    let mut chunk_buf = Vec::new();

    loop {
        match rx.recv() {
            Ok(CollectorOutput::RawChunk {
                seq,
                first_index,
                last_index,
                records,
                pending_slots,
                pending_finalized,
            }) => {
                // Serialize all records
                let serialize_started_at = Instant::now();
                chunk_buf.clear();
                for record in &records {
                    record_buf.clear();
                    let tag = match record.commitment {
                        CommitmentLevel::Processed => 0,
                        CommitmentLevel::Confirmed => 1,
                        CommitmentLevel::Finalized => 2,
                    } | if record.block { 0x80 } else { 0 }
                        | if matches!(record.payload, RawPayload::Reference(_)) {
                            0x40
                        } else {
                            0
                        };
                    record_buf.push(tag);
                    encode_varint(record.slot, &mut record_buf);
                    match &record.payload {
                        RawPayload::Message(message) => {
                            let message_ref: MessageRef = message.into();
                            FilteredUpdate {
                                filters: FilteredUpdateFilters::new(),
                                filtered_update: message_ref.into(),
                            }
                            .encode(&mut record_buf);
                        }
                        RawPayload::Reference(index) => encode_varint(*index, &mut record_buf),
                    }
                    encode_varint(record_buf.len() as u64, &mut chunk_buf);
                    chunk_buf.extend_from_slice(&record_buf);
                }
                gauge!(STORAGE_WRITE_SERIALIZE_SECONDS_TOTAL)
                    .increment(duration_to_seconds(serialize_started_at.elapsed()));
                counter!(CHANNEL_STORAGE_WRITE_COMPRESSOR_INDEX, "thread" => thread_index_str)
                    .absolute(last_index);

                counter!(STORAGE_WRITE_CHUNK_UNCOMPRESSED_BYTES_TOTAL)
                    .increment(chunk_buf.len() as u64);

                // Compress
                let payload = if let Some(compressor) = compressor.as_mut() {
                    let compress_started_at = Instant::now();
                    let compressed = compressor
                        .compress(&chunk_buf)
                        .context("failed to compress chunk")?;
                    gauge!(STORAGE_WRITE_COMPRESS_SECONDS_TOTAL)
                        .increment(duration_to_seconds(compress_started_at.elapsed()));
                    compressed
                } else {
                    std::mem::take(&mut chunk_buf)
                };
                counter!(STORAGE_WRITE_CHUNK_COMPRESSED_BYTES_TOTAL)
                    .increment(payload.len() as u64);

                let compressed = CompressedChunk {
                    compression: compression_tag,
                    first_index,
                    last_index,
                    payload,
                    pending_slots,
                    pending_finalized,
                };

                if tx
                    .send(CompressorOutput::CompressedChunk {
                        seq,
                        chunk: compressed,
                    })
                    .is_err()
                {
                    break;
                }
            }
            Ok(CollectorOutput::Trim { seq, slot, until }) => {
                if tx
                    .send(CompressorOutput::Trim { seq, slot, until })
                    .is_err()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    Ok(())
}

/// Long-lived state for the ordered storage append path.
struct SegmentWriter {
    segments_path: PathBuf,
    segment_target_size: u64,
    metadata: Metadata,
    active_segment: SegmentMeta,
    active_file: File,
}

impl SegmentWriter {
    fn open(
        segments_path: PathBuf,
        segment_target_size: u64,
        metadata: Metadata,
    ) -> anyhow::Result<Self> {
        {
            let catalog = metadata.catalog();
            if catalog.state.active_segment_id == 0 {
                let mut state = catalog.state;
                state.active_segment_id = state.next_segment_id;
                state.next_segment_id += 1;
                let segment = SegmentMeta::empty(state.active_segment_id, 0);
                drop(catalog);
                let path = segments_path.join(segment_file_name(segment.segment_id));
                OpenOptions::new()
                    .create_new(true)
                    .read(true)
                    .write(true)
                    .open(&path)
                    .with_context(|| format!("failed to create segment file: {path:?}"))?;
                metadata.initialize_empty(&state, segment)?;
            }
        }

        let active_segment = {
            let catalog = metadata.catalog();
            catalog
                .segments
                .get(&catalog.state.active_segment_id)
                .copied()
                .context("missing active segment metadata")?
        };
        let active_path = segments_path.join(segment_file_name(active_segment.segment_id));
        let mut active_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&active_path)
            .with_context(|| format!("failed to open active segment: {active_path:?}"))?;
        active_file.seek(SeekFrom::Start(active_segment.file_len))?;

        Ok(Self {
            segments_path,
            segment_target_size,
            metadata,
            active_segment,
            active_file,
        })
    }

    fn run(mut self, rx: kanal::Receiver<CompressorOutput>) -> anyhow::Result<()> {
        let mut next_seq: u64 = 0;
        let mut pending: BTreeMap<u64, CompressorOutput> = BTreeMap::new();

        while let Ok(output) = rx.recv() {
            pending.insert(output.seq(), output);
            while let Some(item) = pending.remove(&next_seq) {
                match item {
                    CompressorOutput::CompressedChunk { chunk, .. } => {
                        self.append_chunk(chunk)?;
                    }
                    CompressorOutput::Trim { slot, until, .. } => {
                        self.handle_trim(slot, until)?;
                    }
                }
                next_seq += 1;
            }
        }

        Ok(())
    }

    fn append_chunk(&mut self, chunk: CompressedChunk) -> anyhow::Result<()> {
        let chunk_meta = ChunkMeta {
            segment_id: self.active_segment.segment_id,
            offset: self.active_segment.file_len,
            size: chunk.payload.len() as u64,
            compression: chunk.compression,
            first_index: chunk.first_index,
            last_index: chunk.last_index,
        };

        let append_started_at = Instant::now();
        self.active_file
            .seek(SeekFrom::Start(self.active_segment.file_len))?;
        self.active_file.write_all(&chunk.payload)?;
        self.active_file.sync_data()?;
        gauge!(STORAGE_WRITE_APPEND_SECONDS_TOTAL)
            .increment(duration_to_seconds(append_started_at.elapsed()));

        let commit_started_at = Instant::now();
        let commit = {
            let catalog = self.metadata.catalog();
            Self::build_chunk_commit(
                &catalog,
                self.active_segment,
                chunk_meta,
                &chunk.pending_slots,
                &chunk.pending_finalized,
            )?
        };
        self.metadata.apply_chunk_commit(&commit)?;
        gauge!(STORAGE_WRITE_COMMIT_SECONDS_TOTAL)
            .increment(duration_to_seconds(commit_started_at.elapsed()));

        counter!(CHANNEL_STORAGE_WRITE_INDEX).absolute(chunk_meta.last_index);
        counter!(STORAGE_SEGMENT_CHUNKS_WRITTEN_TOTAL).increment(1);

        self.active_segment = commit.segment;
        if self.active_segment.file_len >= self.segment_target_size {
            self.rotate_segment()?;
        }
        Ok(())
    }

    fn rotate_segment(&mut self) -> anyhow::Result<()> {
        let rotate_started_at = Instant::now();
        let (new_segment, state) = {
            let catalog = self.metadata.catalog();
            let mut state = catalog.state;
            let new_segment_id = state.next_segment_id;
            state.next_segment_id += 1;
            state.active_segment_id = new_segment_id;
            (SegmentMeta::empty(new_segment_id, 0), state)
        };

        let new_path = self
            .segments_path
            .join(segment_file_name(new_segment.segment_id));
        let new_file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&new_path)
            .with_context(|| format!("failed to create next segment: {new_path:?}"))?;

        let sealed_segment = SegmentMeta {
            sealed: true,
            ..self.active_segment
        };
        let commit = RotationCommit {
            sealed_segment,
            new_segment,
            state,
        };
        self.metadata.apply_rotation_commit(commit)?;

        gauge!(STORAGE_WRITE_ROTATE_SECONDS_TOTAL)
            .increment(duration_to_seconds(rotate_started_at.elapsed()));
        self.active_segment = new_segment;
        self.active_file = new_file;
        Ok(())
    }

    fn handle_trim(&mut self, slot: Slot, until: Option<u64>) -> anyhow::Result<()> {
        let trim_started_at = Instant::now();
        let commit = {
            let catalog = self.metadata.catalog();
            Self::build_trim_commit(&catalog, slot, until)
        };
        self.metadata.apply_trim_commit(&commit)?;

        for segment_id in &commit.deleted_segments {
            let path = self.segments_path.join(segment_file_name(*segment_id));
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error)
                        .with_context(|| format!("failed to delete segment {path:?}"));
                }
            }
        }
        gauge!(STORAGE_WRITE_TRIM_SECONDS_TOTAL)
            .increment(duration_to_seconds(trim_started_at.elapsed()));
        Ok(())
    }

    fn build_chunk_commit(
        catalog: &MetadataMirror,
        active_segment: SegmentMeta,
        chunk: ChunkMeta,
        pending_slots: &[PendingSlot],
        pending_finalized: &HashSet<Slot>,
    ) -> anyhow::Result<MetadataChunkCommit> {
        let mut segment = active_segment;
        segment.last_index = chunk.last_index;
        segment.file_len += chunk.size;
        segment.chunk_count += 1;

        let mut new_slots = Vec::with_capacity(pending_slots.len());
        for pending in pending_slots {
            new_slots.push(SlotMeta {
                slot: pending.slot,
                finalized: pending_finalized.contains(&pending.slot),
                first_index: pending.first_index,
                segment_id: chunk.segment_id,
            });
        }

        let mut updated_slots = Vec::with_capacity(pending_finalized.len());
        for slot in pending_finalized {
            if let Some(meta) = catalog.slots.get(slot)
                && !new_slots.iter().any(|new_meta| new_meta.slot == *slot)
            {
                let mut meta = *meta;
                meta.finalized = true;
                updated_slots.push(meta);
            }
        }

        Ok(MetadataChunkCommit {
            new_slots,
            updated_slots,
            chunk,
            segment,
            state: catalog.state,
        })
    }

    fn build_trim_commit(
        catalog: &MetadataMirror,
        slot: Slot,
        until: Option<u64>,
    ) -> MetadataTrimCommit {
        let deleted_segments = until
            .map(|floor| {
                catalog
                    .segments
                    .values()
                    .filter(|segment| {
                        segment.sealed && segment.chunk_count > 0 && segment.last_index < floor
                    })
                    .map(|segment| segment.segment_id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let deleted_slots = catalog
            .slots
            .values()
            .filter(|meta| deleted_segments.contains(&meta.segment_id))
            .map(|meta| meta.slot)
            .collect::<Vec<_>>();
        let deleted_chunks = catalog
            .chunks
            .iter()
            .filter(|chunk| deleted_segments.contains(&chunk.segment_id))
            .map(|chunk| chunk.first_index)
            .collect::<Vec<_>>();

        MetadataTrimCommit {
            removed_slot: slot,
            deleted_slots,
            deleted_segments,
            deleted_chunks,
        }
    }
}

fn spawn_writer(
    segments_path: PathBuf,
    segment_target_size: u64,
    affinity: Option<Vec<usize>>,
    metadata: Metadata,
    rx: kanal::Receiver<CompressorOutput>,
) -> anyhow::Result<SpawnedThread> {
    let th_name = "richatStrgWrt".to_owned();
    let jh = thread::Builder::new()
        .name(th_name.clone())
        .spawn(move || {
            if let Some(cpus) = affinity {
                affinity_linux::set_thread_affinity(cpus.into_iter())
                    .expect("failed to set affinity");
            }
            SegmentWriter::open(segments_path, segment_target_size, metadata)?.run(rx)
        })?;
    Ok((th_name, Some(jh)))
}

/// Creates channels and spawns collector, compressor, and writer threads.
pub fn spawn_write_pipeline(
    config: &ConfigStorage,
    metadata: Metadata,
) -> anyhow::Result<(kanal::Sender<WriterCommand>, SpawnedThreads)> {
    let (write_tx, write_rx) = kanal::bounded(config.collector_channel_size);
    let (collector_tx, collector_rx) = kanal::bounded(config.compressor_channel_size);
    let (compressor_tx, compressor_rx) = kanal::bounded(config.writer_channel_size);

    let mut threads = vec![];
    threads.push(spawn_collector(
        config.chunk_target_size,
        config.serialize_affinity.clone(),
        write_rx,
        collector_tx,
    )?);
    threads.extend(spawn_compressor_pool(
        config.commitment_mask(),
        config.compressor_threads,
        config.chunk_compression,
        config.compressor_affinity.clone(),
        collector_rx,
        compressor_tx,
    )?);
    threads.push(spawn_writer(
        config.segments_path(),
        config.segment_target_size as u64,
        config.write_affinity.clone(),
        metadata,
        compressor_rx,
    )?);

    Ok((write_tx, threads))
}

fn segment_file_name(segment_id: u64) -> String {
    format!("{segment_id:012}.seg")
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        prost::Message as _,
        richat_filter::message::{Message, MessageParserEncoding},
        richat_proto::geyser::{
            SubscribeUpdate, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        },
        std::borrow::Cow,
    };

    fn slot_bytes(slot: Slot) -> Vec<u8> {
        SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot,
                parent: Some(slot - 1),
                status: SlotStatus::SlotProcessed as i32,
                dead_error: None,
            })),
            created_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
        }
        .encode_to_vec()
    }

    #[test]
    fn legacy_processed_records_remain_readable_and_start_is_inclusive() {
        let mut data = vec![];
        for slot in 10..13 {
            let mut record = vec![];
            encode_varint(slot, &mut record);
            record.extend(slot_bytes(slot));
            encode_varint(record.len() as u64, &mut data);
            data.extend(record);
        }
        for parser in [MessageParserEncoding::Prost, MessageParserEncoding::Limited] {
            let chunk = DecompressedChunk::fixture(42, 0, data.clone(), 3, 1, parser).unwrap();
            let records = chunk.collect::<anyhow::Result<Vec<_>>>().unwrap();
            assert_eq!(
                records
                    .iter()
                    .map(|record| record.index)
                    .collect::<Vec<_>>(),
                [43, 44]
            );
            assert_eq!(
                records
                    .iter()
                    .map(|record| record.message.slot())
                    .collect::<Vec<_>>(),
                [11, 12]
            );
            assert!(
                records
                    .iter()
                    .all(|record| record.commitment == CommitmentLevel::Processed)
            );
        }
    }

    #[test]
    fn collector_never_splits_a_commitment_promotion_across_chunks() {
        let (tx, rx) = kanal::unbounded();
        let (output_tx, output_rx) = kanal::unbounded();
        for (index, commitment) in [
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
            CommitmentLevel::Processed,
        ]
        .into_iter()
        .enumerate()
        {
            tx.send(WriterCommand::PushMessage {
                init: index == 0,
                slot: 10,
                head: 0,
                index: index as u64,
                commitment,
                message: Message::parse(Cow::Owned(slot_bytes(10)), MessageParserEncoding::Prost)
                    .unwrap()
                    .into(),
            })
            .unwrap();
        }
        drop(tx);
        run_collector(1, rx, output_tx).unwrap();
        let CollectorOutput::RawChunk {
            records,
            first_index,
            last_index,
            ..
        } = output_rx.recv().unwrap()
        else {
            panic!("expected a data chunk")
        };
        assert_eq!(records.len(), 3);
        assert_eq!((first_index, last_index), (0, 2));
        assert!(output_rx.recv().is_err());
    }
}
