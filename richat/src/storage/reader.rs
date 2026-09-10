//! Indexed, bounded replay reads. Commitment headers are inspected before payload
//! resolution or protobuf decoding; publication references share one payload.
use {
    super::{
        metadata::{ChunkMeta, Metadata},
        segments::{COMMITMENT_RECORDS, ChunkCompression, REFERENCE_RECORDS},
    },
    crate::{
        channel::ParsedMessage,
        metrics::{STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL, STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL},
    },
    anyhow::{Context, anyhow},
    metrics::counter,
    prost::encoding::decode_varint,
    richat_filter::message::{Message, MessageParserEncoding},
    richat_shared::mutex_lock,
    solana_clock::Slot,
    solana_commitment_config::CommitmentLevel,
    std::{
        borrow::Cow,
        collections::VecDeque,
        fs::File,
        io::{Read, Seek, SeekFrom},
        ops::Range,
        sync::{Arc, Mutex},
    },
};

#[derive(Debug)]
pub struct ReplayRecord {
    pub index: u64,
    pub payload_index: u64,
    pub commitment: CommitmentLevel,
    pub message: ParsedMessage,
}

#[derive(Debug)]
pub struct ScannedRecord {
    pub index: u64,
    pub payload_index: Option<u64>,
    pub commitment: CommitmentLevel,
    /// None advances the journal cursor without reading/decoding an excluded payload.
    pub message: Option<ParsedMessage>,
}

#[derive(Clone, Copy, Debug)]
pub struct ReplaySelection {
    pub commitment: CommitmentLevel,
    pub from_slot: Option<Slot>,
}

#[derive(Debug)]
struct ChunkData {
    first_index: u64,
    format: u8,
    bytes: Vec<u8>,
    records: Vec<Range<usize>>,
}

impl ChunkData {
    fn new(first_index: u64, format: u8, bytes: Vec<u8>, count: usize) -> anyhow::Result<Self> {
        let mut records = Vec::with_capacity(count.min(bytes.len()));
        let mut offset = 0;
        while offset < bytes.len() {
            let mut remaining = &bytes[offset..];
            let len = usize::try_from(decode_varint(&mut remaining)?)
                .context("record length overflow")?;
            let start = bytes.len() - remaining.len();
            offset = start.checked_add(len).context("record boundary overflow")?;
            anyhow::ensure!(offset <= bytes.len(), "record extends past chunk boundary");
            records.push(start..offset);
        }
        anyhow::ensure!(
            records.len() == count,
            "chunk record count does not match metadata"
        );
        Ok(Self {
            first_index,
            format,
            bytes,
            records,
        })
    }

    fn record(&self, index: u64) -> anyhow::Result<&[u8]> {
        let index = usize::try_from(
            index
                .checked_sub(self.first_index)
                .context("record before chunk")?,
        )?;
        self.records
            .get(index)
            .map(|range| &self.bytes[range.clone()])
            .context("record outside chunk")
    }

    const fn contains(&self, index: u64) -> bool {
        index >= self.first_index && index - self.first_index < self.records.len() as u64
    }

    const fn size(&self) -> usize {
        self.bytes
            .len()
            .saturating_add(self.records.capacity() * std::mem::size_of::<Range<usize>>())
    }
}

struct RecordHeader<'a> {
    commitment: CommitmentLevel,
    slot: Slot,
    block: bool,
    reference: bool,
    payload: &'a [u8],
}

impl<'a> RecordHeader<'a> {
    fn parse(mut record: &'a [u8], format: u8) -> anyhow::Result<Self> {
        let mut commitment = CommitmentLevel::Processed;
        let mut block = false;
        let mut reference = false;
        if format & COMMITMENT_RECORDS != 0 {
            let (&tag, rest) = record.split_first().context("missing commitment header")?;
            record = rest;
            reference = format & REFERENCE_RECORDS != 0 && tag & 0x40 != 0;
            block = tag & 0x80 != 0;
            let level = if format & REFERENCE_RECORDS != 0 {
                tag & 0x3f
            } else {
                tag & 0x7f
            };
            commitment = match level {
                0 => CommitmentLevel::Processed,
                1 => CommitmentLevel::Confirmed,
                2 => CommitmentLevel::Finalized,
                _ => return Err(anyhow!("invalid record commitment: {tag}")),
            };
        }
        let slot = decode_varint(&mut record).context("invalid record slot")?;
        Ok(Self {
            commitment,
            slot,
            block,
            reference,
            payload: record,
        })
    }

    fn decode(&self, parser: MessageParserEncoding) -> anyhow::Result<ParsedMessage> {
        let message = Message::parse(
            Cow::Borrowed(self.payload),
            if self.block {
                MessageParserEncoding::Prost
            } else {
                parser
            },
        )
        .context("failed to parse replay message")?;
        anyhow::ensure!(
            message.slot() == self.slot,
            "payload slot does not match record header"
        );
        Ok(message.into())
    }
}

/// Two recently used chunks avoid decompressing the same payload chunk for each
/// reference. Cache residency is capped at 16 MiB or one oversized chunk.
#[derive(Debug)]
struct ChunkLoader {
    metadata: Metadata,
    file: Option<(u64, File)>,
    cache: VecDeque<Arc<ChunkData>>,
    cache_bytes: usize,
}

impl ChunkLoader {
    const CACHE_BYTES: usize = 16 * 1024 * 1024;

    fn load(&mut self, meta: ChunkMeta) -> anyhow::Result<Arc<ChunkData>> {
        if let Some(pos) = self
            .cache
            .iter()
            .position(|chunk| chunk.first_index == meta.first_index)
        {
            let chunk = self.cache.remove(pos).unwrap();
            self.cache.push_back(Arc::clone(&chunk));
            return Ok(chunk);
        }
        if self
            .file
            .as_ref()
            .is_none_or(|(id, _)| *id != meta.segment_id)
        {
            let path = self
                .metadata
                .segments_path()
                .join(format!("{:012}.seg", meta.segment_id));
            self.file = Some((
                meta.segment_id,
                File::open(&path)
                    .with_context(|| format!("failed to open replay segment {path:?}"))?,
            ));
        }
        let file = &mut self.file.as_mut().unwrap().1;
        file.seek(SeekFrom::Start(meta.offset))?;
        let size = usize::try_from(meta.size).context("chunk size overflow")?;
        let mut payload = vec![0; size];
        file.read_exact(&mut payload)?;
        counter!(STORAGE_REPLAY_COMPRESSED_BYTES_TOTAL).increment(size as u64);
        let bytes = match ChunkCompression::from_tag(meta.compression & 0x03)? {
            ChunkCompression::None => payload,
            ChunkCompression::Zstd(_) => zstd::stream::decode_all(payload.as_slice())
                .context("failed to decompress replay chunk")?,
        };
        counter!(STORAGE_REPLAY_DECOMPRESSED_BYTES_TOTAL).increment(bytes.len() as u64);
        let count = usize::try_from(
            meta.last_index
                .checked_sub(meta.first_index)
                .context("invalid chunk indices")?
                + 1,
        )?;
        let chunk = Arc::new(ChunkData::new(
            meta.first_index,
            meta.compression,
            bytes,
            count,
        )?);
        while !self.cache.is_empty()
            && (self.cache.len() >= 2
                || self.cache_bytes.saturating_add(chunk.size()) > Self::CACHE_BYTES)
        {
            self.cache_bytes -= self.cache.pop_front().unwrap().size();
        }
        self.cache_bytes += chunk.size();
        self.cache.push_back(Arc::clone(&chunk));
        Ok(chunk)
    }

    fn payload_chunk(&mut self, index: u64) -> anyhow::Result<Arc<ChunkData>> {
        // Check the catalog even on cache hits: an expired reference must never
        // make a pruned history silently appear complete.
        let meta = {
            let catalog = self.metadata.catalog();
            let pos = catalog
                .chunks
                .partition_point(|chunk| chunk.last_index < index);
            catalog
                .chunks
                .get(pos)
                .copied()
                .filter(|chunk| chunk.first_index <= index)
                .context("replay payload has been pruned")?
        };
        self.load(meta)
    }
}

#[derive(Debug)]
pub struct DecompressedChunk {
    data: Arc<ChunkData>,
    cursor: usize,
    parser: MessageParserEncoding,
    loader: Option<Arc<Mutex<ChunkLoader>>>,
    failed: bool,
}

impl DecompressedChunk {
    pub fn next_selected(
        &mut self,
        selection: Option<ReplaySelection>,
    ) -> Option<anyhow::Result<ScannedRecord>> {
        if self.failed || self.cursor == self.data.records.len() {
            return None;
        }
        let index = self.data.first_index + self.cursor as u64;
        self.cursor += 1;
        let result = self.read(index, selection);
        self.failed = result.is_err();
        Some(result)
    }

    fn read(
        &self,
        index: u64,
        selection: Option<ReplaySelection>,
    ) -> anyhow::Result<ScannedRecord> {
        let header = RecordHeader::parse(self.data.record(index)?, self.data.format)?;
        let mut result = ScannedRecord {
            index,
            payload_index: None,
            commitment: header.commitment,
            message: None,
        };
        if selection.is_some_and(|selection| {
            selection.commitment != header.commitment
                || selection.from_slot.is_some_and(|slot| header.slot < slot)
        }) {
            return Ok(result);
        }
        result.message = Some(if header.reference {
            let mut payload = header.payload;
            let target = decode_varint(&mut payload).context("invalid payload reference")?;
            anyhow::ensure!(
                payload.is_empty() && target < index,
                "invalid forward or trailing payload reference"
            );
            let data = if self.data.contains(target) {
                Arc::clone(&self.data)
            } else {
                mutex_lock(self.loader.as_ref().context("reference reader missing")?)
                    .payload_chunk(target)?
            };
            let original = RecordHeader::parse(data.record(target)?, data.format)?;
            anyhow::ensure!(
                !original.reference
                    && original.slot == header.slot
                    && original.block == header.block,
                "payload reference does not match publication"
            );
            result.payload_index = Some(target);
            original.decode(self.parser)?
        } else {
            result.payload_index = Some(index);
            header.decode(self.parser)?
        });
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn record_counts(&self) -> (usize, usize, usize) {
        let references = self
            .data
            .records
            .iter()
            .filter(|range| {
                RecordHeader::parse(&self.data.bytes[(*range).clone()], self.data.format)
                    .unwrap()
                    .reference
            })
            .count();
        (
            self.data.records.len() - references,
            references,
            self.data.bytes.len(),
        )
    }

    #[cfg(test)]
    pub(crate) fn fixture(
        first_index: u64,
        format: u8,
        bytes: Vec<u8>,
        count: usize,
        skip: usize,
        parser: MessageParserEncoding,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            data: Arc::new(ChunkData::new(first_index, format, bytes, count)?),
            cursor: skip,
            parser,
            loader: None,
            failed: false,
        })
    }
}

impl Iterator for DecompressedChunk {
    type Item = anyhow::Result<ReplayRecord>;
    fn next(&mut self) -> Option<Self::Item> {
        self.next_selected(None).map(|result| {
            result.map(|record| ReplayRecord {
                index: record.index,
                payload_index: record.payload_index.expect("decoded payload has an index"),
                commitment: record.commitment,
                message: record.message.expect("unfiltered read has a payload"),
            })
        })
    }
}

#[derive(Debug)]
pub struct SegmentReader {
    parser: MessageParserEncoding,
    next_index: u64,
    loader: Arc<Mutex<ChunkLoader>>,
    failed: bool,
}

impl SegmentReader {
    pub fn new(metadata: &Metadata, start_index: u64, parser: MessageParserEncoding) -> Self {
        Self {
            parser,
            next_index: start_index,
            loader: Arc::new(Mutex::new(ChunkLoader {
                metadata: metadata.clone(),
                file: None,
                cache: VecDeque::new(),
                cache_bytes: 0,
            })),
            failed: false,
        }
    }

    fn load_next(&mut self) -> anyhow::Result<Option<DecompressedChunk>> {
        let mut loader = mutex_lock(&self.loader);
        let meta = {
            let catalog = loader.metadata.catalog();
            let pos = catalog
                .chunks
                .partition_point(|chunk| chunk.last_index < self.next_index);
            let Some(meta) = catalog.chunks.get(pos).copied() else {
                return Ok(None);
            };
            meta
        };
        anyhow::ensure!(
            meta.first_index <= self.next_index,
            "replay records were pruned before index {}",
            self.next_index
        );
        let data = loader.load(meta)?;
        let cursor = usize::try_from(self.next_index - meta.first_index)?;
        self.next_index = meta.last_index + 1;
        Ok(Some(DecompressedChunk {
            data,
            cursor,
            parser: self.parser,
            loader: Some(Arc::clone(&self.loader)),
            failed: false,
        }))
    }
}

impl Iterator for SegmentReader {
    type Item = anyhow::Result<DecompressedChunk>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        match self.load_next() {
            Ok(Some(chunk)) => Some(Ok(chunk)),
            Ok(None) => None,
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        prost::{Message as _, encoding::encode_varint},
        richat_proto::geyser::{
            SubscribeUpdate, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
        },
    };

    fn frame(tag: u8, slot: u64, payload: &[u8]) -> Vec<u8> {
        let mut record = vec![tag];
        encode_varint(slot, &mut record);
        record.extend(payload);
        let mut bytes = vec![];
        encode_varint(record.len() as u64, &mut bytes);
        bytes.extend(record);
        bytes
    }

    #[test]
    fn excluded_commitment_and_slot_skip_protobuf_and_reference_reads() {
        for (tag, slot, payload) in [(0, 10, vec![0xff]), (0x40, 10, vec![0]), (2, 9, vec![0xff])] {
            let bytes = frame(tag, slot, &payload);
            let mut chunk = DecompressedChunk::fixture(
                100,
                COMMITMENT_RECORDS | REFERENCE_RECORDS,
                bytes,
                1,
                0,
                MessageParserEncoding::Prost,
            )
            .unwrap();
            let record = chunk
                .next_selected(Some(ReplaySelection {
                    commitment: CommitmentLevel::Finalized,
                    from_slot: Some(10),
                }))
                .unwrap()
                .unwrap();
            assert_eq!(record.index, 100);
            assert!(
                record.message.is_none(),
                "excluded payload must never be decoded or resolved"
            );
        }
    }

    #[test]
    fn references_resolve_before_the_replay_start_within_the_same_chunk() {
        let payload = SubscribeUpdate {
            filters: vec![],
            created_at: Some(prost_types::Timestamp {
                seconds: 1,
                nanos: 0,
            }),
            update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: 10,
                parent: Some(9),
                status: 0,
                dead_error: None,
            })),
        }
        .encode_to_vec();
        let mut bytes = frame(0, 10, &payload);
        bytes.extend(frame(0x41, 10, &[42]));
        let mut chunk = DecompressedChunk::fixture(
            42,
            COMMITMENT_RECORDS | REFERENCE_RECORDS,
            bytes,
            2,
            1,
            MessageParserEncoding::Prost,
        )
        .unwrap();
        let record = chunk
            .next_selected(Some(ReplaySelection {
                commitment: CommitmentLevel::Confirmed,
                from_slot: Some(10),
            }))
            .unwrap()
            .unwrap();
        assert_eq!(record.index, 43);
        assert_eq!(record.message.unwrap().slot(), 10);
    }

    #[test]
    fn malformed_reference_is_a_terminal_error() {
        let bytes = frame(0x42, 10, &[100]); // self-reference
        let mut chunk = DecompressedChunk::fixture(
            100,
            COMMITMENT_RECORDS | REFERENCE_RECORDS,
            bytes,
            1,
            0,
            MessageParserEncoding::Prost,
        )
        .unwrap();
        assert!(chunk.next().unwrap().is_err());
        assert!(chunk.next().is_none());
    }
}
