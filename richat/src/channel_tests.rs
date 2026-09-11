use {
    super::*,
    crate::{config::ConfigStorage, storage::segments::ChunkCompression},
    prost::Message as _,
    richat_filter::{config::ConfigFilter, filter::Filter},
    richat_proto::geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocks,
        SubscribeRequestFilterSlots, SubscribeUpdate, SubscribeUpdateAccount,
        SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta, SubscribeUpdateEntry,
        SubscribeUpdateSlot, SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
        subscribe_update::UpdateOneof,
    },
    std::{borrow::Cow, thread, time::Instant},
};

const LEVELS: [CommitmentLevel; 3] = [
    CommitmentLevel::Processed,
    CommitmentLevel::Confirmed,
    CommitmentLevel::Finalized,
];

fn request(level: CommitmentLevel) -> SubscribeRequest {
    SubscribeRequest {
        commitment: Some(match level {
            CommitmentLevel::Processed => 0,
            CommitmentLevel::Confirmed => 1,
            CommitmentLevel::Finalized => 2,
        }),
        slots: HashMap::from([(
            "slots".into(),
            SubscribeRequestFilterSlots {
                filter_by_commitment: Some(true),
                ..Default::default()
            },
        )]),
        accounts: HashMap::from([("accounts".into(), SubscribeRequestFilterAccounts::default())]),
        blocks: HashMap::from([(
            "blocks".into(),
            SubscribeRequestFilterBlocks {
                include_accounts: Some(true),
                include_transactions: Some(true),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        transactions: HashMap::from([("transactions".into(), Default::default())]),
        transactions_status: HashMap::from([("status".into(), Default::default())]),
        blocks_meta: HashMap::from([("meta".into(), Default::default())]),
        entry: HashMap::from([("entries".into(), Default::default())]),
        ..Default::default()
    }
}

fn filter(level: CommitmentLevel) -> Filter {
    Filter::new(&ConfigFilter::try_from(request(level)).unwrap())
}

fn message(update: UpdateOneof, parser: MessageParserEncoding) -> Message {
    Message::parse(
        Cow::Owned(
            SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(update),
                created_at: Some(prost_types::Timestamp {
                    seconds: 1,
                    nanos: 0,
                }),
            }
            .encode_to_vec(),
        ),
        parser,
    )
    .unwrap()
}

fn status(slot: Slot, status: SlotStatus) -> UpdateOneof {
    UpdateOneof::Slot(SubscribeUpdateSlot {
        slot,
        parent: Some(slot.saturating_sub(1)),
        status: status as i32,
        dead_error: None,
    })
}

fn account(slot: Slot, version: u64) -> UpdateOneof {
    UpdateOneof::Account(SubscribeUpdateAccount {
        slot,
        is_startup: false,
        account: Some(SubscribeUpdateAccountInfo {
            pubkey: vec![1; 32],
            owner: vec![2; 32],
            lamports: version,
            write_version: version,
            data: vec![3; 100],
            ..Default::default()
        }),
    })
}

fn transaction(slot: Slot) -> UpdateOneof {
    use richat_proto::solana::storage::confirmed_block::{
        Message as TransactionMessage, MessageHeader, Transaction, TransactionStatusMeta,
    };
    UpdateOneof::Transaction(SubscribeUpdateTransaction {
        slot,
        transaction: Some(SubscribeUpdateTransactionInfo {
            signature: vec![5; 64],
            transaction: Some(Transaction {
                signatures: vec![vec![5; 64]],
                message: Some(TransactionMessage {
                    header: Some(MessageHeader {
                        num_required_signatures: 1,
                        ..Default::default()
                    }),
                    account_keys: vec![vec![1; 32]],
                    recent_blockhash: vec![6; 32],
                    ..Default::default()
                }),
            }),
            meta: Some(TransactionStatusMeta {
                pre_balances: vec![10],
                post_balances: vec![9],
                ..Default::default()
            }),
            ..Default::default()
        }),
    })
}

fn block(slot: Slot) -> Vec<UpdateOneof> {
    vec![
        status(slot, SlotStatus::SlotCreatedBank),
        account(slot, 1),
        account(slot, 2),
        transaction(slot),
        UpdateOneof::Entry(SubscribeUpdateEntry {
            slot,
            hash: vec![4; 32],
            executed_transaction_count: 1,
            ..Default::default()
        }),
        UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            parent_slot: slot - 1,
            block_height: Some(
                richat_proto::solana::storage::confirmed_block::BlockHeight { block_height: slot },
            ),
            entries_count: 1,
            executed_transaction_count: 1,
            ..Default::default()
        }),
        status(slot, SlotStatus::SlotProcessed),
        status(slot, SlotStatus::SlotConfirmed),
        status(slot, SlotStatus::SlotFinalized),
    ]
}

fn output(message: &ParsedMessage, level: CommitmentLevel) -> Vec<SubscribeUpdate> {
    filter(level)
        .get_updates_ref(message.into(), level)
        .iter()
        .map(|update| SubscribeUpdate::decode(update.encode_to_vec().as_slice()).unwrap())
        .collect()
}

fn update_slot(update: &SubscribeUpdate) -> Slot {
    match update.update_oneof.as_ref().unwrap() {
        UpdateOneof::Slot(msg) => msg.slot,
        UpdateOneof::Account(msg) => msg.slot,
        UpdateOneof::Entry(msg) => msg.slot,
        UpdateOneof::BlockMeta(msg) => msg.slot,
        UpdateOneof::Block(msg) => msg.slot,
        UpdateOneof::Transaction(msg) => msg.slot,
        UpdateOneof::TransactionStatus(msg) => msg.slot,
        _ => panic!("unexpected test update"),
    }
}

struct Harness {
    messages: Option<Messages>,
    sender: Option<Sender>,
    threads: SpawnedThreads,
    shutdown: CancellationToken,
    parser: MessageParserEncoding,
    heads: [u64; 3],
    expected: [Vec<SubscribeUpdate>; 3],
}

impl Harness {
    fn open(
        path: &std::path::Path,
        parser: MessageParserEncoding,
        max_slots: usize,
        compression: bool,
    ) -> Self {
        Self::open_with_levels(
            path,
            parser,
            max_slots,
            compression,
            serde_json::json!(["processed", "confirmed", "finalized"]),
        )
    }

    fn open_with_levels(
        path: &std::path::Path,
        parser: MessageParserEncoding,
        max_slots: usize,
        compression: bool,
        commitments: serde_json::Value,
    ) -> Self {
        let mut config: ConfigStorage = serde_json::from_value(serde_json::json!({ "path": path, "max_slots": max_slots, "commitments": commitments, "replay_threads": 1, "replay_decode_per_tick": 2, "compressor_threads": 2 })).unwrap();
        config.chunk_target_size = 2048;
        config.segment_target_size = 4096;
        if compression {
            config.chunk_compression = Some(ChunkCompression::Zstd(1));
        }
        Self::with_storage(parser, Some(config), 8)
    }

    fn with_storage(
        parser: MessageParserEncoding,
        storage: Option<ConfigStorage>,
        max_messages: usize,
    ) -> Self {
        let shutdown = CancellationToken::new();
        let (mut messages, threads) = Messages::new(
            parser,
            ConfigChannelInner {
                max_messages,
                max_bytes: 1_000_000,
                storage,
            },
            false,
            true,
            false,
            shutdown.clone(),
        )
        .unwrap();
        let (sender, _) = messages.to_sender(1).unwrap();
        let heads = LEVELS.map(|level| messages.get_current_tail(level) + 1);
        Self {
            messages: Some(messages),
            sender: Some(sender),
            threads,
            shutdown,
            parser,
            heads,
            expected: Default::default(),
        }
    }

    fn messages(&self) -> &Messages {
        self.messages.as_ref().unwrap()
    }

    fn push(&mut self, update: UpdateOneof) {
        self.sender
            .as_mut()
            .unwrap()
            .push(false, "test", message(update, self.parser));
        let receiver = self.messages().to_receiver();
        for (idx, level) in LEVELS.into_iter().enumerate() {
            while let Some(msg) = receiver.try_recv(level, self.heads[idx]).unwrap() {
                self.heads[idx] += 1;
                self.expected[idx].extend(output(&msg, level));
            }
        }
    }

    fn flush(&self) {
        let deadline = Instant::now() + Duration::from_secs(10);
        let expected = self.sender.as_ref().unwrap().index;
        while self.messages().storage.as_ref().unwrap().next_index() < expected {
            assert!(Instant::now() < deadline, "writer did not flush");
            thread::sleep(Duration::from_millis(5));
        }
    }

    fn client(&self, from: Slot, level: CommitmentLevel) -> SubscribeClient {
        let client = SubscribeClient::new(1, 256, 256, Arc::from("test"));
        let mut state = client.state_lock();
        state.head = self
            .messages()
            .get_current_tail_with_replay(level, Some(from))
            .unwrap();
        state.commitment = level;
        state.replay_from_slot = Some(from);
        state.replay_generation = 1;
        state.recovery_epoch = self.messages().recovery_epoch(level);
        state.filter = Some(filter(level));
        assert!(
            matches!(state.head, IndexLocation::Storage(_)),
            "test must start on disk"
        );
        self.messages()
            .replay_from_storage(client.clone(), Gauge::noop(), level, 1)
            .unwrap();
        drop(state);
        client
    }

    fn finish_replay(&self, client: &SubscribeClient) -> Vec<SubscribeUpdate> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut updates = vec![];
        loop {
            while let Some(data) = client.pop_for_test() {
                updates.push(SubscribeUpdate::decode(data.unwrap().as_slice()).unwrap());
            }
            let state = client.state_lock();
            if let IndexLocation::Memory(mut head) = state.head {
                // Drain anything published just before the worker switched to memory.
                while let Some(data) = client.pop_for_test() {
                    updates.push(SubscribeUpdate::decode(data.unwrap().as_slice()).unwrap());
                }
                let receiver = self.messages().to_receiver();
                while let Some(msg) = receiver.try_recv(state.commitment, head).unwrap() {
                    head += 1;
                    if state.replay_from_slot.is_none_or(|from| msg.slot() >= from) {
                        updates.extend(output(&msg, state.commitment));
                    }
                }
                return updates;
            }
            drop(state);
            assert!(Instant::now() < deadline, "replay never joined memory");
            thread::sleep(Duration::from_millis(1));
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
        self.sender.take();
        self.messages.take();
        for (_, thread) in self.threads.drain(..) {
            if let Some(thread) = thread {
                thread.join().unwrap().unwrap();
            }
        }
    }
}

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "richat-replay-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn disk_replay_matches_live_at_every_commitment_and_parser() {
    for parser in [MessageParserEncoding::Prost, MessageParserEncoding::Limited] {
        for compressed in [false, true] {
            let dir = TempDir::new();
            let mut harness = Harness::open(&dir.0, parser, 3000, compressed);
            for slot in 10..25 {
                for event in block(slot) {
                    harness.push(event);
                }
            }
            harness.flush();
            for (idx, level) in LEVELS.into_iter().enumerate() {
                let client = harness.client(11, level);
                let actual = harness.finish_replay(&client);
                let expected: Vec<_> = harness.expected[idx]
                    .iter()
                    .filter(|msg| update_slot(msg) >= 11)
                    .cloned()
                    .collect();
                assert_eq!(actual, expected, "{parser:?}, {compressed}, {level:?}");
                assert_eq!(
                    actual
                        .iter()
                        .filter(|msg| matches!(msg.update_oneof, Some(UpdateOneof::Block(_))))
                        .count(),
                    14
                );
            }
        }
    }
}

#[test]
fn replay_survives_restart_and_joins_new_live_events() {
    let dir = TempDir::new();
    let mut first = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for slot in 10..15 {
        for event in block(slot) {
            first.push(event);
        }
    }
    first.flush();
    let expected = first.expected.clone();
    let next_index = first.sender.as_ref().unwrap().index;
    drop(first);
    let mut second = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    assert_eq!(second.sender.as_ref().unwrap().index, next_index);
    let upstream_resume = second
        .sender
        .as_ref()
        .unwrap()
        .global_replay_from_slot
        .clone();
    assert_eq!(
        upstream_resume.load(),
        Some(15),
        "stored finalized slots 10..=14 must not be requested upstream"
    );
    // Serve historical subscriptions before any upstream reconnect or new input.
    for (idx, level) in LEVELS.into_iter().enumerate() {
        let client = second.client(10, level);
        assert_eq!(second.finish_replay(&client), expected[idx]);
        assert_eq!(
            upstream_resume.load(),
            Some(15),
            "downstream disk replay must not rewind the upstream subscription"
        );
    }
    for event in block(16) {
        second.push(event);
    } // slot 15 skipped
    second.flush();
    for (idx, level) in LEVELS.into_iter().enumerate() {
        let client = second.client(10, level);
        let mut wanted = expected[idx].clone();
        wanted.extend(second.expected[idx].clone());
        assert_eq!(second.finish_replay(&client), wanted);
    }
}

#[test]
fn stale_replay_generation_cannot_publish_after_commitment_change() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for slot in 10..20 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    let client = SubscribeClient::new(1, 256, 256, Arc::from("test"));
    {
        let mut state = client.state_lock();
        state.head = harness
            .messages()
            .get_current_tail_with_replay(CommitmentLevel::Processed, Some(10))
            .unwrap();
        state.commitment = CommitmentLevel::Processed;
        state.filter = Some(filter(CommitmentLevel::Processed));
        state.replay_from_slot = Some(10);
        state.replay_generation = 1;
        harness
            .messages()
            .replay_from_storage(client.clone(), Gauge::noop(), CommitmentLevel::Processed, 1)
            .unwrap();
        state.commitment = CommitmentLevel::Finalized;
        state.filter = Some(filter(CommitmentLevel::Finalized));
        state.replay_generation = 2;
        harness
            .messages()
            .replay_from_storage(client.clone(), Gauge::noop(), CommitmentLevel::Finalized, 2)
            .unwrap();
    }
    assert_eq!(harness.finish_replay(&client), harness.expected[2]);
}

#[test]
fn retention_is_exactly_3000_slot_numbers_including_skips() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for slot in 1..=3001 {
        harness.push(status(slot, SlotStatus::SlotProcessed));
    }
    harness.flush();
    assert_eq!(harness.messages().get_first_available_slot(), Some(2));
    for level in LEVELS {
        assert!(
            harness
                .messages()
                .get_current_tail_with_replay(level, Some(1))
                .is_err()
        );
        assert!(matches!(
            harness
                .messages()
                .get_current_tail_with_replay(level, Some(2))
                .unwrap(),
            IndexLocation::Storage(_)
        ));
    }
    let client = harness.client(2, CommitmentLevel::Processed);
    assert_eq!(harness.finish_replay(&client).len(), 3000);
    harness.push(status(6001, SlotStatus::SlotProcessed));
    assert_eq!(
        mutex_lock(harness.messages().replay_info.as_ref().unwrap())
            .keys()
            .next(),
        Some(&6001)
    );
    // The smaller disk window must not discard still-complete local memory.
    assert!(matches!(
        harness
            .messages()
            .get_current_tail_with_replay(CommitmentLevel::Processed, Some(3001))
            .unwrap(),
        IndexLocation::Memory(_)
    ));
}

#[test]
fn evicted_slot_is_not_readvertised_when_late_status_arrives() {
    let shared = Arc::new(SharedChannel::new(2, false));
    let mut sender = SenderShared::new(&shared, 2, 10000, CommitmentLevel::Processed);
    for (index, slot) in [10, 11, 12, 10].into_iter().enumerate() {
        sender.push(
            slot,
            message(
                status(slot, SlotStatus::SlotProcessed),
                MessageParserEncoding::Prost,
            )
            .into(),
            Some(index as u64),
        );
    }
    assert!(!shared.slots_lock().contains_key(&10));
    assert_eq!(shared.get_head_by_replay_index(1), None);
    assert_eq!(shared.get_head_by_replay_index(2), Some(5));
    assert_eq!(shared.get_head_by_replay_index(4), Some(7));
}

#[test]
fn no_storage_replays_all_commitments_from_local_memory() {
    let (mut messages, threads) = Messages::new(
        MessageParserEncoding::Prost,
        ConfigChannelInner {
            max_messages: 128,
            max_bytes: 1_000_000,
            storage: None,
        },
        false,
        true,
        false,
        CancellationToken::new(),
    )
    .unwrap();
    assert!(threads.is_empty());
    assert!(messages.storage.is_none());
    let (mut sender, upstream_replay) = messages.to_sender(1).unwrap();
    assert_eq!(upstream_replay.load(), None);
    for slot in 10..13 {
        for event in block(slot) {
            sender.push(false, "test", message(event, MessageParserEncoding::Prost));
        }
    }
    let upstream_before = upstream_replay.load();
    for level in LEVELS {
        let IndexLocation::Memory(mut head) = messages
            .get_current_tail_with_replay(level, Some(11))
            .unwrap()
        else {
            panic!("expected memory replay")
        };
        let receiver = messages.to_receiver();
        let mut updates = vec![];
        while let Some(msg) = receiver.try_recv(level, head).unwrap() {
            head += 1;
            if msg.slot() >= 11 {
                updates.extend(output(&msg, level));
            }
        }
        assert_eq!(
            updates
                .iter()
                .filter(|msg| matches!(msg.update_oneof, Some(UpdateOneof::Block(_))))
                .count(),
            2
        );
        assert!(
            messages
                .get_current_tail_with_replay(level, Some(9))
                .is_err()
        );
        assert!(matches!(
            messages
                .get_current_tail_with_replay(level, Some(20))
                .unwrap(),
            IndexLocation::Memory(_)
        ));
    }
    assert_eq!(
        upstream_replay.load(),
        upstream_before,
        "downstream replay never changes the upstream subscription"
    );
}

#[test]
fn storage_defaults_remain_processed_and_1024_slots() {
    let config: ConfigStorage =
        serde_json::from_value(serde_json::json!({ "path": "unused" })).unwrap();
    assert_eq!(config.max_slots, 1024);
    assert_eq!(config.commitment_mask(), 1);
}

#[test]
fn only_configured_commitments_are_available_on_disk() {
    let dir = TempDir::new();
    let mut harness = Harness::open_with_levels(
        &dir.0,
        MessageParserEncoding::Prost,
        3000,
        true,
        serde_json::json!(["finalized"]),
    );
    for slot in 10..20 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    for level in [CommitmentLevel::Processed, CommitmentLevel::Confirmed] {
        assert!(
            harness
                .messages()
                .get_current_tail_with_replay(level, Some(10))
                .unwrap_err()
                .contains("not enabled")
        );
    }
    let client = harness.client(10, CommitmentLevel::Finalized);
    assert_eq!(harness.finish_replay(&client), harness.expected[2]);
}

#[test]
fn enabling_commitment_after_restart_rejects_incomplete_history() {
    let dir = TempDir::new();
    let mut first = Harness::open_with_levels(
        &dir.0,
        MessageParserEncoding::Prost,
        3000,
        true,
        serde_json::json!(["processed"]),
    );
    for slot in 10..15 {
        for event in block(slot) {
            first.push(event);
        }
    }
    first.flush();
    drop(first);
    let mut second = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for slot in 16..25 {
        for event in block(slot) {
            second.push(event);
        }
    }
    second.flush();
    assert!(
        second
            .messages()
            .get_current_tail_with_replay(CommitmentLevel::Confirmed, Some(10))
            .unwrap_err()
            .contains("predates")
    );
    let client = second.client(16, CommitmentLevel::Confirmed);
    assert_eq!(second.finish_replay(&client), second.expected[1]);
}

#[test]
fn unfinalized_restart_deduplicates_replayed_source_events() {
    let dir = TempDir::new();
    let mut first = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for event in block(10) {
        first.push(event);
    }
    let pending: Vec<_> = block(11).into_iter().filter(|event| !matches!(event, UpdateOneof::Slot(slot) if slot.status == SlotStatus::SlotFinalized as i32)).collect();
    for event in pending.clone() {
        first.push(event);
    }
    first.flush();
    let before = first.expected.clone();
    let next_index = first.sender.as_ref().unwrap().index;
    drop(first);
    let mut second = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    assert_eq!(
        second
            .sender
            .as_ref()
            .unwrap()
            .global_replay_from_slot
            .load(),
        Some(11),
        "slot 10 is complete on disk, but slot 11 still needs its finalization"
    );
    for event in pending {
        second.push(event);
    }
    assert_eq!(
        second.sender.as_ref().unwrap().index,
        next_index,
        "already durable events must not be appended again"
    );
    second.push(status(11, SlotStatus::SlotFinalized));
    for event in block(12) {
        second.push(event);
    }
    second.flush();
    let (payloads, references) = second
        .messages()
        .storage
        .as_ref()
        .unwrap()
        .read_messages_from_index(0, second.parser)
        .map(|chunk| {
            let counts = chunk.unwrap().record_counts();
            (counts.0, counts.1)
        })
        .fold((0, 0), |totals, counts| {
            (totals.0 + counts.0, totals.1 + counts.1)
        });
    assert_eq!(
        (payloads, references),
        (30, 54),
        "restart must reuse already stored payloads when the slot finalizes"
    );

    for (idx, level) in LEVELS.into_iter().enumerate() {
        let client = second.client(10, level);
        let mut expected = before[idx].clone();
        expected.extend(second.expected[idx].clone());
        assert_eq!(second.finish_replay(&client), expected);
    }
}

#[test]
fn late_data_on_unconfirmed_fork_does_not_leak_into_confirmed_stream() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    harness.push(status(10, SlotStatus::SlotCreatedBank));
    harness.push(status(11, SlotStatus::SlotConfirmed));
    harness.push(account(10, 1));
    harness.push(account(11, 1));
    assert!(!harness.expected[1].iter().any(|update| matches!(&update.update_oneof, Some(UpdateOneof::Account(account)) if account.slot == 10)));
    assert!(harness.expected[1].iter().any(|update| matches!(&update.update_oneof, Some(UpdateOneof::Account(account)) if account.slot == 11)));
}

#[test]
fn damaged_segment_returns_data_loss_instead_of_silently_skipping_history() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for slot in 10..20 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    let first_segment = std::fs::read_dir(dir.0.join("segments"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .min()
        .unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(first_segment)
        .unwrap()
        .set_len(0)
        .unwrap();
    let client = harness.client(10, CommitmentLevel::Finalized);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(result) = client.pop_for_test() {
            assert_eq!(result.unwrap_err().code(), tonic::Code::DataLoss);
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
}

// Uses the official yellowstone-grpc-proto client over a real HTTP/2 connection
// carried by an in-process duplex transport, with Richat's actual gRPC service.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn yellowstone_client_replays_blocks_and_all_event_types_without_old_rejection() {
    check_yellowstone_wire(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn yellowstone_client_subscribes_before_filter_and_receives_live_blocks() {
    check_yellowstone_wire(false).await;
}

async fn check_yellowstone_wire(replay: bool) {
    use {
        crate::grpc::server::{GrpcServer, geyser_gen::geyser_server::GeyserServer},
        hyper_util::rt::TokioIo,
        richat_proto::geyser::geyser_client::GeyserClient,
        std::{
            future::{Ready, ready},
            task::{Context, Poll},
        },
        tonic::{codegen::Service, transport::Endpoint},
    };
    struct Connector(Option<tokio::io::DuplexStream>);
    impl Service<hyper::Uri> for Connector {
        type Response = TokioIo<tokio::io::DuplexStream>;
        type Error = std::io::Error;
        type Future = Ready<Result<Self::Response, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: hyper::Uri) -> Self::Future {
            ready(
                self.0
                    .take()
                    .map(TokioIo::new)
                    .ok_or_else(|| std::io::Error::other("test transport already connected")),
            )
        }
    }
    let dir = TempDir::new();
    let mut harness = if replay {
        Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true)
    } else {
        Harness::with_storage(MessageParserEncoding::Prost, None, 256)
    };
    if replay {
        for slot in 10..25 {
            for event in block(slot) {
                harness.push(event);
            }
        }
        harness.flush();
    }
    let shutdown = CancellationToken::new();
    struct CancelOnDrop(CancellationToken);
    impl Drop for CancelOnDrop {
        fn drop(&mut self) {
            self.0.cancel();
        }
    }
    let _cancel = CancelOnDrop(shutdown.clone());
    let (service, worker) = GrpcServer::test_service(harness.messages().clone(), shutdown.clone());
    let (client_io, server_io) = tokio::io::duplex(8192);
    let cancellation = shutdown.clone();
    let server = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(GeyserServer::new(service))
            .serve_with_incoming_shutdown(
                futures::stream::iter([Ok::<_, std::io::Error>(server_io)])
                    .chain(futures::stream::pending()),
                cancellation.cancelled(),
            )
            .await
            .unwrap();
    });
    let channel = tokio::time::timeout(
        Duration::from_secs(5),
        Endpoint::from_static("http://richat.test")
            .connect_with_connector(Connector(Some(client_io))),
    )
    .await
    .expect("connect timeout")
    .unwrap();
    let mut client = GeyserClient::new(channel);
    client
        .get_version(richat_proto::geyser::GetVersionRequest {})
        .await
        .unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let mut initial = request(CommitmentLevel::Processed);
    initial.from_slot = Some(11);
    let requests = futures::stream::unfold(rx, |mut rx| async {
        rx.recv().await.map(|request| (request, rx))
    });
    let mut stream = tokio::time::timeout(Duration::from_secs(5), client.subscribe(requests))
        .await
        .expect("subscribe timeout")
        .unwrap()
        .into_inner();
    if replay {
        tx.send(initial).unwrap();
    } else {
        // Match the Node SDK usage: getVersion -> await subscribe -> write filter.
        tx.send(SubscribeRequest {
            commitment: Some(0),
            blocks: HashMap::from([(
                "filteredBlocks".into(),
                SubscribeRequestFilterBlocks {
                    account_include: vec![Pubkey::from([1; 32]).to_string()],
                    include_transactions: Some(true),
                    include_accounts: Some(false),
                    include_entries: Some(false),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        })
        .unwrap();
        // A pong acknowledges all earlier requests on the same HTTP/2 stream.
        tx.send(SubscribeRequest {
            ping: Some(richat_proto::geyser::SubscribeRequestPing { id: 42 }),
            ..Default::default()
        })
        .unwrap();
        loop {
            let update = tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let Some(UpdateOneof::Pong(pong)) = update.update_oneof {
                assert_eq!(pong.id, 42);
                break;
            }
        }
        for event in block(10) {
            harness.push(event);
        }
        loop {
            let update = tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let Some(UpdateOneof::Block(block)) = update.update_oneof {
                assert_eq!(update.filters, ["filteredBlocks"]);
                assert_eq!(block.slot, 10);
                assert_eq!(block.transactions.len(), 1);
                assert!(block.accounts.is_empty());
                assert!(block.entries.is_empty());
                break;
            }
        }
        // A live subscriber must stay connected across the upstream fallback.
        harness.sender.as_mut().unwrap().begin_live_epoch().unwrap();
        for event in block(1000) {
            harness.push(event);
        }
        loop {
            let update = tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            if let Some(UpdateOneof::Block(block)) = update.update_oneof {
                assert_eq!(block.slot, 1000);
                break;
            }
        }
        // The same subscriber explicitly asking for missing history must fail,
        // never inherit the upstream-only fallback-to-live policy.
        let mut historical = request(CommitmentLevel::Processed);
        historical.from_slot = Some(10);
        tx.send(historical).unwrap();
        loop {
            match tokio::time::timeout(Duration::from_secs(5), stream.message())
                .await
                .unwrap()
            {
                Err(status) => {
                    assert_eq!(status.code(), tonic::Code::InvalidArgument);
                    assert!(status.message().contains("replay position"));
                    break;
                }
                Ok(Some(_)) => {}
                Ok(None) => panic!("missing client history must return an explicit error"),
            }
        }
    }
    if replay {
        for (round, level) in [
            CommitmentLevel::Processed,
            CommitmentLevel::Confirmed,
            CommitmentLevel::Finalized,
            CommitmentLevel::Finalized,
        ]
        .into_iter()
        .enumerate()
        {
            // The last round repeats from_slot without changing commitment.
            if round != 0 {
                let mut next = request(level);
                next.from_slot = Some(11);
                tx.send(next).unwrap();
            }
            let idx = LEVELS
                .iter()
                .position(|candidate| *candidate == level)
                .unwrap();
            let expected: Vec<_> = harness.expected[idx]
                .iter()
                .filter(|update| update_slot(update) >= 11)
                .cloned()
                .collect();
            let mut actual = vec![];
            while actual.len() < expected.len() {
                let update = tokio::time::timeout(Duration::from_secs(10), stream.message())
                    .await
                    .unwrap()
                    .expect("from_slot must not reject blocks or any other supported filter")
                    .unwrap();
                if !matches!(
                    update.update_oneof,
                    Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_))
                ) {
                    actual.push(update);
                }
            }
            assert_eq!(
                actual, expected,
                "official Yellowstone client at {level:?}, round {round}"
            );
        }
        // Exercise focused filters through the wire codec as well as the mixed stream.
        for category in 0..3 {
            let level = if category == 1 {
                CommitmentLevel::Processed
            } else {
                CommitmentLevel::Finalized
            };
            let mut focused = SubscribeRequest {
                from_slot: Some(11),
                commitment: Some(if category == 1 { 0 } else { 2 }),
                ..Default::default()
            };
            let idx = LEVELS
                .iter()
                .position(|candidate| *candidate == level)
                .unwrap();
            let expected: Vec<_> = harness.expected[idx]
                .iter()
                .filter(|update| update_slot(update) >= 11)
                .filter_map(|update| {
                    let mut update = update.clone();
                    match (&mut update.update_oneof, category) {
                        (Some(UpdateOneof::Block(block)), 0) => {
                            block.accounts.clear();
                            block.transactions.clear();
                            block.entries.clear();
                            Some(update)
                        }
                        (Some(UpdateOneof::Account(account)), 1)
                            if account.account.as_ref().unwrap().lamports == 2 =>
                        {
                            account.account.as_mut().unwrap().data = vec![3; 7];
                            Some(update)
                        }
                        (
                            Some(UpdateOneof::Transaction(_) | UpdateOneof::TransactionStatus(_)),
                            2,
                        ) => Some(update),
                        _ => None,
                    }
                })
                .collect();
            match category {
                0 => {
                    focused.blocks.insert(
                        "blocks".into(),
                        SubscribeRequestFilterBlocks {
                            include_accounts: Some(false),
                            include_transactions: Some(false),
                            include_entries: Some(false),
                            ..Default::default()
                        },
                    );
                }
                1 => {
                    use richat_proto::geyser::{
                        SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccountsFilter,
                        SubscribeRequestFilterAccountsFilterLamports,
                        subscribe_request_filter_accounts_filter::Filter as AccountFilter,
                        subscribe_request_filter_accounts_filter_lamports::Cmp,
                    };
                    focused.accounts.insert(
                        "accounts".into(),
                        SubscribeRequestFilterAccounts {
                            owner: vec![Pubkey::from([2; 32]).to_string()],
                            filters: vec![SubscribeRequestFilterAccountsFilter {
                                filter: Some(AccountFilter::Lamports(
                                    SubscribeRequestFilterAccountsFilterLamports {
                                        cmp: Some(Cmp::Eq(2)),
                                    },
                                )),
                            }],
                            ..Default::default()
                        },
                    );
                    focused
                        .accounts_data_slice
                        .push(SubscribeRequestAccountsDataSlice {
                            offset: 2,
                            length: 7,
                        });
                }
                _ => {
                    let transactions = richat_proto::geyser::SubscribeRequestFilterTransactions {
                        vote: Some(false),
                        failed: Some(false),
                        account_include: vec![Pubkey::from([1; 32]).to_string()],
                        ..Default::default()
                    };
                    focused
                        .transactions
                        .insert("transactions".into(), transactions.clone());
                    focused
                        .transactions_status
                        .insert("status".into(), transactions);
                }
            }
            tx.send(focused).unwrap();
            let mut actual = vec![];
            while actual.len() < expected.len() {
                let update = tokio::time::timeout(Duration::from_secs(10), stream.message())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                if !matches!(
                    update.update_oneof,
                    Some(UpdateOneof::Ping(_) | UpdateOneof::Pong(_))
                ) {
                    actual.push(update);
                }
            }
            assert!(!expected.is_empty());
            assert_eq!(actual, expected, "focused filter category {category}");
        }
    }
    drop(stream);
    drop(tx);
    drop(client);
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    tokio::task::spawn_blocking(move || worker.join().unwrap().unwrap())
        .await
        .unwrap();
}

#[test]
fn all_commitments_share_one_payload_per_event_on_disk() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, false);
    for slot in 10..20 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    let mut payloads = 0;
    let mut references = 0;
    let mut stored_bytes = 0;
    let mut duplicated_bytes = 0;
    for chunk in harness
        .messages()
        .storage
        .as_ref()
        .unwrap()
        .read_messages_from_index(0, harness.parser)
    {
        let chunk = chunk.unwrap();
        let counts = chunk.record_counts();
        payloads += counts.0;
        references += counts.1;
        stored_bytes += counts.2;
        for record in chunk {
            let message = record.unwrap().message;
            duplicated_bytes += FilteredUpdate {
                filters: Default::default(),
                filtered_update: MessageRef::from(&message).into(),
            }
            .encode_to_vec()
            .len();
        }
    }
    assert_eq!(
        payloads, 100,
        "each of ten event payloads per slot is written only once"
    );
    assert_eq!(
        references, 180,
        "later commitment publications contain only references"
    );
    assert!(
        stored_bytes * 100 < duplicated_bytes * 60,
        "reference journal {stored_bytes} bytes vs repeated payloads {duplicated_bytes}"
    );
    eprintln!(
        "reference journal: {payloads} payloads, {references} references, {stored_bytes} bytes; repeated payload baseline: {duplicated_bytes} bytes"
    );
}

#[test]
fn backpressured_clients_do_not_block_another_replay() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for slot in 10..25 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    let blocked: Vec<_> = (0..8)
        .map(|_| harness.client(10, CommitmentLevel::Processed))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(5);
    while blocked
        .iter()
        .any(|client| client.messages_len.load(Ordering::Relaxed) < client.messages_replay_len_max)
    {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    let ready = harness.client(10, CommitmentLevel::Finalized);
    assert_eq!(harness.finish_replay(&ready), harness.expected[2]);
    assert!(
        blocked
            .iter()
            .all(|client| matches!(client.state_lock().head, IndexLocation::Storage(_)))
    );
}

#[test]
fn committed_accounts_and_blocks_keep_only_the_highest_write_version() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    let mut events = block(10);
    events.swap(1, 2); // same account arrives with write_version 2 before version 1
    for event in events {
        harness.push(event);
    }
    for slot in 11..15 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    for level in [CommitmentLevel::Confirmed, CommitmentLevel::Finalized] {
        let client = harness.client(10, level);
        let updates = harness.finish_replay(&client);
        let accounts: Vec<_> = updates
            .iter()
            .filter_map(|update| match &update.update_oneof {
                Some(UpdateOneof::Account(account)) if account.slot == 10 => {
                    Some(account.account.as_ref().unwrap())
                }
                _ => None,
            })
            .collect();
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].write_version, 2);
        let block = updates
            .iter()
            .find_map(|update| match &update.update_oneof {
                Some(UpdateOneof::Block(block)) if block.slot == 10 => Some(block),
                _ => None,
            })
            .unwrap();
        assert_eq!(block.accounts.len(), 1);
        assert_eq!(block.accounts[0].write_version, 2);
    }
}

#[test]
fn upstream_gap_invalidates_replay_and_persists_the_new_capture_boundary() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    for slot in 10..20 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    let client = harness.client(10, CommitmentLevel::Processed);
    // Hold the output queue full so replay remains active across the gap.
    client.messages_len.store(usize::MAX, Ordering::Relaxed);
    let boundary = harness.sender.as_ref().unwrap().index;
    harness.sender.as_mut().unwrap().begin_live_epoch().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(Err(status)) = client.pop_for_test() {
            assert_eq!(status.code(), tonic::Code::DataLoss);
            assert!(status.message().contains("gap"));
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(1));
    }
    drop(client);
    drop(harness);
    // Even a restart before the first new live event must not reuse the old cursor.
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    assert_eq!(
        harness
            .sender
            .as_ref()
            .unwrap()
            .global_replay_from_slot
            .load(),
        None
    );
    assert_eq!(harness.sender.as_ref().unwrap().index, boundary);
    for level in LEVELS {
        assert!(
            harness
                .messages()
                .get_current_tail_with_replay(level, Some(10))
                .is_err()
        );
    }
    for slot in 1000..1010 {
        for event in block(slot) {
            harness.push(event);
        }
    }
    harness.flush();
    for (idx, level) in LEVELS.into_iter().enumerate() {
        assert!(
            harness
                .messages()
                .get_current_tail_with_replay(level, Some(10))
                .is_err()
        );
        assert_eq!(
            harness.finish_replay(&harness.client(1000, level)),
            harness.expected[idx]
        );
    }
    drop(harness);
    let harness = Harness::open(&dir.0, MessageParserEncoding::Prost, 3000, true);
    assert_eq!(
        harness
            .sender
            .as_ref()
            .unwrap()
            .global_replay_from_slot
            .load(),
        Some(1010)
    );
    assert_eq!(harness.messages().get_first_available_slot(), Some(1000));
}
