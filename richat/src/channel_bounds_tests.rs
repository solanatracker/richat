//! Long-running bounds of the channel state: per-slot, per-bank and dedup maps must stay
//! bounded by the finalization lag and the ring size, whatever happens to the slots.

use super::*;

/// Slots between a processed slot and its finalized status
const FINALIZATION_LAG: Slot = 32;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Sizes {
    sender_slots: usize,
    sender_banks: usize,
    sender_cleared_banks: usize,
    sender_dedup: usize,
    sender_dedup_banks: usize,
    replay: usize,
    ring_slots: [usize; 3],
    wakers: usize,
}

impl Sizes {
    fn max(self, other: Self) -> Self {
        Self {
            sender_slots: self.sender_slots.max(other.sender_slots),
            sender_banks: self.sender_banks.max(other.sender_banks),
            sender_cleared_banks: self.sender_cleared_banks.max(other.sender_cleared_banks),
            sender_dedup: self.sender_dedup.max(other.sender_dedup),
            sender_dedup_banks: self.sender_dedup_banks.max(other.sender_dedup_banks),
            replay: self.replay.max(other.replay),
            ring_slots: [0, 1, 2].map(|idx| self.ring_slots[idx].max(other.ring_slots[idx])),
            wakers: self.wakers.max(other.wakers),
        }
    }
}

fn sizes(messages: &Messages, sender: &Sender) -> Sizes {
    Sizes {
        sender_slots: sender.slots.len(),
        sender_banks: sender.slots.values().map(|info| info.banks.len()).sum(),
        sender_cleared_banks: sender
            .slots
            .values()
            .map(|info| info.cleared_banks.len())
            .sum(),
        sender_dedup: sender.dedup.len(),
        sender_dedup_banks: sender
            .dedup
            .values()
            .map(|info| info.banks.len() + info.slots.len())
            .sum(),
        replay: mutex_lock(&sender.replay).len(),
        ring_slots: LEVELS.map(|level| messages.get_shared(level).slots_lock().len()),
        wakers: messages
            .shared_processed
            .wakers_lock()
            .map(|wakers| wakers.len())
            .unwrap_or_default(),
    }
}

/// Synthetic mainnet-like stream: two deduplicated sources, Alpenglow candidates, cleared banks,
/// skipped slots, forks that are never confirmed, and finalized statuses `FINALIZATION_LAG`
/// behind. Statuses of skipped and forked slots never arrive.
struct Stream {
    parser: MessageParserEncoding,
}

impl Stream {
    /// How the slot ends: `None` for slots without any event
    fn kind(slot: Slot) -> Option<SlotKind> {
        match slot % 10 {
            0 => None,
            1 => Some(SlotKind::Fork),
            2 => Some(SlotKind::ClearedCandidate),
            3 => Some(SlotKind::ProcessedOnly),
            _ => Some(SlotKind::Block),
        }
    }

    fn push(&self, sender: &mut Sender, source: &'static str, update: UpdateOneof) {
        sender.push(true, source, message(update, self.parser));
    }

    fn push_slot(&self, sender: &mut Sender, slot: Slot) {
        match Self::kind(slot) {
            None => {}
            Some(SlotKind::Fork) => {
                for event in candidate(slot, 1) {
                    self.push(sender, "a", event);
                }
                self.push(sender, "a", status(slot, SlotStatus::SlotProcessed));
            }
            Some(SlotKind::ClearedCandidate) => {
                for source in ["a", "b"] {
                    for event in candidate(slot, 1)
                        .into_iter()
                        .chain([update_parent(slot, 1)])
                        .chain(candidate(slot, 2))
                    {
                        self.push(sender, source, event);
                    }
                    self.push(sender, source, status(slot, SlotStatus::SlotProcessed));
                }
            }
            Some(SlotKind::ProcessedOnly | SlotKind::Block) => {
                for source in ["a", "b"] {
                    for event in block(slot)
                        .into_iter()
                        .filter(|event| !is_commitment_status(event))
                    {
                        self.push(sender, source, event);
                    }
                }
            }
        }

        // statuses of the slot that left the finalization window
        if let Some(slot) = slot.checked_sub(FINALIZATION_LAG)
            && matches!(
                Self::kind(slot),
                Some(SlotKind::ClearedCandidate | SlotKind::Block)
            )
        {
            for source in ["b", "a"] {
                self.push(sender, source, status(slot, SlotStatus::SlotConfirmed));
                self.push(sender, source, status(slot, SlotStatus::SlotFinalized));
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SlotKind {
    /// candidate of a fork, processed but never confirmed or finalized
    Fork,
    /// two candidates, the first one cleared by `EntryUpdateParent`
    ClearedCandidate,
    /// processed, the cluster skips it later
    ProcessedOnly,
    Block,
}

fn is_commitment_status(event: &UpdateOneof) -> bool {
    matches!(event, UpdateOneof::Slot(msg) if msg.status == SlotStatus::SlotConfirmed as i32
        || msg.status == SlotStatus::SlotFinalized as i32)
}

fn run(harness: &mut Harness, slots: Slot) -> Sizes {
    let stream = Stream {
        parser: harness.parser,
    };
    let messages = harness.messages.as_ref().unwrap();
    let sender = harness.sender.as_mut().unwrap();
    let mut max = Sizes::default();
    for slot in 1..=slots {
        stream.push_slot(sender, slot);
        max = max.max(sizes(messages, sender));
    }
    max
}

#[test]
fn sender_state_is_bounded_by_finalization_lag_over_many_slots() {
    for parser in [MessageParserEncoding::Prost, MessageParserEncoding::Limited] {
        let mut harness = Harness::with_storage(parser, None, 4096);
        let max = run(&mut harness, 20_000);

        // slots above the last finalized one: the lag, plus up to two slots whose statuses
        // never arrive, plus the finalized slot itself in `dedup`
        let window = FINALIZATION_LAG as usize + 3;
        assert!(max.sender_slots <= window, "{parser:?}: {max:?}");
        assert!(max.sender_dedup <= window, "{parser:?}: {max:?}");
        // at most two candidates per slot, the cleared one is dropped
        assert!(max.sender_banks <= 2 * window, "{parser:?}: {max:?}");
        assert!(max.sender_cleared_banks <= window, "{parser:?}: {max:?}");
        // `None` bank for statuses plus two candidates, in `slots` and `banks`
        assert!(max.sender_dedup_banks <= 6 * window, "{parser:?}: {max:?}");
        // the replay map is used only with storage
        assert_eq!(max.replay, 0, "{parser:?}: {max:?}");
        // each slot has at least one message in the ring
        for (idx, ring_slots) in max.ring_slots.into_iter().enumerate() {
            assert!(ring_slots <= 4096, "{parser:?}, {:?}: {max:?}", LEVELS[idx]);
        }

        let sender = harness.sender.as_ref().unwrap();
        let first = sender.slots.keys().next().copied().unwrap();
        assert!(
            first > 20_000 - FINALIZATION_LAG,
            "{parser:?}: forks, skipped and processed-only slots below the finalized slot are dropped, first slot is {first}"
        );
    }
}

#[test]
fn storage_replay_index_is_bounded_by_max_slots_over_many_slots() {
    let dir = TempDir::new();
    let mut harness = Harness::open(&dir.0, MessageParserEncoding::Limited, 256, false);
    let max = run(&mut harness, 3_000);
    harness.flush();

    assert!(max.replay <= 256, "{max:?}");
    assert!(max.sender_slots <= FINALIZATION_LAG as usize + 3, "{max:?}");
    let replay = mutex_lock(harness.messages().replay_info.as_ref().unwrap());
    // dedup ids are kept only for slots restored after a restart
    assert!(replay.values().all(|info| info.messages.is_none()));
    let (first, last) = (
        replay.keys().next().unwrap(),
        replay.keys().next_back().unwrap(),
    );
    assert!(last - first < 256, "{first}..={last}");
}

#[test]
fn receiver_wakers_are_released_by_the_next_message() {
    let (mut messages, _threads) = Messages::new(
        MessageParserEncoding::Prost,
        ConfigChannelInner {
            max_messages: 64,
            max_bytes: 1_000_000,
            max_unfinalized_slots: 1_024,
            storage: None,
        },
        true,
        false,
        false,
        CancellationToken::new(),
    )
    .unwrap();
    let (mut sender, _) = messages.to_sender(1).unwrap();
    let push = |sender: &mut Sender, slot| {
        sender.push(
            false,
            "test",
            message(
                status(slot, SlotStatus::SlotProcessed),
                MessageParserEncoding::Prost,
            ),
        )
    };
    push(&mut sender, 1);
    let mut receivers = (0..100)
        .map(|_| messages.subscribe(None, None).unwrap())
        .collect::<Vec<_>>();
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    for slot in 2..1_000 {
        for receiver in receivers.iter_mut() {
            while let Poll::Ready(Some(item)) = receiver.poll_next_unpin(&mut cx) {
                item.unwrap();
            }
        }
        assert_eq!(messages.shared_processed.wakers_lock().unwrap().len(), 100);
        push(&mut sender, slot);
        assert!(messages.shared_processed.wakers_lock().unwrap().is_empty());
    }
    // receivers of disconnected clients leave nothing behind after the next message
    receivers.clear();
    push(&mut sender, 1_000);
    assert!(messages.shared_processed.wakers_lock().unwrap().is_empty());
}

#[test]
fn unfinalized_slots_are_bounded_when_finalized_stalls() {
    let parser = MessageParserEncoding::Prost;
    let (mut messages, _threads) = Messages::new(
        parser,
        ConfigChannelInner {
            max_messages: 4096,
            max_bytes: 100_000_000,
            max_unfinalized_slots: 64,
            storage: None,
        },
        false,
        true,
        false,
        CancellationToken::new(),
    )
    .unwrap();
    let (mut sender, _) = messages.to_sender(2).unwrap();
    let stream = Stream { parser };

    // finalized never arrives: only processed data and statuses
    for slot in 1..=2_000 {
        for source in ["a", "b"] {
            for event in block(slot)
                .into_iter()
                .filter(|event| !is_commitment_status(event))
            {
                stream.push(&mut sender, source, event);
            }
        }
        assert!(sender.slots.len() <= 64, "{slot}: {}", sender.slots.len());
        assert!(sender.dedup.len() <= 64, "{slot}: {}", sender.dedup.len());
    }
    assert_eq!(sender.slots.keys().next().copied(), Some(2_000 - 63));

    // late data of a dropped slot is not tracked again
    for event in block(100) {
        stream.push(&mut sender, "a", event);
    }
    assert!(!sender.slots.contains_key(&100));

    // finalized resumes on a kept slot: it is still confirmed / finalized and cleaned
    for source in ["a", "b"] {
        stream.push(
            &mut sender,
            source,
            status(1_990, SlotStatus::SlotConfirmed),
        );
        stream.push(
            &mut sender,
            source,
            status(1_990, SlotStatus::SlotFinalized),
        );
    }
    assert_eq!(sender.slots.keys().next().copied(), Some(1_991));
    assert!(
        messages
            .get_shared(CommitmentLevel::Finalized)
            .slots_lock()
            .contains_key(&1_990)
    );
}

/// Soak run, prints RSS and map sizes while synthetic slots flow through the channel:
/// `RICHAT_SOAK_SLOTS=1000000 cargo test -p richat --release -- --ignored --nocapture soak_`
#[test]
#[ignore = "soak run, minutes long"]
fn soak_channel_reports_rss_and_map_sizes() {
    let slots: Slot = std::env::var("RICHAT_SOAK_SLOTS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000);
    let report_every = (slots / 20).max(1);
    let mut harness = Harness::with_storage(MessageParserEncoding::Limited, None, 1 << 16);
    let stream = Stream {
        parser: harness.parser,
    };
    let messages = harness.messages.as_ref().unwrap();
    let sender = harness.sender.as_mut().unwrap();
    let started = Instant::now();
    let mut rss_samples = vec![];
    for slot in 1..=slots {
        stream.push_slot(sender, slot);
        if slot % report_every == 0 {
            let rss = rss_kib();
            rss_samples.push(rss);
            println!(
                "slot {slot:>9} | {:>6.1}s | rss {:>8} KiB | {:?}",
                started.elapsed().as_secs_f64(),
                rss.map_or_else(|| "?".to_owned(), |rss| rss.to_string()),
                sizes(messages, sender),
            );
        }
    }

    // the ring fills during the first reports, after that RSS must stay flat
    let rss = rss_samples.into_iter().flatten().collect::<Vec<_>>();
    if rss.len() >= 10 {
        let settled = rss[rss.len() / 2];
        let last = *rss.last().unwrap();
        assert!(
            last <= settled + settled / 5,
            "RSS grew from {settled} KiB to {last} KiB in the second half"
        );
    }
}

fn rss_kib() -> Option<u64> {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()?;
    String::from_utf8(output.stdout).ok()?.trim().parse().ok()
}
