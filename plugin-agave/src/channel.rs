// Based on https://github.com/tokio-rs/tokio/blob/master/tokio/src/sync/broadcast.rs
use {
    crate::{
        config::ConfigChannel,
        metrics,
        plugin::PluginNotification,
        protobuf::{ProtobufEncoder, ProtobufMessage},
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
    futures::stream::{Stream, StreamExt},
    log::{debug, error},
    metrics_exporter_prometheus::PrometheusRecorder,
    richat_metrics::{MaybeRecorder, counter, gauge},
    richat_proto::richat::RichatFilter,
    richat_shared::{
        mutex_lock,
        transports::{RecvError, RecvItem, RecvStream, Subscribe, SubscribeError},
    },
    smallvec::SmallVec,
    solana_clock::Slot,
    std::{
        collections::BTreeMap,
        fmt,
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex, MutexGuard},
        task::{Context, Poll, Waker},
    },
};

#[derive(Debug, Clone)]
pub struct Sender {
    shared: Arc<Shared>,
    recorder: Arc<MaybeRecorder<PrometheusRecorder>>,
}

impl Sender {
    pub fn new(config: ConfigChannel, recorder: Arc<MaybeRecorder<PrometheusRecorder>>) -> Self {
        let max_messages = config.max_messages.next_power_of_two();
        let mut buffer = Vec::with_capacity(max_messages);
        for i in 0..max_messages {
            buffer.push(Mutex::new(Item {
                pos: i as u64,
                slot: 0,
                data: None,
                closed: false,
            }));
        }

        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                head: max_messages as u64 + 1,
                tail: max_messages as u64,
                slots: BTreeMap::new(),
                bytes_total: 0,
                bytes_max: config.max_bytes,
                wakers: Vec::with_capacity(16),
            }),
            mask: (max_messages - 1) as u64,
            buffer: buffer.into_boxed_slice(),
        });

        Self { shared, recorder }
    }

    pub fn push(&self, message: ProtobufMessage, encoder: ProtobufEncoder) {
        // encode message and allocate shared buffer before taking the lock
        let data = Arc::new(message.encode(encoder));

        // acquire state lock
        let mut state = self.shared.state_lock();

        // In March 2023 in Triton One we noticed that sometimes we do not receive
        // slots with Confirmed status, I'm not sure that this still a case but for
        // safety I added this hack
        let slot_status = if let ProtobufMessage::Slot { slot, status, .. } = &message {
            Some((*slot, *status))
        } else {
            None
        };

        let mut messages = SmallVec::<[(ProtobufMessage, RecvItem); 2]>::new();
        messages.push((message, data));

        if let Some((slot, status)) = slot_status {
            let mut slots = SmallVec::<[Slot; 4]>::new();
            slots.push(slot);

            while let Some((parent, Some(entry))) = slots
                .pop()
                .and_then(|slot| state.slots.get(&slot))
                .and_then(|entry| entry.parent_slot)
                .map(|parent| (parent, state.slots.get_mut(&parent)))
            {
                if (*status == SlotStatus::Confirmed && !entry.confirmed)
                    || (*status == SlotStatus::Rooted && !entry.finalized)
                {
                    slots.push(parent);

                    let message = ProtobufMessage::Slot {
                        slot: parent,
                        parent: entry.parent_slot,
                        status,
                        // bank of the parent is not known here
                        bank_id: None,
                    };
                    let data = Arc::new(message.encode(encoder));
                    messages.push((message, data));

                    error!("missed slot status update for {} ({:?})", parent, *status);
                    if matches!(status, SlotStatus::Confirmed | SlotStatus::Rooted) {
                        counter!(&self.recorder, metrics::GEYSER_MISSED_SLOT_STATUS, "status" => status.as_str())
                            .increment(1);
                    }
                }
            }
        }

        // push messages
        let mut evicted = SmallVec::<[RecvItem; 8]>::new();
        for (message, data) in messages.into_iter().rev() {
            self.push_msg(&mut state, message, data, &mut evicted);
        }

        // wake receivers and free evicted messages after the lock is released,
        // callbacks from replay threads wait on this lock
        let wakers = if state.wakers.is_empty() {
            Vec::new()
        } else {
            std::mem::take(&mut state.wakers)
        };
        drop(state);
        for waker in wakers {
            waker.wake();
        }
        drop(evicted);
    }

    fn push_msg(
        &self,
        state: &mut MutexGuard<'_, State>,
        message: ProtobufMessage,
        data: RecvItem,
        evicted: &mut SmallVec<[RecvItem; 8]>,
    ) {
        let mut removed_max_slot = None;

        // bump current tail
        state.tail = state.tail.wrapping_add(1);

        // update slots info
        // messages not related to any slot (contact info) are attached to the latest known slot
        let slot = match message.get_slot() {
            Some(slot) => {
                let head = state.tail;
                let entry = state.slots.entry(slot).or_insert_with(|| SlotInfo {
                    head,
                    parent_slot: None,
                    confirmed: false,
                    finalized: false,
                });
                if let ProtobufMessage::Slot { parent, status, .. } = &message {
                    if let Some(parent) = parent {
                        entry.parent_slot = Some(*parent);
                    }
                    if **status == SlotStatus::Confirmed {
                        entry.confirmed = true;
                    } else if **status == SlotStatus::Rooted {
                        entry.finalized = true;
                    }
                }
                slot
            }
            None => state
                .slots
                .last_key_value()
                .map(|(slot, _info)| *slot)
                .unwrap_or_default(),
        };

        // lock and update item
        state.bytes_total += data.len();
        let idx = self.shared.get_idx(state.tail);
        let mut item = self.shared.buffer_idx(idx);
        if let Some((_, data)) = item.data.take() {
            state.head = state.head.wrapping_add(1);
            state.bytes_total -= data.len();
            removed_max_slot = Some(item.slot);
            evicted.push(data);
        }
        item.pos = state.tail;
        item.slot = slot;
        item.data = Some((PluginNotification::from(&message), data));
        drop(item);

        // drop extra messages by max bytes
        while state.bytes_total >= state.bytes_max && state.head < state.tail {
            let idx = self.shared.get_idx(state.head);
            let mut item = self.shared.buffer_idx(idx);
            let Some((_, data)) = item.data.take() else {
                panic!("nothing to remove to keep bytes under limit")
            };

            state.head = state.head.wrapping_add(1);
            state.bytes_total -= data.len();
            evicted.push(data);
            removed_max_slot = Some(match removed_max_slot {
                Some(slot) => item.slot.max(slot),
                None => item.slot,
            });
        }

        // remove not-complete slots
        if let Some(remove_upto) = removed_max_slot {
            loop {
                match state.slots.first_key_value() {
                    Some((slot, _)) if *slot <= remove_upto => {
                        let slot = *slot;
                        state.slots.remove(&slot);
                    }
                    _ => break,
                }
            }
        }

        // update metrics
        if let ProtobufMessage::Slot { status, .. } = message {
            if !matches!(status, SlotStatus::Dead(_)) {
                gauge!(&self.recorder, metrics::GEYSER_SLOT_STATUS, "status" => status.as_str())
                    .set(slot as f64);
            }
            if *status == SlotStatus::Processed {
                debug!(
                    "new processed {slot} / {} messages / {} slots / {} bytes",
                    state.tail - state.head,
                    state.slots.len(),
                    state.bytes_total
                );

                gauge!(&self.recorder, metrics::CHANNEL_MESSAGES_TOTAL)
                    .set((state.tail - state.head) as f64);
                gauge!(&self.recorder, metrics::CHANNEL_SLOTS_TOTAL).set(state.slots.len() as f64);
                gauge!(&self.recorder, metrics::CHANNEL_BYTES_TOTAL).set(state.bytes_total as f64);
            }
        }
    }

    pub fn close(&self) {
        for idx in 0..self.shared.buffer.len() {
            self.shared.buffer_idx(idx).closed = true;
        }

        let mut state = self.shared.state_lock();
        for waker in state.wakers.drain(..) {
            waker.wake();
        }
    }
}

impl Subscribe for Sender {
    fn subscribe(
        &self,
        replay_from_slot: Option<Slot>,
        filter: Option<RichatFilter>,
    ) -> Result<RecvStream, SubscribeError> {
        let shared = Arc::clone(&self.shared);

        let state = shared.state_lock();
        let next = match replay_from_slot {
            Some(slot) => state.slots.get(&slot).map(|s| s.head).ok_or_else(|| {
                match state.slots.first_key_value() {
                    Some((key, _value)) => SubscribeError::SlotNotAvailable {
                        first_available: *key,
                    },
                    None => SubscribeError::NotInitialized,
                }
            })?,
            None => state.tail,
        };
        drop(state);

        let filter = filter.unwrap_or_default();

        Ok(Receiver {
            shared,
            next,
            finished: false,
            enable_notifications_accounts: !filter.disable_accounts,
            enable_notifications_transactions: !filter.disable_transactions,
            enable_notifications_entries: !filter.disable_entries,
            enable_notifications_deshred_transactions: filter.enable_deshred_transactions,
            enable_notifications_contact_info: filter.enable_contact_info,
            enable_notifications_block_footers: filter.enable_block_footers,
            enable_notifications_entry_update_parents: filter.enable_entry_update_parents,
        }
        .boxed())
    }
}

#[derive(Debug)]
pub struct Receiver {
    shared: Arc<Shared>,
    next: u64,
    finished: bool,
    enable_notifications_accounts: bool,
    enable_notifications_transactions: bool,
    enable_notifications_entries: bool,
    enable_notifications_deshred_transactions: bool,
    enable_notifications_contact_info: bool,
    enable_notifications_block_footers: bool,
    enable_notifications_entry_update_parents: bool,
}

impl Receiver {
    pub async fn recv(&mut self) -> Result<RecvItem, RecvError> {
        Recv::new(self).await
    }

    pub fn recv_ref(&mut self, waker: &Waker) -> Result<Option<RecvItem>, RecvError> {
        loop {
            // read item with next value
            let idx = self.shared.get_idx(self.next);
            let mut item = self.shared.buffer_idx(idx);
            if item.closed {
                return Err(RecvError::Closed);
            }

            if item.pos != self.next {
                // release lock before attempting to acquire state
                drop(item);

                // acquire state to store waker
                let mut state = self.shared.state_lock();

                // make sure that position did not changed
                item = self.shared.buffer_idx(idx);
                if item.closed {
                    return Err(RecvError::Closed);
                }
                if item.pos != self.next {
                    return if item.pos < self.next {
                        state.wakers.push(waker.clone());
                        Ok(None)
                    } else {
                        Err(RecvError::Lagged)
                    };
                }
            }

            self.next = self.next.wrapping_add(1);
            // check the type before cloning, skipped messages do not touch the shared `Arc`
            let Some((plugin_notification, data)) = item.data.as_ref() else {
                return Err(RecvError::Lagged);
            };
            let enabled = match plugin_notification {
                PluginNotification::Account => self.enable_notifications_accounts,
                PluginNotification::Transaction => self.enable_notifications_transactions,
                PluginNotification::Entry => self.enable_notifications_entries,
                PluginNotification::DeshredTransaction => {
                    self.enable_notifications_deshred_transactions
                }
                PluginNotification::ContactInfo => self.enable_notifications_contact_info,
                PluginNotification::BlockFooter => self.enable_notifications_block_footers,
                PluginNotification::EntryUpdateParent => {
                    self.enable_notifications_entry_update_parents
                }
                PluginNotification::Slot | PluginNotification::BlockMeta => true,
            };
            if enabled {
                break Ok(Some(Arc::clone(data)));
            }
        }
    }
}

struct Recv<'a> {
    receiver: &'a mut Receiver,
}

impl<'a> Recv<'a> {
    const fn new(receiver: &'a mut Receiver) -> Self {
        Self { receiver }
    }
}

impl Future for Recv<'_> {
    type Output = Result<RecvItem, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let me = self.get_mut();
        let receiver: &mut Receiver = me.receiver;

        match receiver.recv_ref(cx.waker()) {
            Ok(Some(value)) => Poll::Ready(Ok(value)),
            Ok(None) => Poll::Pending,
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl Stream for Receiver {
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

struct Shared {
    state: Mutex<State>,
    mask: u64,
    buffer: Box<[Mutex<Item>]>,
}

impl fmt::Debug for Shared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Shared").field("mask", &self.mask).finish()
    }
}

impl Shared {
    #[inline]
    const fn get_idx(&self, pos: u64) -> usize {
        (pos & self.mask) as usize
    }

    #[inline]
    fn state_lock(&self) -> MutexGuard<'_, State> {
        mutex_lock(&self.state)
    }

    #[inline]
    fn buffer_idx(&self, idx: usize) -> MutexGuard<'_, Item> {
        mutex_lock(&self.buffer[idx])
    }
}

struct State {
    head: u64,
    tail: u64,
    slots: BTreeMap<Slot, SlotInfo>,
    bytes_total: usize,
    bytes_max: usize,
    wakers: Vec<Waker>,
}

struct SlotInfo {
    head: u64,
    parent_slot: Option<Slot>,
    confirmed: bool,
    finalized: bool,
}

struct Item {
    pos: u64,
    slot: Slot,
    data: Option<(PluginNotification, RecvItem)>,
    closed: bool,
}

#[cfg(test)]
mod tests {
    use {
        super::Sender,
        crate::{
            config::ConfigChannel,
            protobuf::{ProtobufEncoder, ProtobufMessage},
        },
        agave_geyser_plugin_interface::geyser_plugin_interface::SlotStatus,
        futures::{FutureExt, StreamExt},
        prost::Message,
        richat_benches::fixtures::generate_block_footers,
        richat_metrics::MaybeRecorder,
        richat_proto::{
            geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
            richat::RichatFilter,
        },
        richat_shared::transports::{RecvError, Subscribe, SubscribeError},
        solana_clock::Slot,
        std::{sync::Arc, time::Duration},
    };

    fn decoded_slot(data: &[u8]) -> Slot {
        match SubscribeUpdate::decode(data).unwrap().update_oneof {
            Some(UpdateOneof::Slot(msg)) => msg.slot,
            update => panic!("unexpected update: {update:?}"),
        }
    }

    fn sender(max_messages: usize, max_bytes: usize) -> Sender {
        Sender::new(
            ConfigChannel {
                encoder: ProtobufEncoder::Raw,
                max_messages,
                max_bytes,
            },
            Arc::new(MaybeRecorder::Noop),
        )
    }

    fn slot(slot: Slot) -> ProtobufMessage<'static> {
        ProtobufMessage::Slot {
            slot,
            parent: Some(slot - 1),
            status: &SlotStatus::Processed,
            bank_id: None,
        }
    }

    #[tokio::test]
    async fn receiver_is_woken_and_skips_disabled_notifications() {
        let sender = sender(16, 1 << 20);
        sender.push(slot(1), ProtobufEncoder::Raw);
        let mut rx = sender
            .subscribe(None, Some(RichatFilter::default()))
            .unwrap();
        // a new receiver starts from the latest message
        let data = rx.next().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(decoded_slot(&data), 1);
        assert!(rx.next().now_or_never().is_none(), "nothing to receive yet");

        let waiting = tokio::spawn(async move { rx.next().await });
        tokio::time::sleep(Duration::from_millis(10)).await;
        // footers are disabled in the default `RichatFilter`
        let footers = generate_block_footers();
        sender.push(
            ProtobufMessage::BlockFooter {
                slot: 2,
                bank_id: footers[0].bank_id,
                block_footer: &footers[0].block_footer,
            },
            ProtobufEncoder::Raw,
        );
        sender.push(slot(2), ProtobufEncoder::Raw);

        let data = tokio::time::timeout(Duration::from_secs(5), waiting)
            .await
            .expect("receiver was not woken")
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(decoded_slot(&data), 2);
    }

    #[test]
    fn evicted_messages_lag_receivers_and_drop_slots() {
        let sender = sender(4, 1 << 20);
        sender.push(slot(1), ProtobufEncoder::Raw);
        let mut rx = sender.subscribe(Some(1), None).unwrap();
        for slot_value in 2..10 {
            sender.push(slot(slot_value), ProtobufEncoder::Raw);
        }
        assert_eq!(
            rx.next().now_or_never(),
            Some(Some(Err(RecvError::Lagged))),
            "max_messages evicted unread messages"
        );
        assert!(matches!(
            sender.subscribe(Some(1), None),
            Err(SubscribeError::SlotNotAvailable { first_available: 6 })
        ));

        // max_bytes keeps only the last message
        let size = slot(10).encode(ProtobufEncoder::Raw).len();
        let sender = self::sender(16, size + 1);
        for slot_value in 10..13 {
            sender.push(slot(slot_value), ProtobufEncoder::Raw);
        }
        let mut rx = sender.subscribe(Some(12), None).unwrap();
        let data = rx.next().now_or_never().unwrap().unwrap().unwrap();
        assert_eq!(decoded_slot(&data), 12);
        assert!(matches!(
            sender.subscribe(Some(11), None),
            Err(SubscribeError::SlotNotAvailable {
                first_available: 12
            })
        ));
    }
}
