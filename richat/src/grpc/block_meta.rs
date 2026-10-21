use {
    crate::{channel::ParsedMessage, metrics},
    ::metrics::gauge,
    foldhash::quality::RandomState,
    futures::future::TryFutureExt,
    richat_proto::geyser::{CommitmentLevel as CommitmentLevelProto, SlotStatus},
    solana_clock::{BankId, MAX_PROCESSING_AGE, Slot},
    std::{collections::HashMap, future::Future, sync::Arc},
    tokio::sync::oneshot,
    tonic::Status,
};

#[derive(Debug, Default, Clone)]
pub struct BlockMeta {
    pub slot: Slot,
    pub blockhash: Arc<String>,
    pub block_height: Slot,

    bank_id: Option<BankId>,
    /// bank selected by confirmed / finalized status
    selected_bank_id: Option<BankId>,
    processed: bool, // flag, means that we received block meta message
    confirmed: bool,
    finalized: bool,
}

impl BlockMeta {
    /// Alpenglow: replace with the candidate (bank) selected by confirmed / finalized status
    fn select_candidate(&mut self, candidates: Option<&Vec<BlockMeta>>, bank_id: Option<BankId>) {
        if self.selected_bank_id.is_none() {
            self.selected_bank_id = bank_id;
        }
        if let Some(bank_id) = bank_id
            && self.bank_id != Some(bank_id)
            && let Some(candidate) = candidates
                .and_then(|candidates| candidates.iter().find(|c| c.bank_id == Some(bank_id)))
        {
            self.set_candidate(candidate);
        }
    }

    fn set_candidate(&mut self, candidate: &BlockMeta) {
        self.slot = candidate.slot;
        self.blockhash = Arc::clone(&candidate.blockhash);
        self.block_height = candidate.block_height;
        self.bank_id = candidate.bank_id;
    }

    /// Without selected bank the latest candidate is used
    const fn accepts(&self, candidate: &BlockMeta) -> bool {
        match (self.selected_bank_id, candidate.bank_id) {
            (Some(selected), Some(bank_id)) => selected == bank_id,
            _ => true,
        }
    }
}

#[derive(Debug, Default)]
struct BlockStatus {
    last_valid_block_height: Slot,

    processed: bool, // flag, means that we received block meta message
    confirmed: bool,
    finalized: bool,
}

#[derive(Debug, Clone)]
pub struct BlockMetaStorage {
    messages_tx: kanal::AsyncSender<ParsedMessage>,
    requests_tx: kanal::AsyncSender<Request>,
}

impl BlockMetaStorage {
    pub fn new(request_queue_size: usize) -> (Self, impl Future<Output = anyhow::Result<()>>) {
        let (messages_tx, messages_rx) = kanal::unbounded_async();
        let (requests_tx, requests_rx) = kanal::bounded_async(request_queue_size);

        let me = Self {
            messages_tx,
            requests_tx,
        };
        let fut = tokio::spawn(Self::work(messages_rx, requests_rx)).map_err(anyhow::Error::new);

        (me, fut)
    }

    async fn work(
        messages_rx: kanal::AsyncReceiver<ParsedMessage>,
        requests_rx: kanal::AsyncReceiver<Request>,
    ) {
        let mut state = State::default();
        loop {
            tokio::select! {
                biased;
                message = messages_rx.recv() => match message {
                    Ok(message) => state.handle_message(message),
                    Err(_) => break,
                },
                request = requests_rx.recv() => {
                    gauge!(metrics::GRPC_BLOCK_META_QUEUE_SIZE).decrement(1);
                    match request {
                        Ok(Request::GetBlock(tx, commitment)) => {
                            let _ = tx.send(state.get_block(commitment));
                        }
                        Ok(Request::IsBlockhashValid(tx, blockhash, commitment)) => {
                            let _ = tx.send(state.is_blockhash_valid(blockhash, commitment));
                        }
                        Err(_) => break,
                    }
                }
            };
        }
    }

    pub fn push(&self, message: ParsedMessage) {
        let _ = self.messages_tx.try_send(message);
    }

    async fn send_request<T>(
        &self,
        request: Request,
        rx: oneshot::Receiver<Option<T>>,
    ) -> tonic::Result<T> {
        if self.requests_tx.try_send(request).is_err() {
            return Err(tonic::Status::resource_exhausted("queue channel is full"));
        }

        gauge!(metrics::GRPC_BLOCK_META_QUEUE_SIZE).increment(1);
        match rx.await {
            Ok(Some(block)) => Ok(block),
            Ok(None) => Err(Status::aborted("failed to get result")),
            Err(_) => Err(Status::aborted("failed to wait response")),
        }
    }

    pub async fn get_block(&self, commitment: CommitmentLevelProto) -> tonic::Result<BlockMeta> {
        let (tx, rx) = oneshot::channel();
        let request = Request::GetBlock(tx, commitment);
        self.send_request(request, rx).await
    }

    pub async fn is_blockhash_valid(
        &self,
        blockhash: String,
        commitment: CommitmentLevelProto,
    ) -> tonic::Result<(bool, Slot)> {
        let (tx, rx) = oneshot::channel();
        let request = Request::IsBlockhashValid(tx, blockhash, commitment);
        self.send_request(request, rx).await
    }
}

enum Request {
    GetBlock(oneshot::Sender<Option<BlockMeta>>, CommitmentLevelProto),
    IsBlockhashValid(
        oneshot::Sender<Option<(bool, Slot)>>,
        String,
        CommitmentLevelProto,
    ),
}

#[derive(Debug, Default)]
struct State {
    blocks: HashMap<Slot, BlockMeta, RandomState>,
    /// Alpenglow candidate blocks of not finalized slots
    candidates: HashMap<Slot, Vec<BlockMeta>, RandomState>,
    blockhashes: HashMap<Arc<String>, BlockStatus, RandomState>,
    processed: Slot,
    confirmed: Slot,
    finalized: Slot,
}

impl State {
    fn handle_message(&mut self, message: ParsedMessage) {
        let Self {
            blocks,
            candidates,
            blockhashes,
            processed,
            confirmed,
            finalized,
        } = self;
        match message {
            ParsedMessage::Slot(msg) => {
                let slot = msg.slot();
                let status = msg.status();
                if status == SlotStatus::SlotConfirmed {
                    let entry = blocks.entry(slot).or_default();
                    entry.select_candidate(candidates.get(&slot), msg.bank_id());
                    entry.confirmed = true;
                    // only block meta adds blockhashes, statuses can not bring back removed ones
                    if let Some(bentry) = blockhashes.get_mut(&entry.blockhash) {
                        bentry.confirmed = true;
                    }
                    *confirmed = slot;
                    gauge!(metrics::GRPC_BLOCK_META_SLOT, "commitment" => "confirmed")
                        .set(slot as f64);
                } else if status == SlotStatus::SlotFinalized {
                    let entry = blocks.entry(slot).or_default();
                    if !entry.confirmed {
                        entry.select_candidate(candidates.get(&slot), msg.bank_id());
                    }
                    entry.finalized = true;
                    if let Some(bentry) = blockhashes.get_mut(&entry.blockhash) {
                        bentry.finalized = true;
                    }
                    *finalized = slot;
                    gauge!(metrics::GRPC_BLOCK_META_SLOT, "commitment" => "finalized")
                        .set(slot as f64);

                    // cleanup, keep blockhashes that are still valid at the finalized block height
                    let block_height = entry.block_height;
                    blockhashes.retain(|_blockhash, bentry| {
                        bentry.last_valid_block_height >= block_height
                    });
                    blocks.retain(|bslot, _block| *bslot >= slot);
                    candidates.retain(|bslot, _block| *bslot > slot);
                }
            }
            ParsedMessage::BlockMeta(msg) => {
                let slot = msg.slot();
                let candidate = BlockMeta {
                    slot,
                    blockhash: Arc::new(msg.blockhash().to_owned()),
                    block_height: msg.block_height(),
                    bank_id: msg.bank_id(),
                    selected_bank_id: None,
                    processed: true,
                    confirmed: false,
                    finalized: false,
                };
                let bentry = blockhashes
                    .entry(Arc::clone(&candidate.blockhash))
                    .or_default();
                bentry.last_valid_block_height = candidate.block_height + MAX_PROCESSING_AGE as u64;
                let entry = blocks.entry(slot).or_default();
                if entry.accepts(&candidate) {
                    entry.set_candidate(&candidate);
                }
                entry.processed = true;
                if candidate.bank_id.is_some() {
                    candidates.entry(slot).or_default().push(candidate);
                }
                bentry.processed = true;
                *processed = (*processed).max(slot);
                gauge!(metrics::GRPC_BLOCK_META_SLOT, "commitment" => "processed").set(slot as f64);
            }
            ParsedMessage::EntryUpdateParent(msg) => {
                // Alpenglow: the bank was cleared, its block meta is invalid
                let slot = msg.slot();
                let cleared = Some(msg.cleared_bank_id());
                if let Some(list) = candidates.get_mut(&slot) {
                    for candidate in list.iter().filter(|c| c.bank_id == cleared) {
                        if blockhashes
                            .get(&candidate.blockhash)
                            .is_some_and(|b| !b.confirmed && !b.finalized)
                        {
                            blockhashes.remove(&candidate.blockhash);
                        }
                    }
                    list.retain(|c| c.bank_id != cleared);
                }
                if let Some(entry) = blocks.get_mut(&slot)
                    && entry.bank_id == cleared
                    && !entry.confirmed
                    && !entry.finalized
                {
                    match candidates.get(&slot).and_then(|list| list.last()) {
                        Some(candidate) => entry.set_candidate(candidate),
                        None => {
                            blocks.remove(&slot);
                        }
                    }
                }
            }
            _ => {}
        }
    }

    const fn commitment_slot(&self, commitment: CommitmentLevelProto) -> Slot {
        match commitment {
            CommitmentLevelProto::Processed => self.processed,
            CommitmentLevelProto::Confirmed => self.confirmed,
            CommitmentLevelProto::Finalized => self.finalized,
        }
    }

    fn get_block(&self, commitment: CommitmentLevelProto) -> Option<BlockMeta> {
        self.blocks.get(&self.commitment_slot(commitment)).cloned()
    }

    fn is_blockhash_valid(
        &self,
        blockhash: String,
        commitment: CommitmentLevelProto,
    ) -> Option<(bool, Slot)> {
        let block = self.blocks.get(&self.commitment_slot(commitment))?;
        let entry = self.blockhashes.get(&blockhash)?;
        let valid = block.block_height < entry.last_valid_block_height;
        Some((valid, block.slot))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        prost::Message as _,
        richat_filter::message::{Message, MessageParserEncoding},
        richat_proto::{
            geyser::{
                SubscribeUpdate, SubscribeUpdateBlockMeta, SubscribeUpdateSlot,
                subscribe_update::UpdateOneof,
            },
            solana::storage::confirmed_block::BlockHeight,
        },
        std::borrow::Cow,
    };

    fn parse(update: UpdateOneof) -> ParsedMessage {
        let data = SubscribeUpdate {
            filters: vec![],
            update_oneof: Some(update),
            created_at: Some(prost_types::Timestamp::default()),
        }
        .encode_to_vec();
        Message::parse(Cow::Owned(data), MessageParserEncoding::Prost)
            .unwrap()
            .into()
    }

    fn block_meta(slot: Slot) -> ParsedMessage {
        parse(UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
            slot,
            blockhash: blockhash(slot),
            parent_slot: slot - 1,
            block_height: Some(BlockHeight { block_height: slot }),
            ..Default::default()
        }))
    }

    fn status(slot: Slot, status: SlotStatus) -> ParsedMessage {
        parse(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent: Some(slot - 1),
            status: status as i32,
            ..Default::default()
        }))
    }

    fn blockhash(slot: Slot) -> String {
        format!("blockhash-{slot}")
    }

    #[test]
    fn blockhashes_are_bounded_by_max_processing_age_and_stay_valid() {
        const LAG: Slot = 32;
        let mut state = State::default();
        let mut blockhashes_max = 0;
        for slot in 1..=20_000 {
            // every 10th slot is skipped
            if slot % 10 != 0 {
                state.handle_message(block_meta(slot));
                state.handle_message(status(slot, SlotStatus::SlotProcessed));
            }
            // statuses arrive after block meta of the next slots, like on mainnet
            if let Some(slot) = slot.checked_sub(LAG / 2)
                && slot % 10 != 0
            {
                state.handle_message(status(slot, SlotStatus::SlotConfirmed));
            }
            if let Some(slot) = slot.checked_sub(LAG)
                && slot % 10 != 0
            {
                state.handle_message(status(slot, SlotStatus::SlotFinalized));
            }
            blockhashes_max = blockhashes_max.max(state.blockhashes.len());
            assert!(state.blocks.len() <= LAG as usize + 2, "slot {slot}");
        }

        // valid hashes of the finalization window and of the next `MAX_PROCESSING_AGE` blocks
        assert!(
            blockhashes_max <= LAG as usize + MAX_PROCESSING_AGE + 1,
            "{blockhashes_max}"
        );
        // recent blockhashes are known and valid
        for (slot, commitment) in [
            (20_000 - 1, CommitmentLevelProto::Processed),
            (20_000 - 1, CommitmentLevelProto::Finalized),
            (20_000 - LAG - 100, CommitmentLevelProto::Finalized),
        ] {
            assert!(
                matches!(
                    state.is_blockhash_valid(blockhash(slot), commitment),
                    Some((true, _))
                ),
                "{slot} {commitment:?}"
            );
        }
        // expired blockhash is removed
        assert_eq!(
            state.is_blockhash_valid(blockhash(1), CommitmentLevelProto::Finalized),
            None
        );
    }
}
