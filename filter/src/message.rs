use {
    crate::protobuf::decode::{
        LimitedDecode, SubscribeUpdateLimitedDecode, UpdateOneofLimitedDecode,
        UpdateOneofLimitedDecodeAccount, UpdateOneofLimitedDecodeEntry,
        UpdateOneofLimitedDecodeSlot, UpdateOneofLimitedDecodeTransaction,
        UpdateOneofLimitedDecodeTransactionInfo, nonzero_bank_id,
    },
    prost::{
        Message as _,
        bytes::Buf,
        encoding::{decode_varint, encode_varint, encoded_len_varint},
    },
    prost_types::Timestamp,
    richat_proto::{
        convert_from, convert_to,
        geyser::{
            SlotStatus, SubscribeUpdate, SubscribeUpdateAccountInfo, SubscribeUpdateBlockFooter,
            SubscribeUpdateBlockMeta, SubscribeUpdateEntry, SubscribeUpdateEntryUpdateParent,
            SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
        },
        richat::{
            SubscribeUpdateBlockFooter as SubscribeUpdateBlockFooterRichat,
            SubscribeUpdateEntryUpdateParent as SubscribeUpdateEntryUpdateParentRichat,
            SubscribeUpdateRichat, subscribe_update_richat::UpdateOneof as UpdateOneofRichat,
        },
        solana::storage::confirmed_block::{TransactionError, TransactionStatusMeta},
    },
    serde::{Deserialize, Serialize},
    solana_account::ReadableAccount,
    solana_clock::{BankId, Epoch, Slot},
    solana_pubkey::{PUBKEY_BYTES, Pubkey},
    solana_signature::{SIGNATURE_BYTES, Signature},
    solana_transaction_status::{
        ConfirmedBlock, TransactionWithStatusMeta, VersionedTransactionWithStatusMeta,
    },
    std::{
        borrow::Cow,
        collections::HashSet,
        ops::{Deref, Range},
        sync::{Arc, OnceLock},
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum MessageParseError {
    #[error(transparent)]
    Prost(#[from] prost::DecodeError),
    #[error("Field `{0}` should be defined")]
    FieldNotDefined(&'static str),
    #[error("Invalid enum value: {0}")]
    InvalidEnumValue(i32),
    #[error("Invalid pubkey length")]
    InvalidPubkey,
    #[error("Invalid update: {0}")]
    InvalidUpdateMessage(&'static str),
    #[error("Incompatible encoding")]
    IncompatibleEncoding,
    #[error("Invalid block footer: {0}")]
    InvalidBlockFooter(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageParserEncoding {
    /// Use optimized parser to extract only required fields
    Limited,
    /// Parse full message with `prost`
    Prost,
}

#[derive(Debug, Clone, Copy)]
pub enum MessageRef<'a> {
    Slot(&'a MessageSlot),
    Account(&'a MessageAccount),
    Transaction(&'a MessageTransaction),
    Entry(&'a MessageEntry),
    BlockMeta(&'a MessageBlockMeta),
    Block(&'a MessageBlock),
    BlockFooter(&'a MessageBlockFooter),
    EntryUpdateParent(&'a MessageEntryUpdateParent),
}

impl<'a> From<&'a Message> for MessageRef<'a> {
    fn from(message: &'a Message) -> Self {
        match message {
            Message::Slot(msg) => Self::Slot(msg),
            Message::Account(msg) => Self::Account(msg),
            Message::Transaction(msg) => Self::Transaction(msg),
            Message::Entry(msg) => Self::Entry(msg),
            Message::BlockMeta(msg) => Self::BlockMeta(msg),
            Message::Block(msg) => Self::Block(msg),
            Message::BlockFooter(msg) => Self::BlockFooter(msg),
            Message::EntryUpdateParent(msg) => Self::EntryUpdateParent(msg),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Message {
    Slot(MessageSlot),
    Account(MessageAccount),
    Transaction(MessageTransaction),
    Entry(MessageEntry),
    BlockMeta(MessageBlockMeta),
    Block(MessageBlock),
    BlockFooter(MessageBlockFooter),
    EntryUpdateParent(MessageEntryUpdateParent),
}

impl Message {
    pub fn parse(
        data: Cow<'_, [u8]>,
        parser: MessageParserEncoding,
    ) -> Result<Self, MessageParseError> {
        match parser {
            MessageParserEncoding::Limited => MessageParserLimited::parse(data),
            MessageParserEncoding::Prost => MessageParserProst::parse(data),
        }
    }

    pub fn create_block(
        accounts: Vec<Arc<MessageAccount>>,
        transactions: Vec<Arc<MessageTransaction>>,
        entries: Vec<Arc<MessageEntry>>,
        block_meta: Arc<MessageBlockMeta>,
        created_at: impl Into<MessageBlockCreatedAt>,
    ) -> Result<Self, MessageParseError> {
        let created_at = created_at.into();
        let created_at_encoding = created_at.encoding();

        for encoding in std::iter::once(block_meta.encoding())
            .chain(accounts.iter().map(|x| x.encoding()))
            .chain(transactions.iter().map(|x| x.encoding()))
            .chain(entries.iter().map(|x| x.encoding()))
        {
            if encoding != created_at_encoding {
                return Err(MessageParseError::IncompatibleEncoding);
            }
        }

        Ok(Self::Block(Self::unchecked_create_block(
            accounts,
            transactions,
            entries,
            block_meta,
            created_at,
        )))
    }

    pub const fn unchecked_create_block(
        accounts: Vec<Arc<MessageAccount>>,
        transactions: Vec<Arc<MessageTransaction>>,
        entries: Vec<Arc<MessageEntry>>,
        block_meta: Arc<MessageBlockMeta>,
        created_at: MessageBlockCreatedAt,
    ) -> MessageBlock {
        MessageBlock {
            accounts,
            transactions,
            entries,
            block_meta,
            created_at,
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
            Self::BlockFooter(msg) => msg.slot(),
            Self::EntryUpdateParent(msg) => msg.slot(),
        }
    }

    /// Bank of the message, `None` for messages not tied to a bank
    /// (`FirstShredReceived` / `Completed` / `Dead` slot statuses, sources without Alpenglow support)
    pub fn bank_id(&self) -> Option<BankId> {
        match self {
            Self::Slot(msg) => msg.bank_id(),
            Self::Account(msg) => msg.bank_id(),
            Self::Transaction(msg) => msg.bank_id(),
            Self::Entry(msg) => msg.bank_id(),
            Self::BlockMeta(msg) => msg.bank_id(),
            Self::Block(msg) => msg.bank_id(),
            Self::BlockFooter(msg) => msg.bank_id(),
            Self::EntryUpdateParent(msg) => Some(msg.cleared_bank_id()),
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Slot(msg) => msg.created_at(),
            Self::Account(msg) => msg.created_at(),
            Self::Transaction(msg) => msg.created_at(),
            Self::Entry(msg) => msg.created_at(),
            Self::BlockMeta(msg) => msg.created_at(),
            Self::Block(msg) => msg.created_at(),
            Self::BlockFooter(msg) => msg.created_at(),
            Self::EntryUpdateParent(msg) => msg.created_at(),
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
            Self::BlockFooter(msg) => msg.size(),
            Self::EntryUpdateParent(msg) => msg.size(),
        }
    }

    fn parse_richat(
        update: SubscribeUpdateRichat,
        size: usize,
        encoding: MessageParserEncoding,
    ) -> Result<Self, MessageParseError> {
        let created_at = update
            .created_at
            .ok_or(MessageParseError::FieldNotDefined("created_at"))?;
        match update.update_oneof {
            Some(UpdateOneofRichat::BlockFooter(footer)) => {
                Self::parse_richat_block_footer(footer, created_at, size, encoding)
            }
            Some(UpdateOneofRichat::EntryUpdateParent(update)) => Ok(
                Self::parse_richat_entry_update_parent(update, created_at, size, encoding),
            ),
            Some(_) => Err(MessageParseError::InvalidUpdateMessage("Richat")),
            None => Err(MessageParseError::FieldNotDefined("update_oneof")),
        }
    }

    fn parse_richat_block_footer(
        footer: SubscribeUpdateBlockFooterRichat,
        created_at: Timestamp,
        size: usize,
        encoding: MessageParserEncoding,
    ) -> Result<Self, MessageParseError> {
        let footer = convert_to::create_block_footer_from_wincode(
            footer.slot,
            footer.bank_id,
            &footer.footer,
        )
        .map_err(MessageParseError::InvalidBlockFooter)?;
        Ok(Self::BlockFooter(MessageBlockFooter::new(
            footer, created_at, size, encoding,
        )))
    }

    fn parse_richat_entry_update_parent(
        update: SubscribeUpdateEntryUpdateParentRichat,
        created_at: Timestamp,
        size: usize,
        encoding: MessageParserEncoding,
    ) -> Self {
        Self::EntryUpdateParent(MessageEntryUpdateParent::new(
            SubscribeUpdateEntryUpdateParent {
                slot: update.slot,
                cleared_bank_id: update.cleared_bank_id,
                parent_slot: update.parent_slot,
                parent_block_id: update.parent_block_id,
            },
            created_at,
            size,
            encoding,
        ))
    }
}

#[derive(Debug)]
pub struct MessageParserLimited;

impl MessageParserLimited {
    pub fn parse(data: Cow<'_, [u8]>) -> Result<Message, MessageParseError> {
        let data = data.into_owned();
        let update = SubscribeUpdateLimitedDecode::decode(data.as_slice())?;
        let created_at = update
            .created_at
            .ok_or(MessageParseError::FieldNotDefined("created_at"))?;

        let Some(update_oneof) = update.update_oneof else {
            return if update.richat_update {
                // Richat-only updates are rare (block footers / update parents), decode with prost
                let size = data.len();
                Message::parse_richat(
                    SubscribeUpdateRichat::decode(data.as_slice())?,
                    size,
                    MessageParserEncoding::Limited,
                )
            } else {
                Err(MessageParseError::FieldNotDefined("update_oneof"))
            };
        };

        Ok(match update_oneof {
            UpdateOneofLimitedDecode::Slot(range) => {
                let message =
                    UpdateOneofLimitedDecodeSlot::decode(&data.as_slice()[range.start..range.end])?;
                Message::Slot(MessageSlot::Limited {
                    slot: message.slot,
                    parent: message.parent,
                    status: SlotStatus::try_from(message.status)
                        .map_err(|_| MessageParseError::InvalidEnumValue(message.status))?,
                    dead_error: message.dead_error,
                    bank_id: message.bank_id,
                    created_at,
                    buffer: data,
                    range,
                })
            }
            UpdateOneofLimitedDecode::Account(range) => {
                let message = UpdateOneofLimitedDecodeAccount::decode(
                    &data.as_slice()[range.start..range.end],
                )?;

                if message.account == usize::MAX {
                    return Err(MessageParseError::FieldNotDefined("account"));
                }

                let mut data_range = message.data;
                data_range.start += range.start;
                data_range.end += range.start;

                Message::Account(MessageAccount::Limited {
                    pubkey: message.pubkey,
                    owner: message.owner,
                    lamports: message.lamports,
                    executable: message.executable,
                    rent_epoch: message.rent_epoch,
                    data: data_range,
                    txn_signature_offset: message
                        .txn_signature_offset
                        .map(|offset| offset + range.start),
                    write_version: message.write_version + range.start,
                    slot: message.slot,
                    is_startup: message.is_startup,
                    bank_id: message.bank_id,
                    created_at,
                    buffer: data,
                    account_offset: message.account + range.start,
                    range,
                })
            }
            UpdateOneofLimitedDecode::Transaction(range) => {
                let message = UpdateOneofLimitedDecodeTransaction::decode(
                    &data.as_slice()[range.start..range.end],
                )?;
                let mut transaction_range = message
                    .transaction
                    .ok_or(MessageParseError::FieldNotDefined("transaction"))?;
                transaction_range.start += range.start;
                transaction_range.end += range.start;

                let tx_info = UpdateOneofLimitedDecodeTransactionInfo::decode(
                    &data.as_slice()[transaction_range.start..transaction_range.end],
                )?;

                let error = tx_info
                    .err
                    .map(|err_range| {
                        TransactionError::decode(
                            &data.as_slice()[transaction_range.start + err_range.start
                                ..transaction_range.start + err_range.end],
                        )
                    })
                    .transpose()?;

                Message::Transaction(MessageTransaction::Limited {
                    signature_offset: tx_info
                        .signature_offset
                        .map(|offset| transaction_range.start + offset),
                    error,
                    account_keys: tx_info.account_keys,
                    is_vote: tx_info.is_vote,
                    index: tx_info.index,
                    transaction_range,
                    transaction: OnceLock::new(),
                    slot: message.slot,
                    bank_id: message.bank_id,
                    created_at,
                    buffer: data,
                    range,
                })
            }
            UpdateOneofLimitedDecode::TransactionStatus(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("TransactionStatus"));
            }
            UpdateOneofLimitedDecode::Entry(range) => {
                let entry = UpdateOneofLimitedDecodeEntry::decode(
                    &data.as_slice()[range.start..range.end],
                )?;
                Message::Entry(MessageEntry::Limited {
                    slot: entry.slot,
                    index: entry.index,
                    executed_transaction_count: entry.executed_transaction_count,
                    bank_id: entry.bank_id,
                    created_at,
                    buffer: data,
                    range,
                })
            }
            UpdateOneofLimitedDecode::BlockMeta(range) => {
                let block_meta =
                    SubscribeUpdateBlockMeta::decode(&data.as_slice()[range.start..range.end])?;

                let block_height = block_meta
                    .block_height
                    .map(|v| v.block_height)
                    .ok_or(MessageParseError::FieldNotDefined("block_height"))?;

                Message::BlockMeta(MessageBlockMeta::Limited {
                    block_meta,
                    block_height,
                    created_at,
                    buffer: data,
                    range,
                })
            }
            UpdateOneofLimitedDecode::Block(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("Block"));
            }
            UpdateOneofLimitedDecode::Ping(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("Ping"));
            }
            UpdateOneofLimitedDecode::Pong(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("Pong"));
            }
            UpdateOneofLimitedDecode::BlockFooter(range) => {
                let footer =
                    SubscribeUpdateBlockFooter::decode(&data.as_slice()[range.start..range.end])?;
                Message::BlockFooter(MessageBlockFooter::new(
                    footer,
                    created_at,
                    data.len(),
                    MessageParserEncoding::Limited,
                ))
            }
            UpdateOneofLimitedDecode::EntryUpdateParent(range) => {
                let update = SubscribeUpdateEntryUpdateParent::decode(
                    &data.as_slice()[range.start..range.end],
                )?;
                Message::EntryUpdateParent(MessageEntryUpdateParent::new(
                    update,
                    created_at,
                    data.len(),
                    MessageParserEncoding::Limited,
                ))
            }
        })
    }
}

#[derive(Debug)]
pub struct MessageParserProst;

impl MessageParserProst {
    pub fn parse(data: Cow<'_, [u8]>) -> Result<Message, MessageParseError> {
        let update = SubscribeUpdate::decode(data.deref())?;
        let encoded_len = data.len();

        let created_at = update
            .created_at
            .ok_or(MessageParseError::FieldNotDefined("created_at"))?;

        let Some(update_oneof) = update.update_oneof else {
            // Yellowstone `SubscribeUpdate` skips Richat-only updates (tags `100+`)
            return Message::parse_richat(
                SubscribeUpdateRichat::decode(data.deref())?,
                encoded_len,
                MessageParserEncoding::Prost,
            );
        };

        Ok(match update_oneof {
            UpdateOneof::Slot(message) => Message::Slot(MessageSlot::Prost {
                slot: message.slot,
                parent: message.parent,
                status: SlotStatus::try_from(message.status)
                    .map_err(|_| MessageParseError::InvalidEnumValue(message.status))?,
                dead_error: message.dead_error,
                bank_id: message.bank_id,
                created_at,
                size: encoded_len,
            }),
            UpdateOneof::Account(message) => {
                let account = message
                    .account
                    .ok_or(MessageParseError::FieldNotDefined("account"))?;
                Message::Account(MessageAccount::Prost {
                    pubkey: account
                        .pubkey
                        .as_slice()
                        .try_into()
                        .map_err(|_| MessageParseError::InvalidPubkey)?,
                    owner: account
                        .owner
                        .as_slice()
                        .try_into()
                        .map_err(|_| MessageParseError::InvalidPubkey)?,
                    account,
                    slot: message.slot,
                    is_startup: message.is_startup,
                    bank_id: message.bank_id,
                    created_at,
                    size: PUBKEY_BYTES + PUBKEY_BYTES + encoded_len + 20,
                })
            }
            UpdateOneof::Transaction(message) => {
                let transaction = message
                    .transaction
                    .ok_or(MessageParseError::FieldNotDefined("transaction"))?;
                let meta = transaction
                    .meta
                    .as_ref()
                    .ok_or(MessageParseError::FieldNotDefined("meta"))?;

                let account_keys = MessageTransaction::gen_account_keys_prost(&transaction, meta)?;
                let account_keys_capacity = account_keys.capacity();

                Message::Transaction(MessageTransaction::Prost {
                    error: meta.err.clone(),
                    account_keys,
                    transaction,
                    slot: message.slot,
                    bank_id: message.bank_id,
                    created_at,
                    size: encoded_len + SIGNATURE_BYTES + account_keys_capacity * PUBKEY_BYTES,
                })
            }
            UpdateOneof::TransactionStatus(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("TransactionStatus"));
            }
            UpdateOneof::Entry(entry) => Message::Entry(MessageEntry::Prost {
                entry,
                created_at,
                size: encoded_len,
            }),
            UpdateOneof::BlockMeta(block_meta) => {
                let block_height = block_meta
                    .block_height
                    .map(|v| v.block_height)
                    .ok_or(MessageParseError::FieldNotDefined("block_height"))?;
                Message::BlockMeta(MessageBlockMeta::Prost {
                    block_meta,
                    block_height,
                    created_at,
                    size: encoded_len,
                })
            }
            UpdateOneof::Block(message) => {
                let accounts = message
                    .accounts
                    .into_iter()
                    .map(|account| {
                        let encoded_len = account.encoded_len();
                        Ok(Arc::new(MessageAccount::Prost {
                            pubkey: account
                                .pubkey
                                .as_slice()
                                .try_into()
                                .map_err(|_| MessageParseError::InvalidPubkey)?,
                            owner: account
                                .owner
                                .as_slice()
                                .try_into()
                                .map_err(|_| MessageParseError::InvalidPubkey)?,
                            account,
                            slot: message.slot,
                            is_startup: false,
                            bank_id: nonzero_bank_id(message.bank_id),
                            created_at,
                            size: PUBKEY_BYTES + PUBKEY_BYTES + encoded_len + 32,
                        }))
                    })
                    .collect::<Result<_, MessageParseError>>()?;

                let transactions = message
                    .transactions
                    .into_iter()
                    .map(|transaction| {
                        let meta = transaction
                            .meta
                            .as_ref()
                            .ok_or(MessageParseError::FieldNotDefined("meta"))?;

                        let account_keys =
                            MessageTransaction::gen_account_keys_prost(&transaction, meta)?;
                        let account_keys_capacity = account_keys.capacity();

                        Ok(Arc::new(MessageTransaction::Prost {
                            error: meta.err.clone(),
                            account_keys,
                            transaction,
                            slot: message.slot,
                            bank_id: message.bank_id,
                            created_at,
                            size: encoded_len
                                + SIGNATURE_BYTES
                                + account_keys_capacity * PUBKEY_BYTES,
                        }))
                    })
                    .collect::<Result<_, MessageParseError>>()?;

                let entries = message
                    .entries
                    .into_iter()
                    .map(|entry| {
                        let encoded_len = entry.encoded_len();
                        Arc::new(MessageEntry::Prost {
                            entry,
                            created_at,
                            size: encoded_len,
                        })
                    })
                    .collect();

                let block_meta = SubscribeUpdateBlockMeta {
                    slot: message.slot,
                    blockhash: message.blockhash,
                    rewards: message.rewards,
                    block_time: message.block_time,
                    block_height: message.block_height,
                    parent_slot: message.parent_slot,
                    parent_blockhash: message.parent_blockhash,
                    executed_transaction_count: message.executed_transaction_count,
                    entries_count: message.entries_count,
                    bank_id: message.bank_id,
                };
                let encoded_len = block_meta.encoded_len();
                let block_height = block_meta
                    .block_height
                    .map(|v| v.block_height)
                    .ok_or(MessageParseError::FieldNotDefined("block_height"))?;

                Message::Block(MessageBlock {
                    accounts,
                    transactions,
                    entries,
                    block_meta: Arc::new(MessageBlockMeta::Prost {
                        block_meta,
                        block_height,
                        created_at,
                        size: encoded_len,
                    }),
                    created_at: MessageBlockCreatedAt::Prost(created_at),
                })
            }
            UpdateOneof::Ping(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("Ping"));
            }
            UpdateOneof::Pong(_) => {
                return Err(MessageParseError::InvalidUpdateMessage("Pong"));
            }
            UpdateOneof::BlockFooter(footer) => Message::BlockFooter(MessageBlockFooter::new(
                footer,
                created_at,
                encoded_len,
                MessageParserEncoding::Prost,
            )),
            UpdateOneof::EntryUpdateParent(update) => {
                Message::EntryUpdateParent(MessageEntryUpdateParent::new(
                    update,
                    created_at,
                    encoded_len,
                    MessageParserEncoding::Prost,
                ))
            }
        })
    }
}

#[derive(Debug, Clone)]
pub enum MessageSlot {
    Limited {
        slot: Slot,
        parent: Option<Slot>,
        status: SlotStatus,
        dead_error: Option<Range<usize>>,
        bank_id: Option<BankId>,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        slot: Slot,
        parent: Option<Slot>,
        status: SlotStatus,
        dead_error: Option<String>,
        bank_id: Option<BankId>,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageSlot {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { slot, .. } => *slot,
        }
    }

    pub const fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() + 64,
            Self::Prost { size, .. } => *size,
        }
    }

    pub const fn status(&self) -> SlotStatus {
        match self {
            Self::Limited { status, .. } => *status,
            Self::Prost { status, .. } => *status,
        }
    }

    pub const fn parent(&self) -> Option<Slot> {
        match self {
            Self::Limited { parent, .. } => *parent,
            Self::Prost { parent, .. } => *parent,
        }
    }

    /// `None` for `FirstShredReceived`, `Completed` and `Dead` statuses
    pub const fn bank_id(&self) -> Option<BankId> {
        match self {
            Self::Limited { bank_id, .. } => *bank_id,
            Self::Prost { bank_id, .. } => *bank_id,
        }
    }

    pub fn dead_error(&self) -> Option<&str> {
        match self {
            Self::Limited {
                dead_error, buffer, ..
            } => dead_error.as_ref().map(|range| unsafe {
                std::str::from_utf8_unchecked(&buffer.as_slice()[range.start..range.end])
            }),
            Self::Prost { dead_error, .. } => dead_error.as_deref(),
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
#[cfg_attr(test, derive(PartialEq))]
pub enum MessageAccount {
    Limited {
        pubkey: Pubkey,
        owner: Pubkey,
        lamports: u64,
        executable: bool,
        rent_epoch: Epoch,
        data: Range<usize>,
        txn_signature_offset: Option<usize>,
        write_version: usize,
        slot: Slot,
        is_startup: bool,
        bank_id: Option<BankId>,
        created_at: Timestamp,
        buffer: Vec<u8>,
        account_offset: usize,
        range: Range<usize>,
    },
    Prost {
        pubkey: Pubkey,
        owner: Pubkey,
        account: SubscribeUpdateAccountInfo,
        slot: Slot,
        is_startup: bool,
        bank_id: Option<BankId>,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageAccount {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { slot, .. } => *slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub const fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() + PUBKEY_BYTES * 2 + 86,
            Self::Prost { size, .. } => *size,
        }
    }

    pub const fn pubkey(&self) -> &Pubkey {
        match self {
            Self::Limited { pubkey, .. } => pubkey,
            Self::Prost { pubkey, .. } => pubkey,
        }
    }

    /// `None` for startup accounts
    pub const fn bank_id(&self) -> Option<BankId> {
        match self {
            Self::Limited { bank_id, .. } => *bank_id,
            Self::Prost { bank_id, .. } => *bank_id,
        }
    }

    pub fn write_version(&self) -> u64 {
        match self {
            Self::Limited {
                write_version,
                buffer,
                ..
            } => {
                let mut buffer = &buffer.as_slice()[*write_version..];
                decode_varint(&mut buffer).expect("already verified")
            }
            Self::Prost { account, .. } => account.write_version,
        }
    }

    pub fn txn_signature(&self) -> Option<&[u8]> {
        match self {
            MessageAccount::Limited {
                txn_signature_offset,
                buffer,
                ..
            } => txn_signature_offset.map(|offset| &buffer.as_slice()[offset..offset + 64]),
            MessageAccount::Prost { account, .. } => account.txn_signature.as_deref(),
        }
    }

    pub const fn nonempty_txn_signature(&self) -> bool {
        match self {
            Self::Limited {
                txn_signature_offset,
                ..
            } => txn_signature_offset.is_some(),
            Self::Prost { account, .. } => account.txn_signature.is_some(),
        }
    }

    pub fn update_write_version(&mut self, write_version: u64) {
        match self {
            Self::Limited {
                buffer,
                range,
                account_offset,
                write_version: write_version_current,
                data,
                txn_signature_offset,
                ..
            } => {
                // calculate current and new len of write_version
                let mut buf = &mut &buffer.as_slice()[*write_version_current..];
                let start = buf.remaining();
                decode_varint(&mut buf).expect("already verified");
                let wv_size_current = start - buf.remaining();
                let wv_size_new = encoded_len_varint(write_version);

                // calculate current and new len of account msg
                let mut buf = &mut &buffer.as_slice()[*account_offset..];
                let start = buf.remaining();
                let msg_size = decode_varint(&mut buf).expect("already verified");
                let msg_size_current = start - buf.remaining();
                let msg_size = msg_size + wv_size_new as u64 - wv_size_current as u64;
                let msg_size_new = encoded_len_varint(msg_size);

                // resize if required
                let new_end =
                    range.end + msg_size_new - msg_size_current + wv_size_new - wv_size_current;
                if new_end > buffer.len() {
                    buffer.resize(new_end, 0);
                }

                // copy data before write_version
                unsafe {
                    let end_current = *account_offset + msg_size_current;
                    let end_new = *account_offset + msg_size_new;
                    std::ptr::copy(
                        buffer.as_ptr().add(end_current),
                        buffer.as_mut_ptr().add(end_new),
                        *write_version_current - end_current,
                    );
                }

                // copy data after write_version
                let write_version_new = *write_version_current + msg_size_new - msg_size_current;
                unsafe {
                    let end_current = *write_version_current + wv_size_current;
                    let end_new = write_version_new + wv_size_new;
                    std::ptr::copy(
                        buffer.as_ptr().add(end_current),
                        buffer.as_mut_ptr().add(end_new),
                        range.end - end_current,
                    );
                }

                // save new message size and write_version
                encode_varint(msg_size, &mut &mut buffer.as_mut_slice()[*account_offset..]);
                encode_varint(
                    write_version,
                    &mut &mut buffer.as_mut_slice()[write_version_new..],
                );

                // update offsets
                range.end = new_end;
                // update msg_size anyway
                data.start = data.start + msg_size_new - msg_size_current;
                data.end = data.end + msg_size_new - msg_size_current;
                if data.start > write_version_new {
                    data.start = data.start + wv_size_new - wv_size_current;
                    data.end = data.end + wv_size_new - wv_size_current;
                }
                if let Some(txn_signature_offset) = txn_signature_offset {
                    *txn_signature_offset = *txn_signature_offset + msg_size_new - msg_size_current;
                    if *txn_signature_offset > write_version_new {
                        *txn_signature_offset =
                            *txn_signature_offset + wv_size_new - wv_size_current;
                    }
                }
                *write_version_current = write_version_new;
            }
            Self::Prost { account, size, .. } => {
                *size = *size + encoded_len_varint(write_version)
                    - encoded_len_varint(account.write_version);
                account.write_version = write_version;
            }
        }
    }
}

impl ReadableAccount for MessageAccount {
    fn lamports(&self) -> u64 {
        match self {
            Self::Limited { lamports, .. } => *lamports,
            Self::Prost { account, .. } => account.lamports,
        }
    }

    fn data(&self) -> &[u8] {
        match self {
            Self::Limited { data, buffer, .. } => &buffer.as_slice()[data.start..data.end],
            Self::Prost { account, .. } => &account.data,
        }
    }

    fn owner(&self) -> &Pubkey {
        match self {
            Self::Limited { owner, .. } => owner,
            Self::Prost { owner, .. } => owner,
        }
    }

    fn executable(&self) -> bool {
        match self {
            Self::Limited { executable, .. } => *executable,
            Self::Prost { account, .. } => account.executable,
        }
    }

    fn rent_epoch(&self) -> Epoch {
        match self {
            Self::Limited { rent_epoch, .. } => *rent_epoch,
            Self::Prost { account, .. } => account.rent_epoch,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum MessageTransaction {
    Limited {
        signature_offset: Option<usize>,
        error: Option<TransactionError>,
        account_keys: HashSet<Pubkey>,
        is_vote: bool,
        index: u64,
        transaction_range: Range<usize>,
        transaction: OnceLock<Option<SubscribeUpdateTransactionInfo>>,
        slot: Slot,
        bank_id: BankId,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        error: Option<TransactionError>,
        account_keys: HashSet<Pubkey>,
        transaction: SubscribeUpdateTransactionInfo,
        slot: Slot,
        bank_id: BankId,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageTransaction {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { slot, .. } => *slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            Self::Limited {
                account_keys,
                buffer,
                ..
            } => buffer.len() * 2 + account_keys.capacity() * PUBKEY_BYTES,
            Self::Prost { size, .. } => *size,
        }
    }

    pub fn gen_account_keys_prost(
        transaction: &SubscribeUpdateTransactionInfo,
        meta: &TransactionStatusMeta,
    ) -> Result<HashSet<Pubkey>, MessageParseError> {
        let mut account_keys = HashSet::new();

        // static account keys
        if let Some(pubkeys) = transaction
            .transaction
            .as_ref()
            .ok_or(MessageParseError::FieldNotDefined("transaction"))?
            .message
            .as_ref()
            .map(|msg| msg.account_keys.as_slice())
        {
            for pubkey in pubkeys {
                account_keys.insert(
                    Pubkey::try_from(pubkey.as_slice())
                        .map_err(|_| MessageParseError::InvalidPubkey)?,
                );
            }
        }
        // dynamic account keys
        for pubkey in meta.loaded_writable_addresses.iter() {
            account_keys.insert(
                Pubkey::try_from(pubkey.as_slice())
                    .map_err(|_| MessageParseError::InvalidPubkey)?,
            );
        }
        for pubkey in meta.loaded_readonly_addresses.iter() {
            account_keys.insert(
                Pubkey::try_from(pubkey.as_slice())
                    .map_err(|_| MessageParseError::InvalidPubkey)?,
            );
        }

        Ok(account_keys)
    }

    pub fn signature(&self) -> Signature {
        Signature::from(
            <[u8; SIGNATURE_BYTES]>::try_from(self.signature_ref())
                .expect("signature must be 64 bytes"),
        )
    }

    pub fn signature_ref(&self) -> &[u8] {
        match self {
            Self::Limited {
                signature_offset,
                buffer,
                ..
            } => match signature_offset {
                Some(offset) => &buffer[*offset..*offset + SIGNATURE_BYTES],
                None => panic!("transaction should have valid signature"),
            },
            Self::Prost { transaction, .. } => &transaction.signature,
        }
    }

    pub const fn vote(&self) -> bool {
        match self {
            Self::Limited { is_vote, .. } => *is_vote,
            Self::Prost { transaction, .. } => transaction.is_vote,
        }
    }

    pub const fn index(&self) -> u64 {
        match self {
            Self::Limited { index, .. } => *index,
            Self::Prost { transaction, .. } => transaction.index,
        }
    }

    pub const fn bank_id(&self) -> Option<BankId> {
        match self {
            Self::Limited { bank_id, .. } => nonzero_bank_id(*bank_id),
            Self::Prost { bank_id, .. } => nonzero_bank_id(*bank_id),
        }
    }

    pub const fn failed(&self) -> bool {
        match self {
            Self::Limited { error, .. } => error.is_some(),
            Self::Prost { error, .. } => error.is_some(),
        }
    }

    pub const fn error(&self) -> &Option<TransactionError> {
        match self {
            Self::Limited { error, .. } => error,
            Self::Prost { error, .. } => error,
        }
    }

    fn transaction(&self) -> Result<&SubscribeUpdateTransactionInfo, &'static str> {
        match self {
            Self::Limited {
                transaction,
                transaction_range,
                buffer,
                ..
            } => transaction
                .get_or_init(|| {
                    SubscribeUpdateTransactionInfo::decode(
                        &buffer.as_slice()[transaction_range.clone()],
                    )
                    .ok()
                })
                .as_ref()
                .ok_or("FailedToDecode"),
            Self::Prost { transaction, .. } => Ok(transaction),
        }
    }

    pub fn transaction_meta(&self) -> Result<&TransactionStatusMeta, &'static str> {
        self.transaction()
            .and_then(|tx| tx.meta.as_ref().ok_or("FieldNotDefined"))
    }

    pub fn as_versioned_transaction_with_status_meta(
        &self,
    ) -> Result<VersionedTransactionWithStatusMeta, &'static str> {
        match convert_from::create_tx_with_meta(self.transaction()?.clone())? {
            TransactionWithStatusMeta::Complete(tx) => Ok(tx),
            TransactionWithStatusMeta::MissingMetadata(_) => Err("FieldNotDefined"),
        }
    }

    pub const fn account_keys(&self) -> &HashSet<Pubkey> {
        match self {
            Self::Limited { account_keys, .. } => account_keys,
            Self::Prost { account_keys, .. } => account_keys,
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageEntry {
    Limited {
        slot: Slot,
        index: u64,
        executed_transaction_count: u64,
        bank_id: BankId,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        entry: SubscribeUpdateEntry,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageEntry {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { slot, .. } => *slot,
            Self::Prost { entry, .. } => entry.slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub const fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() + 52,
            Self::Prost { size, .. } => *size,
        }
    }

    pub const fn index(&self) -> u64 {
        match self {
            Self::Limited { index, .. } => *index,
            Self::Prost { entry, .. } => entry.index,
        }
    }

    pub const fn executed_transaction_count(&self) -> u64 {
        match self {
            Self::Limited {
                executed_transaction_count,
                ..
            } => *executed_transaction_count,
            Self::Prost { entry, .. } => entry.executed_transaction_count,
        }
    }

    pub const fn bank_id(&self) -> Option<BankId> {
        match self {
            Self::Limited { bank_id, .. } => nonzero_bank_id(*bank_id),
            Self::Prost { entry, .. } => nonzero_bank_id(entry.bank_id),
        }
    }
}

#[derive(Debug, Clone)]
pub enum MessageBlockMeta {
    Limited {
        block_meta: SubscribeUpdateBlockMeta,
        block_height: Slot,
        created_at: Timestamp,
        buffer: Vec<u8>,
        range: Range<usize>,
    },
    Prost {
        block_meta: SubscribeUpdateBlockMeta,
        block_height: Slot,
        created_at: Timestamp,
        size: usize,
    },
}

impl MessageBlockMeta {
    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited { .. } => MessageParserEncoding::Limited,
            Self::Prost { .. } => MessageParserEncoding::Prost,
        }
    }

    pub const fn slot(&self) -> Slot {
        match self {
            Self::Limited { block_meta, .. } => block_meta.slot,
            Self::Prost { block_meta, .. } => block_meta.slot,
        }
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        match self {
            Self::Limited { created_at, .. } => MessageBlockCreatedAt::Limited(*created_at),
            Self::Prost { created_at, .. } => MessageBlockCreatedAt::Prost(*created_at),
        }
    }

    pub const fn size(&self) -> usize {
        match self {
            Self::Limited { buffer, .. } => buffer.len() * 2,
            Self::Prost { size, .. } => *size,
        }
    }

    // clippy bug?
    #[allow(clippy::missing_const_for_fn)]
    pub fn blockhash(&self) -> &str {
        match self {
            Self::Limited { block_meta, .. } => &block_meta.blockhash,
            Self::Prost { block_meta, .. } => &block_meta.blockhash,
        }
    }

    pub const fn block_height(&self) -> Slot {
        match self {
            Self::Limited { block_height, .. } => *block_height,
            Self::Prost { block_height, .. } => *block_height,
        }
    }

    pub const fn executed_transaction_count(&self) -> u64 {
        match self {
            Self::Limited { block_meta, .. } => block_meta.executed_transaction_count,
            Self::Prost { block_meta, .. } => block_meta.executed_transaction_count,
        }
    }

    pub const fn entries_count(&self) -> u64 {
        match self {
            Self::Limited { block_meta, .. } => block_meta.entries_count,
            Self::Prost { block_meta, .. } => block_meta.entries_count,
        }
    }

    pub const fn bank_id(&self) -> Option<BankId> {
        match self {
            Self::Limited { block_meta, .. } => nonzero_bank_id(block_meta.bank_id),
            Self::Prost { block_meta, .. } => nonzero_bank_id(block_meta.bank_id),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MessageBlock {
    pub accounts: Vec<Arc<MessageAccount>>,
    pub transactions: Vec<Arc<MessageTransaction>>,
    pub entries: Vec<Arc<MessageEntry>>,
    pub block_meta: Arc<MessageBlockMeta>,
    pub created_at: MessageBlockCreatedAt,
}

impl MessageBlock {
    pub const fn encoding(&self) -> MessageParserEncoding {
        self.created_at.encoding()
    }

    pub fn slot(&self) -> Slot {
        self.block_meta.as_ref().slot()
    }

    pub fn bank_id(&self) -> Option<BankId> {
        self.block_meta.as_ref().bank_id()
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        self.created_at
    }

    pub fn size(&self) -> usize {
        self.accounts
            .iter()
            .map(|m| m.size())
            .chain(self.transactions.iter().map(|m| m.size()))
            .chain(self.entries.iter().map(|m| m.size()))
            .sum::<usize>()
            + self.block_meta.size()
    }

    pub fn as_confirmed_block(&self) -> Result<ConfirmedBlock, &'static str> {
        Ok(match self.block_meta.as_ref() {
            MessageBlockMeta::Limited { block_meta, .. } => ConfirmedBlock {
                previous_blockhash: block_meta.parent_blockhash.clone(),
                blockhash: block_meta.blockhash.clone(),
                parent_slot: block_meta.parent_slot,
                transactions: self
                    .transactions
                    .iter()
                    .map(|tx| {
                        tx.as_versioned_transaction_with_status_meta()
                            .map(TransactionWithStatusMeta::Complete)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                rewards: block_meta
                    .rewards
                    .as_ref()
                    .map(|r| {
                        r.rewards
                            .iter()
                            .cloned()
                            .map(convert_from::create_reward)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_default(),
                num_partitions: block_meta
                    .rewards
                    .as_ref()
                    .and_then(|r| r.num_partitions)
                    .map(|np| np.num_partitions),
                block_time: block_meta.block_time.map(|bt| bt.timestamp),
                block_height: block_meta.block_height.map(|bh| bh.block_height),
            },
            MessageBlockMeta::Prost { block_meta, .. } => ConfirmedBlock {
                previous_blockhash: block_meta.parent_blockhash.clone(),
                blockhash: block_meta.blockhash.clone(),
                parent_slot: block_meta.parent_slot,
                transactions: self
                    .transactions
                    .iter()
                    .map(|tx| {
                        tx.as_versioned_transaction_with_status_meta()
                            .map(TransactionWithStatusMeta::Complete)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                rewards: block_meta
                    .rewards
                    .as_ref()
                    .map(|r| {
                        r.rewards
                            .iter()
                            .cloned()
                            .map(convert_from::create_reward)
                            .collect::<Result<Vec<_>, _>>()
                    })
                    .transpose()?
                    .unwrap_or_default(),
                num_partitions: block_meta
                    .rewards
                    .as_ref()
                    .and_then(|r| r.num_partitions)
                    .map(|np| np.num_partitions),
                block_time: block_meta.block_time.map(|bt| bt.timestamp),
                block_height: block_meta.block_height.map(|bh| bh.block_height),
            },
        })
    }
}

/// Alpenglow block footer, always stored with certificates
/// (stripped on encode when the filter does not request them)
#[derive(Debug, Clone)]
pub struct MessageBlockFooter {
    footer: SubscribeUpdateBlockFooter,
    created_at: MessageBlockCreatedAt,
    size: usize,
}

impl MessageBlockFooter {
    pub const fn new(
        footer: SubscribeUpdateBlockFooter,
        created_at: Timestamp,
        size: usize,
        encoding: MessageParserEncoding,
    ) -> Self {
        Self {
            footer,
            created_at: MessageBlockCreatedAt::new(created_at, encoding),
            size,
        }
    }

    pub const fn encoding(&self) -> MessageParserEncoding {
        self.created_at.encoding()
    }

    pub const fn slot(&self) -> Slot {
        self.footer.slot
    }

    pub const fn bank_id(&self) -> Option<BankId> {
        nonzero_bank_id(self.footer.bank_id)
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        self.created_at
    }

    pub const fn size(&self) -> usize {
        self.size
    }

    pub const fn footer(&self) -> &SubscribeUpdateBlockFooter {
        &self.footer
    }
}

/// Alpenglow `UpdateParent` processed during replay: bank `cleared_bank_id` of the slot was
/// cleared (all updates of that bank are invalid), the replacement bank is reported with
/// `SlotStatus::CreatedBank`
#[derive(Debug, Clone)]
pub struct MessageEntryUpdateParent {
    update: SubscribeUpdateEntryUpdateParent,
    created_at: MessageBlockCreatedAt,
    size: usize,
}

impl MessageEntryUpdateParent {
    pub const fn new(
        update: SubscribeUpdateEntryUpdateParent,
        created_at: Timestamp,
        size: usize,
        encoding: MessageParserEncoding,
    ) -> Self {
        Self {
            update,
            created_at: MessageBlockCreatedAt::new(created_at, encoding),
            size,
        }
    }

    pub const fn encoding(&self) -> MessageParserEncoding {
        self.created_at.encoding()
    }

    pub const fn slot(&self) -> Slot {
        self.update.slot
    }

    pub const fn cleared_bank_id(&self) -> BankId {
        self.update.cleared_bank_id
    }

    pub const fn created_at(&self) -> MessageBlockCreatedAt {
        self.created_at
    }

    pub const fn size(&self) -> usize {
        self.size
    }

    pub const fn update(&self) -> &SubscribeUpdateEntryUpdateParent {
        &self.update
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageBlockCreatedAt {
    Limited(Timestamp),
    Prost(Timestamp),
}

impl From<MessageBlockCreatedAt> for Timestamp {
    fn from(value: MessageBlockCreatedAt) -> Self {
        match value {
            MessageBlockCreatedAt::Limited(timestamp) => timestamp,
            MessageBlockCreatedAt::Prost(timestamp) => timestamp,
        }
    }
}

impl MessageBlockCreatedAt {
    pub const fn new(created_at: Timestamp, encoding: MessageParserEncoding) -> Self {
        match encoding {
            MessageParserEncoding::Limited => Self::Limited(created_at),
            MessageParserEncoding::Prost => Self::Prost(created_at),
        }
    }

    pub const fn encoding(&self) -> MessageParserEncoding {
        match self {
            Self::Limited(_) => MessageParserEncoding::Limited,
            Self::Prost(_) => MessageParserEncoding::Prost,
        }
    }

    pub const fn as_millis(&self) -> u64 {
        match self {
            Self::Limited(ts) => ts.seconds as u64 * 1_000 + (ts.nanos / 1_000_000) as u64,
            Self::Prost(ts) => ts.seconds as u64 * 1_000 + (ts.nanos / 1_000_000) as u64,
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Message, MessageAccount, MessageParseError, MessageParserEncoding, MessageRef},
        crate::{
            config::{ConfigFilter, ConfigFilterAccounts, ConfigFilterAccountsDataSlice},
            filter::{Filter, FilteredUpdate},
        },
        maplit::hashmap,
        solana_account::ReadableAccount,
        solana_commitment_config::CommitmentLevel,
    };

    static MESSAGE: &str = "0a0012af010aa6010a2088f1ffa3a2dfe617bdc4e3573251a322e3fcae81e5a457390e64751c00a465e210e0d54a1a2006aa09548b50476ad462f91f89a3015033264fc9abd5270020a9d142334742fb28ffffffffffffffffff013208c921f474e044612838e3e1acc2b53042405bd620fab28d3c0b78b3ead9f04d1c4d6dffeac4ffa7c679a6570b0226557c10b4c4016d937e06044b4e49d9d7916524d5dfa26297c5f638c3d11f846410bc0510e5ddaca2015a0c08e1c79ec10610ebef838601";

    fn parse(data: Vec<u8>, parser: MessageParserEncoding) -> MessageAccount {
        if let Message::Account(msg) = Message::parse(data.into(), parser).expect("valid message") {
            assert!(
                match parser {
                    MessageParserEncoding::Prost => matches!(msg, MessageAccount::Prost { .. }),
                    MessageParserEncoding::Limited => matches!(msg, MessageAccount::Limited { .. }),
                },
                "unexpected msg encoding"
            );

            msg
        } else {
            panic!("expected account message");
        }
    }

    fn encode(msg: &MessageAccount, data_slice: Option<(u64, u64)>) -> Vec<u8> {
        let filter = Filter::new(&ConfigFilter {
            accounts: hashmap! { "".to_owned() => ConfigFilterAccounts::default() },
            accounts_data_slice: data_slice
                .map(|(offset, length)| vec![ConfigFilterAccountsDataSlice { offset, length }])
                .unwrap_or_default(),
            ..Default::default()
        });

        let message = Message::Account(msg.clone());
        let message_ref: MessageRef = (&message).into();

        let updates = filter.get_updates_ref(message_ref, CommitmentLevel::Processed);
        assert_eq!(updates.len(), 1, "unexpected number of updates");
        updates[0].encode_to_vec()
    }

    #[test]
    fn test_richat_update_is_skipped() {
        use {
            prost::Message as _,
            richat_proto::{
                geyser::SubscribeUpdateContactInfoRemoved,
                richat::{
                    SubscribeUpdateRichat,
                    subscribe_update_richat::UpdateOneof as UpdateOneofRichat,
                },
            },
        };

        let data = SubscribeUpdateRichat {
            update_oneof: Some(UpdateOneofRichat::ContactInfoRemoved(
                SubscribeUpdateContactInfoRemoved {
                    pubkey: vec![42; 32],
                },
            )),
            created_at: Some(prost_types::Timestamp {
                seconds: 1_757_500_000,
                nanos: 42,
            }),
        }
        .encode_to_vec();

        for parser in [MessageParserEncoding::Limited, MessageParserEncoding::Prost] {
            assert!(
                matches!(
                    Message::parse(data.clone().into(), parser),
                    Err(MessageParseError::InvalidUpdateMessage("Richat"))
                ),
                "unexpected result for {parser:?}"
            );
        }
    }

    fn richat_footer_bytes() -> Vec<u8> {
        // wincode `VersionedBlockFooter::V1` with a fast finalization cert only
        let mut footer = vec![1];
        footer.extend_from_slice(&[7; 32]); // bank_hash
        footer.extend_from_slice(&42u64.to_le_bytes()); // block_producer_time_nanos
        footer.push(0); // block_user_agent
        footer.push(1); // block_final_cert
        footer.extend_from_slice(&100u64.to_le_bytes());
        footer.extend_from_slice(&[9; 32]);
        footer.extend_from_slice(&[1; 96]);
        footer.extend_from_slice(&1u16.to_le_bytes());
        footer.push(0b101);
        footer.push(0); // notar_aggregate
        footer.push(0); // skip_reward_cert
        footer.push(0); // notar_reward_cert
        footer
    }

    #[test]
    fn test_block_footer_and_update_parent() {
        use {
            crate::config::ConfigFilterBlockFooter,
            maplit::hashset,
            prost::Message as _,
            richat_proto::{
                geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
                richat::{
                    SubscribeUpdateBlockFooter, SubscribeUpdateEntryUpdateParent,
                    SubscribeUpdateRichat,
                    subscribe_update_richat::UpdateOneof as UpdateOneofRichat,
                },
            },
        };

        let created_at = prost_types::Timestamp {
            seconds: 1_757_500_000,
            nanos: 42,
        };
        let footer = SubscribeUpdateRichat {
            update_oneof: Some(UpdateOneofRichat::BlockFooter(SubscribeUpdateBlockFooter {
                slot: 101,
                bank_id: 5,
                version: 1,
                bank_hash: vec![7; 32],
                block_producer_time_nanos: 42,
                block_user_agent: vec![],
                footer: richat_footer_bytes(),
            })),
            created_at: Some(created_at),
        }
        .encode_to_vec();
        let update_parent = SubscribeUpdateRichat {
            update_oneof: Some(UpdateOneofRichat::EntryUpdateParent(
                SubscribeUpdateEntryUpdateParent {
                    slot: 101,
                    cleared_bank_id: 5,
                    parent_slot: 99,
                    parent_block_id: vec![3; 32],
                },
            )),
            created_at: Some(created_at),
        }
        .encode_to_vec();

        let filter = Filter::new(&ConfigFilter {
            entries: hashset! { "entry".to_owned(), "entry_parent".to_owned() },
            entries_update_parent: hashset! { "entry_parent".to_owned() },
            block_footer: hashmap! {
                "footer".to_owned() => ConfigFilterBlockFooter::default(),
                "footer_certs".to_owned() => ConfigFilterBlockFooter {
                    include_certificates: Some(true),
                },
            },
            ..Default::default()
        });

        for parser in [MessageParserEncoding::Limited, MessageParserEncoding::Prost] {
            let message = Message::parse(footer.clone().into(), parser).expect("valid footer");
            let Message::BlockFooter(msg) = &message else {
                panic!("expected block footer for {parser:?}");
            };
            assert_eq!(msg.encoding(), parser);
            assert_eq!(message.slot(), 101);
            assert_eq!(message.bank_id(), Some(5));

            let updates = filter.get_updates(&message, CommitmentLevel::Processed);
            assert_eq!(updates.len(), 2, "{parser:?}");
            for update in updates {
                let update = SubscribeUpdate::decode(update.encode_to_vec().as_slice())
                    .expect("valid update");
                assert_eq!(update.created_at, Some(created_at));
                let Some(UpdateOneof::BlockFooter(footer)) = update.update_oneof else {
                    panic!("expected Yellowstone block footer");
                };
                assert_eq!((footer.slot, footer.bank_id), (101, 5));
                assert_eq!(footer.bank_hash, vec![7; 32]);
                assert_eq!(footer.block_producer_time_nanos, 42);
                match update.filters.as_slice() {
                    [name] if name == "footer" => assert_eq!(footer.block_final_cert, None),
                    [name] if name == "footer_certs" => {
                        let cert = footer.block_final_cert.expect("final cert");
                        assert_eq!(cert.slot, 100);
                        let aggregate = cert.final_aggregate.expect("final aggregate");
                        assert_eq!(aggregate.block_id, vec![9; 32]);
                        assert_eq!(aggregate.signer_bitmap, vec![0b101]);
                    }
                    filters => panic!("unexpected filters: {filters:?}"),
                }
            }

            let message =
                Message::parse(update_parent.clone().into(), parser).expect("valid update parent");
            let Message::EntryUpdateParent(msg) = &message else {
                panic!("expected entry update parent for {parser:?}");
            };
            assert_eq!((msg.slot(), msg.cleared_bank_id()), (101, 5));

            let updates = filter.get_updates(&message, CommitmentLevel::Processed);
            assert_eq!(updates.len(), 1, "{parser:?}");
            let update = SubscribeUpdate::decode(updates[0].encode_to_vec().as_slice())
                .expect("valid update");
            assert_eq!(update.filters, vec!["entry_parent".to_owned()]);
            let Some(UpdateOneof::EntryUpdateParent(update)) = update.update_oneof else {
                panic!("expected Yellowstone entry update parent");
            };
            assert_eq!(update.cleared_bank_id, 5);
            assert_eq!(update.parent_slot, 99);

            // Yellowstone v14 encoding of the same messages (re-serve from another Richat)
            for data in [footer.clone(), update_parent.clone()] {
                let message = Message::parse(data.into(), parser).expect("valid message");
                let data = FilteredUpdate {
                    filters: Default::default(),
                    filtered_update: MessageRef::from(&message).into(),
                }
                .encode_to_vec();
                let message2 = Message::parse(data.into(), parser).expect("valid message");
                assert_eq!(message2.slot(), message.slot());
                assert_eq!(message2.bank_id(), message.bank_id());
                assert_eq!(message2.size() > 0, message.size() > 0);
            }
        }
    }

    #[test]
    fn test_bank_id() {
        use {
            prost::Message as _,
            richat_proto::geyser::{
                SlotStatus, SubscribeUpdate, SubscribeUpdateSlot, subscribe_update::UpdateOneof,
            },
        };

        for bank_id in [None, Some(0), Some(42)] {
            let data = SubscribeUpdate {
                filters: vec![],
                update_oneof: Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                    slot: 101,
                    parent: Some(100),
                    status: SlotStatus::SlotConfirmed as i32,
                    dead_error: None,
                    bank_id,
                })),
                created_at: Some(prost_types::Timestamp::default()),
            }
            .encode_to_vec();
            for parser in [MessageParserEncoding::Limited, MessageParserEncoding::Prost] {
                let message = Message::parse(data.clone().into(), parser).expect("valid slot");
                assert_eq!(message.bank_id(), bank_id, "{parser:?}");
            }
        }
    }

    #[test]
    fn test_limited() {
        let mut msg = parse(
            const_hex::decode(MESSAGE).expect("valid hex"),
            MessageParserEncoding::Limited,
        );
        assert_eq!(msg.write_version(), 1663633666275, "valid write version");

        msg.update_write_version(1);
        assert_eq!(msg.write_version(), 1, "dec valid write version");
        let mut msg2 = parse(encode(&msg, None), MessageParserEncoding::Limited);
        if let (
            MessageAccount::Limited { buffer, .. },
            MessageAccount::Limited {
                buffer: buffer2, ..
            },
        ) = (&msg, &mut msg2)
        {
            *buffer2 = buffer.clone(); // ignore buffer
        }
        assert_eq!(msg, msg2, "write version update failed");
        // check with data slice
        let mut msg2 = parse(encode(&msg, Some((1, 3))), MessageParserEncoding::Limited);
        assert_eq!(msg.write_version(), msg2.write_version());
        assert_eq!(&msg.data()[1..4], msg2.data());
        if let (
            MessageAccount::Limited {
                buffer,
                txn_signature_offset,
                write_version,
                data,
                range,
                ..
            },
            MessageAccount::Limited {
                buffer: buffer2,
                txn_signature_offset: txn_signature_offset2,
                write_version: write_version2,
                data: data2,
                range: range2,
                ..
            },
        ) = (&msg, &mut msg2)
        {
            let txn_offset = txn_signature_offset.unwrap();
            let txn_offset2 = txn_signature_offset2.unwrap();
            assert_eq!(
                &buffer[txn_offset..txn_offset + 64],
                &buffer2[txn_offset2..txn_offset2 + 64]
            );
            *buffer2 = buffer.clone(); // ignore buffer
            *txn_signature_offset2 = *txn_signature_offset;
            *write_version2 = *write_version;
            *data2 = data.clone();
            *range2 = range.clone();
        }
        assert_eq!(msg, msg2, "write version update failed");

        msg.update_write_version(u64::MAX);
        assert_eq!(msg.write_version(), u64::MAX, "inc valid write version");
        let mut msg2 = parse(encode(&msg, None), MessageParserEncoding::Limited);
        if let (
            MessageAccount::Limited { buffer, .. },
            MessageAccount::Limited {
                buffer: buffer2, ..
            },
        ) = (&msg, &mut msg2)
        {
            *buffer2 = buffer.clone(); // ignore buffer
        }
        assert_eq!(msg, msg2, "write version update failed");
        // check with data slice
        let mut msg2 = parse(encode(&msg, Some((1, 3))), MessageParserEncoding::Limited);
        assert_eq!(msg.write_version(), msg2.write_version());
        assert_eq!(&msg.data()[1..4], msg2.data());
        if let (
            MessageAccount::Limited {
                buffer,
                txn_signature_offset,
                write_version,
                data,
                range,
                ..
            },
            MessageAccount::Limited {
                buffer: buffer2,
                txn_signature_offset: txn_signature_offset2,
                write_version: write_version2,
                data: data2,
                range: range2,
                ..
            },
        ) = (&msg, &mut msg2)
        {
            let txn_offset = txn_signature_offset.unwrap();
            let txn_offset2 = txn_signature_offset2.unwrap();
            assert_eq!(
                &buffer[txn_offset..txn_offset + 64],
                &buffer2[txn_offset2..txn_offset2 + 64]
            );
            *buffer2 = buffer.clone(); // ignore buffer
            *txn_signature_offset2 = *txn_signature_offset;
            *write_version2 = *write_version;
            *data2 = data.clone();
            *range2 = range.clone();
        }
        assert_eq!(msg, msg2, "write version update failed");
    }

    #[test]
    fn test_prost() {
        let mut msg = parse(
            const_hex::decode(MESSAGE).expect("valid hex"),
            MessageParserEncoding::Prost,
        );
        assert_eq!(msg.write_version(), 1663633666275, "valid write version");

        msg.update_write_version(1);
        assert_eq!(msg.write_version(), 1, "dec valid write version");
        let msg2 = parse(encode(&msg, None), MessageParserEncoding::Prost);
        assert_eq!(msg, msg2, "write version update failed");

        msg.update_write_version(u64::MAX);
        assert_eq!(msg.write_version(), u64::MAX, "inc valid write version");
        let msg2 = parse(encode(&msg, None), MessageParserEncoding::Prost);
        assert_eq!(msg, msg2, "write version update failed");
    }
}
