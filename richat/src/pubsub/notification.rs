use {
    crate::{
        metrics,
        pubsub::{SubscriptionId, solana::SubscribeMethod},
    },
    ::metrics::{Gauge, gauge},
    agave_reserved_account_keys::ReservedAccountKeys,
    jsonrpsee_types::{Extensions, SubscriptionPayload, SubscriptionResponse, TwoPointZero},
    richat_filter::message::{MessageBlock, MessageTransaction},
    richat_shared::five8::signature_encode,
    serde::Serialize,
    solana_clock::Slot,
    solana_message::{VersionedMessage, v0::LoadedMessage},
    solana_rpc_client_api::response::{Response as RpcResponse, RpcResponseContext},
    solana_transaction::versioned::TransactionVersion,
    solana_transaction_status::{
        BlockEncodingOptions, EncodeError, EncodedTransaction, EncodedTransactionWithStatusMeta,
        TransactionDetails, TransactionStatusMeta, UiAccountsList, UiConfirmedBlock,
        UiTransactionEncoding, UiTransactionStatusMeta, VersionedTransactionWithStatusMeta,
        option_serializer::OptionSerializer,
        parse_accounts::{
            parse_legacy_message_accounts, parse_v0_message_accounts, parse_v1_message_accounts,
        },
    },
    std::{
        collections::VecDeque,
        sync::{Arc, Weak},
    },
    tokio::sync::mpsc,
};

#[derive(Debug, Clone)]
pub struct RpcNotification {
    pub subscription_id: SubscriptionId,
    pub method: SubscribeMethod,
    pub is_final: bool,
    pub json: Weak<String>,
}

impl RpcNotification {
    pub fn serialize<T: Serialize>(
        method: &str,
        subscription: SubscriptionId,
        result: T,
    ) -> Arc<String> {
        let response = SubscriptionResponse {
            jsonrpc: TwoPointZero,
            method: method.into(),
            params: SubscriptionPayload {
                subscription: jsonrpsee_types::SubscriptionId::Num(subscription),
                result,
            },
            extensions: Extensions::default(), // doesn't matter, as it is not used in serialize
        };
        Arc::new(serde_json::to_string(&response).expect("json serialization never fail"))
    }

    pub fn serialize_with_context<T: Serialize>(
        method: &str,
        subscription: SubscriptionId,
        slot: Slot,
        value: T,
    ) -> Arc<String> {
        Self::serialize(
            method,
            subscription,
            RpcResponse {
                context: RpcResponseContext {
                    slot,
                    api_version: None,
                },
                value,
            },
        )
    }
}

/// Item of the bounded per-client queue filled by the subscriptions tracker.
#[derive(Debug)]
pub enum ClientNotification {
    Notification(RpcNotification),
    /// The queue was full: the tracker dropped the client and sends nothing more.
    Lagged,
}

/// Sender side of the per-client queue, only the subscriptions tracker owns it.
///
/// The channel is created with one extra slot, reserved for [`ClientNotification::Lagged`], so
/// the tracker can always tell the client why it was dropped without waiting for it.
#[derive(Debug)]
pub struct ClientSender {
    tx: mpsc::Sender<ClientNotification>,
}

impl ClientSender {
    pub fn channel(max_count: usize) -> (Self, mpsc::Receiver<ClientNotification>) {
        let capacity = max_count.clamp(1, tokio::sync::Semaphore::MAX_PERMITS - 1) + 1;
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, rx)
    }

    /// Never blocks. Returns `false` if the client is lagged (queue full) and must be dropped.
    pub fn send(&self, notification: RpcNotification) -> bool {
        // the tracker is the only sender, so capacity can't shrink between the check and the send
        if self.tx.capacity() <= 1 {
            let _ = self.tx.try_send(ClientNotification::Lagged);
            return false;
        }
        // `Closed` means the client is gone, `ClientRequest::Remove` follows
        let _ = self
            .tx
            .try_send(ClientNotification::Notification(notification));
        true
    }
}

/// Keeps the latest notifications alive; clients get `Weak` references, a client that reads a
/// notification already evicted from here is lagged.
#[derive(Debug)]
pub struct RpcNotifications {
    items: VecDeque<Arc<String>>,
    bytes_total: usize,
    max_count: usize,
    max_bytes: usize,
    gauge_count: Gauge,
    gauge_bytes: Gauge,
}

impl RpcNotifications {
    pub fn new(max_count: usize, max_bytes: usize) -> Self {
        Self {
            items: VecDeque::with_capacity(max_count + 1),
            bytes_total: 0,
            max_count,
            max_bytes,
            gauge_count: gauge!(metrics::PUBSUB_STORED_MESSAGES_COUNT_TOTAL),
            gauge_bytes: gauge!(metrics::PUBSUB_STORED_MESSAGES_BYTES_TOTAL),
        }
    }

    pub fn push(&mut self, json: Arc<String>) -> Weak<String> {
        let weak = Arc::downgrade(&json);

        self.bytes_total += json.len();
        while self.bytes_total >= self.max_bytes || self.items.len() >= self.max_count {
            let Some(item) = self.items.pop_front() else {
                break;
            };
            self.bytes_total -= item.len();
        }
        self.items.push_back(json);

        self.gauge_count.set(self.items.len() as f64);
        self.gauge_bytes.set(self.bytes_total as f64);

        weak
    }
}

#[derive(Debug, Serialize)]
pub struct RpcTransactionUpdate {
    slot: Slot,
    #[serde(skip_serializing_if = "Option::is_none")]
    signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction: Option<EncodedTransactionWithStatusMeta>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcTransactionUpdateError>,
}

impl RpcTransactionUpdate {
    pub fn new(
        message: &MessageTransaction,
        encoding: UiTransactionEncoding,
        transaction_details: TransactionDetails,
        show_rewards: bool,
        max_supported_transaction_version: Option<u8>,
    ) -> Self {
        let (signature, transaction) = match transaction_details {
            TransactionDetails::Full => (
                None,
                Some(Self::tx_encode_full(
                    message,
                    encoding,
                    show_rewards,
                    max_supported_transaction_version,
                )),
            ),
            TransactionDetails::Signatures => {
                (Some(signature_encode(message.signature().as_array())), None)
            }
            TransactionDetails::None => (None, None),
            TransactionDetails::Accounts => (
                None,
                Some(Self::tx_encode_accounts(
                    message,
                    show_rewards,
                    max_supported_transaction_version,
                )),
            ),
        };

        let (transaction, error) = match transaction {
            Some(Ok(transaction)) => (Some(transaction), None),
            Some(Err(error)) => (None, Some(error)),
            None => (None, None),
        };

        Self {
            slot: message.slot(),
            signature,
            transaction,
            error,
        }
    }

    fn tx_encode_full(
        message: &MessageTransaction,
        encoding: UiTransactionEncoding,
        show_rewards: bool,
        max_supported_transaction_version: Option<u8>,
    ) -> Result<EncodedTransactionWithStatusMeta, RpcTransactionUpdateError> {
        let tx_with_meta = message
            .as_versioned_transaction_with_status_meta()
            .map_err(RpcTransactionUpdateError::Parse)?;
        tx_with_meta
            .encode(encoding, max_supported_transaction_version, show_rewards)
            .map_err(|EncodeError::UnsupportedTransactionVersion(version)| {
                RpcTransactionUpdateError::UnsupportedTransactionVersion(version)
            })
    }

    // https://docs.rs/solana-transaction-status/2.1.11/src/solana_transaction_status/lib.rs.html#486-508
    fn tx_validate_version(
        tx_with_meta: &VersionedTransactionWithStatusMeta,
        max_supported_transaction_version: Option<u8>,
    ) -> Result<Option<TransactionVersion>, RpcTransactionUpdateError> {
        match (
            max_supported_transaction_version,
            tx_with_meta.transaction.version(),
        ) {
            // Set to none because old clients can't handle this field
            (None, TransactionVersion::LEGACY) => Ok(None),
            (None, TransactionVersion::Number(version)) => {
                Err(EncodeError::UnsupportedTransactionVersion(version))
            }
            (Some(_), TransactionVersion::LEGACY) => Ok(Some(TransactionVersion::LEGACY)),
            (Some(max_version), TransactionVersion::Number(version)) => {
                if version <= max_version {
                    Ok(Some(TransactionVersion::Number(version)))
                } else {
                    Err(EncodeError::UnsupportedTransactionVersion(version))
                }
            }
        }
        .map_err(|error| match error {
            EncodeError::UnsupportedTransactionVersion(version) => {
                RpcTransactionUpdateError::UnsupportedTransactionVersion(version)
            }
        })
    }

    // https://docs.rs/solana-transaction-status/3.0.4/src/solana_transaction_status/lib.rs.html#161
    fn build_simple_ui_transaction_status_meta(
        meta: TransactionStatusMeta,
        show_rewards: bool,
    ) -> UiTransactionStatusMeta {
        UiTransactionStatusMeta {
            err: meta.status.clone().map_err(Into::into).err(),
            status: meta.status.map_err(Into::into),
            fee: meta.fee,
            pre_balances: meta.pre_balances,
            post_balances: meta.post_balances,
            inner_instructions: OptionSerializer::Skip,
            log_messages: OptionSerializer::Skip,
            pre_token_balances: meta
                .pre_token_balances
                .map(|balance| balance.into_iter().map(Into::into).collect())
                .into(),
            post_token_balances: meta
                .post_token_balances
                .map(|balance| balance.into_iter().map(Into::into).collect())
                .into(),
            rewards: if show_rewards {
                meta.rewards.into()
            } else {
                OptionSerializer::Skip
            },
            loaded_addresses: OptionSerializer::Skip,
            return_data: OptionSerializer::Skip,
            compute_units_consumed: OptionSerializer::Skip,
            cost_units: OptionSerializer::Skip,
        }
    }

    // https://docs.rs/solana-transaction-status/latest/src/solana_transaction_status/lib.rs.html#545
    fn tx_encode_accounts(
        message: &MessageTransaction,
        show_rewards: bool,
        max_supported_transaction_version: Option<u8>,
    ) -> Result<EncodedTransactionWithStatusMeta, RpcTransactionUpdateError> {
        let tx_with_meta = message
            .as_versioned_transaction_with_status_meta()
            .map_err(RpcTransactionUpdateError::Parse)?;

        let version = Self::tx_validate_version(&tx_with_meta, max_supported_transaction_version)?;
        let reserved_account_keys = ReservedAccountKeys::new_all_activated();

        let account_keys = match &tx_with_meta.transaction.message {
            VersionedMessage::Legacy(message) => parse_legacy_message_accounts(message),
            VersionedMessage::V0(message) => {
                let loaded_message = LoadedMessage::new_borrowed(
                    message,
                    &tx_with_meta.meta.loaded_addresses,
                    &reserved_account_keys.active,
                );
                parse_v0_message_accounts(&loaded_message)
            }
            VersionedMessage::V1(message) => parse_v1_message_accounts(message),
        };

        Ok(EncodedTransactionWithStatusMeta {
            transaction: EncodedTransaction::Accounts(UiAccountsList {
                signatures: tx_with_meta
                    .transaction
                    .signatures
                    .iter()
                    .map(|sig| signature_encode(&(*sig).into()))
                    .collect(),
                account_keys,
            }),
            meta: Some(Self::build_simple_ui_transaction_status_meta(
                tx_with_meta.meta,
                show_rewards,
            )),
            version,
        })
    }
}

#[derive(Debug, Clone, Serialize, thiserror::Error)]
pub enum RpcTransactionUpdateError {
    #[error("unsupported transaction version ({0})")]
    UnsupportedTransactionVersion(u8),
    #[error("failed to parse proto: {0}")]
    Parse(&'static str),
}

#[derive(Serialize, Debug, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RpcBlockUpdate {
    pub slot: Slot,
    pub block: Option<UiConfirmedBlock>,
    pub err: Option<RpcBlockUpdateError>,
}

impl RpcBlockUpdate {
    pub fn new(
        message: &MessageBlock,
        encoding: UiTransactionEncoding,
        options: BlockEncodingOptions,
    ) -> Self {
        let (block, err) = match message
            .as_confirmed_block()
            .map_err(RpcBlockUpdateError::Parse)
            .and_then(|block| {
                block.encode_with_options(encoding, options).map_err(
                    |EncodeError::UnsupportedTransactionVersion(version)| {
                        RpcBlockUpdateError::UnsupportedTransactionVersion(version)
                    },
                )
            }) {
            Ok(block) => (Some(block), None),
            Err(error) => (None, Some(error)),
        };
        Self {
            slot: message.slot(),
            block,
            err,
        }
    }
}

#[derive(Clone, Serialize, Debug, thiserror::Error, Eq, PartialEq)]
pub enum RpcBlockUpdateError {
    #[error("block store error")]
    BlockStoreError,

    #[error("unsupported transaction version ({0})")]
    UnsupportedTransactionVersion(u8),

    #[error("failed to parse proto: {0}")]
    Parse(&'static str),
}
