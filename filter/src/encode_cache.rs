//! Encode a filtered update once and share it between clients.
//!
//! `SubscribeUpdate` is encoded as `filters` (field 1) followed by the update
//! and `created_at`. Only `filters` depends on the client, so the rest (the
//! body) can be encoded once per message and filter output shape, and every
//! client gets its own small `filters` prefix in front of the shared body.

use {
    crate::{
        filter::{FilterAccountDataSlices, FilteredUpdate, FilteredUpdateType},
        protobuf::{bytes_encode, bytes_encoded_len},
    },
    prost::bytes::{BufMut, Bytes},
    smallvec::SmallVec,
};

/// Encoded `filters` field; inline for typical filter names
pub type EncodedFilters = SmallVec<[u8; 64]>;

pub fn encode_filters(filters: &[&str]) -> EncodedFilters {
    let mut buf = EncodedFilters::from_elem(0, encoded_filters_len(filters));
    let mut slice = buf.as_mut_slice();
    for filter in filters {
        bytes_encode(1, filter.as_bytes(), &mut slice);
    }
    buf
}

pub fn encoded_filters_len(filters: &[&str]) -> usize {
    filters
        .iter()
        .map(|filter| bytes_encoded_len(1, filter.as_bytes()))
        .sum()
}

/// Encode everything except `filters`
pub fn encode_body(update: &FilteredUpdate<'_>) -> Vec<u8> {
    FilteredUpdate {
        filters: SmallVec::new(),
        filtered_update: update.filtered_update.clone(),
    }
    .encode_to_vec()
}

/// Client `filters` prefix and the body shared between clients;
/// `filters ++ body` is the same bytes as [`FilteredUpdate::encode_to_vec`]
#[derive(Debug, Clone, Default)]
pub struct SharedEncodedUpdate {
    pub filters: EncodedFilters,
    pub body: Bytes,
}

impl SharedEncodedUpdate {
    pub fn new(filters: &[&str], body: Bytes) -> Self {
        Self {
            filters: encode_filters(filters),
            body,
        }
    }

    pub fn len(&self) -> usize {
        self.filters.len() + self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn encode(&self, buf: &mut impl BufMut) {
        buf.put_slice(&self.filters);
        buf.put_slice(&self.body);
    }

    pub fn to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.len());
        self.encode(&mut buf);
        buf
    }
}

impl From<Vec<u8>> for SharedEncodedUpdate {
    fn from(body: Vec<u8>) -> Self {
        Self {
            filters: EncodedFilters::new(),
            body: body.into(),
        }
    }
}

/// Everything besides the message itself that changes the encoded body
#[derive(Debug, Clone, PartialEq, Eq)]
enum BodyKey {
    Slot,
    Account {
        data_slices: FilterAccountDataSlices,
    },
    Transaction,
    TransactionStatus,
    Entry,
    BlockMeta,
    Block {
        accounts: Vec<usize>,
        transactions: Vec<usize>,
        entries: bool,
        data_slices: FilterAccountDataSlices,
    },
    BlockFooter {
        include_certificates: bool,
    },
    EntryUpdateParent,
}

impl BodyKey {
    fn new(update: &FilteredUpdateType<'_>) -> Self {
        match update {
            FilteredUpdateType::Slot { .. } => Self::Slot,
            FilteredUpdateType::Account { data_slices, .. } => Self::Account {
                data_slices: (*data_slices).clone(),
            },
            FilteredUpdateType::Transaction { .. } => Self::Transaction,
            FilteredUpdateType::TransactionStatus { .. } => Self::TransactionStatus,
            FilteredUpdateType::Entry { .. } => Self::Entry,
            FilteredUpdateType::BlockMeta { .. } => Self::BlockMeta,
            FilteredUpdateType::Block {
                accounts,
                transactions,
                entries,
                data_slices,
                ..
            } => Self::Block {
                accounts: accounts.clone(),
                transactions: transactions.clone(),
                entries: *entries,
                data_slices: (*data_slices).clone(),
            },
            FilteredUpdateType::BlockFooter {
                include_certificates,
                ..
            } => Self::BlockFooter {
                include_certificates: *include_certificates,
            },
            FilteredUpdateType::EntryUpdateParent { .. } => Self::EntryUpdateParent,
        }
    }

    fn is_match(&self, update: &FilteredUpdateType<'_>) -> bool {
        match (self, update) {
            (Self::Slot, FilteredUpdateType::Slot { .. })
            | (Self::Transaction, FilteredUpdateType::Transaction { .. })
            | (Self::TransactionStatus, FilteredUpdateType::TransactionStatus { .. })
            | (Self::Entry, FilteredUpdateType::Entry { .. })
            | (Self::BlockMeta, FilteredUpdateType::BlockMeta { .. })
            | (Self::EntryUpdateParent, FilteredUpdateType::EntryUpdateParent { .. }) => true,
            (
                Self::BlockFooter {
                    include_certificates,
                },
                FilteredUpdateType::BlockFooter {
                    include_certificates: update_include_certificates,
                    ..
                },
            ) => include_certificates == update_include_certificates,
            (
                Self::Account { data_slices },
                FilteredUpdateType::Account {
                    data_slices: update_data_slices,
                    ..
                },
            ) => data_slices == *update_data_slices,
            (
                Self::Block {
                    accounts,
                    transactions,
                    entries,
                    data_slices,
                },
                FilteredUpdateType::Block {
                    accounts: update_accounts,
                    transactions: update_transactions,
                    entries: update_entries,
                    data_slices: update_data_slices,
                    ..
                },
            ) => {
                entries == update_entries
                    && data_slices == *update_data_slices
                    && accounts == update_accounts
                    && transactions == update_transactions
            }
            _ => false,
        }
    }
}

/// Encoded bodies of one message, keyed by filter output shape.
///
/// A body is encoded separately only when a second client asks for the same
/// shape, so a message with a single subscriber costs one regular encode.
///
/// The caller owns the message identity: keep one cache per message and
/// clear it when the slot is reused for another message.
#[derive(Debug, Default)]
pub struct EncodeCache {
    bodies: SmallVec<[(BodyKey, Option<Bytes>); 2]>,
    encoded_bytes: usize,
}

impl EncodeCache {
    /// Shapes beyond this are encoded per client
    pub const MAX_BODIES: usize = 8;

    pub fn clear(&mut self) {
        self.bodies.clear();
        self.encoded_bytes = 0;
    }

    /// Total size of cached bodies
    pub const fn encoded_bytes(&self) -> usize {
        self.encoded_bytes
    }

    fn find(&self, update: &FilteredUpdate<'_>) -> Option<usize> {
        self.bodies
            .iter()
            .position(|(key, _body)| key.is_match(&update.filtered_update))
    }

    pub fn get(&self, update: &FilteredUpdate<'_>) -> Option<&Bytes> {
        self.find(update)
            .and_then(|idx| self.bodies[idx].1.as_ref())
    }

    /// Returns `true` if the shape was requested before; otherwise remembers
    /// it (if there is room) and returns `false`
    pub fn mark_seen(&mut self, update: &FilteredUpdate<'_>) -> bool {
        if self.find(update).is_some() {
            return true;
        }
        if self.bodies.len() < Self::MAX_BODIES {
            self.bodies
                .push((BodyKey::new(&update.filtered_update), None));
        }
        false
    }

    /// Returns `false` if the cache is full
    pub fn insert(&mut self, update: &FilteredUpdate<'_>, body: Bytes) -> bool {
        let encoded_bytes = body.len();
        match self.find(update) {
            Some(idx) => {
                if let Some(prev) = self.bodies[idx].1.replace(body) {
                    self.encoded_bytes -= prev.len();
                }
            }
            None if self.bodies.len() < Self::MAX_BODIES => {
                self.bodies
                    .push((BodyKey::new(&update.filtered_update), Some(body)));
            }
            None => return false,
        }
        self.encoded_bytes += encoded_bytes;
        true
    }

    /// Shares the cached body; encodes and caches it on the second request
    /// of a shape; otherwise encodes the update in one piece
    pub fn get_or_encode(&mut self, update: &FilteredUpdate<'_>) -> SharedEncodedUpdate {
        if let Some(body) = self.get(update) {
            return SharedEncodedUpdate::new(&update.filters, body.clone());
        }
        if !self.mark_seen(update) {
            return update.encode_to_vec().into();
        }
        let body = Bytes::from(encode_body(update));
        self.insert(update, body.clone());
        SharedEncodedUpdate::new(&update.filters, body)
    }
}
