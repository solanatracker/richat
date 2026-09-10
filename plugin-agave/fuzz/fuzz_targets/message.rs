//! Fuzz types shared between `transaction` and `deshred_transaction` targets

use {
    arbitrary::Arbitrary,
    solana_hash::{HASH_BYTES, Hash},
    solana_message::{
        LegacyMessage, MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction,
        legacy, v0, v1,
    },
    solana_pubkey::{PUBKEY_BYTES, Pubkey},
    std::borrow::Cow,
};

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzMessageHeader {
    pub num_required_signatures: u8,
    pub num_readonly_signed_accounts: u8,
    pub num_readonly_unsigned_accounts: u8,
}

impl From<FuzzMessageHeader> for MessageHeader {
    fn from(value: FuzzMessageHeader) -> Self {
        MessageHeader {
            num_required_signatures: value.num_required_signatures,
            num_readonly_signed_accounts: value.num_readonly_signed_accounts,
            num_readonly_unsigned_accounts: value.num_readonly_unsigned_accounts,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzCompiledInstruction {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

impl From<FuzzCompiledInstruction> for CompiledInstruction {
    fn from(fuzz: FuzzCompiledInstruction) -> Self {
        Self {
            program_id_index: fuzz.program_id_index,
            accounts: fuzz.accounts,
            data: fuzz.data,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzLegacyMessageInner {
    pub header: FuzzMessageHeader,
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
    pub recent_blockhash: [u8; HASH_BYTES],
    pub instructions: Vec<FuzzCompiledInstruction>,
}

impl From<FuzzLegacyMessageInner> for legacy::Message {
    fn from(fuzz: FuzzLegacyMessageInner) -> Self {
        Self {
            header: fuzz.header.into(),
            account_keys: fuzz
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            recent_blockhash: Hash::new_from_array(fuzz.recent_blockhash),
            instructions: fuzz.instructions.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Arbitrary)]
pub struct FuzzLegacyMessage {
    pub message: FuzzLegacyMessageInner,
    pub is_writable_account_cache: Vec<bool>,
}

impl From<FuzzLegacyMessage> for LegacyMessage<'static> {
    fn from(fuzz: FuzzLegacyMessage) -> Self {
        Self {
            message: Cow::Owned(fuzz.message.into()),
            is_writable_account_cache: fuzz.is_writable_account_cache,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzLoadedMessageInner {
    pub header: FuzzMessageHeader,
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
    pub recent_blockhash: [u8; HASH_BYTES],
    pub instructions: Vec<FuzzCompiledInstruction>,
    pub address_table_lookups: Vec<FuzzMessageAddressTableLookup>,
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzMessageAddressTableLookup {
    pub account_key: [u8; PUBKEY_BYTES],
    pub writable_indexes: Vec<u8>,
    pub readonly_indexes: Vec<u8>,
}

impl From<FuzzMessageAddressTableLookup> for v0::MessageAddressTableLookup {
    fn from(fuzz: FuzzMessageAddressTableLookup) -> Self {
        Self {
            account_key: Pubkey::new_from_array(fuzz.account_key),
            writable_indexes: fuzz.writable_indexes,
            readonly_indexes: fuzz.readonly_indexes,
        }
    }
}

impl From<FuzzLoadedMessageInner> for v0::Message {
    fn from(fuzz: FuzzLoadedMessageInner) -> Self {
        Self {
            header: fuzz.header.into(),
            account_keys: fuzz
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            recent_blockhash: Hash::new_from_array(fuzz.recent_blockhash),
            instructions: fuzz.instructions.into_iter().map(Into::into).collect(),
            address_table_lookups: fuzz
                .address_table_lookups
                .into_iter()
                .map(Into::into)
                .collect(),
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzLoadedAddresses {
    pub writable: Vec<[u8; PUBKEY_BYTES]>,
    pub readonly: Vec<[u8; PUBKEY_BYTES]>,
}

impl From<FuzzLoadedAddresses> for v0::LoadedAddresses {
    fn from(fuzz: FuzzLoadedAddresses) -> Self {
        Self {
            writable: fuzz
                .writable
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            readonly: fuzz
                .readonly
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
        }
    }
}

#[derive(Debug, Arbitrary)]
pub struct FuzzLoadedMessage {
    pub message: FuzzLoadedMessageInner,
    pub loaded_addresses: FuzzLoadedAddresses,
    pub is_writable_account_cache: Vec<bool>,
}

impl From<FuzzLoadedMessage> for v0::LoadedMessage<'static> {
    fn from(fuzz: FuzzLoadedMessage) -> v0::LoadedMessage<'static> {
        Self {
            message: Cow::Owned(fuzz.message.into()),
            loaded_addresses: Cow::Owned(fuzz.loaded_addresses.into()),
            is_writable_account_cache: fuzz.is_writable_account_cache,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzTransactionConfig {
    pub priority_fee: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub loaded_accounts_data_size_limit: Option<u32>,
    pub heap_size: Option<u32>,
}

impl From<FuzzTransactionConfig> for v1::TransactionConfig {
    fn from(fuzz: FuzzTransactionConfig) -> Self {
        Self {
            priority_fee: fuzz.priority_fee,
            compute_unit_limit: fuzz.compute_unit_limit,
            loaded_accounts_data_size_limit: fuzz.loaded_accounts_data_size_limit,
            heap_size: fuzz.heap_size,
        }
    }
}

#[derive(Debug, Clone, Arbitrary)]
pub struct FuzzV1Message {
    pub header: FuzzMessageHeader,
    pub config: FuzzTransactionConfig,
    pub lifetime_specifier: [u8; HASH_BYTES],
    pub account_keys: Vec<[u8; PUBKEY_BYTES]>,
    pub instructions: Vec<FuzzCompiledInstruction>,
}

impl From<FuzzV1Message> for v1::Message {
    fn from(fuzz: FuzzV1Message) -> Self {
        Self {
            header: fuzz.header.into(),
            config: fuzz.config.into(),
            lifetime_specifier: Hash::new_from_array(fuzz.lifetime_specifier),
            account_keys: fuzz
                .account_keys
                .into_iter()
                .map(Pubkey::new_from_array)
                .collect(),
            instructions: fuzz.instructions.into_iter().map(Into::into).collect(),
        }
    }
}

#[derive(Debug, Arbitrary)]
pub enum FuzzSanitizedMessage {
    Legacy(FuzzLegacyMessage),
    V0(FuzzLoadedMessage),
    V1(FuzzV1Message),
}

impl From<FuzzSanitizedMessage> for VersionedMessage {
    fn from(fuzz: FuzzSanitizedMessage) -> Self {
        match fuzz {
            FuzzSanitizedMessage::Legacy(legacy) => Self::Legacy(legacy.message.into()),
            FuzzSanitizedMessage::V0(v0) => Self::V0(v0.message.into()),
            FuzzSanitizedMessage::V1(v1) => Self::V1(v1.into()),
        }
    }
}
