#![no_main]

use {
    agave_geyser_plugin_interface::geyser_plugin_interface::ReplicaDeshredTransactionInfoV2,
    arbitrary::Arbitrary,
    richat_plugin_agave::protobuf::ProtobufMessage,
    solana_message::v0::LoadedAddresses,
    solana_signature::{SIGNATURE_BYTES, Signature},
    solana_transaction::versioned::VersionedTransaction,
    std::time::SystemTime,
};

#[path = "message.rs"]
mod message;

use message::*;

#[derive(Debug, Arbitrary)]
struct FuzzDeshredTransaction {
    slot: u64,
    signature: [u8; SIGNATURE_BYTES],
    is_vote: bool,
    message: FuzzSanitizedMessage,
    loaded_addresses: Option<FuzzLoadedAddresses>,
    completed_data_set_starting_shred_index: u32,
    completed_data_set_ending_shred_index_exclusive: u32,
}

libfuzzer_sys::fuzz_target!(|fuzz: FuzzDeshredTransaction| {
    let signature = Signature::from(fuzz.signature);
    let versioned_transaction = VersionedTransaction {
        signatures: vec![signature],
        message: fuzz.message.into(),
    };
    let loaded_addresses: Option<LoadedAddresses> = fuzz.loaded_addresses.map(Into::into);

    let replica = ReplicaDeshredTransactionInfoV2 {
        signature: &signature,
        is_vote: fuzz.is_vote,
        transaction: &versioned_transaction,
        loaded_addresses: loaded_addresses.as_ref(),
        completed_data_set_starting_shred_index: fuzz.completed_data_set_starting_shred_index,
        completed_data_set_ending_shred_index_exclusive: fuzz
            .completed_data_set_ending_shred_index_exclusive,
    };

    let message = ProtobufMessage::DeshredTransaction {
        slot: fuzz.slot,
        transaction: &replica,
    };
    let created_at = SystemTime::now();

    let vec_prost = message.encode_prost(created_at);
    let vec_raw = message.encode_raw(created_at);

    assert_eq!(
        vec_prost,
        vec_raw,
        "prost hex: {}",
        const_hex::encode(&vec_prost)
    );
});
