mod encoding;
mod message;

pub use {
    encoding::{Account, BlockMeta, Entry, Slot, Transaction, bytes_encode, bytes_encoded_len},
    message::{ProtobufEncoder, ProtobufMessage},
};

#[cfg(test)]
mod tests {
    use {
        super::{ProtobufEncoder, ProtobufMessage},
        prost::Message,
        richat_benches::fixtures::{
            generate_accounts, generate_block_footers, generate_block_metas,
            generate_contact_infos, generate_deshred_transactions, generate_entries,
            generate_slots, generate_transactions, generate_update_parents,
        },
        richat_proto::{
            geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
            richat::{
                SubscribeUpdateRichat, subscribe_update_richat::UpdateOneof as UpdateOneofRichat,
            },
        },
        std::time::SystemTime,
    };

    fn assert_richat_encoding(
        msg_richat: &ProtobufMessage<'_>,
        update_oneof: UpdateOneofRichat,
        created_at: SystemTime,
        name: &str,
    ) {
        let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
        let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
        assert_eq!(vec_richat1, vec_richat2, "{name}: {msg_richat:?}");

        let msg_prost = SubscribeUpdateRichat {
            update_oneof: Some(update_oneof),
            created_at: Some(created_at.into()),
        };
        let vec_prost = msg_prost.encode_to_vec();
        assert_eq!(vec_richat1, vec_prost, "{name}: {msg_richat:?}");

        // Yellowstone `SubscribeUpdate` should skip Richat-only update
        let update = SubscribeUpdate::decode(vec_richat1.as_slice()).expect("failed to decode");
        assert_eq!(update.update_oneof, None, "{name}: {msg_richat:?}");
        assert_eq!(
            update.created_at,
            Some(created_at.into()),
            "{name}: {msg_richat:?}"
        );
    }

    #[test]
    pub fn test_encode_account() {
        let created_at = SystemTime::now();
        for item in generate_accounts() {
            let (slot, replica) = item.to_replica();
            let msg_richat = ProtobufMessage::Account {
                slot,
                account: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "account: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Account(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "account: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_block_meta() {
        let created_at = SystemTime::now();
        for item in generate_block_metas() {
            let replica = item.to_replica();
            let msg_richat = ProtobufMessage::BlockMeta {
                blockinfo: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "block meta: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::BlockMeta(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "block meta: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_entry() {
        let created_at = SystemTime::now();
        for item in generate_entries() {
            let replica = item.to_replica();
            let msg_richat = ProtobufMessage::Entry { entry: &replica };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "entry: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Entry(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "entry: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_slot() {
        let created_at = SystemTime::now();
        for item in generate_slots() {
            let (slot, parent, status) = item.to_replica();
            let msg_richat = ProtobufMessage::Slot {
                slot,
                parent,
                status,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "slot: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Slot(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "slot: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_transaction() {
        let created_at = SystemTime::now();
        for item in generate_transactions() {
            let (slot, replica) = item.to_replica();
            let msg_richat = ProtobufMessage::Transaction {
                slot,
                transaction: &replica,
            };
            let vec_richat1 = msg_richat.encode_with_timestamp(ProtobufEncoder::Prost, created_at);
            let vec_richat2 = msg_richat.encode_with_timestamp(ProtobufEncoder::Raw, created_at);
            assert_eq!(vec_richat1, vec_richat2, "transaction: {item:?}");

            let msg_prost = SubscribeUpdate {
                filters: Vec::new(),
                update_oneof: Some(UpdateOneof::Transaction(item.to_prost())),
                created_at: Some(created_at.into()),
            };
            let vec_prost = msg_prost.encode_to_vec();
            assert_eq!(vec_richat1, vec_prost, "transaction: {item:?}");
        }
    }

    #[test]
    pub fn test_encode_deshred_transaction() {
        let created_at = SystemTime::now();
        for item in generate_deshred_transactions() {
            let (slot, replica) = item.to_replica();
            let msg_richat = ProtobufMessage::DeshredTransaction {
                slot,
                transaction: &replica,
            };
            assert_richat_encoding(
                &msg_richat,
                UpdateOneofRichat::DeshredTransaction(item.to_prost()),
                created_at,
                "deshred transaction",
            );
        }
    }

    #[test]
    pub fn test_encode_contact_info() {
        let created_at = SystemTime::now();
        for item in generate_contact_infos() {
            let replica = item.to_replica();
            let msg_richat = ProtobufMessage::ContactInfo { info: &replica };
            assert_richat_encoding(
                &msg_richat,
                UpdateOneofRichat::ContactInfo(item.to_prost()),
                created_at,
                "contact info",
            );

            let msg_richat = ProtobufMessage::ContactInfoRemoved {
                pubkey: item.pubkey.as_array(),
            };
            assert_richat_encoding(
                &msg_richat,
                UpdateOneofRichat::ContactInfoRemoved(item.to_prost_removed()),
                created_at,
                "contact info removed",
            );
        }
    }

    #[test]
    pub fn test_encode_block_footer() {
        let created_at = SystemTime::now();
        for item in generate_block_footers() {
            let msg_richat = ProtobufMessage::BlockFooter {
                slot: item.slot,
                bank_id: item.bank_id,
                block_footer: &item.block_footer,
            };
            assert_richat_encoding(
                &msg_richat,
                UpdateOneofRichat::BlockFooter(item.to_prost()),
                created_at,
                "block footer",
            );
        }
    }

    #[test]
    pub fn test_encode_update_parent() {
        let created_at = SystemTime::now();
        for item in generate_update_parents() {
            let replica = item.to_replica_entry();
            let msg_richat = ProtobufMessage::EntryUpdateParent { info: &replica };
            assert_richat_encoding(
                &msg_richat,
                UpdateOneofRichat::EntryUpdateParent(item.to_prost_entry()),
                created_at,
                "entry update parent",
            );

            let replica = item.to_replica_deshred();
            let msg_richat = ProtobufMessage::DeshredUpdateParent { info: &replica };
            assert_richat_encoding(
                &msg_richat,
                UpdateOneofRichat::DeshredUpdateParent(item.to_prost_deshred()),
                created_at,
                "deshred update parent",
            );
        }
    }
}
