use {
    crate::protobuf::encoding,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaContactInfoV0_0_1,
        ReplicaDeshredTransactionInfoV2, ReplicaDeshredUpdateParentInfo, ReplicaEntryInfoV2,
        ReplicaEntryUpdateParentInfo, ReplicaTransactionInfoV3, SlotStatus as GeyserSlotStatus,
    },
    prost::encoding::message,
    prost_types::Timestamp,
    solana_clock::{BankId, Slot},
    solana_entry::block_component::VersionedBlockFooter,
    std::time::SystemTime,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtobufEncoder {
    Prost,
    Raw,
}

#[derive(Debug)]
pub enum ProtobufMessage<'a> {
    Account {
        slot: Slot,
        account: &'a ReplicaAccountInfoV3<'a>,
    },
    Slot {
        slot: Slot,
        parent: Option<u64>,
        status: &'a GeyserSlotStatus,
    },
    Transaction {
        slot: Slot,
        transaction: &'a ReplicaTransactionInfoV3<'a>,
    },
    Entry {
        entry: &'a ReplicaEntryInfoV2<'a>,
    },
    BlockMeta {
        blockinfo: &'a ReplicaBlockInfoV4<'a>,
    },
    // Richat-only messages, see `SubscribeUpdateRichat` in `richat.proto`
    DeshredTransaction {
        slot: Slot,
        transaction: &'a ReplicaDeshredTransactionInfoV2<'a>,
    },
    ContactInfo {
        info: &'a ReplicaContactInfoV0_0_1<'a>,
    },
    ContactInfoRemoved {
        pubkey: &'a [u8],
    },
    BlockFooter {
        slot: Slot,
        bank_id: BankId,
        block_footer: &'a VersionedBlockFooter,
    },
    EntryUpdateParent {
        info: &'a ReplicaEntryUpdateParentInfo<'a>,
    },
    DeshredUpdateParent {
        info: &'a ReplicaDeshredUpdateParentInfo<'a>,
    },
}

impl ProtobufMessage<'_> {
    /// Slot of the message, `None` for messages not related to any slot (contact info)
    pub const fn get_slot(&self) -> Option<Slot> {
        Some(match self {
            Self::Account { slot, .. } => *slot,
            Self::Slot { slot, .. } => *slot,
            Self::Transaction { slot, .. } => *slot,
            Self::Entry { entry } => entry.slot,
            Self::BlockMeta { blockinfo } => blockinfo.slot,
            Self::DeshredTransaction { slot, .. } => *slot,
            Self::ContactInfo { .. } | Self::ContactInfoRemoved { .. } => return None,
            Self::BlockFooter { slot, .. } => *slot,
            Self::EntryUpdateParent { info } => info.slot,
            Self::DeshredUpdateParent { info } => info.slot,
        })
    }

    pub fn encode(&self, encoder: ProtobufEncoder) -> Vec<u8> {
        self.encode_with_timestamp(encoder, SystemTime::now())
    }

    pub fn encode_with_timestamp(
        &self,
        encoder: ProtobufEncoder,
        created_at: impl Into<Timestamp>,
    ) -> Vec<u8> {
        match encoder {
            ProtobufEncoder::Prost => self.encode_prost(created_at),
            ProtobufEncoder::Raw => self.encode_raw(created_at),
        }
    }

    pub fn encode_prost(&self, created_at: impl Into<Timestamp>) -> Vec<u8> {
        use {
            prost::Message,
            richat_proto::{
                convert_to,
                geyser::{
                    SlotStatus, SubscribeUpdate, SubscribeUpdateAccount,
                    SubscribeUpdateAccountInfo, SubscribeUpdateBlockMeta,
                    SubscribeUpdateContactInfoNode, SubscribeUpdateContactInfoRemoved,
                    SubscribeUpdateDeshredTransaction, SubscribeUpdateDeshredTransactionInfo,
                    SubscribeUpdateEntry, SubscribeUpdateSlot, SubscribeUpdateTransaction,
                    SubscribeUpdateTransactionInfo, subscribe_update::UpdateOneof,
                },
                richat::{
                    SubscribeUpdateBlockFooter, SubscribeUpdateDeshredUpdateParent,
                    SubscribeUpdateEntryUpdateParent, SubscribeUpdateRichat,
                    subscribe_update_richat::UpdateOneof as UpdateOneofRichat,
                },
            },
        };

        let update_oneof = match self {
            Self::Account { slot, account } => UpdateOneof::Account(SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: account.pubkey.to_vec(),
                    lamports: account.lamports,
                    owner: account.owner.to_vec(),
                    executable: account.executable,
                    rent_epoch: account.rent_epoch,
                    data: account.data.to_vec(),
                    write_version: account.write_version,
                    txn_signature: account
                        .txn
                        .as_ref()
                        .map(|transaction| transaction.signature().as_ref().to_vec()),
                }),
                slot: *slot,
                is_startup: false,
            }),
            Self::Slot {
                slot,
                parent,
                status,
            } => UpdateOneof::Slot(SubscribeUpdateSlot {
                slot: *slot,
                parent: *parent,
                status: match status {
                    GeyserSlotStatus::Processed => SlotStatus::SlotProcessed,
                    GeyserSlotStatus::Rooted => SlotStatus::SlotFinalized,
                    GeyserSlotStatus::Confirmed => SlotStatus::SlotConfirmed,
                    GeyserSlotStatus::FirstShredReceived => SlotStatus::SlotFirstShredReceived,
                    GeyserSlotStatus::Completed => SlotStatus::SlotCompleted,
                    GeyserSlotStatus::CreatedBank => SlotStatus::SlotCreatedBank,
                    GeyserSlotStatus::Dead(_) => SlotStatus::SlotDead,
                } as i32,
                dead_error: if let GeyserSlotStatus::Dead(error) = status {
                    Some(error.clone())
                } else {
                    None
                },
            }),
            Self::Transaction { slot, transaction } => {
                UpdateOneof::Transaction(SubscribeUpdateTransaction {
                    transaction: Some(SubscribeUpdateTransactionInfo {
                        signature: transaction.signature.as_ref().to_vec(),
                        is_vote: transaction.is_vote,
                        transaction: Some(convert_to::create_transaction(transaction.transaction)),
                        meta: Some(convert_to::create_transaction_meta(
                            transaction.transaction_status_meta,
                        )),
                        index: transaction.index as u64,
                    }),
                    slot: *slot,
                })
            }
            Self::BlockMeta { blockinfo } => UpdateOneof::BlockMeta(SubscribeUpdateBlockMeta {
                slot: blockinfo.slot,
                blockhash: blockinfo.blockhash.to_string(),
                rewards: Some(convert_to::create_rewards_obj(
                    &blockinfo.rewards.rewards,
                    blockinfo.rewards.num_partitions,
                )),
                block_time: blockinfo.block_time.map(convert_to::create_timestamp),
                block_height: blockinfo.block_height.map(convert_to::create_block_height),
                parent_slot: blockinfo.parent_slot,
                parent_blockhash: blockinfo.parent_blockhash.to_string(),
                executed_transaction_count: blockinfo.executed_transaction_count,
                entries_count: blockinfo.entry_count,
            }),
            Self::Entry { entry } => UpdateOneof::Entry(SubscribeUpdateEntry {
                slot: entry.slot,
                index: entry.index as u64,
                num_hashes: entry.num_hashes,
                hash: entry.hash.to_vec(),
                executed_transaction_count: entry.executed_transaction_count,
                starting_transaction_index: entry.starting_transaction_index as u64,
            }),
            Self::DeshredTransaction { slot, transaction } => {
                return SubscribeUpdateRichat {
                    update_oneof: Some(UpdateOneofRichat::DeshredTransaction(
                        SubscribeUpdateDeshredTransaction {
                            transaction: Some(SubscribeUpdateDeshredTransactionInfo {
                                signature: transaction.signature.as_ref().to_vec(),
                                is_vote: transaction.is_vote,
                                transaction: Some(convert_to::create_transaction(
                                    transaction.transaction,
                                )),
                                loaded_writable_addresses: transaction
                                    .loaded_addresses
                                    .map(|addresses| {
                                        convert_to::create_pubkeys(&addresses.writable)
                                    })
                                    .unwrap_or_default(),
                                loaded_readonly_addresses: transaction
                                    .loaded_addresses
                                    .map(|addresses| {
                                        convert_to::create_pubkeys(&addresses.readonly)
                                    })
                                    .unwrap_or_default(),
                                completed_data_set_starting_shred_index: transaction
                                    .completed_data_set_starting_shred_index,
                                completed_data_set_ending_shred_index_exclusive: transaction
                                    .completed_data_set_ending_shred_index_exclusive,
                            }),
                            slot: *slot,
                        },
                    )),
                    created_at: Some(created_at.into()),
                }
                .encode_to_vec();
            }
            Self::ContactInfo { info } => {
                let socket_addr = |addr: Option<std::net::SocketAddr>| addr.map(|a| a.to_string());
                return SubscribeUpdateRichat {
                    update_oneof: Some(UpdateOneofRichat::ContactInfo(
                        SubscribeUpdateContactInfoNode {
                            pubkey: info.pubkey.to_vec(),
                            wallclock: info.wallclock,
                            outset: info.outset,
                            shred_version: info.shred_version as u32,
                            version_major: info.version_major as u32,
                            version_minor: info.version_minor as u32,
                            version_patch: info.version_patch as u32,
                            version_commit: info.version_commit,
                            version_feature_set: info.version_feature_set,
                            version_client_id: info.version_client_id as u32,
                            gossip: socket_addr(info.gossip),
                            tpu_quic: socket_addr(info.tpu_quic),
                            tpu_forwards_quic: socket_addr(info.tpu_forwards_quic),
                            tpu_vote_udp: socket_addr(info.tpu_vote_udp),
                            tpu_vote_quic: socket_addr(info.tpu_vote_quic),
                            tvu_udp: socket_addr(info.tvu_udp),
                            tvu_quic: socket_addr(info.tvu_quic),
                            serve_repair_udp: socket_addr(info.serve_repair_udp),
                            serve_repair_quic: socket_addr(info.serve_repair_quic),
                            rpc: socket_addr(info.rpc),
                            rpc_pubsub: socket_addr(info.rpc_pubsub),
                            alpenglow: socket_addr(info.alpenglow),
                        },
                    )),
                    created_at: Some(created_at.into()),
                }
                .encode_to_vec();
            }
            Self::ContactInfoRemoved { pubkey } => {
                return SubscribeUpdateRichat {
                    update_oneof: Some(UpdateOneofRichat::ContactInfoRemoved(
                        SubscribeUpdateContactInfoRemoved {
                            pubkey: pubkey.to_vec(),
                        },
                    )),
                    created_at: Some(created_at.into()),
                }
                .encode_to_vec();
            }
            Self::BlockFooter {
                slot,
                bank_id,
                block_footer,
            } => {
                let view = encoding::BlockFooterView::new(block_footer);
                return SubscribeUpdateRichat {
                    update_oneof: Some(UpdateOneofRichat::BlockFooter(
                        SubscribeUpdateBlockFooter {
                            slot: *slot,
                            bank_id: *bank_id,
                            version: view.version,
                            bank_hash: view.bank_hash.to_vec(),
                            block_producer_time_nanos: view.block_producer_time_nanos,
                            block_user_agent: view.block_user_agent.to_vec(),
                            footer: view.footer,
                        },
                    )),
                    created_at: Some(created_at.into()),
                }
                .encode_to_vec();
            }
            Self::EntryUpdateParent { info } => {
                return SubscribeUpdateRichat {
                    update_oneof: Some(UpdateOneofRichat::EntryUpdateParent(
                        SubscribeUpdateEntryUpdateParent {
                            slot: info.slot,
                            cleared_bank_id: info.cleared_bank_id,
                            parent_slot: info.parent_slot,
                            parent_block_id: info.parent_block_id.to_bytes().to_vec(),
                        },
                    )),
                    created_at: Some(created_at.into()),
                }
                .encode_to_vec();
            }
            Self::DeshredUpdateParent { info } => {
                return SubscribeUpdateRichat {
                    update_oneof: Some(UpdateOneofRichat::DeshredUpdateParent(
                        SubscribeUpdateDeshredUpdateParent {
                            slot: info.slot,
                            update_parent_fec_set_index: info.update_parent_fec_set_index,
                            parent_slot: info.parent_slot,
                            parent_block_id: info.parent_block_id.to_bytes().to_vec(),
                        },
                    )),
                    created_at: Some(created_at.into()),
                }
                .encode_to_vec();
            }
        };

        SubscribeUpdate {
            filters: Vec::new(),
            update_oneof: Some(update_oneof),
            created_at: Some(created_at.into()),
        }
        .encode_to_vec()
    }

    pub fn encode_raw(&self, created_at: impl Into<Timestamp>) -> Vec<u8> {
        let created_at = created_at.into();

        macro_rules! encode_raw {
            ($($variant:pat => ($tag:expr, $message:expr)),+ $(,)?) => {
                match self {
                    $($variant => {
                        let msg = $message;
                        let size = message::encoded_len($tag, &msg)
                            + message::encoded_len(11, &created_at);

                        let mut vec = Vec::with_capacity(size);
                        let buffer = &mut vec;

                        // prost encodes fields in tag order:
                        // `created_at` (11) goes before Richat-only updates (100+)
                        if $tag < 11 {
                            message::encode($tag, &msg, buffer);
                            message::encode(11, &created_at, buffer);
                        } else {
                            message::encode(11, &created_at, buffer);
                            message::encode($tag, &msg, buffer);
                        }

                        vec
                    })+
                }
            };
        }

        // Tags of `SubscribeUpdateRichat` (100+) are the same as in Yellowstone `SubscribeUpdate`
        encode_raw! {
            Self::Account { slot, account } => (2, encoding::Account::new(*slot, account)),
            Self::Slot { slot, parent, status } => (3, encoding::Slot::new(*slot, *parent, status)),
            Self::Transaction { slot, transaction } => (4, encoding::Transaction::new(*slot, transaction)),
            Self::BlockMeta { blockinfo } => (7, encoding::BlockMeta::new(blockinfo)),
            Self::Entry { entry } => (8, encoding::Entry::new(entry)),
            Self::DeshredTransaction { slot, transaction } => (100, encoding::DeshredTransaction::new(*slot, transaction)),
            Self::ContactInfo { info } => (101, encoding::ContactInfo::new(info)),
            Self::ContactInfoRemoved { pubkey } => (102, encoding::ContactInfoRemoved::new(pubkey)),
            Self::BlockFooter { slot, bank_id, block_footer } => (103, encoding::BlockFooter::new(*slot, *bank_id, block_footer)),
            Self::EntryUpdateParent { info } => (104, encoding::EntryUpdateParent::new(info)),
            Self::DeshredUpdateParent { info } => (105, encoding::DeshredUpdateParent::new(info)),
        }
    }
}
