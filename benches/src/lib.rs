pub mod fixtures {
    use {
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaContactInfoV0_0_1,
            ReplicaDeshredTransactionInfoV2, ReplicaDeshredUpdateParentInfo, ReplicaEntryInfoV2,
            ReplicaEntryUpdateParentInfo, ReplicaTransactionInfoV3, SlotStatus as GeyserSlotStatus,
        },
        prost::Message,
        richat_proto::{
            convert_to,
            geyser::{
                SlotStatus, SubscribeUpdateAccount, SubscribeUpdateAccountInfo,
                SubscribeUpdateBlockMeta, SubscribeUpdateContactInfoNode,
                SubscribeUpdateContactInfoRemoved, SubscribeUpdateDeshredTransaction,
                SubscribeUpdateDeshredTransactionInfo, SubscribeUpdateEntry, SubscribeUpdateSlot,
                SubscribeUpdateTransaction, SubscribeUpdateTransactionInfo,
            },
            richat::{
                SubscribeUpdateBlockFooter, SubscribeUpdateDeshredUpdateParent,
                SubscribeUpdateEntryUpdateParent,
            },
        },
        solana_clock::{BankId, Slot},
        solana_entry::block_component::{BlockFooterV1, VersionedBlockFooter},
        solana_hash::{HASH_BYTES, Hash},
        solana_message::{SimpleAddressLoader, v0::LoadedAddresses},
        solana_pubkey::Pubkey,
        solana_signature::Signature,
        solana_storage_proto::convert::generated,
        solana_transaction::{
            sanitized::{MessageHash, SanitizedTransaction},
            versioned::VersionedTransaction,
        },
        solana_transaction_status::{
            ConfirmedBlock, RewardsAndNumPartitions, TransactionStatusMeta,
        },
        std::{
            collections::HashSet,
            fs,
            net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV6},
        },
    };

    pub fn load_predefined_blocks() -> Vec<(Slot, ConfirmedBlock)> {
        fs::read_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/fixtures/blocks"))
            .expect("failed to read `fixtures` directory")
            .map(|entry| {
                let entry = entry.expect("failed to read entry directory");
                let path = entry.path();

                let file_name = path.file_name().expect("failed to get fixture file name");
                let extension = path.extension().expect("failed to get fixture extension");
                let slot = file_name.to_str().expect("failed to stringify file name")
                    [0..extension.len()]
                    .parse::<u64>()
                    .expect("failed to parse file name");

                let data = fs::read(path).expect("failed to read fixture");
                let block = generated::ConfirmedBlock::decode(data.as_slice())
                    .expect("failed to decode fixture")
                    .try_into()
                    .expect("failed to parse block");

                (slot, block)
            })
            .collect::<Vec<_>>()
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedAccount {
        pub pubkey: Pubkey,
        pub lamports: u64,
        pub owner: Pubkey,
        pub executable: bool,
        pub rent_epoch: u64,
        pub data: Vec<u8>,
        pub write_version: u64,
        pub transaction_signature: Option<SanitizedTransaction>,
        pub slot: Slot,
    }

    impl GeneratedAccount {
        pub fn to_replica(&self) -> (Slot, ReplicaAccountInfoV3<'_>) {
            let replica = ReplicaAccountInfoV3 {
                pubkey: self.pubkey.as_ref(),
                lamports: self.lamports,
                owner: self.owner.as_ref(),
                executable: self.executable,
                rent_epoch: self.rent_epoch,
                data: &self.data,
                write_version: self.write_version,
                txn: self.transaction_signature.as_ref(),
            };
            (self.slot, replica)
        }

        pub fn to_prost(&self) -> SubscribeUpdateAccount {
            SubscribeUpdateAccount {
                account: Some(SubscribeUpdateAccountInfo {
                    pubkey: self.pubkey.as_ref().to_vec(),
                    lamports: self.lamports,
                    owner: self.owner.as_ref().to_vec(),
                    executable: self.executable,
                    rent_epoch: self.rent_epoch,
                    data: self.data.clone(),
                    write_version: self.write_version,
                    txn_signature: self
                        .transaction_signature
                        .as_ref()
                        .map(|transaction| transaction.signature().as_ref().to_vec()),
                }),
                slot: self.slot,
                is_startup: false,
            }
        }
    }

    pub fn generate_accounts() -> Vec<GeneratedAccount> {
        const PUBKEY: Pubkey =
            Pubkey::from_str_const("28Dncoh8nmzXYEGLUcBA5SUw5WDwDBn15uUCwrWBbyuu");
        const OWNER: Pubkey =
            Pubkey::from_str_const("5jrPJWVGrFvQ2V9wRZC3kHEZhxo9pmMir15x73oHT6mn");

        let blocks = load_predefined_blocks();
        let versioned_transaction = blocks
            .iter()
            .flat_map(|(_slot, confirmed_block)| {
                confirmed_block
                    .transactions
                    .iter()
                    .map(|tx| tx.get_transaction())
            })
            .next()
            .expect("failed to get first `VersionedTransaction`");
        let address_loader = match versioned_transaction.message.address_table_lookups() {
            Some(vec_atl) => SimpleAddressLoader::Enabled(LoadedAddresses {
                writable: vec_atl.iter().map(|atl| atl.account_key).collect(),
                readonly: vec_atl.iter().map(|atl| atl.account_key).collect(),
            }),
            None => SimpleAddressLoader::Disabled,
        };
        let sanitized_transaction = SanitizedTransaction::try_create(
            versioned_transaction,
            MessageHash::Compute, // message_hash
            None,                 // is_simple_vote_tx
            address_loader,
            &HashSet::new(), // reserved_account_keys
        )
        .expect("failed to create sanitized transaction");

        let mut accounts = Vec::new();
        for lamports in [0, 8123] {
            for executable in [true, false] {
                for rent_epoch in [0, 4242] {
                    for data in [
                        vec![],
                        vec![42; 165],
                        vec![42; 1024],
                        vec![42; 2 * 1024 * 1024],
                    ] {
                        for write_version in [0, 1] {
                            for transaction_signature in [None, Some(&sanitized_transaction)] {
                                for slot in [0, 310639056] {
                                    accounts.push(GeneratedAccount {
                                        pubkey: PUBKEY,
                                        lamports,
                                        owner: OWNER,
                                        executable,
                                        rent_epoch,
                                        data: data.to_owned(),
                                        write_version,
                                        transaction_signature: transaction_signature.cloned(),
                                        slot,
                                    })
                                }
                            }
                        }
                    }
                }
            }
        }

        accounts
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedBlockMeta {
        slot: Slot,
        block: ConfirmedBlock,
        rewards: RewardsAndNumPartitions,
        entry_count: u64,
    }

    impl GeneratedBlockMeta {
        pub fn to_replica(&self) -> ReplicaBlockInfoV4<'_> {
            ReplicaBlockInfoV4 {
                parent_slot: self.block.parent_slot,
                slot: self.slot,
                parent_blockhash: &self.block.previous_blockhash,
                blockhash: &self.block.blockhash,
                rewards: &self.rewards,
                block_time: self.block.block_time,
                block_height: self.block.block_height,
                executed_transaction_count: self.block.transactions.len() as u64,
                entry_count: self.entry_count,
            }
        }

        pub fn to_prost(&self) -> SubscribeUpdateBlockMeta {
            SubscribeUpdateBlockMeta {
                slot: self.slot,
                blockhash: self.block.blockhash.clone(),
                rewards: Some(convert_to::create_rewards_obj(
                    &self.rewards.rewards,
                    self.rewards.num_partitions,
                )),
                block_time: self.block.block_time.map(convert_to::create_timestamp),
                block_height: self.block.block_height.map(convert_to::create_block_height),
                parent_slot: self.block.parent_slot,
                parent_blockhash: self.block.previous_blockhash.clone(),
                executed_transaction_count: self.block.transactions.len() as u64,
                entries_count: self.entry_count,
            }
        }
    }

    pub fn generate_block_metas() -> Vec<GeneratedBlockMeta> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|(slot, block)| {
                let rewards = RewardsAndNumPartitions {
                    rewards: block.rewards.to_owned(),
                    num_partitions: block.num_partitions,
                };

                [
                    GeneratedBlockMeta {
                        slot,
                        block: block.clone(),
                        rewards: rewards.clone(),
                        entry_count: 0,
                    },
                    GeneratedBlockMeta {
                        slot,
                        block,
                        rewards,
                        entry_count: 42,
                    },
                ]
            })
            .collect::<Vec<_>>()
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedEntry {
        pub slot: Slot,
        pub index: usize,
        pub num_hashes: u64,
        pub hash: Hash,
        pub executed_transaction_count: u64,
        pub starting_transaction_index: usize,
    }

    impl GeneratedEntry {
        pub fn to_replica(&self) -> ReplicaEntryInfoV2<'_> {
            ReplicaEntryInfoV2 {
                slot: self.slot,
                index: self.index,
                num_hashes: self.num_hashes,
                hash: self.hash.as_ref(),
                executed_transaction_count: self.executed_transaction_count,
                starting_transaction_index: self.starting_transaction_index,
            }
        }

        pub fn to_prost(&self) -> SubscribeUpdateEntry {
            SubscribeUpdateEntry {
                slot: self.slot,
                index: self.index as u64,
                num_hashes: self.num_hashes,
                hash: self.hash.as_ref().to_vec(),
                executed_transaction_count: self.executed_transaction_count,
                starting_transaction_index: self.starting_transaction_index as u64,
            }
        }
    }

    pub fn generate_entries() -> Vec<GeneratedEntry> {
        const ENTRY_HASHES: [Hash; 4] = [
            Hash::new_from_array([0; HASH_BYTES]),
            Hash::new_from_array([42; HASH_BYTES]),
            Hash::new_from_array([98; HASH_BYTES]),
            Hash::new_from_array([255; HASH_BYTES]),
        ];

        let mut entries = Vec::new();
        for slot in [0, 42, 310629080] {
            for index in [0, 42] {
                for num_hashes in [0, 128] {
                    for hash in &ENTRY_HASHES {
                        for executed_transaction_count in [0, 32] {
                            for starting_transaction_index in [0, 96, 1067] {
                                entries.push(GeneratedEntry {
                                    slot,
                                    index,
                                    num_hashes,
                                    hash: *hash,
                                    executed_transaction_count,
                                    starting_transaction_index,
                                });
                            }
                        }
                    }
                }
            }
        }
        entries
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedSlot {
        pub slot: Slot,
        pub parent: Option<Slot>,
        pub status: GeyserSlotStatus,
    }

    impl GeneratedSlot {
        pub const fn to_replica(&self) -> (Slot, Option<Slot>, &GeyserSlotStatus) {
            (self.slot, self.parent, &self.status)
        }

        pub fn to_prost(&self) -> SubscribeUpdateSlot {
            SubscribeUpdateSlot {
                slot: self.slot,
                parent: self.parent,
                status: match &self.status {
                    GeyserSlotStatus::Processed => SlotStatus::SlotProcessed,
                    GeyserSlotStatus::Rooted => SlotStatus::SlotFinalized,
                    GeyserSlotStatus::Confirmed => SlotStatus::SlotConfirmed,
                    GeyserSlotStatus::FirstShredReceived => SlotStatus::SlotFirstShredReceived,
                    GeyserSlotStatus::Completed => SlotStatus::SlotCompleted,
                    GeyserSlotStatus::CreatedBank => SlotStatus::SlotCreatedBank,
                    GeyserSlotStatus::Dead(_error) => SlotStatus::SlotDead,
                } as i32,
                dead_error: if let GeyserSlotStatus::Dead(error) = &self.status {
                    Some(error.clone())
                } else {
                    None
                },
            }
        }
    }

    pub fn generate_slots() -> Vec<GeneratedSlot> {
        let mut slots = Vec::new();
        for slot in [0, 42, 310629080] {
            for parent in [None, Some(0), Some(42)] {
                for status in [
                    GeyserSlotStatus::Processed,
                    GeyserSlotStatus::Rooted,
                    GeyserSlotStatus::Confirmed,
                    GeyserSlotStatus::FirstShredReceived,
                    GeyserSlotStatus::Completed,
                    GeyserSlotStatus::CreatedBank,
                    GeyserSlotStatus::Dead("".to_owned()),
                    GeyserSlotStatus::Dead("42".to_owned()),
                ] {
                    slots.push(GeneratedSlot {
                        slot,
                        parent,
                        status,
                    })
                }
            }
        }
        slots
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedTransaction {
        pub slot: Slot,
        pub signature: Signature,
        pub message_hash: Hash,
        pub is_vote: bool,
        pub versioned_transaction: VersionedTransaction,
        pub transaction_status_meta: TransactionStatusMeta,
        pub index: usize,
    }

    impl GeneratedTransaction {
        pub const fn to_replica(&self) -> (Slot, ReplicaTransactionInfoV3<'_>) {
            let replica = ReplicaTransactionInfoV3 {
                signature: &self.signature,
                message_hash: &self.message_hash,
                is_vote: self.is_vote,
                transaction: &self.versioned_transaction,
                transaction_status_meta: &self.transaction_status_meta,
                index: self.index,
            };
            (self.slot, replica)
        }

        pub fn to_prost(&self) -> SubscribeUpdateTransaction {
            SubscribeUpdateTransaction {
                transaction: Some(SubscribeUpdateTransactionInfo {
                    signature: self.signature.as_ref().to_vec(),
                    is_vote: self.is_vote,
                    transaction: Some(convert_to::create_transaction(&self.versioned_transaction)),
                    meta: Some(convert_to::create_transaction_meta(
                        &self.transaction_status_meta,
                    )),
                    index: self.index as u64,
                }),
                slot: self.slot,
            }
        }
    }

    pub fn generate_transactions() -> Vec<GeneratedTransaction> {
        load_predefined_blocks()
            .into_iter()
            .flat_map(|(slot, block)| {
                let mut transactions = block
                    .transactions
                    .into_iter()
                    .enumerate()
                    .map(|(index, transaction)| {
                        let versioned_transaction = transaction.get_transaction();
                        let address_loader =
                            match versioned_transaction.message.address_table_lookups() {
                                Some(vec_atl) => SimpleAddressLoader::Enabled(LoadedAddresses {
                                    writable: vec_atl.iter().map(|atl| atl.account_key).collect(),
                                    readonly: vec_atl.iter().map(|atl| atl.account_key).collect(),
                                }),
                                None => SimpleAddressLoader::Disabled,
                            };
                        let sanitized_transaction = SanitizedTransaction::try_create(
                            versioned_transaction.clone(),
                            MessageHash::Compute, // message_hash
                            None,                 // is_simple_vote_tx
                            address_loader,
                            &HashSet::new(), // reserved_account_keys
                        )
                        .expect("failed to create sanitized transaction");

                        GeneratedTransaction {
                            slot,
                            signature: *sanitized_transaction.signature(),
                            message_hash: *sanitized_transaction.message_hash(),
                            is_vote: sanitized_transaction.is_simple_vote_transaction(),
                            versioned_transaction,
                            transaction_status_meta: transaction
                                .get_status_meta()
                                .expect("failed to get transaction status meta"),
                            index,
                        }
                    })
                    .collect::<Vec<_>>();

                if let Some(tx) = transactions.first() {
                    let mut tx = tx.clone();
                    tx.slot = 0;
                    transactions.push(tx);
                }

                transactions
            })
            .collect::<Vec<_>>()
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedDeshredTransaction {
        pub slot: Slot,
        pub signature: Signature,
        pub is_vote: bool,
        pub versioned_transaction: VersionedTransaction,
        pub loaded_addresses: Option<LoadedAddresses>,
        pub completed_data_set_starting_shred_index: u32,
        pub completed_data_set_ending_shred_index_exclusive: u32,
    }

    impl GeneratedDeshredTransaction {
        pub const fn to_replica(&self) -> (Slot, ReplicaDeshredTransactionInfoV2<'_>) {
            let replica = ReplicaDeshredTransactionInfoV2 {
                signature: &self.signature,
                is_vote: self.is_vote,
                transaction: &self.versioned_transaction,
                loaded_addresses: self.loaded_addresses.as_ref(),
                completed_data_set_starting_shred_index: self
                    .completed_data_set_starting_shred_index,
                completed_data_set_ending_shred_index_exclusive: self
                    .completed_data_set_ending_shred_index_exclusive,
            };
            (self.slot, replica)
        }

        pub fn to_prost(&self) -> SubscribeUpdateDeshredTransaction {
            SubscribeUpdateDeshredTransaction {
                transaction: Some(SubscribeUpdateDeshredTransactionInfo {
                    signature: self.signature.as_ref().to_vec(),
                    is_vote: self.is_vote,
                    transaction: Some(convert_to::create_transaction(&self.versioned_transaction)),
                    loaded_writable_addresses: self
                        .loaded_addresses
                        .as_ref()
                        .map(|addresses| convert_to::create_pubkeys(&addresses.writable))
                        .unwrap_or_default(),
                    loaded_readonly_addresses: self
                        .loaded_addresses
                        .as_ref()
                        .map(|addresses| convert_to::create_pubkeys(&addresses.readonly))
                        .unwrap_or_default(),
                    completed_data_set_starting_shred_index: self
                        .completed_data_set_starting_shred_index,
                    completed_data_set_ending_shred_index_exclusive: self
                        .completed_data_set_ending_shred_index_exclusive,
                }),
                slot: self.slot,
            }
        }
    }

    pub fn generate_deshred_transactions() -> Vec<GeneratedDeshredTransaction> {
        generate_transactions()
            .into_iter()
            .enumerate()
            .flat_map(|(index, tx)| {
                let loaded_addresses = LoadedAddresses {
                    writable: tx.transaction_status_meta.loaded_addresses.writable.clone(),
                    readonly: tx.transaction_status_meta.loaded_addresses.readonly.clone(),
                };
                let base = GeneratedDeshredTransaction {
                    slot: tx.slot,
                    signature: tx.signature,
                    is_vote: tx.is_vote,
                    versioned_transaction: tx.versioned_transaction,
                    loaded_addresses: None,
                    completed_data_set_starting_shred_index: 0,
                    completed_data_set_ending_shred_index_exclusive: 0,
                };
                let mut with_addresses = base.clone();
                with_addresses.loaded_addresses = Some(loaded_addresses);
                with_addresses.completed_data_set_starting_shred_index = index as u32;
                with_addresses.completed_data_set_ending_shred_index_exclusive = index as u32 + 3;
                [base, with_addresses]
            })
            .collect()
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedContactInfo {
        pub pubkey: Pubkey,
        pub wallclock: u64,
        pub outset: u64,
        pub shred_version: u16,
        pub version_major: u16,
        pub version_minor: u16,
        pub version_patch: u16,
        pub version_commit: u32,
        pub version_feature_set: u32,
        pub version_client_id: u16,
        pub gossip: Option<SocketAddr>,
        pub tpu_quic: Option<SocketAddr>,
        pub tpu_forwards_quic: Option<SocketAddr>,
        pub tpu_vote_udp: Option<SocketAddr>,
        pub tpu_vote_quic: Option<SocketAddr>,
        pub tvu_udp: Option<SocketAddr>,
        pub tvu_quic: Option<SocketAddr>,
        pub serve_repair_udp: Option<SocketAddr>,
        pub serve_repair_quic: Option<SocketAddr>,
        pub rpc: Option<SocketAddr>,
        pub rpc_pubsub: Option<SocketAddr>,
        pub alpenglow: Option<SocketAddr>,
    }

    impl GeneratedContactInfo {
        pub const fn to_replica(&self) -> ReplicaContactInfoV0_0_1<'_> {
            ReplicaContactInfoV0_0_1 {
                pubkey: self.pubkey.as_array(),
                wallclock: self.wallclock,
                outset: self.outset,
                shred_version: self.shred_version,
                version_major: self.version_major,
                version_minor: self.version_minor,
                version_patch: self.version_patch,
                version_commit: self.version_commit,
                version_feature_set: self.version_feature_set,
                version_client_id: self.version_client_id,
                gossip: self.gossip,
                tpu_quic: self.tpu_quic,
                tpu_forwards_quic: self.tpu_forwards_quic,
                tpu_vote_udp: self.tpu_vote_udp,
                tpu_vote_quic: self.tpu_vote_quic,
                tvu_udp: self.tvu_udp,
                tvu_quic: self.tvu_quic,
                serve_repair_udp: self.serve_repair_udp,
                serve_repair_quic: self.serve_repair_quic,
                rpc: self.rpc,
                rpc_pubsub: self.rpc_pubsub,
                alpenglow: self.alpenglow,
            }
        }

        pub fn to_prost(&self) -> SubscribeUpdateContactInfoNode {
            let addr = |addr: Option<SocketAddr>| addr.map(|addr| addr.to_string());
            SubscribeUpdateContactInfoNode {
                pubkey: self.pubkey.to_bytes().to_vec(),
                wallclock: self.wallclock,
                outset: self.outset,
                shred_version: self.shred_version as u32,
                version_major: self.version_major as u32,
                version_minor: self.version_minor as u32,
                version_patch: self.version_patch as u32,
                version_commit: self.version_commit,
                version_feature_set: self.version_feature_set,
                version_client_id: self.version_client_id as u32,
                gossip: addr(self.gossip),
                tpu_quic: addr(self.tpu_quic),
                tpu_forwards_quic: addr(self.tpu_forwards_quic),
                tpu_vote_udp: addr(self.tpu_vote_udp),
                tpu_vote_quic: addr(self.tpu_vote_quic),
                tvu_udp: addr(self.tvu_udp),
                tvu_quic: addr(self.tvu_quic),
                serve_repair_udp: addr(self.serve_repair_udp),
                serve_repair_quic: addr(self.serve_repair_quic),
                rpc: addr(self.rpc),
                rpc_pubsub: addr(self.rpc_pubsub),
                alpenglow: addr(self.alpenglow),
            }
        }

        pub fn to_prost_removed(&self) -> SubscribeUpdateContactInfoRemoved {
            SubscribeUpdateContactInfoRemoved {
                pubkey: self.pubkey.to_bytes().to_vec(),
            }
        }
    }

    pub fn generate_contact_infos() -> Vec<GeneratedContactInfo> {
        let v4 = |port: u16| {
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)),
                port,
            ))
        };
        let v6 = |port: u16| {
            Some(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::new(
                    0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
                )),
                port,
            ))
        };

        vec![
            GeneratedContactInfo {
                pubkey: Pubkey::new_unique(),
                wallclock: 1_757_500_000_000,
                outset: 1_757_400_000_000_000,
                shred_version: 50093,
                version_major: 4,
                version_minor: 3,
                version_patch: 0,
                version_commit: 0xdead_beef,
                version_feature_set: 0x1234_5678,
                version_client_id: 3,
                gossip: v4(8001),
                tpu_quic: v4(8009),
                tpu_forwards_quic: v4(8010),
                tpu_vote_udp: v4(8005),
                tpu_vote_quic: v4(8006),
                tvu_udp: v4(8002),
                tvu_quic: v4(8003),
                serve_repair_udp: v4(8008),
                serve_repair_quic: v4(8007),
                rpc: v4(8899),
                rpc_pubsub: v4(8900),
                alpenglow: v4(8011),
            },
            GeneratedContactInfo {
                pubkey: Pubkey::new_unique(),
                wallclock: u64::MAX,
                outset: 0,
                shred_version: u16::MAX,
                version_major: u16::MAX,
                version_minor: 0,
                version_patch: u16::MAX,
                version_commit: u32::MAX,
                version_feature_set: 0,
                version_client_id: u16::MAX,
                gossip: v6(u16::MAX),
                tpu_quic: None,
                tpu_forwards_quic: v6(1),
                tpu_vote_udp: None,
                tpu_vote_quic: v6(0),
                tvu_udp: None,
                tvu_quic: None,
                serve_repair_udp: None,
                serve_repair_quic: None,
                rpc: Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 8899)),
                // longest possible `SocketAddr` display: full IPv6 + max scope id + max port
                rpc_pubsub: Some(SocketAddr::V6(SocketAddrV6::new(
                    Ipv6Addr::new(
                        0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
                    ),
                    u16::MAX,
                    0,
                    u32::MAX,
                ))),
                alpenglow: None,
            },
            GeneratedContactInfo {
                pubkey: Pubkey::default(),
                wallclock: 0,
                outset: 0,
                shred_version: 0,
                version_major: 0,
                version_minor: 0,
                version_patch: 0,
                version_commit: 0,
                version_feature_set: 0,
                version_client_id: 0,
                gossip: None,
                tpu_quic: None,
                tpu_forwards_quic: None,
                tpu_vote_udp: None,
                tpu_vote_quic: None,
                tvu_udp: None,
                tvu_quic: None,
                serve_repair_udp: None,
                serve_repair_quic: None,
                rpc: None,
                rpc_pubsub: None,
                alpenglow: None,
            },
        ]
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedBlockFooter {
        pub slot: Slot,
        pub bank_id: BankId,
        pub block_footer: VersionedBlockFooter,
    }

    impl GeneratedBlockFooter {
        pub fn to_prost(&self) -> SubscribeUpdateBlockFooter {
            let VersionedBlockFooter::V1(footer) = &self.block_footer;
            SubscribeUpdateBlockFooter {
                slot: self.slot,
                bank_id: self.bank_id,
                version: 1,
                bank_hash: footer.bank_hash.to_bytes().to_vec(),
                block_producer_time_nanos: footer.block_producer_time_nanos,
                block_user_agent: footer.block_user_agent.clone(),
                footer: wincode::serialize(&self.block_footer).expect("failed to serialize"),
            }
        }
    }

    pub fn generate_block_footers() -> Vec<GeneratedBlockFooter> {
        vec![
            GeneratedBlockFooter {
                slot: 362_000_001,
                bank_id: 42,
                block_footer: VersionedBlockFooter::V1(BlockFooterV1 {
                    bank_hash: Hash::new_from_array([7; HASH_BYTES]),
                    block_producer_time_nanos: 1_757_500_000_000_000_000,
                    block_user_agent: b"agave/4.3.0".to_vec(),
                    block_final_cert: None,
                    skip_reward_cert: None,
                    notar_reward_cert: None,
                }),
            },
            GeneratedBlockFooter {
                slot: 0,
                bank_id: 0,
                block_footer: VersionedBlockFooter::V1(BlockFooterV1 {
                    bank_hash: Hash::default(),
                    block_producer_time_nanos: 0,
                    block_user_agent: vec![],
                    block_final_cert: None,
                    skip_reward_cert: None,
                    notar_reward_cert: None,
                }),
            },
        ]
    }

    #[derive(Debug, Clone)]
    pub struct GeneratedUpdateParent {
        pub slot: Slot,
        pub cleared_bank_id: BankId,
        pub update_parent_fec_set_index: u32,
        pub parent_slot: Slot,
        pub parent_block_id: Hash,
    }

    impl GeneratedUpdateParent {
        pub const fn to_replica_entry(&self) -> ReplicaEntryUpdateParentInfo<'_> {
            ReplicaEntryUpdateParentInfo {
                slot: self.slot,
                cleared_bank_id: self.cleared_bank_id,
                parent_slot: self.parent_slot,
                parent_block_id: &self.parent_block_id,
            }
        }

        pub const fn to_replica_deshred(&self) -> ReplicaDeshredUpdateParentInfo<'_> {
            ReplicaDeshredUpdateParentInfo {
                slot: self.slot,
                update_parent_fec_set_index: self.update_parent_fec_set_index,
                parent_slot: self.parent_slot,
                parent_block_id: &self.parent_block_id,
            }
        }

        pub fn to_prost_entry(&self) -> SubscribeUpdateEntryUpdateParent {
            SubscribeUpdateEntryUpdateParent {
                slot: self.slot,
                cleared_bank_id: self.cleared_bank_id,
                parent_slot: self.parent_slot,
                parent_block_id: self.parent_block_id.to_bytes().to_vec(),
            }
        }

        pub fn to_prost_deshred(&self) -> SubscribeUpdateDeshredUpdateParent {
            SubscribeUpdateDeshredUpdateParent {
                slot: self.slot,
                update_parent_fec_set_index: self.update_parent_fec_set_index,
                parent_slot: self.parent_slot,
                parent_block_id: self.parent_block_id.to_bytes().to_vec(),
            }
        }
    }

    pub fn generate_update_parents() -> Vec<GeneratedUpdateParent> {
        vec![
            GeneratedUpdateParent {
                slot: 362_000_002,
                cleared_bank_id: 43,
                update_parent_fec_set_index: 96,
                parent_slot: 362_000_000,
                parent_block_id: Hash::new_from_array([9; HASH_BYTES]),
            },
            GeneratedUpdateParent {
                slot: 0,
                cleared_bank_id: 0,
                update_parent_fec_set_index: 0,
                parent_slot: 0,
                parent_block_id: Hash::default(),
            },
        ]
    }
}
