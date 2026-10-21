use {
    solana_clock::{BankId, Slot, UnixTimestamp},
    solana_entry::block_component::{VersionedBlockFooter, VotesAggregate},
    solana_message::{
        MessageHeader, VersionedMessage, compiled_instruction::CompiledInstruction,
        v0::MessageAddressTableLookup, v1::TransactionConfig,
    },
    solana_pubkey::Pubkey,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_context::transaction::TransactionReturnData,
    solana_transaction_error::TransactionError,
    solana_transaction_status::{
        InnerInstruction, InnerInstructions, Reward, RewardType, TransactionStatusMeta,
        TransactionTokenBalance,
    },
    yellowstone_grpc_proto::prelude as proto,
};

pub fn create_transaction(tx: &VersionedTransaction) -> proto::Transaction {
    proto::Transaction {
        signatures: tx
            .signatures
            .iter()
            .map(|signature| signature.as_ref().into())
            .collect(),
        message: Some(create_message(&tx.message)),
    }
}

pub fn create_message(message: &VersionedMessage) -> proto::Message {
    match message {
        VersionedMessage::Legacy(message) => proto::Message {
            header: Some(create_header(&message.header)),
            account_keys: create_pubkeys(&message.account_keys),
            recent_blockhash: message.recent_blockhash.to_bytes().into(),
            instructions: create_instructions(&message.instructions),
            versioned: false,
            address_table_lookups: vec![],
            config: None,
        },
        VersionedMessage::V0(message) => proto::Message {
            header: Some(create_header(&message.header)),
            account_keys: create_pubkeys(&message.account_keys),
            recent_blockhash: message.recent_blockhash.to_bytes().into(),
            instructions: create_instructions(&message.instructions),
            versioned: true,
            address_table_lookups: create_lookups(&message.address_table_lookups),
            config: None,
        },
        // V1 (SIMD-0385): the lifetime specifier is carried in `recent_blockhash`,
        // there are no address table lookups and `config` is always set.
        VersionedMessage::V1(message) => proto::Message {
            header: Some(create_header(&message.header)),
            account_keys: create_pubkeys(&message.account_keys),
            recent_blockhash: message.lifetime_specifier.to_bytes().into(),
            instructions: create_instructions(&message.instructions),
            versioned: true,
            address_table_lookups: vec![],
            config: Some(create_transaction_config(&message.config)),
        },
    }
}

pub const fn create_transaction_config(config: &TransactionConfig) -> proto::TransactionConfig {
    proto::TransactionConfig {
        priority_fee: config.priority_fee,
        compute_unit_limit: config.compute_unit_limit,
        loaded_accounts_data_size_limit: config.loaded_accounts_data_size_limit,
        heap_size: config.heap_size,
    }
}

pub const fn create_header(header: &MessageHeader) -> proto::MessageHeader {
    proto::MessageHeader {
        num_required_signatures: header.num_required_signatures as u32,
        num_readonly_signed_accounts: header.num_readonly_signed_accounts as u32,
        num_readonly_unsigned_accounts: header.num_readonly_unsigned_accounts as u32,
    }
}

pub fn create_pubkeys<T: AsRef<[u8]>>(pubkeys: &[T]) -> Vec<Vec<u8>> {
    pubkeys.iter().map(|key| key.as_ref().into()).collect()
}

pub fn create_instructions(ixs: &[CompiledInstruction]) -> Vec<proto::CompiledInstruction> {
    ixs.iter().map(create_instruction).collect()
}

pub fn create_instruction(ix: &CompiledInstruction) -> proto::CompiledInstruction {
    proto::CompiledInstruction {
        program_id_index: ix.program_id_index as u32,
        accounts: ix.accounts.clone(),
        data: ix.data.clone(),
    }
}

pub fn create_lookups(
    lookups: &[MessageAddressTableLookup],
) -> Vec<proto::MessageAddressTableLookup> {
    lookups.iter().map(create_lookup).collect()
}

pub fn create_lookup(lookup: &MessageAddressTableLookup) -> proto::MessageAddressTableLookup {
    proto::MessageAddressTableLookup {
        account_key: <Pubkey as AsRef<[u8]>>::as_ref(&lookup.account_key).into(),
        writable_indexes: lookup.writable_indexes.clone(),
        readonly_indexes: lookup.readonly_indexes.clone(),
    }
}

pub fn create_transaction_meta(meta: &TransactionStatusMeta) -> proto::TransactionStatusMeta {
    let TransactionStatusMeta {
        status,
        fee,
        pre_balances,
        post_balances,
        inner_instructions,
        log_messages,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_addresses,
        return_data,
        compute_units_consumed,
        cost_units,
    } = meta;
    let err = create_transaction_error(status);
    let inner_instructions_none = inner_instructions.is_none();
    let inner_instructions = inner_instructions
        .as_deref()
        .map(create_inner_instructions_vec)
        .unwrap_or_default();
    let log_messages_none = log_messages.is_none();
    let log_messages = log_messages.clone().unwrap_or_default();
    let pre_token_balances = pre_token_balances
        .as_deref()
        .map(create_token_balances)
        .unwrap_or_default();
    let post_token_balances = post_token_balances
        .as_deref()
        .map(create_token_balances)
        .unwrap_or_default();
    let rewards = rewards.as_deref().map(create_rewards).unwrap_or_default();
    let loaded_writable_addresses = create_pubkeys(&loaded_addresses.writable);
    let loaded_readonly_addresses = create_pubkeys(&loaded_addresses.readonly);

    proto::TransactionStatusMeta {
        err,
        fee: *fee,
        pre_balances: pre_balances.clone(),
        post_balances: post_balances.clone(),
        inner_instructions,
        inner_instructions_none,
        log_messages,
        log_messages_none,
        pre_token_balances,
        post_token_balances,
        rewards,
        loaded_writable_addresses,
        loaded_readonly_addresses,
        return_data: return_data.as_ref().map(create_return_data),
        return_data_none: return_data.is_none(),
        compute_units_consumed: *compute_units_consumed,
        cost_units: *cost_units,
    }
}

pub fn create_transaction_error(
    status: &Result<(), TransactionError>,
) -> Option<proto::TransactionError> {
    match status {
        Ok(()) => None,
        Err(err) => Some(proto::TransactionError {
            err: bincode::serialize(&err).expect("transaction error to serialize to bytes"),
        }),
    }
}

pub fn create_inner_instructions_vec(ixs: &[InnerInstructions]) -> Vec<proto::InnerInstructions> {
    ixs.iter().map(create_inner_instructions).collect()
}

pub fn create_inner_instructions(instructions: &InnerInstructions) -> proto::InnerInstructions {
    proto::InnerInstructions {
        index: instructions.index as u32,
        instructions: create_inner_instruction_vec(&instructions.instructions),
    }
}

pub fn create_inner_instruction_vec(ixs: &[InnerInstruction]) -> Vec<proto::InnerInstruction> {
    ixs.iter().map(create_inner_instruction).collect()
}

pub fn create_inner_instruction(instruction: &InnerInstruction) -> proto::InnerInstruction {
    proto::InnerInstruction {
        program_id_index: instruction.instruction.program_id_index as u32,
        accounts: instruction.instruction.accounts.clone(),
        data: instruction.instruction.data.clone(),
        stack_height: instruction.stack_height,
    }
}

pub fn create_token_balances(balances: &[TransactionTokenBalance]) -> Vec<proto::TokenBalance> {
    balances.iter().map(create_token_balance).collect()
}

pub fn create_token_balance(balance: &TransactionTokenBalance) -> proto::TokenBalance {
    proto::TokenBalance {
        account_index: balance.account_index as u32,
        mint: balance.mint.clone(),
        ui_token_amount: Some(proto::UiTokenAmount {
            ui_amount: balance.ui_token_amount.ui_amount.unwrap_or_default(),
            decimals: balance.ui_token_amount.decimals as u32,
            amount: balance.ui_token_amount.amount.clone(),
            ui_amount_string: balance.ui_token_amount.ui_amount_string.clone(),
        }),
        owner: balance.owner.clone(),
        program_id: balance.program_id.clone(),
    }
}

pub fn create_rewards_obj(rewards: &[Reward], num_partitions: Option<u64>) -> proto::Rewards {
    proto::Rewards {
        rewards: create_rewards(rewards),
        num_partitions: num_partitions.map(create_num_partitions),
    }
}

pub fn create_rewards(rewards: &[Reward]) -> Vec<proto::Reward> {
    rewards.iter().map(create_reward).collect()
}

pub fn create_reward(reward: &Reward) -> proto::Reward {
    proto::Reward {
        pubkey: reward.pubkey.clone(),
        lamports: reward.lamports,
        post_balance: reward.post_balance,
        reward_type: create_reward_type(reward.reward_type) as i32,
        commission: reward.commission.map(|c| c.to_string()).unwrap_or_default(),
        commission_bps: reward
            .commission_bps
            .map(|c| c.to_string())
            .unwrap_or_default(),
    }
}

pub const fn create_reward_type(reward_type: Option<RewardType>) -> proto::RewardType {
    match reward_type {
        None => proto::RewardType::Unspecified,
        Some(RewardType::Fee) => proto::RewardType::Fee,
        Some(RewardType::Rent) => proto::RewardType::Rent,
        Some(RewardType::Staking) => proto::RewardType::Staking,
        Some(RewardType::Voting) => proto::RewardType::Voting,
        Some(RewardType::DeactivatedStake) => proto::RewardType::DeactivatedStake,
        Some(RewardType::VATDebit) => proto::RewardType::VatDebit,
    }
}

pub const fn create_num_partitions(num_partitions: u64) -> proto::NumPartitions {
    proto::NumPartitions { num_partitions }
}

pub fn create_return_data(return_data: &TransactionReturnData) -> proto::ReturnData {
    proto::ReturnData {
        program_id: return_data.program_id.to_bytes().into(),
        data: return_data.data.clone(),
    }
}

pub const fn create_block_height(block_height: u64) -> proto::BlockHeight {
    proto::BlockHeight { block_height }
}

pub const fn create_timestamp(timestamp: UnixTimestamp) -> proto::UnixTimestamp {
    proto::UnixTimestamp { timestamp }
}

/// Yellowstone `SubscribeUpdateBlockFooter` from a wincode-serialized `VersionedBlockFooter`
/// (Richat-only `SubscribeUpdateBlockFooter.footer`).
pub fn create_block_footer_from_wincode(
    slot: Slot,
    bank_id: BankId,
    footer: &[u8],
) -> Result<proto::SubscribeUpdateBlockFooter, &'static str> {
    let footer = wincode::deserialize::<VersionedBlockFooter>(footer)
        .map_err(|_| "failed to deserialize block footer")?;
    create_block_footer(slot, bank_id, &footer)
}

/// Yellowstone `SubscribeUpdateBlockFooter` from `VersionedBlockFooter`, certificates are
/// always included (filters strip them when `include_certificates` is not set).
pub fn create_block_footer(
    slot: Slot,
    bank_id: BankId,
    footer: &VersionedBlockFooter,
) -> Result<proto::SubscribeUpdateBlockFooter, &'static str> {
    let VersionedBlockFooter::V1(footer) = footer;
    Ok(proto::SubscribeUpdateBlockFooter {
        slot,
        bank_id,
        bank_hash: footer.bank_hash.to_bytes().into(),
        block_producer_time_nanos: footer.block_producer_time_nanos,
        block_user_agent: footer.block_user_agent.clone(),
        block_final_cert: footer
            .block_final_cert
            .as_ref()
            .map(|cert| {
                // Fast finalization: `final_aggregate` alone, on the block.
                // Slow finalization: `final_aggregate` on the slot, `notar_aggregate` on the block.
                let block_id = cert.block_id.to_bytes().to_vec();
                Ok::<_, &'static str>(match &cert.notar_aggregate {
                    Some(notar_aggregate) => proto::BlockFooterFinalCert {
                        slot: cert.slot,
                        final_aggregate: Some(create_votes_aggregate(
                            &cert.final_aggregate,
                            vec![],
                        )?),
                        notar_aggregate: Some(create_votes_aggregate(notar_aggregate, block_id)?),
                    },
                    None => proto::BlockFooterFinalCert {
                        slot: cert.slot,
                        final_aggregate: Some(create_votes_aggregate(
                            &cert.final_aggregate,
                            block_id,
                        )?),
                        notar_aggregate: None,
                    },
                })
            })
            .transpose()?,
        skip_reward_cert: footer.skip_reward_cert.as_ref().map(|cert| {
            proto::BlockFooterSkipRewardCert {
                slot: cert.slot,
                aggregate: Some(proto::BlockFooterVotesAggregate {
                    signature_kind: proto::BlockFooterSignatureKind::CompressedBls12381G2 as i32,
                    signature: cert.signature.0.to_vec(),
                    block_id: vec![],
                    signer_bitmap: cert.to_bitmap().to_vec(),
                }),
            }
        }),
        notar_reward_cert: footer.notar_reward_cert.as_ref().map(|cert| {
            proto::BlockFooterNotarRewardCert {
                slot: cert.slot,
                aggregate: Some(proto::BlockFooterVotesAggregate {
                    signature_kind: proto::BlockFooterSignatureKind::CompressedBls12381G2 as i32,
                    signature: cert.signature.0.to_vec(),
                    block_id: cert.block_id.to_bytes().to_vec(),
                    signer_bitmap: cert.bitmap().to_vec(),
                }),
            }
        }),
    })
}

fn create_votes_aggregate(
    aggregate: &VotesAggregate,
    block_id: Vec<u8>,
) -> Result<proto::BlockFooterVotesAggregate, &'static str> {
    // `VotesAggregate` does not expose the compressed signature, its wincode layout is
    // `[u8; 96]` compressed signature followed by `u16` LE bitmap length and the bitmap.
    const SIGNATURE_SIZE: usize = 96;
    let bitmap = aggregate.clone().into_bitmap();
    let bytes = wincode::serialize(aggregate).map_err(|_| "failed to serialize votes aggregate")?;
    if bytes.len() != SIGNATURE_SIZE + 2 + bitmap.len() || bytes[SIGNATURE_SIZE + 2..] != bitmap {
        return Err("unexpected votes aggregate layout");
    }
    Ok(proto::BlockFooterVotesAggregate {
        signature_kind: proto::BlockFooterSignatureKind::CompressedBls12381G2 as i32,
        signature: bytes[..SIGNATURE_SIZE].to_vec(),
        block_id,
        signer_bitmap: bitmap,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{create_block_footer, create_block_footer_from_wincode},
        crate::geyser::BlockFooterSignatureKind,
        solana_entry::block_component::VersionedBlockFooter,
    };

    const KIND: i32 = BlockFooterSignatureKind::CompressedBls12381G2 as i32;

    fn votes_aggregate(signature: u8, bitmap: &[u8]) -> Vec<u8> {
        let mut bytes = vec![signature; 96];
        bytes.extend_from_slice(&(bitmap.len() as u16).to_le_bytes());
        bytes.extend_from_slice(bitmap);
        bytes
    }

    // wincode layout of `VersionedBlockFooter::V1`, bitmaps of reward certs are `ShortU16`
    // length-prefixed (one byte for lengths below 128)
    fn footer(slow: bool) -> Vec<u8> {
        let mut bytes = vec![1]; // V1
        bytes.extend_from_slice(&[7; 32]); // bank_hash
        bytes.extend_from_slice(&42u64.to_le_bytes()); // block_producer_time_nanos
        bytes.push(5);
        bytes.extend_from_slice(b"agave"); // block_user_agent
        // block_final_cert
        bytes.push(1);
        bytes.extend_from_slice(&100u64.to_le_bytes());
        bytes.extend_from_slice(&[9; 32]); // block_id
        bytes.extend_from_slice(&votes_aggregate(1, &[0b101, 0b1]));
        if slow {
            bytes.push(1);
            bytes.extend_from_slice(&votes_aggregate(2, &[0b11]));
        } else {
            bytes.push(0);
        }
        // skip_reward_cert
        bytes.push(1);
        bytes.extend_from_slice(&98u64.to_le_bytes());
        bytes.extend_from_slice(&[3; 96]);
        bytes.extend_from_slice(&[1, 0b111]);
        // notar_reward_cert
        bytes.push(1);
        bytes.extend_from_slice(&99u64.to_le_bytes());
        bytes.extend_from_slice(&[8; 32]);
        bytes.extend_from_slice(&[4; 96]);
        bytes.extend_from_slice(&[2, 0b1, 0b10]);
        bytes
    }

    #[test]
    fn test_block_footer_certificates() {
        for slow in [false, true] {
            let bytes = footer(slow);
            let decoded = wincode::deserialize::<VersionedBlockFooter>(&bytes).expect("decode");
            assert_eq!(wincode::serialize(&decoded).expect("encode"), bytes);

            let footer = create_block_footer_from_wincode(101, 7, &bytes).expect("convert");
            assert_eq!(
                footer,
                create_block_footer(101, 7, &decoded).expect("convert")
            );
            assert_eq!(footer.slot, 101);
            assert_eq!(footer.bank_id, 7);
            assert_eq!(footer.bank_hash, vec![7; 32]);
            assert_eq!(footer.block_producer_time_nanos, 42);
            assert_eq!(footer.block_user_agent, b"agave");

            let cert = footer.block_final_cert.expect("final cert");
            assert_eq!(cert.slot, 100);
            let final_aggregate = cert.final_aggregate.expect("final aggregate");
            assert_eq!(final_aggregate.signature_kind, KIND);
            assert_eq!(final_aggregate.signature, vec![1; 96]);
            assert_eq!(final_aggregate.signer_bitmap, vec![0b101, 0b1]);
            if slow {
                // finalization vote is on the slot, notarization on the block
                assert!(final_aggregate.block_id.is_empty());
                let notar_aggregate = cert.notar_aggregate.expect("notar aggregate");
                assert_eq!(notar_aggregate.signature, vec![2; 96]);
                assert_eq!(notar_aggregate.block_id, vec![9; 32]);
                assert_eq!(notar_aggregate.signer_bitmap, vec![0b11]);
            } else {
                assert_eq!(final_aggregate.block_id, vec![9; 32]);
                assert_eq!(cert.notar_aggregate, None);
            }

            let skip = footer.skip_reward_cert.expect("skip reward cert");
            assert_eq!(skip.slot, 98);
            let skip = skip.aggregate.expect("aggregate");
            assert_eq!(skip.signature_kind, KIND);
            assert_eq!(skip.signature, vec![3; 96]);
            assert!(skip.block_id.is_empty());
            assert_eq!(skip.signer_bitmap, vec![0b111]);

            let notar = footer.notar_reward_cert.expect("notar reward cert");
            assert_eq!(notar.slot, 99);
            let notar = notar.aggregate.expect("aggregate");
            assert_eq!(notar.signature, vec![4; 96]);
            assert_eq!(notar.block_id, vec![8; 32]);
            assert_eq!(notar.signer_bitmap, vec![0b1, 0b10]);
        }
    }

    #[test]
    fn test_block_footer_invalid() {
        assert!(create_block_footer_from_wincode(1, 1, &[]).is_err());
        assert!(create_block_footer_from_wincode(1, 1, &[2]).is_err());
    }
}
