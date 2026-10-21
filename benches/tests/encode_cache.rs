use {
    prost::Message as _,
    prost_types::Timestamp,
    richat_benches::fixtures::{
        generate_accounts, generate_block_metas, generate_entries, generate_slots,
        generate_transactions,
    },
    richat_filter::{
        config::{
            ConfigFilter, ConfigFilterAccounts, ConfigFilterAccountsDataSlice, ConfigFilterBlocks,
            ConfigFilterSlots, ConfigFilterTransactions,
        },
        encode_cache::EncodeCache,
        filter::Filter,
        message::{Message, MessageBlockCreatedAt, MessageParserEncoding},
    },
    richat_proto::{
        geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
        solana::storage::confirmed_block::BlockHeight,
    },
    solana_commitment_config::CommitmentLevel,
    solana_pubkey::Pubkey,
    std::{borrow::Cow, collections::HashMap, sync::Arc},
};

const PARSERS: [MessageParserEncoding; 2] =
    [MessageParserEncoding::Limited, MessageParserEncoding::Prost];

const CREATED_AT: Timestamp = Timestamp {
    seconds: 1_757_500_000,
    nanos: 42,
};

fn filter_names() -> Vec<Vec<String>> {
    vec![
        vec!["a".to_owned()],
        vec!["a".to_owned(), "b".to_owned(), "client-filter".to_owned()],
        // spills the inline filters buffer
        vec!["x".repeat(100), "y".repeat(28)],
        (0..10).map(|i| format!("filter-{i}")).collect(),
    ]
}

fn parse(update: UpdateOneof, parser: MessageParserEncoding) -> Message {
    let data = SubscribeUpdate {
        filters: vec![],
        update_oneof: Some(update),
        created_at: Some(CREATED_AT),
    }
    .encode_to_vec();
    Message::parse(Cow::Owned(data), parser).expect("valid message")
}

fn named<T: Clone>(names: &[String], value: T) -> HashMap<String, T> {
    names
        .iter()
        .map(|name| (name.clone(), value.clone()))
        .collect()
}

fn data_slices(slices: &[(u64, u64)]) -> Vec<ConfigFilterAccountsDataSlice> {
    slices
        .iter()
        .map(|&(offset, length)| ConfigFilterAccountsDataSlice { offset, length })
        .collect()
}

/// Every client gets the same bytes as an independent encode, and clients
/// with the same filter output shape share the body
fn check(messages: &[Message], filters: &[Filter]) -> usize {
    let mut updates_total = 0;
    for message in messages {
        let mut cache = EncodeCache::default();
        for pass in 0..2 {
            for filter in filters {
                for update in filter.get_updates_ref(message.into(), CommitmentLevel::Processed) {
                    if pass == 1 {
                        assert!(cache.get(&update).is_some(), "body should be cached");
                    }
                    let encoded = cache.get_or_encode(&update);
                    assert_eq!(encoded.to_vec(), update.encode_to_vec());
                    assert_eq!(encoded.len(), update.encode_to_vec().len());
                    updates_total += 1;
                }
            }
        }
    }
    updates_total
}

#[test]
fn test_encode_cache_slot() {
    for parser in PARSERS {
        let messages = generate_slots()
            .iter()
            .map(|slot| parse(UpdateOneof::Slot(slot.to_prost()), parser))
            .collect::<Vec<_>>();
        let filters = filter_names()
            .iter()
            .flat_map(|names| {
                [(None, None), (Some(true), Some(true)), (None, Some(true))].map(
                    |(filter_by_commitment, interslot_updates)| {
                        Filter::new(&ConfigFilter {
                            slots: named(
                                names,
                                ConfigFilterSlots {
                                    filter_by_commitment,
                                    interslot_updates,
                                },
                            ),
                            ..Default::default()
                        })
                    },
                )
            })
            .collect::<Vec<_>>();
        assert!(check(&messages, &filters) > 0);
    }
}

#[test]
fn test_encode_cache_account() {
    for parser in PARSERS {
        let messages = generate_accounts()
            .iter()
            .map(|account| parse(UpdateOneof::Account(account.to_prost()), parser))
            .collect::<Vec<_>>();
        let filters = filter_names()
            .iter()
            .flat_map(|names| {
                [vec![], vec![(0, 4)], vec![(1, 3), (10, 5)], vec![(0, 0)]].map(|slices| {
                    Filter::new(&ConfigFilter {
                        accounts: named(names, ConfigFilterAccounts::default()),
                        accounts_data_slice: data_slices(&slices),
                        ..Default::default()
                    })
                })
            })
            .collect::<Vec<_>>();
        assert!(check(&messages, &filters) > 0);
    }
}

#[test]
fn test_encode_cache_transaction() {
    for parser in PARSERS {
        let messages = generate_transactions()
            .iter()
            .take(200)
            .map(|tx| parse(UpdateOneof::Transaction(tx.to_prost()), parser))
            .collect::<Vec<_>>();
        let filters = filter_names()
            .iter()
            .flat_map(|names| {
                [
                    // transaction and transaction status
                    ConfigFilter {
                        transactions: named(names, ConfigFilterTransactions::default()),
                        transactions_status: named(names, ConfigFilterTransactions::default()),
                        ..Default::default()
                    },
                    ConfigFilter {
                        transactions: named(names, ConfigFilterTransactions::default()),
                        ..Default::default()
                    },
                ]
                .map(|config| Filter::new(&config))
            })
            .collect::<Vec<_>>();
        assert!(check(&messages, &filters) > 0);
    }
}

#[test]
fn test_encode_cache_block() {
    let accounts = generate_accounts();
    let transactions = generate_transactions();
    let entries = generate_entries();
    // fixtures have no block height, required to parse block meta
    let mut block_meta = generate_block_metas().remove(0).to_prost();
    block_meta.block_height = Some(BlockHeight { block_height: 42 });
    let account_include: Pubkey = accounts[0].pubkey;

    for parser in PARSERS {
        let unwrap = |message: Message| match message {
            Message::Account(msg) => (Some(Arc::new(msg)), None, None, None),
            Message::Transaction(msg) => (None, Some(Arc::new(msg)), None, None),
            Message::Entry(msg) => (None, None, Some(Arc::new(msg)), None),
            Message::BlockMeta(msg) => (None, None, None, Some(Arc::new(msg))),
            _ => unreachable!(),
        };
        let block = Message::create_block(
            accounts
                .iter()
                .map(|account| {
                    unwrap(parse(UpdateOneof::Account(account.to_prost()), parser))
                        .0
                        .expect("account")
                })
                .collect(),
            transactions
                .iter()
                .take(50)
                .map(|tx| {
                    unwrap(parse(UpdateOneof::Transaction(tx.to_prost()), parser))
                        .1
                        .expect("transaction")
                })
                .collect(),
            entries
                .iter()
                .map(|entry| {
                    unwrap(parse(UpdateOneof::Entry(entry.to_prost()), parser))
                        .2
                        .expect("entry")
                })
                .collect(),
            unwrap(parse(UpdateOneof::BlockMeta(block_meta.clone()), parser))
                .3
                .expect("block meta"),
            match parser {
                MessageParserEncoding::Limited => MessageBlockCreatedAt::Limited(CREATED_AT),
                MessageParserEncoding::Prost => MessageBlockCreatedAt::Prost(CREATED_AT),
            },
        )
        .expect("valid block");

        let filters = filter_names()
            .iter()
            .flat_map(|names| {
                [
                    (ConfigFilterBlocks::default(), vec![]),
                    (
                        ConfigFilterBlocks {
                            include_accounts: Some(true),
                            include_entries: Some(true),
                            ..Default::default()
                        },
                        vec![],
                    ),
                    (
                        ConfigFilterBlocks {
                            include_accounts: Some(true),
                            ..Default::default()
                        },
                        vec![(0, 2)],
                    ),
                    (
                        ConfigFilterBlocks {
                            account_include: vec![account_include],
                            include_accounts: Some(true),
                            include_transactions: Some(false),
                            ..Default::default()
                        },
                        vec![],
                    ),
                ]
                .map(|(blocks, slices)| {
                    Filter::new(&ConfigFilter {
                        blocks: named(names, blocks),
                        accounts_data_slice: data_slices(&slices),
                        ..Default::default()
                    })
                })
            })
            .collect::<Vec<_>>();
        assert!(check(&[block], &filters) > 0);
    }
}
