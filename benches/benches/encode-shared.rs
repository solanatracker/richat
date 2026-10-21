//! Per-client cost of encoding filtered updates for N clients subscribed to
//! the same stream: encode for every client vs encode once and share.

use {
    criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main},
    prost::Message as _,
    prost_types::Timestamp,
    richat_benches::fixtures::{
        generate_accounts, generate_block_metas, generate_entries, generate_slots,
        generate_transactions,
    },
    richat_filter::{
        config::{
            ConfigFilter, ConfigFilterAccounts, ConfigFilterBlocks, ConfigFilterTransactions,
        },
        encode_cache::EncodeCache,
        filter::{Filter, FilteredUpdate},
        message::{Message, MessageBlockCreatedAt, MessageParserEncoding},
    },
    richat_proto::{
        geyser::{SubscribeUpdate, subscribe_update::UpdateOneof},
        solana::storage::confirmed_block::BlockHeight,
    },
    solana_commitment_config::CommitmentLevel,
    std::{borrow::Cow, collections::HashMap, hint::black_box, sync::Arc},
};

const CLIENTS: [usize; 3] = [1, 10, 100];

const CREATED_AT: Timestamp = Timestamp {
    seconds: 1_757_500_000,
    nanos: 42,
};

fn parse(update: UpdateOneof) -> Message {
    let data = SubscribeUpdate {
        filters: vec![],
        update_oneof: Some(update),
        created_at: Some(CREATED_AT),
    }
    .encode_to_vec();
    Message::parse(Cow::Owned(data), MessageParserEncoding::Limited).expect("valid message")
}

fn create_block() -> Message {
    let accounts = generate_accounts()
        .iter()
        .map(
            |account| match parse(UpdateOneof::Account(account.to_prost())) {
                Message::Account(msg) => Arc::new(msg),
                _ => unreachable!(),
            },
        )
        .collect();
    let transactions = generate_transactions()
        .iter()
        .map(|tx| match parse(UpdateOneof::Transaction(tx.to_prost())) {
            Message::Transaction(msg) => Arc::new(msg),
            _ => unreachable!(),
        })
        .collect();
    let entries = generate_entries()
        .iter()
        .map(|entry| match parse(UpdateOneof::Entry(entry.to_prost())) {
            Message::Entry(msg) => Arc::new(msg),
            _ => unreachable!(),
        })
        .collect();
    // fixtures have no block height, required to parse block meta
    let mut block_meta = generate_block_metas().remove(0).to_prost();
    block_meta.block_height = Some(BlockHeight { block_height: 42 });
    let block_meta = match parse(UpdateOneof::BlockMeta(block_meta)) {
        Message::BlockMeta(msg) => Arc::new(msg),
        _ => unreachable!(),
    };
    Message::create_block(
        accounts,
        transactions,
        entries,
        block_meta,
        MessageBlockCreatedAt::Limited(CREATED_AT),
    )
    .expect("valid block")
}

/// One filter per client, same shape but its own filter name
fn create_filters(clients: usize, config: impl Fn(String) -> ConfigFilter) -> Vec<Filter> {
    (0..clients)
        .map(|client| Filter::new(&config(format!("client-{client}"))))
        .collect()
}

fn bench_group(
    criterion: &mut Criterion,
    name: &str,
    messages: &[Message],
    config: impl Fn(String) -> ConfigFilter,
) {
    let mut group = criterion.benchmark_group(format!("encode_shared/{name}"));
    for clients in CLIENTS {
        let filters = create_filters(clients, &config);
        // updates per message, per client
        let updates = messages
            .iter()
            .map(|message| {
                filters
                    .iter()
                    .flat_map(|filter| {
                        filter.get_updates_ref(message.into(), CommitmentLevel::Processed)
                    })
                    .collect::<Vec<FilteredUpdate>>()
            })
            .collect::<Vec<_>>();

        // per client message count, so the reported time is per client
        group.throughput(Throughput::Elements(clients as u64));
        // the server copies every update into the gRPC frame buffer
        let mut frame = Vec::with_capacity(16 * 1024 * 1024);

        group.bench_with_input(
            BenchmarkId::new("per_client", clients),
            &updates,
            |criterion, updates| {
                criterion.iter(|| {
                    for message_updates in updates {
                        for update in message_updates {
                            let data = update.encode_to_vec();
                            frame.clear();
                            frame.extend_from_slice(&data);
                            black_box(&frame);
                        }
                    }
                })
            },
        );

        group.bench_with_input(
            BenchmarkId::new("shared", clients),
            &updates,
            |criterion, updates| {
                let mut cache = EncodeCache::default();
                criterion.iter(|| {
                    for message_updates in updates {
                        cache.clear();
                        for update in message_updates {
                            let data = cache.get_or_encode(update);
                            frame.clear();
                            data.encode(&mut frame);
                            black_box(&frame);
                        }
                    }
                })
            },
        );
    }
    group.finish();
}

fn bench_encode_shared(criterion: &mut Criterion) {
    let slots = generate_slots()
        .iter()
        .map(|slot| parse(UpdateOneof::Slot(slot.to_prost())))
        .collect::<Vec<_>>();
    bench_group(criterion, "slot", &slots, |name| ConfigFilter {
        slots: HashMap::from([(name, Default::default())]),
        ..Default::default()
    });

    let accounts = generate_accounts()
        .iter()
        .map(|account| parse(UpdateOneof::Account(account.to_prost())))
        .collect::<Vec<_>>();
    bench_group(criterion, "account", &accounts, |name| ConfigFilter {
        accounts: HashMap::from([(name, ConfigFilterAccounts::default())]),
        ..Default::default()
    });

    let transactions = generate_transactions()
        .iter()
        .take(100)
        .map(|tx| parse(UpdateOneof::Transaction(tx.to_prost())))
        .collect::<Vec<_>>();
    bench_group(criterion, "transaction", &transactions, |name| {
        ConfigFilter {
            transactions: HashMap::from([(name, ConfigFilterTransactions::default())]),
            ..Default::default()
        }
    });

    let block = [create_block()];
    bench_group(criterion, "block", &block, |name| ConfigFilter {
        blocks: HashMap::from([(
            name,
            ConfigFilterBlocks {
                include_accounts: Some(true),
                include_entries: Some(true),
                ..Default::default()
            },
        )]),
        ..Default::default()
    });
}

criterion_group!(benches, bench_encode_shared);
criterion_main!(benches);
