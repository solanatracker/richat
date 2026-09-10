use {
    crate::stream::{StreamUpdate, handle_stream},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaBlockInfoV4, ReplicaContactInfoV0_0_1,
        ReplicaDeshredTransactionInfoV2, ReplicaDeshredUpdateParentInfo, ReplicaEntryInfoV2,
        ReplicaEntryUpdateParentInfo, ReplicaTransactionInfoV3, SlotStatus as GeyserSlotStatus,
    },
    anyhow::Context,
    clap::{Args, Subcommand},
    futures::stream::{BoxStream, StreamExt, TryStreamExt},
    indicatif::MultiProgress,
    richat_client::{
        error::ReceiveError,
        grpc::GrpcClient,
        quic::{QuicClient, QuicClientBuilder},
    },
    richat_plugin_agave::protobuf::{ProtobufEncoder, ProtobufMessage},
    richat_proto::{
        convert_from,
        geyser::{
            SlotStatus, SubscribeUpdate, SubscribeUpdateAccount, SubscribeUpdateDeshredTransaction,
            SubscribeUpdateSlot, SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
        },
        richat::{
            GrpcSubscribeRequest, RichatFilter, SubscribeUpdateRichat,
            subscribe_update_richat::UpdateOneof as UpdateOneofRichat,
        },
    },
    richat_shared::transports::{grpc::ConfigGrpcServer, quic::ConfigQuicServer},
    solana_clock::Slot,
    solana_entry::block_component::VersionedBlockFooter,
    solana_hash::Hash,
    solana_message::{LegacyMessage, Message, SanitizedMessage},
    solana_transaction::sanitized::SanitizedTransaction,
    solana_transaction_status::TransactionWithStatusMeta,
    std::{collections::HashSet, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration},
    tonic::service::Interceptor,
    tracing::info,
};

type SubscribeStreamInput = BoxStream<'static, Result<Vec<u8>, ReceiveError>>;

#[derive(Debug, Args)]
pub struct ArgsAppStreamRichat {
    #[command(subcommand)]
    action: ArgsAppStreamSelect,

    /// Disable streaming accounts
    #[clap(long)]
    disable_accounts: bool,

    /// Disable streaming transactions
    #[clap(long)]
    disable_transactions: bool,

    /// Disable streaming entries
    #[clap(long)]
    disable_entries: bool,

    /// Enable streaming deshred transactions (should be enabled in the plugin config)
    #[clap(long)]
    enable_deshred_transactions: bool,

    /// Enable streaming gossip contact info (should be enabled in the plugin config)
    #[clap(long)]
    enable_contact_info: bool,

    /// Enable streaming Alpenglow block footers (should be enabled in the plugin config)
    #[clap(long)]
    enable_block_footers: bool,

    /// Enable streaming Alpenglow entry update parents
    #[clap(long)]
    enable_entry_update_parents: bool,

    /// Subscribe on stream from slot
    #[clap(long)]
    replay_from_slot: Option<Slot>,

    /// Access token
    #[clap(long)]
    x_token: Option<String>,

    /// Do not verify messages with prost
    #[clap(long)]
    no_verify: bool,

    /// Show total stat instead of messages
    #[clap(long)]
    stats: bool,
}

impl ArgsAppStreamRichat {
    async fn subscribe(
        self,
        replay_from_slot: Option<Slot>,
    ) -> anyhow::Result<SubscribeStreamInput> {
        let filter = RichatFilter {
            disable_accounts: self.disable_accounts,
            disable_transactions: self.disable_transactions,
            disable_entries: self.disable_entries,
            enable_deshred_transactions: self.enable_deshred_transactions,
            enable_contact_info: self.enable_contact_info,
            enable_block_footers: self.enable_block_footers,
            enable_entry_update_parents: self.enable_entry_update_parents,
        };
        let x_token = self.x_token.map(|xt| xt.into_bytes());
        match self.action {
            ArgsAppStreamSelect::Quic(args) => {
                args.subscribe(replay_from_slot, filter, x_token).await
            }
            ArgsAppStreamSelect::Grpc(args) => {
                args.subscribe(replay_from_slot, filter, x_token).await
            }
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let pb_multi = Arc::new(MultiProgress::new());
        let replay_from_slot = self.replay_from_slot;
        let verify = !self.no_verify;
        let stats = self.stats;
        let pb_multi_stream = Arc::clone(&pb_multi);
        let stream = self
            .subscribe(replay_from_slot)
            .await?
            .and_then(move |vec| {
                let pb_multi_stream = Arc::clone(&pb_multi_stream);
                async move {
                    let msg = StreamUpdate::decode(vec.as_slice())?;
                    if verify {
                        match convert_prost_to_raw(&msg) {
                            Ok(Some(vec_raw)) if vec != vec_raw => pb_multi_stream.println(
                                format!("encoding doesn't match: {}", const_hex::encode(&vec)),
                            ),
                            Err(error) => pb_multi_stream
                                .println(format!("failed to encode with raw: {error:?}")),
                            _ => Ok(()),
                        }
                        .unwrap();
                    }
                    Ok(msg)
                }
            })
            .boxed();

        handle_stream(stream, pb_multi, stats).await
    }
}

#[derive(Debug, Subcommand)]
enum ArgsAppStreamSelect {
    /// Stream over Quic
    Quic(ArgsAppStreamQuic),
    /// Stream over gRPC
    Grpc(ArgsAppStreamGrpc),
}

#[derive(Debug, Args)]
struct ArgsAppStreamQuic {
    /// Richat Geyser plugin Quic Server endpoint
    #[clap(default_value_t = ConfigQuicServer::default_endpoint().to_string())]
    endpoint: String,

    #[clap(long, default_value_t = QuicClientBuilder::default().local_addr)]
    local_addr: SocketAddr,

    #[clap(long, default_value_t = QuicClientBuilder::default().expected_rtt)]
    expected_rtt: u32,

    #[clap(long, default_value_t = QuicClientBuilder::default().max_stream_bandwidth)]
    max_stream_bandwidth: u32,

    #[clap(long, default_value_t = QuicClientBuilder::default().max_idle_timeout.unwrap().as_millis() as u64)]
    max_idle_timeout: u64,

    #[clap(long)]
    server_name: Option<String>,

    #[clap(long, default_value_t = QuicClientBuilder::default().recv_streams)]
    recv_streams: u32,

    #[clap(long)]
    max_backlog: Option<u32>,

    #[clap(long)]
    insecure: bool,

    #[clap(long)]
    cert: Option<PathBuf>,
}

impl ArgsAppStreamQuic {
    async fn subscribe(
        self,
        replay_from_slot: Option<Slot>,
        filter: RichatFilter,
        x_token: Option<Vec<u8>>,
    ) -> anyhow::Result<SubscribeStreamInput> {
        let builder = QuicClient::builder()
            .set_local_addr(Some(self.local_addr))
            .set_expected_rtt(self.expected_rtt)
            .set_max_stream_bandwidth(self.max_stream_bandwidth)
            .set_max_idle_timeout(Some(Duration::from_millis(self.max_idle_timeout)))
            .set_server_name(self.server_name.clone())
            .set_recv_streams(self.recv_streams)
            .set_max_backlog(self.max_backlog)
            .set_x_token(x_token);

        let client = if self.insecure {
            builder.insecure().connect(self.endpoint.clone()).await
        } else {
            builder
                .secure(self.cert)
                .connect(self.endpoint.clone())
                .await
        }
        .context("failed to connect")?;
        info!("connected to {} over Quic", self.endpoint);

        let stream = client
            .subscribe(replay_from_slot, Some(filter))
            .await
            .context("failed to subscribe")?;
        info!("subscribed");
        info!("version: {}", stream.get_version());

        Ok(stream.boxed())
    }
}

#[derive(Debug, Args)]
struct ArgsAppStreamGrpc {
    /// Richat Geyser plugin gRPC Server endpoint
    #[clap(default_value_t = format!("http://{}", ConfigGrpcServer::default().endpoint))]
    endpoint: String,

    /// Path of a certificate authority file
    #[clap(long)]
    ca_certificate: Option<PathBuf>,

    /// Apply a timeout to connecting to the uri.
    #[clap(long)]
    connect_timeout_ms: Option<u64>,

    /// Sets the tower service default internal buffer size, default is 1024
    #[clap(long)]
    buffer_size: Option<usize>,

    /// Sets whether to use an adaptive flow control. Uses hyper’s default otherwise.
    #[clap(long)]
    http2_adaptive_window: Option<bool>,

    /// Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    #[clap(long)]
    http2_keep_alive_interval_ms: Option<u64>,

    /// Sets the max connection-level flow control for HTTP2, default is 65,535
    #[clap(long)]
    initial_connection_window_size: Option<u32>,

    ///Sets the SETTINGS_INITIAL_WINDOW_SIZE option for HTTP2 stream-level flow control, default is 65,535
    #[clap(long)]
    initial_stream_window_size: Option<u32>,

    ///Set http2 KEEP_ALIVE_TIMEOUT. Uses hyper’s default otherwise.
    #[clap(long)]
    keep_alive_timeout_ms: Option<u64>,

    /// Set http2 KEEP_ALIVE_WHILE_IDLE. Uses hyper’s default otherwise.
    #[clap(long)]
    keep_alive_while_idle: Option<bool>,

    /// Set whether TCP keepalive messages are enabled on accepted connections.
    #[clap(long)]
    tcp_keepalive_ms: Option<u64>,

    /// Set the value of TCP_NODELAY option for accepted connections. Enabled by default.
    #[clap(long)]
    tcp_nodelay: Option<bool>,

    /// Apply a timeout to each request.
    #[clap(long)]
    timeout_ms: Option<u64>,

    /// Max message size before decoding
    #[clap(long, default_value_t = 64 * 1024 * 1024)]
    max_decoding_message_size: usize,
}

impl ArgsAppStreamGrpc {
    async fn connect(
        self,
        x_token: Option<Vec<u8>>,
    ) -> anyhow::Result<GrpcClient<impl Interceptor>> {
        let mut builder = GrpcClient::build_from_shared(self.endpoint)?
            .x_token(x_token)?
            .tls_config_native_roots(self.ca_certificate.as_ref())
            .await?
            .max_decoding_message_size(self.max_decoding_message_size);

        if let Some(duration) = self.connect_timeout_ms {
            builder = builder.connect_timeout(Duration::from_millis(duration));
        }
        if let Some(sz) = self.buffer_size {
            builder = builder.buffer_size(sz);
        }
        if let Some(enabled) = self.http2_adaptive_window {
            builder = builder.http2_adaptive_window(enabled);
        }
        if let Some(duration) = self.http2_keep_alive_interval_ms {
            builder = builder.http2_keep_alive_interval(Duration::from_millis(duration));
        }
        if let Some(sz) = self.initial_connection_window_size {
            builder = builder.initial_connection_window_size(sz);
        }
        if let Some(sz) = self.initial_stream_window_size {
            builder = builder.initial_stream_window_size(sz);
        }
        if let Some(duration) = self.keep_alive_timeout_ms {
            builder = builder.keep_alive_timeout(Duration::from_millis(duration));
        }
        if let Some(enabled) = self.keep_alive_while_idle {
            builder = builder.keep_alive_while_idle(enabled);
        }
        if let Some(duration) = self.tcp_keepalive_ms {
            builder = builder.tcp_keepalive(Some(Duration::from_millis(duration)));
        }
        if let Some(enabled) = self.tcp_nodelay {
            builder = builder.tcp_nodelay(enabled);
        }
        if let Some(duration) = self.timeout_ms {
            builder = builder.timeout(Duration::from_millis(duration));
        }

        builder.connect().await.map_err(Into::into)
    }

    async fn subscribe(
        self,
        replay_from_slot: Option<Slot>,
        filter: RichatFilter,
        x_token: Option<Vec<u8>>,
    ) -> anyhow::Result<SubscribeStreamInput> {
        let endpoint = self.endpoint.clone();
        let mut client = self.connect(x_token).await.context("failed to connect")?;
        info!("connected to {endpoint} over gRPC");

        let version = client.get_version().await?;
        info!("version: {}", version.version);

        let stream = client
            .subscribe_richat(GrpcSubscribeRequest {
                replay_from_slot,
                filter: Some(filter),
            })
            .await
            .context("failed to subscribe")?;
        info!("subscribed");

        Ok(stream.map_err(Into::into).boxed())
    }
}

fn convert_prost_to_raw(msg: &StreamUpdate) -> anyhow::Result<Option<Vec<u8>>> {
    match msg {
        StreamUpdate::Geyser(msg) => convert_prost_to_raw_geyser(msg),
        StreamUpdate::Richat(msg) => convert_prost_to_raw_richat(msg),
    }
}

fn convert_prost_to_raw_geyser(msg: &SubscribeUpdate) -> anyhow::Result<Option<Vec<u8>>> {
    let Some(created_at) = msg.created_at else {
        return Ok(None);
    };

    Ok(Some(match &msg.update_oneof {
        Some(UpdateOneof::Account(SubscribeUpdateAccount {
            slot,
            account: Some(account),
            ..
        })) => {
            let txn = account
                .txn_signature
                .as_ref()
                .map(|signature| {
                    Ok::<_, anyhow::Error>(SanitizedTransaction::new_for_tests(
                        SanitizedMessage::Legacy(LegacyMessage::new(
                            Message::default(),
                            &HashSet::new(),
                        )),
                        vec![signature.as_slice().try_into()?],
                        false,
                    ))
                })
                .transpose()
                .context("failed to create txn")?;
            let msg = ProtobufMessage::Account {
                slot: *slot,
                account: &ReplicaAccountInfoV3 {
                    pubkey: account.pubkey.as_ref(),
                    lamports: account.lamports,
                    owner: account.owner.as_ref(),
                    executable: account.executable,
                    rent_epoch: account.rent_epoch,
                    data: &account.data,
                    write_version: account.write_version,
                    txn: txn.as_ref(),
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneof::Slot(SubscribeUpdateSlot {
            slot,
            parent,
            status,
            dead_error,
        })) => {
            let msg = ProtobufMessage::Slot {
                slot: *slot,
                parent: *parent,
                status: &match SlotStatus::try_from(*status) {
                    Ok(SlotStatus::SlotProcessed) => GeyserSlotStatus::Processed,
                    Ok(SlotStatus::SlotConfirmed) => GeyserSlotStatus::Confirmed,
                    Ok(SlotStatus::SlotFinalized) => GeyserSlotStatus::Rooted,
                    Ok(SlotStatus::SlotFirstShredReceived) => GeyserSlotStatus::FirstShredReceived,
                    Ok(SlotStatus::SlotCompleted) => GeyserSlotStatus::Completed,
                    Ok(SlotStatus::SlotCreatedBank) => GeyserSlotStatus::CreatedBank,
                    Ok(SlotStatus::SlotDead) => {
                        GeyserSlotStatus::Dead(dead_error.clone().unwrap_or_default())
                    }
                    Err(value) => anyhow::bail!("invalid status: {value}"),
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
            transaction: Some(tx),
            slot,
        })) => {
            let tx_with_meta = match convert_from::create_tx_with_meta(tx.clone())
                .map_err(|error| anyhow::anyhow!(error))?
            {
                TransactionWithStatusMeta::Complete(tx_with_meta) => tx_with_meta,
                TransactionWithStatusMeta::MissingMetadata(_) => {
                    anyhow::bail!("missing transaction metadata")
                }
            };
            let versioned_transaction = tx_with_meta.transaction;
            let transaction_status_meta = tx_with_meta.meta;
            let signature = tx
                .signature
                .as_slice()
                .try_into()
                .context("failed to create signature")?;
            let message_hash = versioned_transaction.message.hash();

            let msg = ProtobufMessage::Transaction {
                slot: *slot,
                transaction: &ReplicaTransactionInfoV3 {
                    signature: &signature,
                    message_hash: &message_hash,
                    is_vote: tx.is_vote,
                    transaction: &versioned_transaction,
                    transaction_status_meta: &transaction_status_meta,
                    index: tx.index as usize,
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneof::Entry(entry)) => {
            let msg = ProtobufMessage::Entry {
                entry: &ReplicaEntryInfoV2 {
                    slot: entry.slot,
                    index: entry.index as usize,
                    num_hashes: entry.num_hashes,
                    hash: entry.hash.as_ref(),
                    executed_transaction_count: entry.executed_transaction_count,
                    starting_transaction_index: entry.starting_transaction_index as usize,
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneof::BlockMeta(meta)) => {
            let msg = ProtobufMessage::BlockMeta {
                blockinfo: &ReplicaBlockInfoV4 {
                    parent_slot: meta.parent_slot,
                    slot: meta.slot,
                    parent_blockhash: &meta.parent_blockhash,
                    blockhash: &meta.blockhash,
                    rewards: &convert_from::create_rewards_obj(
                        meta.rewards
                            .clone()
                            .ok_or(anyhow::anyhow!("no rewards message"))?,
                    )
                    .map_err(|error| anyhow::anyhow!(error))?,
                    block_time: meta.block_time.map(|b| b.timestamp),
                    block_height: meta.block_height.map(|b| b.block_height),
                    executed_transaction_count: meta.executed_transaction_count,
                    entry_count: meta.entries_count,
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        _ => return Ok(None),
    }))
}

fn convert_prost_to_raw_richat(msg: &SubscribeUpdateRichat) -> anyhow::Result<Option<Vec<u8>>> {
    let Some(created_at) = msg.created_at else {
        return Ok(None);
    };

    let hash = |hash: &[u8]| {
        <[u8; 32]>::try_from(hash)
            .map(Hash::new_from_array)
            .context("invalid hash")
    };
    let socket_addr = |addr: &Option<String>| {
        addr.as_deref()
            .map(|addr| addr.parse::<SocketAddr>())
            .transpose()
            .context("invalid socket addr")
    };

    Ok(Some(match &msg.update_oneof {
        Some(UpdateOneofRichat::DeshredTransaction(SubscribeUpdateDeshredTransaction {
            transaction: Some(tx),
            slot,
        })) => {
            let transaction = convert_from::create_tx_versioned(
                tx.transaction
                    .clone()
                    .ok_or(anyhow::anyhow!("no transaction"))?,
            )
            .map_err(|error| anyhow::anyhow!(error))?;
            let loaded_addresses = if tx.loaded_writable_addresses.is_empty()
                && tx.loaded_readonly_addresses.is_empty()
            {
                None
            } else {
                Some(
                    convert_from::create_loaded_addresses(
                        tx.loaded_writable_addresses.clone(),
                        tx.loaded_readonly_addresses.clone(),
                    )
                    .map_err(|error| anyhow::anyhow!(error))?,
                )
            };
            let signature = tx
                .signature
                .as_slice()
                .try_into()
                .context("failed to create signature")?;

            let msg = ProtobufMessage::DeshredTransaction {
                slot: *slot,
                transaction: &ReplicaDeshredTransactionInfoV2 {
                    signature: &signature,
                    is_vote: tx.is_vote,
                    transaction: &transaction,
                    loaded_addresses: loaded_addresses.as_ref(),
                    completed_data_set_starting_shred_index: tx
                        .completed_data_set_starting_shred_index,
                    completed_data_set_ending_shred_index_exclusive: tx
                        .completed_data_set_ending_shred_index_exclusive,
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneofRichat::ContactInfo(info)) => {
            let msg = ProtobufMessage::ContactInfo {
                info: &ReplicaContactInfoV0_0_1 {
                    pubkey: &info.pubkey,
                    wallclock: info.wallclock,
                    outset: info.outset,
                    shred_version: info.shred_version.try_into()?,
                    version_major: info.version_major.try_into()?,
                    version_minor: info.version_minor.try_into()?,
                    version_patch: info.version_patch.try_into()?,
                    version_commit: info.version_commit,
                    version_feature_set: info.version_feature_set,
                    version_client_id: info.version_client_id.try_into()?,
                    gossip: socket_addr(&info.gossip)?,
                    tpu_quic: socket_addr(&info.tpu_quic)?,
                    tpu_forwards_quic: socket_addr(&info.tpu_forwards_quic)?,
                    tpu_vote_udp: socket_addr(&info.tpu_vote_udp)?,
                    tpu_vote_quic: socket_addr(&info.tpu_vote_quic)?,
                    tvu_udp: socket_addr(&info.tvu_udp)?,
                    tvu_quic: socket_addr(&info.tvu_quic)?,
                    serve_repair_udp: socket_addr(&info.serve_repair_udp)?,
                    serve_repair_quic: socket_addr(&info.serve_repair_quic)?,
                    rpc: socket_addr(&info.rpc)?,
                    rpc_pubsub: socket_addr(&info.rpc_pubsub)?,
                    alpenglow: socket_addr(&info.alpenglow)?,
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneofRichat::ContactInfoRemoved(info)) => {
            let msg = ProtobufMessage::ContactInfoRemoved {
                pubkey: &info.pubkey,
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneofRichat::BlockFooter(footer)) => {
            let block_footer: VersionedBlockFooter =
                wincode::deserialize(&footer.footer).context("failed to decode block footer")?;
            let msg = ProtobufMessage::BlockFooter {
                slot: footer.slot,
                bank_id: footer.bank_id,
                block_footer: &block_footer,
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneofRichat::EntryUpdateParent(info)) => {
            let msg = ProtobufMessage::EntryUpdateParent {
                info: &ReplicaEntryUpdateParentInfo {
                    slot: info.slot,
                    cleared_bank_id: info.cleared_bank_id,
                    parent_slot: info.parent_slot,
                    parent_block_id: &hash(&info.parent_block_id)?,
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        Some(UpdateOneofRichat::DeshredUpdateParent(info)) => {
            let msg = ProtobufMessage::DeshredUpdateParent {
                info: &ReplicaDeshredUpdateParentInfo {
                    slot: info.slot,
                    update_parent_fec_set_index: info.update_parent_fec_set_index,
                    parent_slot: info.parent_slot,
                    parent_block_id: &hash(&info.parent_block_id)?,
                },
            };
            msg.encode_with_timestamp(ProtobufEncoder::Raw, created_at)
        }
        _ => return Ok(None),
    }))
}
