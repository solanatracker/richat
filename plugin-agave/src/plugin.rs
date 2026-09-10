use {
    crate::{
        channel::Sender,
        config::{Config, ConfigNotifications},
        metrics,
        protobuf::{ProtobufEncoder, ProtobufMessage},
        version::VERSION,
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions,
        ReplicaBlockFooterInfoVersions, ReplicaBlockInfoVersions, ReplicaContactInfoVersions,
        ReplicaDeshredTransactionInfoV2, ReplicaDeshredTransactionInfoVersions,
        ReplicaDeshredUpdateParentInfoVersions, ReplicaEntryInfoVersions,
        ReplicaEntryUpdateParentInfoVersions, ReplicaTransactionInfoVersions,
        Result as PluginResult, SlotStatus,
    },
    futures::future::BoxFuture,
    log::error,
    richat_metrics::{MaybeRecorder, gauge},
    richat_shared::transports::{grpc::GrpcServer, quic::QuicServer},
    solana_clock::{BankId, Slot},
    std::{fmt, sync::Arc, time::Duration},
    tokio::{runtime::Runtime, task::JoinError},
    tokio_util::sync::CancellationToken,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PluginNotification {
    Slot,
    Account,
    Transaction,
    Entry,
    BlockMeta,
    // Richat-only notifications
    DeshredTransaction,
    ContactInfo,
    BlockFooter,
    EntryUpdateParent,
}

impl From<&ProtobufMessage<'_>> for PluginNotification {
    fn from(value: &ProtobufMessage<'_>) -> Self {
        match value {
            ProtobufMessage::Account { .. } => Self::Account,
            ProtobufMessage::Slot { .. } => Self::Slot,
            ProtobufMessage::Transaction { .. } => Self::Transaction,
            ProtobufMessage::Entry { .. } => Self::Entry,
            ProtobufMessage::BlockMeta { .. } => Self::BlockMeta,
            ProtobufMessage::DeshredTransaction { .. }
            | ProtobufMessage::DeshredUpdateParent { .. } => Self::DeshredTransaction,
            ProtobufMessage::ContactInfo { .. } | ProtobufMessage::ContactInfoRemoved { .. } => {
                Self::ContactInfo
            }
            ProtobufMessage::BlockFooter { .. } => Self::BlockFooter,
            ProtobufMessage::EntryUpdateParent { .. } => Self::EntryUpdateParent,
        }
    }
}

struct PluginTask(BoxFuture<'static, Result<(), JoinError>>);

unsafe impl Sync for PluginTask {}

impl fmt::Debug for PluginTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginTask").finish()
    }
}

#[derive(Debug)]
pub struct PluginInner {
    runtime: Runtime,
    messages: Sender,
    encoder: ProtobufEncoder,
    notifications: ConfigNotifications,
    shutdown: CancellationToken,
    tasks: Vec<(&'static str, PluginTask)>,
}

impl PluginInner {
    fn new(config: Config) -> PluginResult<Self> {
        let (metrics_recorder, metrics_handle) = if config.metrics.is_some() {
            let recorder = metrics::setup();
            let handle = recorder.handle();
            (Arc::new(recorder.into()), Some(handle))
        } else {
            (Arc::new(MaybeRecorder::Noop), None)
        };

        // Create Tokio runtime
        let runtime = config
            .tokio
            .build_runtime("richatPlugin")
            .map_err(|error| GeyserPluginError::Custom(Box::new(error)))?;

        // Create messages store
        let messages = Sender::new(config.channel, Arc::clone(&metrics_recorder));

        // Spawn servers
        let (messages, shutdown, tasks) = runtime
            .block_on(async move {
                let shutdown = CancellationToken::new();
                let mut tasks = Vec::with_capacity(4);

                // Start gRPC
                if let Some(config) = config.grpc {
                    let connections_inc = gauge!(&metrics_recorder, metrics::CONNECTIONS_TOTAL, "transport" => "grpc");
                    let connections_dec = connections_inc.clone();
                    tasks.push((
                        "gRPC Server",
                        PluginTask(Box::pin(
                            GrpcServer::spawn(
                                config,
                                messages.clone(),
                                move || connections_inc.increment(1), // on_conn_new_cb
                                move || connections_dec.decrement(1), // on_conn_drop_cb
                                VERSION,
                                shutdown.clone(),
                            )
                            .await?,
                        )),
                    ));
                }

                // Start Quic
                if let Some(config) = config.quic {
                    let connections_inc = gauge!(&metrics_recorder, metrics::CONNECTIONS_TOTAL, "transport" => "quic");
                    let connections_dec = connections_inc.clone();
                    tasks.push((
                        "Quic Server",
                        PluginTask(Box::pin(
                            QuicServer::spawn(
                                config,
                                messages.clone(),
                                move || connections_inc.increment(1), // on_conn_new_cb
                                move || connections_dec.decrement(1), // on_conn_drop_cb
                                VERSION,
                                shutdown.clone(),
                            )
                            .await?,
                        )),
                    ));
                }

                // Start prometheus server
                if let (Some(config), Some(metrics_handle)) = (config.metrics, metrics_handle) {
                    tasks.push((
                        "Prometheus Server",
                        PluginTask(Box::pin(
                            metrics::spawn_server(config, metrics_handle, shutdown.clone().cancelled_owned()).await?,
                        )),
                    ));
                }

                Ok::<_, anyhow::Error>((messages, shutdown, tasks))
            })
            .map_err(|error| GeyserPluginError::Custom(format!("{error:?}").into()))?;

        Ok(Self {
            runtime,
            messages,
            encoder: config.channel.encoder,
            notifications: config.notifications,
            shutdown,
            tasks,
        })
    }
}

#[derive(Debug, Default)]
pub struct Plugin {
    inner: Option<PluginInner>,
}

impl Plugin {
    fn notifications(&self) -> ConfigNotifications {
        self.inner
            .as_ref()
            .map(|inner| inner.notifications)
            .unwrap_or_default()
    }
}

impl GeyserPlugin for Plugin {
    fn name(&self) -> &'static str {
        concat!(env!("CARGO_PKG_NAME"), "-", env!("CARGO_PKG_VERSION"))
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        solana_logger::setup_with_default("info");
        let config = Config::load_from_file(config_file).inspect_err(|error| {
            error!("failed to load config: {error:?}");
        })?;

        // Setup logger from the config
        solana_logger::setup_with_default(&config.logs.level);

        // Create inner
        self.inner = Some(PluginInner::new(config).inspect_err(|error| {
            error!("failed to load plugin from the config: {error:?}");
        })?);

        Ok(())
    }

    fn on_unload(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.messages.close();

            inner.shutdown.cancel();
            inner.runtime.block_on(async {
                for (name, task) in inner.tasks {
                    if let Err(error) = task.0.await {
                        error!("failed to join `{name}` task: {error:?}");
                    }
                }
            });

            inner.runtime.shutdown_timeout(Duration::from_secs(10));
        }
    }

    // Snapshot accounts are never requested (`account_data_snapshot_notifications_enabled`
    // returns `false`), so this is a no-op that also keeps the deprecated
    // `update_account(.., is_startup = true)` path unused.
    fn update_account_from_snapshot(
        &self,
        _account: ReplicaAccountInfoVersions,
        _slot: Slot,
    ) -> PluginResult<()> {
        Ok(())
    }

    fn update_account_for_bank(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: Slot,
        _bank_id: BankId,
    ) -> PluginResult<()> {
        let account = match account {
            ReplicaAccountInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_1 is not supported")
            }
            ReplicaAccountInfoVersions::V0_0_2(_info) => {
                unreachable!("ReplicaAccountInfoVersions::V0_0_2 is not supported")
            }
            ReplicaAccountInfoVersions::V0_0_3(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(ProtobufMessage::Account { slot, account }, inner.encoder);

        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    // Called for statuses that are not tied to a bank:
    // `FirstShredReceived`, `Completed` and `Dead`.
    fn update_slot_status(
        &self,
        slot: Slot,
        parent: Option<u64>,
        status: &SlotStatus,
    ) -> PluginResult<()> {
        let inner = self.inner.as_ref().expect("initialized");
        inner.messages.push(
            ProtobufMessage::Slot {
                slot,
                parent,
                status,
            },
            inner.encoder,
        );

        Ok(())
    }

    // Called for statuses tied to a bank:
    // `Confirmed`, `Processed`, `Rooted` and `CreatedBank`.
    fn update_bank_status(
        &self,
        slot: Slot,
        parent: Option<u64>,
        status: &SlotStatus,
        _bank_id: BankId,
    ) -> PluginResult<()> {
        self.update_slot_status(slot, parent, status)
    }

    fn notify_transaction_for_bank(
        &self,
        transaction: ReplicaTransactionInfoVersions<'_>,
        slot: Slot,
        _bank_id: BankId,
    ) -> PluginResult<()> {
        let transaction = match transaction {
            ReplicaTransactionInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaTransactionInfoVersions::V0_0_1 is not supported")
            }
            ReplicaTransactionInfoVersions::V0_0_2(_info) => {
                unreachable!("ReplicaTransactionInfoVersions::V0_0_2 is not supported")
            }
            ReplicaTransactionInfoVersions::V0_0_3(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner.messages.push(
            ProtobufMessage::Transaction { slot, transaction },
            inner.encoder,
        );

        Ok(())
    }

    fn notify_entry_for_bank(
        &self,
        entry: ReplicaEntryInfoVersions,
        _bank_id: BankId,
    ) -> PluginResult<()> {
        #[allow(clippy::infallible_destructuring_match)]
        let entry = match entry {
            ReplicaEntryInfoVersions::V0_0_1(_entry) => {
                unreachable!("ReplicaEntryInfoVersions::V0_0_1 is not supported")
            }
            ReplicaEntryInfoVersions::V0_0_2(entry) => entry,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(ProtobufMessage::Entry { entry }, inner.encoder);

        Ok(())
    }

    fn notify_block_metadata_for_bank(
        &self,
        blockinfo: ReplicaBlockInfoVersions<'_>,
        _bank_id: BankId,
    ) -> PluginResult<()> {
        let blockinfo = match blockinfo {
            ReplicaBlockInfoVersions::V0_0_1(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_1 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_2(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_2 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_3(_info) => {
                unreachable!("ReplicaBlockInfoVersions::V0_0_3 is not supported")
            }
            ReplicaBlockInfoVersions::V0_0_4(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(ProtobufMessage::BlockMeta { blockinfo }, inner.encoder);

        Ok(())
    }

    fn notify_deshred_transaction(
        &self,
        transaction: ReplicaDeshredTransactionInfoVersions<'_>,
        slot: Slot,
    ) -> PluginResult<()> {
        let inner = self.inner.as_ref().expect("initialized");
        let push = |transaction: &ReplicaDeshredTransactionInfoV2<'_>| {
            inner.messages.push(
                ProtobufMessage::DeshredTransaction { slot, transaction },
                inner.encoder,
            )
        };

        match transaction {
            ReplicaDeshredTransactionInfoVersions::V0_0_1(info) => {
                push(&ReplicaDeshredTransactionInfoV2 {
                    signature: info.signature,
                    is_vote: info.is_vote,
                    transaction: info.transaction,
                    loaded_addresses: info.loaded_addresses,
                    completed_data_set_starting_shred_index: 0,
                    completed_data_set_ending_shred_index_exclusive: 0,
                })
            }
            ReplicaDeshredTransactionInfoVersions::V0_0_2(info) => push(info),
        }

        Ok(())
    }

    fn notify_deshred_update_parent(
        &self,
        update_parent: ReplicaDeshredUpdateParentInfoVersions<'_>,
    ) -> PluginResult<()> {
        #[allow(clippy::infallible_destructuring_match)]
        let info = match update_parent {
            ReplicaDeshredUpdateParentInfoVersions::V0_0_1(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(ProtobufMessage::DeshredUpdateParent { info }, inner.encoder);

        Ok(())
    }

    fn notify_entry_update_parent(
        &self,
        update_parent: ReplicaEntryUpdateParentInfoVersions<'_>,
    ) -> PluginResult<()> {
        #[allow(clippy::infallible_destructuring_match)]
        let info = match update_parent {
            ReplicaEntryUpdateParentInfoVersions::V0_0_1(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(ProtobufMessage::EntryUpdateParent { info }, inner.encoder);

        Ok(())
    }

    fn notify_block_footer(
        &self,
        block_footer: ReplicaBlockFooterInfoVersions<'_>,
        bank_id: BankId,
    ) -> PluginResult<()> {
        #[allow(clippy::infallible_destructuring_match)]
        let info = match block_footer {
            ReplicaBlockFooterInfoVersions::V0_0_1(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner.messages.push(
            ProtobufMessage::BlockFooter {
                slot: info.slot,
                bank_id,
                block_footer: info.block_footer,
            },
            inner.encoder,
        );

        Ok(())
    }

    // Delivered for every known node right after the plugin is loaded (`is_startup = true`)
    // and then on every gossip update; all of them are pushed into the channel.
    fn notify_contact_info(
        &self,
        info: ReplicaContactInfoVersions<'_>,
        _is_startup: bool,
    ) -> PluginResult<()> {
        #[allow(clippy::infallible_destructuring_match)]
        let info = match info {
            ReplicaContactInfoVersions::V0_0_1(info) => info,
        };

        let inner = self.inner.as_ref().expect("initialized");
        inner
            .messages
            .push(ProtobufMessage::ContactInfo { info }, inner.encoder);

        Ok(())
    }

    fn notify_contact_info_removed(&self, pubkey: &[u8]) -> PluginResult<()> {
        let inner = self.inner.as_ref().expect("initialized");
        inner.messages.push(
            ProtobufMessage::ContactInfoRemoved { pubkey },
            inner.encoder,
        );

        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn account_data_snapshot_notifications_enabled(&self) -> bool {
        false
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        true
    }

    fn deshred_transaction_notifications_enabled(&self) -> bool {
        self.notifications().deshred_transactions
    }

    fn deshred_transaction_alt_resolution_enabled(&self) -> bool {
        let notifications = self.notifications();
        notifications.deshred_transactions && notifications.deshred_transactions_alt_resolution
    }

    fn contact_info_notifications_enabled(&self) -> bool {
        self.notifications().contact_info
    }

    fn block_footer_notifications_enabled(&self) -> bool {
        self.notifications().block_footers
    }
}

#[cfg(feature = "plugin")]
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    #[cfg(feature = "rustls-install-default-provider")]
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("failed to call CryptoProvider::install_default()");

    let plugin = Plugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
