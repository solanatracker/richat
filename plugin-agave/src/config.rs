use {
    crate::protobuf::ProtobufEncoder,
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        GeyserPluginError, Result as PluginResult,
    },
    richat_metrics::ConfigMetrics,
    richat_shared::{
        config::{ConfigTokio, deserialize_humansize_usize, deserialize_num_str},
        transports::{grpc::ConfigGrpcServer, quic::ConfigQuicServer},
    },
    serde::{
        Deserialize,
        de::{self, Deserializer},
    },
    std::{fs, path::Path},
};

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub libpath: String,
    pub logs: ConfigLogs,
    pub metrics: Option<ConfigMetrics>,
    pub tokio: ConfigTokio,
    pub channel: ConfigChannel,
    pub notifications: ConfigNotifications,
    pub quic: Option<ConfigQuicServer>,
    pub grpc: Option<ConfigGrpcServer>,
}

impl Config {
    /// Parses JSON with comments (JSON5), same as the Agave plugin manager does for `libpath`.
    fn load_from_str(config: &str) -> PluginResult<Self> {
        json5::from_str(config).map_err(|error| GeyserPluginError::ConfigFileReadError {
            msg: error.to_string(),
        })
    }

    pub fn load_from_file<P: AsRef<Path>>(file: P) -> PluginResult<Self> {
        let config = fs::read_to_string(file).map_err(GeyserPluginError::ConfigFileOpenError)?;
        Self::load_from_str(&config)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLogs {
    /// Log level
    pub level: String,
}

impl Default for ConfigLogs {
    fn default() -> Self {
        Self {
            level: "info".to_owned(),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigChannel {
    #[serde(deserialize_with = "ConfigChannel::deserialize_encoder")]
    pub encoder: ProtobufEncoder,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max_messages: usize,
    #[serde(deserialize_with = "deserialize_humansize_usize")]
    pub max_bytes: usize,
}

impl Default for ConfigChannel {
    fn default() -> Self {
        Self {
            encoder: ProtobufEncoder::Raw,
            max_messages: 2_097_152, // aligned to power of 2, ~20k/slot should give us ~100 slots
            max_bytes: 15 * 1024 * 1024 * 1024, // 15GiB with ~150MiB/slot should give us ~100 slots
        }
    }
}

/// Optional Geyser notifications, all enabled by default like in Yellowstone gRPC.
///
/// Accounts, transactions, entries, slots and block metadata are always enabled.
/// Messages produced by these notifications are sent on the Richat stream as
/// `SubscribeUpdateRichat` and only to clients that explicitly enable them in `RichatFilter`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigNotifications {
    /// Transactions reconstructed from shreds before replay (agave 4.1+),
    /// also enables Alpenglow UpdateParent notifications from the deshred stream.
    pub deshred_transactions: bool,
    /// Resolve address lookup tables for deshred transactions
    pub deshred_transactions_alt_resolution: bool,
    /// Gossip contact info updates and removals (agave 4.2+)
    pub contact_info: bool,
    /// Alpenglow block footers (agave 4.3+)
    pub block_footers: bool,
}

impl Default for ConfigNotifications {
    fn default() -> Self {
        Self {
            deshred_transactions: true,
            deshred_transactions_alt_resolution: true,
            contact_info: true,
            block_footers: true,
        }
    }
}

impl ConfigChannel {
    pub fn deserialize_encoder<'de, D>(deserializer: D) -> Result<ProtobufEncoder, D::Error>
    where
        D: Deserializer<'de>,
    {
        match Deserialize::deserialize(deserializer)? {
            "prost" => Ok(ProtobufEncoder::Prost),
            "raw" => Ok(ProtobufEncoder::Raw),
            value => Err(de::Error::custom(format!(
                "failed to decode encoder: {value}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shipped_config_is_valid() {
        let config = Config::load_from_file(concat!(env!("CARGO_MANIFEST_DIR"), "/config.json"))
            .expect("shipped config.json must be loadable");
        assert_eq!(config.channel.max_messages, 2_097_152);
        assert_eq!(config.channel.max_bytes, 16 * 1024 * 1024 * 1024);
        assert!(config.grpc.is_none());
        assert!(config.quic.is_none());
    }

    #[test]
    fn config_with_comments() {
        let config = Config::load_from_str(
            r#"{
                // line comment
                "libpath": "lib.so", /* block comment */
                "tokio": { "worker_threads": null }
            }"#,
        )
        .unwrap();
        assert_eq!(config.libpath, "lib.so");
    }

    #[test]
    fn unknown_fields_are_rejected() {
        assert!(Config::load_from_str(r#"{ "unknown": 1 }"#).is_err());
    }
}
