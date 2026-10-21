use {
    crate::labels::SUBSCRIPTION_IDS_MAX_DEFAULT,
    serde::Deserialize,
    std::net::{IpAddr, Ipv4Addr, SocketAddr},
};

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigMetrics {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
    /// Max distinct `x_subscription_id` label values, the rest is reported as `other`
    pub max_subscription_ids: usize,
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 10123),
            max_subscription_ids: SUBSCRIPTION_IDS_MAX_DEFAULT,
        }
    }
}
