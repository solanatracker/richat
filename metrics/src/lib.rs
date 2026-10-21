pub use ::metrics::*;

mod config;
pub use config::ConfigMetrics;

mod labels;
pub use labels::{
    SUBSCRIPTION_ID_MAX_LEN, SUBSCRIPTION_ID_OTHER, SUBSCRIPTION_IDS_MAX_DEFAULT,
    set_subscription_ids_max, subscription_id_label,
};

mod macros;

mod recorder;
pub use recorder::MaybeRecorder;

mod server;
pub use server::spawn_server;

#[inline]
pub fn duration_to_seconds(d: std::time::Duration) -> f64 {
    d.as_secs() as f64 + d.subsec_nanos() as f64 / 1e9
}
