use std::{
    collections::HashSet,
    sync::{
        Arc, LazyLock, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

/// Default limit of distinct `x_subscription_id` label values
pub const SUBSCRIPTION_IDS_MAX_DEFAULT: usize = 1_024;
/// Label value used once the limit of distinct `x_subscription_id` values is reached
pub const SUBSCRIPTION_ID_OTHER: &str = "other";
/// Longer `x_subscription_id` values are truncated
pub const SUBSCRIPTION_ID_MAX_LEN: usize = 64;

static SUBSCRIPTION_IDS_MAX: AtomicUsize = AtomicUsize::new(SUBSCRIPTION_IDS_MAX_DEFAULT);
static SUBSCRIPTION_IDS: LazyLock<Mutex<HashSet<Arc<str>>>> = LazyLock::new(Default::default);
static SUBSCRIPTION_ID_OTHER_ARC: LazyLock<Arc<str>> =
    LazyLock::new(|| Arc::from(SUBSCRIPTION_ID_OTHER));

pub fn set_subscription_ids_max(max: usize) {
    SUBSCRIPTION_IDS_MAX.store(max, Ordering::Relaxed);
}

/// Metrics label for a client supplied `x_subscription_id`.
///
/// Every distinct value creates series that are never removed, so only the first
/// `max_subscription_ids` values are kept and the rest are reported as `other`.
pub fn subscription_id_label(id: &str) -> Arc<str> {
    let id = truncate(id, SUBSCRIPTION_ID_MAX_LEN);
    let mut ids = SUBSCRIPTION_IDS.lock().expect("unpoisoned");
    if let Some(label) = ids.get(id) {
        return Arc::clone(label);
    }
    if ids.len() < SUBSCRIPTION_IDS_MAX.load(Ordering::Relaxed) {
        let label: Arc<str> = Arc::from(id);
        ids.insert(Arc::clone(&label));
        label
    } else {
        Arc::clone(&SUBSCRIPTION_ID_OTHER_ARC)
    }
}

fn truncate(value: &str, max_len: usize) -> &str {
    if value.len() <= max_len {
        return value;
    }
    let mut end = max_len;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

#[cfg(test)]
mod tests {
    use super::{SUBSCRIPTION_ID_MAX_LEN, SUBSCRIPTION_ID_OTHER, subscription_id_label, truncate};

    #[test]
    fn subscription_id_labels_are_bounded() {
        // global state: the only test in the crate that touches it
        super::set_subscription_ids_max(3);
        assert_eq!(subscription_id_label("a").as_ref(), "a");
        assert_eq!(subscription_id_label("").as_ref(), "");
        assert_eq!(subscription_id_label("b").as_ref(), "b");
        assert_eq!(subscription_id_label("c").as_ref(), SUBSCRIPTION_ID_OTHER);
        assert_eq!(subscription_id_label("a").as_ref(), "a");

        let long = "é".repeat(SUBSCRIPTION_ID_MAX_LEN);
        assert!(truncate(&long, SUBSCRIPTION_ID_MAX_LEN).len() <= SUBSCRIPTION_ID_MAX_LEN);
        assert_eq!(subscription_id_label(&long).as_ref(), SUBSCRIPTION_ID_OTHER);
    }
}
