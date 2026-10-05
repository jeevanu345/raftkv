//! Actual file fsync and sled flush duration, including failures.
use prometheus::{Histogram, HistogramOpts};
use std::{fs::File, sync::OnceLock};
pub fn fsync_histogram() -> Histogram {
    static HISTOGRAM: OnceLock<Histogram> = OnceLock::new();
    HISTOGRAM
        .get_or_init(|| {
            Histogram::with_opts(HistogramOpts::new(
                "raftkv_storage_fsync_duration_seconds",
                "Actual log/snapshot/directory fsync and metadata sled flush latency",
            ))
            .expect("static metric definition")
        })
        .clone()
}
pub(crate) fn sync_data(file: &File) -> std::io::Result<()> {
    let _timer = fsync_histogram().start_timer();
    file.sync_data()
}
pub(crate) fn sync_all(file: &File) -> std::io::Result<()> {
    let _timer = fsync_histogram().start_timer();
    file.sync_all()
}
