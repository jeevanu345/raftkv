//! Prometheus instruments owned by one runtime registry.
use prometheus::core::Collector;
use prometheus::{Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, Opts, Registry};
pub struct Metrics {
    pub registry: Registry,
    pub log_bytes: Gauge,
    pub snapshot_bytes: Gauge,
    pub rpc_duration: Histogram,
    pub storage_duration: Histogram,
    pub term: Gauge,
    pub role: Gauge,
    pub commit: Gauge,
    pub applied: Gauge,
    pub last_log: Gauge,
    pub keys: Gauge,
    pub expiring: Gauge,
    pub lag: GaugeVec,
    pub elections: Counter,
    pub leadership: Counter,
    pub rejections: Counter,
    pub sent: Counter,
    pub received: Counter,
    pub snapshots: Counter,
    pub installs: Counter,
    pub errors: Counter,
    pub requests: CounterVec,
    pub duration: Histogram,
}
impl Metrics {
    pub fn summary(&self, log: u64, snapshot: u64) -> serde_json::Value {
        // The dashboard records successive samples to derive rates. Histogram
        // quantiles are computed from observed buckets, not fabricated waveforms.
        let histogram = self.duration.collect();
        let buckets = histogram
            .first()
            .and_then(|f| f.get_metric().first())
            .map(|m| m.get_histogram().clone());
        let quantile = |q: f64| -> f64 {
            let Some(h) = &buckets else {
                return 0.0;
            };
            if h.get_sample_count() == 0 {
                return 0.0;
            }
            let target = q * h.get_sample_count() as f64;
            h.get_bucket()
                .iter()
                .find(|b| b.cumulative_count() as f64 >= target)
                .map(|b| b.upper_bound() * 1000.0)
                .unwrap_or(0.0)
        };
        serde_json::json!({"requestsPerSecond":0,"readRequestsPerSecond":0,"writeRequestsPerSecond":0,"readRequestsTotal":self.requests.with_label_values(&["read"]).get(),"writeRequestsTotal":self.requests.with_label_values(&["write"]).get(),"latency":{"p50Ms":quantile(0.5),"p95Ms":quantile(0.95),"p99Ms":quantile(0.99)},"electionsTotal":self.elections.get(),"leadershipChangesTotal":self.leadership.get(),"appendRejectionsTotal":self.rejections.get(),"logBytes":log,"snapshotBytes":snapshot})
    }

    pub fn new() -> Result<Self, prometheus::Error> {
        let registry = Registry::new();
        registry.register(Box::new(raft_storage::durability::fsync_histogram()))?;
        let gauge = |name: &str, help: &str| -> Result<Gauge, prometheus::Error> {
            let value = Gauge::new(name, help)?;
            registry.register(Box::new(value.clone()))?;
            Ok(value)
        };
        let counter = |name: &str, help: &str| -> Result<Counter, prometheus::Error> {
            let value = Counter::new(name, help)?;
            registry.register(Box::new(value.clone()))?;
            Ok(value)
        };
        let lag = GaugeVec::new(
            Opts::new(
                "raftkv_raft_replication_lag",
                "Leader log minus peer match index",
            ),
            &["peer"],
        )?;
        registry.register(Box::new(lag.clone()))?;
        let requests = CounterVec::new(
            Opts::new("raftkv_client_requests_total", "Completed client requests"),
            &["kind"],
        )?;
        registry.register(Box::new(requests.clone()))?;
        let duration = Histogram::with_opts(HistogramOpts::new(
            "raftkv_client_request_duration_seconds",
            "Client request duration",
        ))?;
        registry.register(Box::new(duration.clone()))?;
        let rpc_duration = Histogram::with_opts(HistogramOpts::new(
            "raftkv_raft_rpc_duration_seconds",
            "Incoming Raft RPC duration",
        ))?;
        registry.register(Box::new(rpc_duration.clone()))?;
        let storage_duration = Histogram::with_opts(HistogramOpts::new(
            "raftkv_storage_action_duration_seconds",
            "Durable action execution duration",
        ))?;
        registry.register(Box::new(storage_duration.clone()))?;
        Ok(Self {
            log_bytes: gauge("raftkv_storage_log_bytes", "On-disk Raft log bytes")?,
            snapshot_bytes: gauge("raftkv_storage_snapshot_bytes", "On-disk snapshot bytes")?,
            rpc_duration,
            storage_duration,
            term: gauge("raftkv_raft_term", "Current term")?,
            role: gauge(
                "raftkv_raft_role",
                "Role: follower 0, candidate 1, leader 2",
            )?,
            commit: gauge("raftkv_raft_commit_index", "Commit index")?,
            applied: gauge("raftkv_raft_applied_index", "Durable applied index")?,
            last_log: gauge("raftkv_raft_last_log_index", "Last log index")?,
            keys: gauge("raftkv_kv_keys", "KV key count")?,
            expiring: gauge("raftkv_kv_expiring_keys", "Expiring key count")?,
            elections: counter("raftkv_raft_elections_total", "Election rounds")?,
            leadership: counter("raftkv_raft_leadership_changes_total", "Local role changes")?,
            rejections: counter("raftkv_raft_append_rejections_total", "Append rejections")?,
            sent: counter("raftkv_raft_messages_sent_total", "Messages dispatched")?,
            received: counter("raftkv_raft_messages_received_total", "Messages received")?,
            snapshots: counter("raftkv_snapshot_total", "Snapshots created")?,
            installs: counter("raftkv_snapshot_install_total", "Snapshots installed")?,
            errors: counter("raftkv_client_errors_total", "Client errors")?,
            lag,
            requests,
            duration,
            registry,
        })
    }
}
