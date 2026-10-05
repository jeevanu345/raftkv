//! Typed, read-only control-plane snapshots. Log payloads are never exposed.
use serde::Serialize;
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PeerDiagnostics {
    pub id: u64,
    pub match_index: u64,
    pub next_index: u64,
    pub replication_lag: u64,
    pub inflight: u32,
    pub recently_active: bool,
    pub snapshot_in_progress: bool,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntryDiagnostics {
    pub index: u64,
    pub term: u64,
    pub kind: String,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeDiagnostics {
    pub node_id: u64,
    pub role: &'static str,
    pub health: &'static str,
    pub term: u64,
    pub leader_id: Option<u64>,
    pub voted_for: Option<u64>,
    pub commit_index: u64,
    pub applied_index: u64,
    pub last_log_index: u64,
    pub snapshot_index: u64,
    pub state_hash: String,
    pub uptime_seconds: u64,
    pub key_count: usize,
    pub raft_address: Option<String>,
    pub client_address: Option<String>,
    pub admin_address: Option<String>,
    pub storage_bytes: u64,
    pub peers: Vec<PeerDiagnostics>,
    pub voters: Vec<u64>,
    pub configuration_state: &'static str,
    pub configuration: raft_core::ConfigState,
    pub log_entries: Vec<LogEntryDiagnostics>,
    pub snapshot_threshold: u64,
}
