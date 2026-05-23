//! Server configuration.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// One peer in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// Cluster-unique node id.
    pub id: u64,
    /// gRPC address (e.g. http://node2:7000).
    pub raft_addr: String,
}

/// Top-level server config.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// This node's id.
    pub id: u64,
    /// gRPC bind address (Raft peer-to-peer).
    pub raft_listen: String,
    /// RESP bind address (clients).
    pub client_listen: String,
    /// Prometheus metrics scrape endpoint.
    pub metrics_listen: String,
    /// Initial voters (cluster bootstrap). Must be the same on all nodes at
    /// first boot.
    pub peers: Vec<Peer>,
    /// Data directory.
    pub data_dir: PathBuf,
    /// Election timeout in milliseconds (low end of randomized range).
    pub election_timeout_ms: u64,
    /// Heartbeat interval in milliseconds.
    pub heartbeat_ms: u64,
    /// Tick granularity in milliseconds.
    pub tick_ms: u64,
    /// Snapshot threshold: trigger when log exceeds N entries past last snap.
    pub snapshot_entries_threshold: u64,
    /// Enable pre-vote.
    pub pre_vote: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            id: 1,
            raft_listen: "0.0.0.0:7001".into(),
            client_listen: "0.0.0.0:6379".into(),
            metrics_listen: "0.0.0.0:9100".into(),
            peers: vec![Peer { id: 1, raft_addr: "http://127.0.0.1:7001".into() }],
            data_dir: PathBuf::from("./data"),
            election_timeout_ms: 300,
            heartbeat_ms: 50,
            tick_ms: 10,
            snapshot_entries_threshold: 10_000,
            pre_vote: true,
        }
    }
}

impl ServerConfig {
    /// Load config from a TOML file.
    pub fn from_toml_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }
}
