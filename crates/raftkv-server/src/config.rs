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
    /// Browser dashboard HTTP endpoint.
    #[serde(default = "default_ui_listen")]
    pub ui_listen: String,
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
            ui_listen: "127.0.0.1:8080".into(),
            peers: vec![Peer {
                id: 1,
                raft_addr: "http://127.0.0.1:7001".into(),
            }],
            data_dir: PathBuf::from("./data"),
            election_timeout_ms: 300,
            heartbeat_ms: 50,
            tick_ms: 10,
            snapshot_entries_threshold: 10_000,
            pre_vote: true,
        }
    }
}

fn default_ui_listen() -> String {
    "127.0.0.1:8080".into()
}

impl ServerConfig {
    /// Reject configurations that cannot safely drive the runtime.
    pub fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(self.id > 0, "node id must be positive");
        anyhow::ensure!(self.tick_ms > 0, "tick_ms must be positive");
        anyhow::ensure!(
            self.heartbeat_ms >= self.tick_ms,
            "heartbeat_ms must be >= tick_ms"
        );
        anyhow::ensure!(
            self.election_timeout_ms > self.heartbeat_ms,
            "election timeout must exceed heartbeat interval"
        );
        anyhow::ensure!(
            self.snapshot_entries_threshold > 0,
            "snapshot threshold must be positive"
        );
        anyhow::ensure!(
            !self.peers.is_empty(),
            "at least one bootstrap voter is required"
        );
        let mut ids = std::collections::BTreeSet::new();
        for peer in &self.peers {
            anyhow::ensure!(
                peer.id > 0 && ids.insert(peer.id),
                "peer ids must be positive and unique"
            );
            anyhow::ensure!(!peer.raft_addr.is_empty(), "peer address cannot be empty");
        }
        anyhow::ensure!(
            ids.contains(&self.id),
            "local node must be a bootstrap voter"
        );
        Ok(())
    }

    /// Load config from a TOML file.
    pub fn from_toml_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn configuration_rejects_invalid_timers_and_membership() {
        let mut cfg = ServerConfig::default();
        assert!(cfg.validate().is_ok());
        cfg.tick_ms = 0;
        assert!(cfg.validate().is_err());
        cfg.tick_ms = 10;
        cfg.peers.push(cfg.peers[0].clone());
        assert!(cfg.validate().is_err());
        cfg.peers.pop();
        cfg.id = 2;
        assert!(cfg.validate().is_err());
    }
}
