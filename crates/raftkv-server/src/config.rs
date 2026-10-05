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
    /// Advertised RESP endpoint used by MOVED.
    #[serde(default)]
    pub client_addr: String,
    /// Advertised HTTP endpoint used for leader forwarding.
    #[serde(default)]
    pub admin_addr: String,
    /// Nonvoting learner.
    #[serde(default)]
    pub learner: bool,
    /// SHA256 of DER node certificate, required for mTLS identity binding.
    #[serde(default)]
    pub certificate_sha256: Option<String>,
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
    /// Optional bearer token for the HTTP control plane.
    #[serde(default)]
    pub admin_token: Option<String>,
    /// Additional administrator identities and roles.
    #[serde(default)]
    pub admin_users: Vec<AdminUser>,
    /// Peer TLS identity and cluster CA.
    #[serde(default)]
    pub peer_tls: Option<TlsConfig>,
    /// RESP authentication secret (optional for local development).
    #[serde(default)]
    pub client_token: Option<String>,
    /// Optional client-plane TLS certificate and key.
    #[serde(default)]
    pub client_tls: Option<TlsConfig>,
    /// Optional HTTPS certificate and key for the admin plane.
    #[serde(default)]
    pub admin_tls: Option<TlsConfig>,
    /// RESP user permissions and allowed key prefixes.
    #[serde(default)]
    pub client_users: Vec<resp_server::server::AccessUser>,
    /// Allowed development/browser origins for cookie-authenticated mutations.
    #[serde(default)]
    pub allowed_origins: Vec<String>,
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
                client_addr: "127.0.0.1:6379".into(),
                admin_addr: "http://127.0.0.1:8080".into(),
                learner: false,
                certificate_sha256: None,
            }],
            data_dir: PathBuf::from("./data"),
            election_timeout_ms: 300,
            heartbeat_ms: 50,
            tick_ms: 10,
            snapshot_entries_threshold: 10_000,
            pre_vote: true,
            admin_token: None,
            admin_users: vec![],
            peer_tls: None,
            client_token: None,
            client_tls: None,
            admin_tls: None,
            client_users: vec![],
            allowed_origins: vec![
                "http://127.0.0.1:5173".into(),
                "http://localhost:5173".into(),
            ],
        }
    }
}

fn default_ui_listen() -> String {
    "127.0.0.1:8080".into()
}

impl ServerConfig {
    /// Load config from a TOML file.
    pub fn from_toml_file(path: &std::path::Path) -> anyhow::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&s)?)
    }
}

/// HTTP administrative identity; never log its token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminUser {
    pub name: String,
    pub token: String,
    pub role: String,
}
/// PEM files for the node identity and trusted cluster CA.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert: PathBuf,
    pub key: PathBuf,
    pub ca: PathBuf,
}
