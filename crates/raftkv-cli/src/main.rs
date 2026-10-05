//! `raftkv-cli` — operator CLI for the cluster.
//!
//! Communicates with a node over its RESP port. The CLI focuses on
//! observability commands (INFO, CLUSTER NODES) plus the standard KV ops so
//! it can serve as a smoke-test client.

#![deny(unsafe_code)]

use std::time::Duration;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(name = "raftkv-cli", version, about = "raftkv operator CLI")]
struct Cli {
    /// Node RESP endpoint, e.g. 127.0.0.1:6379.
    #[arg(long, default_value = "127.0.0.1:6379", env = "RAFTKV_ADDR")]
    addr: String,
    /// HTTP admin endpoint.
    #[arg(
        long,
        default_value = "http://127.0.0.1:8080",
        env = "RAFTKV_ADMIN_ADDR"
    )]
    admin_addr: String,
    /// Administrative bearer token. Prefer environment to shell history.
    #[arg(long, env = "RAFTKV_ADMIN_TOKEN")]
    admin_token: Option<String>,
    /// Additional PEM CA for HTTPS administration.
    #[arg(long, env = "RAFTKV_CA_CERT")]
    ca_cert: Option<std::path::PathBuf>,
    /// Optional RESP AUTH password.
    #[arg(long, env = "RAFTKV_CLIENT_TOKEN")]
    password: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Portable backup operations.
    Backup {
        #[command(subcommand)]
        command: BackupCmd,
    },
    /// Offline restore into an empty directory and explicitly bootstrap ONE
    /// new voter. Add other members as learners after starting this node.
    Restore {
        file: std::path::PathBuf,
        #[arg(long)]
        data_dir: std::path::PathBuf,
        #[arg(long)]
        bootstrap_node_id: u64,
        #[arg(long)]
        raft_addr: String,
        #[arg(long)]
        client_addr: String,
        #[arg(long)]
        admin_addr: String,
    },
    /// Show cluster status.
    Status,
    /// List cluster members.
    Members,
    /// SET a key.
    Set {
        /// Key.
        key: String,
        /// Value.
        value: String,
    },
    /// GET a key.
    Get {
        /// Key.
        key: String,
    },
    /// DEL keys.
    Del {
        /// Keys to delete.
        keys: Vec<String>,
    },
    /// PING the server.
    Ping,
}

#[derive(Debug, Subcommand)]
enum BackupCmd {
    Create { file: std::path::PathBuf },
    Inspect { file: std::path::PathBuf },
}

async fn maintenance(cli: &Cli) -> Result<bool> {
    use raft_storage::{backup::Backup, MetaStore, SnapshotMeta, SnapshotStore};
    match &cli.cmd {
        Cmd::Backup {
            command: BackupCmd::Create { file },
        } => {
            let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(30));
            if let Some(path) = &cli.ca_cert {
                builder = builder
                    .add_root_certificate(reqwest::Certificate::from_pem(&std::fs::read(path)?)?);
            }
            let client = builder.build()?;
            let mut request = client.get(format!(
                "{}/api/v1/admin/backup",
                cli.admin_addr.trim_end_matches('/')
            ));
            if let Some(token) = &cli.admin_token {
                request = request.bearer_auth(token);
            }
            let bytes = request.send().await?.error_for_status()?.bytes().await?;
            Backup::decode(&bytes)?;
            let mut out = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(file)?;
            use std::io::Write;
            out.write_all(&bytes)?;
            out.sync_all()?;
            println!("Created {}", file.display());
            Ok(true)
        }
        Cmd::Backup {
            command: BackupCmd::Inspect { file },
        } => {
            let (backup, p) = Backup::decode(&std::fs::read(file)?)?;
            kv_state_machine::KvStateMachine::validate_snapshot(&p.state, p.index)?;
            println!("{}", serde_json::to_string_pretty(&backup.manifest)?);
            Ok(true)
        }
        Cmd::Restore {
            file,
            data_dir,
            bootstrap_node_id,
            raft_addr,
            client_addr,
            admin_addr,
        } => {
            anyhow::ensure!(*bootstrap_node_id > 0, "node id must be positive");
            anyhow::ensure!(
                !data_dir.exists() || std::fs::read_dir(data_dir)?.next().is_none(),
                "restore requires an empty offline data directory"
            );
            let (backup, mut package) = Backup::decode(&std::fs::read(file)?)?;
            kv_state_machine::KvStateMachine::validate_snapshot(&package.state, package.index)?;
            let staging = data_dir.with_extension(format!("restore-{}", std::process::id()));
            anyhow::ensure!(
                !staging.exists(),
                "restore staging directory already exists"
            );
            std::fs::create_dir_all(&staging)?;
            let result = (|| -> Result<()> {
                let sm = kv_state_machine::KvStateMachine::open(staging.join("kv"))?;
                sm.restore(&package.state, package.index)?;
                anyhow::ensure!(
                    hex::encode(sm.state_hash()) == backup.manifest.state_hash
                        && sm.len() as u64 == backup.manifest.key_count,
                    "backup state metadata mismatch"
                );
                drop(sm);
                package.config = raft_core::config::ClusterConfig::stable(vec![*bootstrap_node_id]);
                package.config.members.insert(
                    *bootstrap_node_id,
                    raft_core::config::Member {
                        id: *bootstrap_node_id,
                        raft_addr: raft_addr.clone(),
                        client_addr: client_addr.clone(),
                        admin_addr: admin_addr.clone(),
                        learner: false,
                        certificate_sha256: None,
                    },
                );
                let snapshots = SnapshotStore::open(staging.join("snapshots"))?;
                let mut writer = snapshots.begin_write(&SnapshotMeta {
                    last_included_index: package.index,
                    last_included_term: package.term,
                    config: bincode::serialize(&package.config)?,
                })?;
                writer.write_chunk(&bincode::serialize(&package)?)?;
                writer.finish()?;
                drop(snapshots);
                let meta = MetaStore::open(staging.join("meta"))?;
                meta.save_hard_state(&raft_core::HardState {
                    current_term: package.term,
                    voted_for: None,
                    commit_index: package.index,
                })?;
                meta.save_snapshot_pointer(package.index, package.term)?;
                drop(meta);
                Ok(())
            })();
            if let Err(error) = result {
                let _ = std::fs::remove_dir_all(&staging);
                return Err(error);
            }
            if data_dir.exists() {
                std::fs::remove_dir(data_dir)?;
            }
            std::fs::rename(&staging, data_dir)?;
            std::fs::File::open(data_dir.parent().unwrap_or(std::path::Path::new(".")))?
                .sync_all()?;
            println!("Restored new single-voter cluster node {} at {}. Start with the same node ID/endpoints; add fresh learners afterward.",bootstrap_node_id,data_dir.display());
            Ok(true)
        }
        _ => Ok(false),
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    if maintenance(&cli).await? {
        return Ok(());
    }
    let mut sock = TcpStream::connect(&cli.addr)
        .await
        .with_context(|| format!("connecting to {}", cli.addr))?;
    sock.set_nodelay(true).ok();

    if let Some(password) = &cli.password {
        sock.write_all(&resp_array(&["AUTH", password])).await?;
        anyhow::ensure!(
            read_reply(&mut sock).await? == resp_server::codec::RespFrame::Simple("OK".into()),
            "AUTH failed"
        );
    }
    let bytes = match cli.cmd {
        Cmd::Backup { .. } | Cmd::Restore { .. } => unreachable!("maintenance already handled"),
        Cmd::Status => resp_array(&["INFO", "raft"]),
        Cmd::Members => resp_array(&["CLUSTER", "NODES"]),
        Cmd::Set { key, value } => resp_array(&["SET", &key, &value]),
        Cmd::Get { key } => resp_array(&["GET", &key]),
        Cmd::Del { keys } => {
            let mut v = vec!["DEL".to_string()];
            v.extend(keys);
            let s: Vec<&str> = v.iter().map(|s| s.as_str()).collect();
            resp_array(&s)
        }
        Cmd::Ping => resp_array(&["PING"]),
    };
    sock.write_all(&bytes).await?;
    let frame = read_reply(&mut sock).await?;
    let mut buffer = bytes::BytesMut::new();
    tokio_util::codec::Encoder::encode(&mut resp_server::codec::RespCodec, frame, &mut buffer)?;
    std::io::Write::write_all(&mut std::io::stdout(), &buffer)?;
    Ok(())
}

async fn read_reply(sock: &mut TcpStream) -> Result<resp_server::codec::RespFrame> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut buffer = bytes::BytesMut::new();
        loop {
            if let Some(frame) =
                tokio_util::codec::Decoder::decode(&mut resp_server::codec::RespCodec, &mut buffer)?
            {
                return Ok(frame);
            }
            anyhow::ensure!(
                sock.read_buf(&mut buffer).await? > 0,
                "disconnected before complete RESP reply"
            );
        }
    })
    .await?
}
fn resp_array(parts: &[&str]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p.as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    out
}
