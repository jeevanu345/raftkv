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
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let mut sock = TcpStream::connect(&cli.addr).await
        .with_context(|| format!("connecting to {}", cli.addr))?;
    sock.set_nodelay(true).ok();

    let bytes = match cli.cmd {
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
    let mut buf = vec![0u8; 64 * 1024];
    let n = tokio::time::timeout(Duration::from_secs(5), sock.read(&mut buf)).await??;
    print!("{}", String::from_utf8_lossy(&buf[..n]));
    Ok(())
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
