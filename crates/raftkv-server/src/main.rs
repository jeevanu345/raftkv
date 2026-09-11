//! `raftkv-server` binary entry point. Wires together:
//!  * Configuration loading (TOML + CLI override)
//!  * Tracing (env-filtered)
//!  * The runtime (raft core driver)
//!  * gRPC peer server (raft-net)
//!  * RESP client server (resp-server)
//!  * Prometheus metrics endpoint
//!
//! On SIGINT/SIGTERM we cancel the listeners and let the runtime drain.

#![deny(unsafe_code)]

use serde_json::json;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod config;
mod runtime;

use config::ServerConfig;
use runtime::{ClientHandler, Runtime};

#[derive(Debug, Parser)]
#[command(name = "raftkv-server", version)]
struct Cli {
    /// Path to TOML config.
    #[arg(long, env = "RAFTKV_CONFIG")]
    config: Option<PathBuf>,
    /// Override node id.
    #[arg(long, env = "RAFTKV_ID")]
    id: Option<u64>,
    /// Override raft listen address.
    #[arg(long, env = "RAFTKV_RAFT_LISTEN")]
    raft_listen: Option<String>,
    /// Override client (RESP) listen address.
    #[arg(long, env = "RAFTKV_CLIENT_LISTEN")]
    client_listen: Option<String>,
    /// Override metrics listen address.
    #[arg(long, env = "RAFTKV_METRICS_LISTEN")]
    metrics_listen: Option<String>,
    /// Override dashboard/API listen address (localhost by default).
    #[arg(long, env = "RAFTKV_UI_LISTEN")]
    ui_listen: Option<String>,
    /// Override data dir.
    #[arg(long, env = "RAFTKV_DATA_DIR")]
    data_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let mut cfg = match cli.config {
        Some(p) => ServerConfig::from_toml_file(&p)?,
        None => ServerConfig::default(),
    };
    if let Some(v) = cli.id {
        cfg.id = v;
    }
    if let Some(v) = cli.raft_listen {
        cfg.raft_listen = v;
    }
    if let Some(v) = cli.client_listen {
        cfg.client_listen = v;
    }
    if let Some(v) = cli.metrics_listen {
        cfg.metrics_listen = v;
    }
    if let Some(v) = cli.ui_listen {
        cfg.ui_listen = v;
    }
    if let Some(v) = cli.data_dir {
        cfg.data_dir = v;
    }

    tracing::info!(node = cfg.id, raft = %cfg.raft_listen, client = %cfg.client_listen, "starting raftkv-server");

    let raft_addr: std::net::SocketAddr = strip_scheme(&cfg.raft_listen).parse()?;
    let client_addr = cfg.client_listen.clone();
    let metrics_addr: std::net::SocketAddr = strip_scheme(&cfg.metrics_listen).parse()?;
    let ui_addr: std::net::SocketAddr = strip_scheme(&cfg.ui_listen).parse()?;

    let (rt, handles) = Runtime::new(cfg).await?;
    let local_id = rt.local_id();

    rt.clone().spawn(handles);

    let processor: Arc<dyn raft_net::MessageProcessor> = rt.clone();
    let raft_server = raft_net::server::RaftServer::new(local_id, processor);
    let raft_grpc = tokio::spawn(async move {
        let svc = raft_net::pb::raft::raft_server::RaftServer::new(raft_server);
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(svc)
            .serve(raft_addr)
            .await
        {
            tracing::error!(error = %e, "gRPC server exited");
        }
    });

    let handler: Arc<ClientHandler> = Arc::new(ClientHandler::new(rt.clone()));
    let client_handler = handler.clone();
    let resp_task = tokio::spawn(async move {
        if let Err(e) = resp_server::server::serve(&client_addr, client_handler).await {
            tracing::error!(error = %e, "RESP server exited");
        }
    });

    let metrics_task = tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr).await {
            tracing::warn!(error = %e, "metrics server exited");
        }
    });
    let ui_handler = handler.clone();
    let ui_task = tokio::spawn(async move {
        if let Err(e) = serve_ui(ui_addr, ui_handler).await {
            tracing::warn!(error = %e, "dashboard API exited");
        }
    });

    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutdown signal received");
    raft_grpc.abort();
    resp_task.abort();
    metrics_task.abort();
    ui_task.abort();
    Ok(())
}

/// Local-only JSON API for the Vite dashboard.
async fn serve_ui(addr: std::net::SocketAddr, handler: Arc<ClientHandler>) -> std::io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "dashboard API listening");
    loop {
        let (mut sock, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 128 * 1024];
            let n = match sock.read(&mut buf).await {
                Ok(n) => n,
                Err(_) => return,
            };
            let request = String::from_utf8_lossy(&buf[..n]);
            let mut parts = request
                .lines()
                .next()
                .unwrap_or_default()
                .split_whitespace();
            let method = parts.next().unwrap_or_default();
            let path = parts.next().unwrap_or_default();
            let body = request.split("\r\n\r\n").nth(1).unwrap_or_default();
            let result = match (method, path) {
                ("GET", "/api/status") => Ok(handler.status_json()),
                ("GET", "/api/keys") => handler.keys_json().await,
                ("GET", p) if p.starts_with("/api/keys/") => handler.get_json(&p[10..]).await,
                ("PUT", p) if p.starts_with("/api/keys/") => handler.set_json(&p[10..], body).await,
                ("DELETE", p) if p.starts_with("/api/keys/") => handler.delete_json(&p[10..]).await,
                ("OPTIONS", _) => Ok(json!({"ok": true})),
                _ => Err((404, "not found".to_string())),
            };
            let (status, payload) = match result {
                Ok(value) => (200, value),
                Err((status, message)) => (status, json!({"error": message})),
            };
            let encoded = payload.to_string();
            let response = format!("HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: http://127.0.0.1:5173\r\nAccess-Control-Allow-Methods: GET, PUT, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: content-type\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", status, encoded.len(), encoded);
            let _ = sock.write_all(response.as_bytes()).await;
        });
    }
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,raftkv=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_level(true)
        .init();
}

fn strip_scheme(s: &str) -> &str {
    s.strip_prefix("http://")
        .or_else(|| s.strip_prefix("https://"))
        .unwrap_or(s)
}

/// Minimal Prometheus text-format endpoint over plain TCP.
async fn serve_metrics(addr: std::net::SocketAddr) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(%addr, "metrics endpoint listening");
    loop {
        let (mut sock, _) = listener.accept().await?;
        tokio::spawn(async move {
            let body = match prometheus::TextEncoder::new().encode_to_string(&prometheus::gather())
            {
                Ok(s) => s,
                Err(_) => String::new(),
            };
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\n\r\n{}",
                body.len(), body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
    }
}
