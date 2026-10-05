//! `raftkv-server` binary entry point. Wires together:
//!  * Configuration loading (TOML + CLI override)
//!  * Tracing (env-filtered)
//!  * The runtime (raft core driver)
//!  * gRPC peer server (raft-net)
//!  * RESP client server (resp-server)
//!  * Prometheus metrics endpoint
//!
//! On SIGINT/SIGTERM listeners are cancelled. Completed actions are durable;
//! outstanding requests may fail and must be retried after leader discovery.

#![deny(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use tracing_subscriber::EnvFilter;

mod admin;
mod config;
mod control;
mod diagnostics;
mod metrics;
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
    if let Ok(pod) = std::env::var("RAFTKV_POD_NAME") {
        cfg.id = pod
            .rsplit('-')
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid pod name"))?
            .parse::<u64>()?
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("pod ordinal overflow"))?;
    }
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

    if let Ok(token) = std::env::var("RAFTKV_ADMIN_TOKEN") {
        cfg.admin_token = Some(token);
    }
    if let Ok(token) = std::env::var("RAFTKV_CLIENT_TOKEN") {
        cfg.client_token = Some(token);
    }
    for tls in [&mut cfg.peer_tls, &mut cfg.client_tls, &mut cfg.admin_tls]
        .into_iter()
        .flatten()
    {
        tls.cert = PathBuf::from(
            tls.cert
                .to_string_lossy()
                .replace("{id}", &cfg.id.to_string()),
        );
        tls.key = PathBuf::from(
            tls.key
                .to_string_lossy()
                .replace("{id}", &cfg.id.to_string()),
        );
    }
    anyhow::ensure!(
        cfg.tick_ms > 0
            && cfg.heartbeat_ms >= cfg.tick_ms
            && cfg.election_timeout_ms > cfg.heartbeat_ms,
        "invalid Raft timing configuration"
    );
    anyhow::ensure!(
        cfg.id > 0 && cfg.peers.iter().any(|p| p.id == cfg.id),
        "local node must be present in bootstrap peers"
    );
    let ids: std::collections::BTreeSet<_> = cfg.peers.iter().map(|p| p.id).collect();
    anyhow::ensure!(ids.len() == cfg.peers.len(), "duplicate bootstrap peer id");
    for peer in &mut cfg.peers {
        if let Some(hash) = &mut peer.certificate_sha256 {
            hash.make_ascii_lowercase();
            anyhow::ensure!(
                hash.len() == 64 && hex::decode(hash.as_str()).is_ok(),
                "invalid peer certificate fingerprint"
            );
        }
    }
    tracing::info!(node = cfg.id, raft = %cfg.raft_listen, client = %cfg.client_listen, "starting raftkv-server");

    let raft_addr: std::net::SocketAddr = strip_scheme(&cfg.raft_listen).parse()?;
    let client_addr = cfg.client_listen.clone();
    let metrics_addr: std::net::SocketAddr = strip_scheme(&cfg.metrics_listen).parse()?;
    let ui_addr: std::net::SocketAddr = strip_scheme(&cfg.ui_listen).parse()?;

    let client_tls = cfg.client_tls.as_ref().map(tls_acceptor).transpose()?;
    let admin_tls = cfg.admin_tls.as_ref().map(tls_acceptor).transpose()?;
    let client_users = cfg.client_users.clone();
    let mut grpc_builder = tonic::transport::Server::builder();
    if let Some(tls) = &cfg.peer_tls {
        grpc_builder = grpc_builder.tls_config(
            tonic::transport::ServerTlsConfig::new()
                .identity(tonic::transport::Identity::from_pem(
                    std::fs::read(&tls.cert)?,
                    std::fs::read(&tls.key)?,
                ))
                .client_ca_root(tonic::transport::Certificate::from_pem(std::fs::read(
                    &tls.ca,
                )?)),
        )?;
    }
    // Bind every listener before starting the runtime; a missing/occupied port
    // must fail startup rather than leave a partially serving database.
    let grpc_listener = tokio::net::TcpListener::bind(raft_addr).await?;
    let resp_listener = tokio::net::TcpListener::bind(&client_addr).await?;
    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    let ui_listener = tokio::net::TcpListener::bind(ui_addr).await?;
    let peer_tls = cfg.peer_tls.clone();
    let peer_certificates = cfg
        .peers
        .iter()
        .filter_map(|p| p.certificate_sha256.clone().map(|hash| (p.id, hash)))
        .collect::<std::collections::BTreeMap<_, _>>();
    if peer_tls.is_some() {
        anyhow::ensure!(
            peer_certificates.len() == cfg.peers.len(),
            "every mTLS peer needs certificate_sha256"
        );
    }
    let (rt, handles) = Runtime::new(cfg).await?;
    let local_id = rt.local_id();

    rt.clone().spawn(handles);

    let processor: Arc<dyn raft_net::MessageProcessor> = rt.clone();
    let mut raft_server = raft_net::server::RaftServer::new(local_id, processor);
    if peer_tls.is_some() {
        raft_server = raft_server.with_peer_certificates(peer_certificates);
    }
    let admin_service = admin::Services(rt.clone());
    let membership_service = admin::Services(rt.clone());
    let raft_grpc = tokio::spawn(async move {
        let svc = raft_net::pb::raft::raft_server::RaftServer::new(raft_server);
        if let Err(e) = grpc_builder
            .add_service(svc)
            .add_service(raft_net::pb::admin::admin_server::AdminServer::new(
                admin_service,
            ))
            .add_service(
                raft_net::pb::membership::membership_server::MembershipServer::new(
                    membership_service,
                ),
            )
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(
                grpc_listener,
            ))
            .await
        {
            tracing::error!(error = %e, "gRPC server exited");
        }
    });

    let handler: Arc<ClientHandler> = Arc::new(ClientHandler::new(rt.clone()));
    let client_handler = handler.clone();
    let rt_client_token = rt.cfg.client_token.clone();
    let resp_task = tokio::spawn(async move {
        let limit = Arc::new(tokio::sync::Semaphore::new(512));
        loop {
            let Ok((socket, _)) = resp_listener.accept().await else {
                continue;
            };
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                continue;
            };
            let tls = client_tls.clone();
            let handler = client_handler.clone();
            let token = rt_client_token.clone();
            let users = client_users.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Some(tls) = tls {
                    if let Ok(Ok(stream)) =
                        tokio::time::timeout(std::time::Duration::from_secs(5), tls.accept(socket))
                            .await
                    {
                        resp_server::server::serve_connection(stream, handler, token, users).await;
                    }
                } else {
                    resp_server::server::serve_connection(socket, handler, token, users).await;
                }
            });
        }
    });

    let metrics_rt = rt.clone();
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = axum::serve(metrics_listener, control::metrics_router(metrics_rt)).await {
            tracing::warn!(error = %e, "metrics server exited");
        }
    });
    let ui_runtime = rt.clone();
    let ui_task = tokio::spawn(async move {
        if let Err(e) = serve_control(ui_listener, ui_runtime, admin_tls).await {
            tracing::warn!(error = %e, "dashboard API exited");
        }
    });

    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {_ =tokio::signal::ctrl_c()=>{},_=terminate.recv()=>{}}
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("shutdown signal received");
    raft_grpc.abort();
    resp_task.abort();
    metrics_task.abort();
    ui_task.abort();
    Ok(())
}

fn tls_acceptor(config: &config::TlsConfig) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    use std::io::BufReader;
    let certificates =
        rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(&config.cert)?))
            .collect::<Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(&config.key)?))?
        .ok_or_else(|| anyhow::anyhow!("TLS private key missing"))?;
    let mut tls = tokio_rustls::rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certificates, key)?;
    tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(tls)))
}
async fn serve_control(
    listener: tokio::net::TcpListener,
    rt: Arc<Runtime>,
    tls: Option<tokio_rustls::TlsAcceptor>,
) -> std::io::Result<()> {
    let router = control::router(rt);
    if let Some(tls) = tls {
        let limit = Arc::new(tokio::sync::Semaphore::new(512));
        loop {
            let (socket, _) = listener.accept().await?;
            let Ok(permit) = limit.clone().try_acquire_owned() else {
                continue;
            };
            let tls = tls.clone();
            let router = router.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Ok(Ok(stream)) =
                    tokio::time::timeout(std::time::Duration::from_secs(5), tls.accept(socket))
                        .await
                {
                    let io = hyper_util::rt::TokioIo::new(stream);
                    let service = hyper_util::service::TowerToHyperService::new(router);
                    let _ = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    )
                    .serve_connection_with_upgrades(io, service)
                    .await;
                }
            });
        }
    } else {
        axum::serve(listener, router).await
    }
}

fn init_tracing() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,raftkv=info"));
    let json = std::env::var("RAFTKV_LOG_FORMAT").is_ok_and(|v| v == "json");
    if json {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .json()
            .init();
        return;
    }
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
