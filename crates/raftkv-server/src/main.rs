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

    cfg.validate()?;

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

    let metrics_handler = handler.clone();
    let metrics_task = tokio::spawn(async move {
        if let Err(e) = serve_metrics(metrics_addr, metrics_handler).await {
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

const MAX_HTTP_HEADERS: usize = 16 * 1024;
const MAX_HTTP_BODY: usize = 128 * 1024;
const HTTP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

type ApiError = (u16, String);

#[derive(Debug)]
struct HttpRequest {
    method: String,
    path: String,
    body: String,
}

/// One bounded HTTP/1 request per connection; chunked transfer is unsupported.
async fn read_http_request<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<HttpRequest, ApiError> {
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    let header_end = loop {
        if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            if pos + 4 > MAX_HTTP_HEADERS {
                return Err((431, "request headers too large".into()));
            }
            break pos + 4;
        }
        if bytes.len() >= MAX_HTTP_HEADERS {
            return Err((431, "request headers too large".into()));
        }
        let mut chunk = [0u8; 4096];
        let n = reader
            .read(&mut chunk)
            .await
            .map_err(|e| (400, e.to_string()))?;
        if n == 0 {
            return Err((400, "incomplete request headers".into()));
        }
        bytes.extend_from_slice(&chunk[..n]);
    };
    let headers = std::str::from_utf8(&bytes[..header_end])
        .map_err(|_| (400, "headers must be UTF-8".into()))?;
    let mut lines = headers.split("\r\n");
    let mut start = lines.next().unwrap_or_default().split_whitespace();
    let method = start.next().unwrap_or_default().to_owned();
    let target = start.next().unwrap_or_default();
    let version = start.next().unwrap_or_default();
    if method.is_empty()
        || !target.starts_with('/')
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || start.next().is_some()
    {
        return Err((400, "invalid HTTP request line".into()));
    }
    let path = target.split('?').next().unwrap_or_default().to_owned();
    let mut content_length = None;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| (400, "invalid header".into()))?;
        if name.eq_ignore_ascii_case("transfer-encoding") {
            return Err((400, "transfer encoding is unsupported".into()));
        }
        if name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err((400, "duplicate content length".into()));
            }
            let size = value
                .trim()
                .parse::<usize>()
                .map_err(|_| (400, "invalid content length".into()))?;
            if size > MAX_HTTP_BODY {
                return Err((413, "request body too large".into()));
            }
            content_length = Some(size);
        }
    }
    if method == "PUT" && content_length.is_none() {
        return Err((411, "content length required".into()));
    }
    let length = content_length.unwrap_or(0);
    let end = header_end + length;
    if bytes.len() < end {
        let received = bytes.len();
        bytes.resize(end, 0);
        reader
            .read_exact(&mut bytes[received..])
            .await
            .map_err(|_| (400, "incomplete request body".into()))?;
    }
    let body = std::str::from_utf8(&bytes[header_end..end])
        .map_err(|_| (400, "body must be UTF-8".into()))?
        .to_owned();
    Ok(HttpRequest { method, path, body })
}

fn decode_key(encoded: &str) -> Result<String, ApiError> {
    let mut bytes = Vec::with_capacity(encoded.len());
    let mut it = encoded.bytes();
    while let Some(b) = it.next() {
        if b == b'%' {
            let hi = it.next().and_then(|b| (b as char).to_digit(16));
            let lo = it.next().and_then(|b| (b as char).to_digit(16));
            match (hi, lo) {
                (Some(hi), Some(lo)) => bytes.push((hi * 16 + lo) as u8),
                _ => return Err((400, "invalid percent escape in key".into())),
            }
        } else {
            bytes.push(b);
        }
    }
    if bytes.is_empty() {
        return Err((400, "key cannot be empty".into()));
    }
    String::from_utf8(bytes).map_err(|_| (400, "key must be UTF-8".into()))
}

async fn route_ui(
    request: HttpRequest,
    handler: &ClientHandler,
) -> Result<serde_json::Value, ApiError> {
    match (request.method.as_str(), request.path.as_str()) {
        ("GET", "/api/status") => Ok(handler.status_json()),
        ("GET", "/api/log") => handler.log_json(),
        ("GET", "/api/keys") => handler.keys_json().await,
        ("GET" | "PUT" | "DELETE", path) if path.starts_with("/api/keys/") => {
            let key = decode_key(&path[10..])?;
            match request.method.as_str() {
                "GET" => handler.get_json(&key).await,
                "PUT" => handler.set_json(&key, &request.body).await,
                _ => handler.delete_json(&key).await,
            }
        }
        ("OPTIONS", _) => Ok(json!({"ok": true})),
        (_, path)
            if path == "/api/status"
                || path == "/api/log"
                || path == "/api/keys"
                || path.starts_with("/api/keys/") =>
        {
            Err((405, "method not allowed".into()))
        }
        _ => Err((404, "not found".into())),
    }
}

fn http_response(result: Result<serde_json::Value, ApiError>) -> String {
    let (status, payload) = match result {
        Ok(value) => (200, value),
        Err((status, message)) => (status, json!({"error": message})),
    };
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        411 => "Length Required",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        503 => "Service Unavailable",
        _ => "Internal Server Error",
    };
    let encoded = payload.to_string();
    format!("HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: http://127.0.0.1:5173\r\nAccess-Control-Allow-Methods: GET, PUT, DELETE, OPTIONS\r\nAccess-Control-Allow-Headers: content-type\r\nCache-Control: no-store\r\nX-Content-Type-Options: nosniff\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{encoded}", encoded.len())
}

/// JSON API for the local operator dashboard.
async fn serve_ui(addr: std::net::SocketAddr, handler: Arc<ClientHandler>) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let capacity = Arc::new(tokio::sync::Semaphore::new(128));
    tracing::info!(%addr, "dashboard API listening");
    loop {
        let permit = capacity
            .clone()
            .acquire_owned()
            .await
            .expect("API semaphore remains open");
        let (mut sock, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_http_request(&mut sock)).await {
                    Ok(Ok(request)) => route_ui(request, &handler).await,
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err((408, "request read deadline exceeded".into())),
                };
            let response = http_response(result);
            let _ =
                tokio::time::timeout(HTTP_READ_TIMEOUT, sock.write_all(response.as_bytes())).await;
            let _ = sock.shutdown().await;
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

/// Prometheus metrics endpoint with live runtime gauges and counters.
async fn serve_metrics(
    addr: std::net::SocketAddr,
    handler: Arc<ClientHandler>,
) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let capacity = Arc::new(tokio::sync::Semaphore::new(32));
    tracing::info!(%addr, "metrics endpoint listening");
    loop {
        let permit = capacity
            .clone()
            .acquire_owned()
            .await
            .expect("metrics semaphore remains open");
        let (mut sock, _) = listener.accept().await?;
        let handler = handler.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let body = handler.metrics_text();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            );
            let _ =
                tokio::time::timeout(HTTP_READ_TIMEOUT, sock.write_all(response.as_bytes())).await;
            let _ = sock.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    #[tokio::test]
    async fn reads_fragmented_body_using_content_length() {
        let (mut writer, mut reader) = tokio::io::duplex(64);
        let write = tokio::spawn(async move {
            writer
                .write_all(b"PUT /api/keys/a%2Fb HTTP/1.1\r\nContent-Length: 15\r\n\r\n")
                .await
                .unwrap();
            tokio::task::yield_now().await;
            writer.write_all(br#"{"value":"yes"}"#).await.unwrap();
        });
        let request = read_http_request(&mut reader).await.unwrap();
        assert_eq!(request.body, r#"{"value":"yes"}"#);
        assert_eq!(decode_key(&request.path[10..]).unwrap(), "a/b");
        write.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_ambiguous_and_oversized_requests() {
        for (bytes, status) in [
            (
                b"PUT / HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx".as_slice(),
                400,
            ),
            (
                b"PUT / HTTP/1.1\r\nContent-Length: 131073\r\n\r\n".as_slice(),
                413,
            ),
            (b"PUT / HTTP/1.1\r\n\r\n".as_slice(), 411),
            (
                b"GET / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n".as_slice(),
                400,
            ),
        ] {
            let mut reader = bytes;
            assert_eq!(read_http_request(&mut reader).await.unwrap_err().0, status);
        }
        let oversized = format!(
            "GET / HTTP/1.1\r\nX: {}\r\n\r\n",
            "a".repeat(MAX_HTTP_HEADERS)
        );
        assert_eq!(
            read_http_request(&mut oversized.as_bytes())
                .await
                .unwrap_err()
                .0,
            431
        );
    }

    #[test]
    fn keys_decode_utf8_and_reserved_characters_without_form_rules() {
        assert_eq!(
            decode_key("a%2Fb%3Fc%23d+%20%E2%9C%93").unwrap(),
            "a/b?c#d+ ✓"
        );
        assert!(decode_key("%0").is_err());
        assert!(decode_key("%GG").is_err());
        assert!(decode_key("%FF").is_err());
        assert!(decode_key("").is_err());
        assert!(http_response(Err((404, "missing".into()))).starts_with("HTTP/1.1 404 Not Found"));
    }
}
