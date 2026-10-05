//! RESP TCP server.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use crate::codec::{RespCodec, RespFrame};
use crate::commands::parse;
use crate::handler::{dispatch, CommandHandler};

/// Run a RESP server on `addr`. Each connection is handled by a spawned task.
pub async fn serve<H: CommandHandler + 'static>(
    addr: &str,
    handler: Arc<H>,
) -> std::io::Result<()> {
    serve_authenticated(addr, handler, None).await
}

/// RESP server with optional per-connection AUTH gating.
pub async fn serve_authenticated<H: CommandHandler + 'static>(
    addr: &str,
    handler: Arc<H>,
    token: Option<String>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = addr, "RESP server listening");
    loop {
        let (sock, _peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::warn!(error = %e, "accept failed");
                continue;
            }
        };
        let handler = handler.clone();
        let token = token.clone();
        tokio::spawn(async move {
            sock.set_nodelay(true).ok();
            serve_connection(sock, handler, token, vec![]).await;
        });
    }
}

/// ACL identity. Key prefixes are byte prefixes; empty permits all keys.
#[derive(Clone, serde::Serialize, serde::Deserialize, Debug)]
pub struct AccessUser {
    pub name: String,
    pub password: String,
    pub read: bool,
    pub write: bool,
    #[serde(default)]
    pub key_prefixes: Vec<String>,
}

/// Serve one accepted TCP or TLS connection, with AUTH and key ACLs.
pub async fn serve_connection<S, H>(
    sock: S,
    handler: Arc<H>,
    token: Option<String>,
    users: Vec<AccessUser>,
) where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    H: CommandHandler + 'static,
{
    let mut framed = Framed::new(sock, RespCodec);
    let mut authenticated = token.is_none() && users.is_empty();
    let mut principal: Option<AccessUser> = None;
    while let Some(Ok(frame)) = framed.next().await {
        if let RespFrame::Array(parts) = &frame {
            if parts.first().is_some_and(
                |p| matches!(p,RespFrame::Bulk(Some(b)) if b.eq_ignore_ascii_case(b"AUTH")),
            ) {
                let bulk = |i: usize| {
                    parts.get(i).and_then(|p| match p {
                        RespFrame::Bulk(Some(b)) => Some(b.as_slice()),
                        _ => None,
                    })
                };
                principal = None;
                authenticated = if parts.len() == 2 {
                    token
                        .as_ref()
                        .is_some_and(|t| bulk(1) == Some(t.as_bytes()))
                } else if parts.len() == 3 {
                    principal = users
                        .iter()
                        .find(|u| {
                            Some(u.name.as_bytes()) == bulk(1)
                                && Some(u.password.as_bytes()) == bulk(2)
                        })
                        .cloned();
                    principal.is_some()
                } else {
                    false
                };
                if framed
                    .send(if authenticated {
                        RespFrame::ok()
                    } else {
                        RespFrame::err("WRONGPASS invalid credentials")
                    })
                    .await
                    .is_err()
                {
                    break;
                }
                continue;
            }
        }
        let cmd = parse(&frame);
        let response = if !authenticated {
            RespFrame::err("NOAUTH authentication required")
        } else if principal.as_ref().is_some_and(|u| !allowed(u, &cmd)) {
            RespFrame::err("NOPERM command or key not allowed")
        } else {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            dispatch(handler.clone(), cmd, now_ms).await
        };
        if framed.send(response).await.is_err() {
            break;
        }
    }
}
fn allowed(user: &AccessUser, cmd: &crate::commands::ClientCommand) -> bool {
    use crate::commands::ClientCommand::*;
    let (write, keys, global) = match cmd {
        Get(k) | Ttl(k) | PTtl(k) => (false, vec![k.as_slice()], false),
        Exists(ks) | MGet(ks) => (false, ks.iter().map(Vec::as_slice).collect(), false),
        Set { key, .. } | Expire { key, .. } | Incr(key) | Decr(key) | Persist(key) => {
            (true, vec![key.as_slice()], false)
        }
        Del(ks) => (true, ks.iter().map(Vec::as_slice).collect(), false),
        MSet(pairs) => (
            true,
            pairs.iter().map(|(k, _)| k.as_slice()).collect(),
            false,
        ),
        FlushDb => (true, vec![], true),
        Scan { .. } | DbSize => (false, vec![], true),
        _ => (false, vec![], false),
    };
    (if write { user.write } else { user.read })
        && (!global || user.key_prefixes.is_empty())
        && keys.iter().all(|key| {
            user.key_prefixes.is_empty()
                || user
                    .key_prefixes
                    .iter()
                    .any(|p| key.starts_with(p.as_bytes()))
        })
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn acl_covers_multi_key_and_global() {
        let u = AccessUser {
            name: "reader".into(),
            password: "secret".into(),
            read: true,
            write: false,
            key_prefixes: vec!["tenant:".into()],
        };
        assert!(allowed(
            &u,
            &crate::commands::ClientCommand::Get(b"tenant:one".to_vec())
        ));
        assert!(!allowed(
            &u,
            &crate::commands::ClientCommand::MGet(vec![b"tenant:one".to_vec(), b"other".to_vec()])
        ));
        assert!(!allowed(&u, &crate::commands::ClientCommand::FlushDb));
        assert!(!allowed(
            &u,
            &crate::commands::ClientCommand::Scan {
                cursor: "0".into(),
                pattern: "*".into(),
                count: 10
            }
        ));
    }
}
