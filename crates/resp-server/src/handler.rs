//! Translate parsed RESP commands into either:
//!  * Local read against the state machine (gated by linearizable read-index
//!    in the runtime), or
//!  * A `Command` proposed to Raft for replication.

use std::sync::Arc;

use async_trait::async_trait;
use kv_state_machine::{Command, KvStateMachine, Response};

use crate::codec::RespFrame;
use crate::commands::ClientCommand;

/// Trait implemented by the runtime: handles proposing to Raft and waiting for
/// the apply notification.
#[async_trait]
pub trait CommandHandler: Send + Sync + 'static {
    /// Propose a command to Raft and await its application; return the
    /// state-machine response.
    async fn propose(&self, cmd: Command) -> Result<Response, ProposeFailure>;
    /// Read-index → wait until applied >= read_index → execute closure on the
    /// state machine.
    async fn linearizable_read<F>(&self, f: F) -> Result<RespFrame, ProposeFailure>
    where
        F: FnOnce(&KvStateMachine) -> RespFrame + Send + 'static;
    /// Whether this node is currently the leader.
    fn is_leader(&self) -> bool;
    /// Leader-issued monotonically increasing wall timestamp.
    fn clock_ms(&self) -> u64;
    /// Build the INFO string.
    fn info(&self, section: Option<&str>) -> String;
    /// Build CLUSTER NODES output.
    fn cluster_nodes(&self) -> String;
}

/// Failures the handler may return.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ProposeFailure {
    /// Not leader; client is given a hint.
    #[error("not leader")]
    NotLeader {
        /// Optional leader hint string.
        hint: Option<String>,
    },
    /// Timed out waiting for commit/apply.
    #[error("timeout")]
    Timeout,
    /// Generic error.
    #[error("{0}")]
    Other(String),
}

/// Glue: convert a command into a frame.
pub fn response_to_frame(r: Response) -> RespFrame {
    match r {
        Response::Ok => RespFrame::ok(),
        Response::Int(n) => RespFrame::int(n),
        Response::Bulk(b) => RespFrame::Bulk(b),
        Response::Array(a) => RespFrame::Array(a.into_iter().map(RespFrame::Bulk).collect()),
        Response::Error(e) => RespFrame::err(e),
    }
}

/// Top-level dispatch.
pub async fn dispatch<H: CommandHandler + ?Sized>(
    handler: Arc<H>,
    cmd: ClientCommand,
    _now_ms: u64,
) -> RespFrame {
    let now_ms = handler.clock_ms();
    let milliseconds = matches!(&cmd, ClientCommand::PTtl(_));
    match cmd {
        ClientCommand::Ping(arg) => match arg {
            Some(b) => RespFrame::Bulk(Some(b)),
            None => RespFrame::Simple("PONG".into()),
        },
        ClientCommand::Echo(b) => RespFrame::Bulk(Some(b)),
        ClientCommand::Quit => RespFrame::ok(),
        ClientCommand::Select(0) => RespFrame::ok(),
        ClientCommand::Select(_) => RespFrame::err("ERR only database 0 is supported"),
        ClientCommand::Command => RespFrame::Array(vec![]),
        ClientCommand::ClientNoop => RespFrame::ok(),
        ClientCommand::Info(section) => {
            RespFrame::Bulk(Some(handler.info(section.as_deref()).into_bytes()))
        }
        ClientCommand::ClusterNodes => RespFrame::Bulk(Some(handler.cluster_nodes().into_bytes())),
        ClientCommand::ClusterInfo => {
            RespFrame::Bulk(Some(b"cluster_enabled:1\r\ncluster_state:ok\r\n".to_vec()))
        }
        ClientCommand::DbSize => match handler
            .linearizable_read(|sm| RespFrame::int(sm.len() as i64))
            .await
        {
            Ok(f) => f,
            Err(e) => failure_frame(e),
        },
        ClientCommand::Get(key) => {
            match handler
                .linearizable_read(move |sm| match sm.get(&key) {
                    Ok(Some(v)) => RespFrame::Bulk(Some(v)),
                    Ok(None) => RespFrame::nil(),
                    Err(e) => RespFrame::err(format!("ERR {e}")),
                })
                .await
            {
                Ok(f) => f,
                Err(ProposeFailure::NotLeader { hint }) => {
                    RespFrame::err(format!("MOVED 0 {}", hint.unwrap_or_default()))
                }
                Err(e) => failure_frame(e),
            }
        }
        ClientCommand::MGet(keys) => {
            match handler
                .linearizable_read(move |sm| match sm.mget(&keys) {
                    Ok(vs) => RespFrame::Array(vs.into_iter().map(RespFrame::Bulk).collect()),
                    Err(e) => RespFrame::err(format!("ERR {e}")),
                })
                .await
            {
                Ok(f) => f,
                Err(e) => failure_frame(e),
            }
        }
        ClientCommand::Exists(keys) => {
            match handler
                .linearizable_read(move |sm| {
                    let mut n = 0i64;
                    for k in &keys {
                        if sm.exists(k).unwrap_or(false) {
                            n += 1;
                        }
                    }
                    RespFrame::int(n)
                })
                .await
            {
                Ok(f) => f,
                Err(e) => failure_frame(e),
            }
        }
        ClientCommand::Set { key, value, ttl_ms } => {
            let expire_at_ms = ttl_ms.map(|s| now_ms.saturating_add(s));
            match handler
                .propose(Command::Set {
                    key,
                    value,
                    expire_at_ms,
                })
                .await
            {
                Ok(r) => response_to_frame(r),
                Err(ProposeFailure::NotLeader { hint }) => {
                    RespFrame::err(format!("MOVED 0 {}", hint.unwrap_or_default()))
                }
                Err(e) => failure_frame(e),
            }
        }
        ClientCommand::Del(keys) => match handler.propose(Command::Del { keys }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => failure_frame(e),
        },
        ClientCommand::Incr(key) => match handler.propose(Command::Incr { key, delta: 1 }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => failure_frame(e),
        },
        ClientCommand::Decr(key) => match handler.propose(Command::Incr { key, delta: -1 }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => failure_frame(e),
        },
        ClientCommand::MSet(pairs) => match handler.propose(Command::MSet { pairs }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => failure_frame(e),
        },
        ClientCommand::Expire { key, seconds } => {
            let expire_at_ms = now_ms.saturating_add(seconds.saturating_mul(1000));
            match handler.propose(Command::Expire { key, expire_at_ms }).await {
                Ok(r) => response_to_frame(r),
                Err(e) => failure_frame(e),
            }
        }
        ClientCommand::Ttl(key) | ClientCommand::PTtl(key) => {
            match handler
                .linearizable_read(move |sm| match sm.ttl_ms(&key) {
                    Ok(ttl) => RespFrame::int(if milliseconds || ttl < 0 {
                        ttl
                    } else {
                        ttl / 1000
                    }),
                    Err(e) => RespFrame::err(format!("ERR {e}")),
                })
                .await
            {
                Ok(frame) => frame,
                Err(e) => failure_frame(e),
            }
        }
        ClientCommand::Scan {
            cursor,
            pattern,
            count,
        } => match handler
            .linearizable_read(move |sm| match sm.scan(&cursor, count, &pattern) {
                Ok((next, keys)) => RespFrame::Array(vec![
                    RespFrame::bulk(next.into_bytes()),
                    RespFrame::Array(keys.into_iter().map(RespFrame::bulk).collect()),
                ]),
                Err(e) => RespFrame::err(format!("ERR {e}")),
            })
            .await
        {
            Ok(frame) => frame,
            Err(e) => failure_frame(e),
        },
        ClientCommand::Persist(key) => match handler.propose(Command::Persist { key }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => failure_frame(e),
        },
        ClientCommand::FlushDb => match handler.propose(Command::FlushDb).await {
            Ok(r) => response_to_frame(r),
            Err(e) => failure_frame(e),
        },
        ClientCommand::Unknown(c) => RespFrame::err(format!("ERR unknown command '{}'", c)),
    }
}

fn failure_frame(error: ProposeFailure) -> RespFrame {
    match error {
        ProposeFailure::NotLeader { hint } => {
            RespFrame::err(format!("MOVED 0 {}", hint.unwrap_or_default()))
        }
        error => RespFrame::err(format!("ERR {error}")),
    }
}
