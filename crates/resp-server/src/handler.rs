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
    where F: FnOnce(&KvStateMachine) -> RespFrame + Send + 'static;
    /// Whether this node is currently the leader.
    fn is_leader(&self) -> bool;
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
    now_ms: u64,
) -> RespFrame {
    match cmd {
        ClientCommand::Ping(arg) => match arg {
            Some(b) => RespFrame::Bulk(Some(b)),
            None => RespFrame::Simple("PONG".into()),
        },
        ClientCommand::Echo(b) => RespFrame::Bulk(Some(b)),
        ClientCommand::Quit => RespFrame::ok(),
        ClientCommand::Select(_) => RespFrame::ok(),
        ClientCommand::Command => RespFrame::Array(vec![]),
        ClientCommand::ClientNoop => RespFrame::ok(),
        ClientCommand::Info(section) => RespFrame::Bulk(Some(handler.info(section.as_deref()).into_bytes())),
        ClientCommand::ClusterNodes => RespFrame::Bulk(Some(handler.cluster_nodes().into_bytes())),
        ClientCommand::ClusterInfo => RespFrame::Bulk(Some(b"cluster_enabled:1\r\ncluster_state:ok\r\n".to_vec())),
        ClientCommand::DbSize => match handler.linearizable_read(|sm| RespFrame::int(sm.len() as i64)).await {
            Ok(f) => f,
            Err(_) => RespFrame::err("ERR not leader or timeout"),
        },
        ClientCommand::Get(key) => {
            match handler.linearizable_read(move |sm| match sm.get(&key) {
                Ok(Some(v)) => RespFrame::Bulk(Some(v)),
                Ok(None) => RespFrame::nil(),
                Err(e) => RespFrame::err(format!("ERR {e}")),
            }).await {
                Ok(f) => f,
                Err(ProposeFailure::NotLeader { hint }) => RespFrame::err(format!("MOVED 0 {}", hint.unwrap_or_default())),
                Err(_) => RespFrame::err("ERR timeout"),
            }
        }
        ClientCommand::MGet(keys) => {
            match handler.linearizable_read(move |sm| match sm.mget(&keys) {
                Ok(vs) => RespFrame::Array(vs.into_iter().map(RespFrame::Bulk).collect()),
                Err(e) => RespFrame::err(format!("ERR {e}")),
            }).await {
                Ok(f) => f,
                Err(_) => RespFrame::err("ERR timeout"),
            }
        }
        ClientCommand::Exists(keys) => {
            match handler.linearizable_read(move |sm| {
                let mut n = 0i64;
                for k in &keys {
                    if sm.exists(k).unwrap_or(false) { n += 1; }
                }
                RespFrame::int(n)
            }).await {
                Ok(f) => f,
                Err(_) => RespFrame::err("ERR timeout"),
            }
        }
        ClientCommand::Set { key, value, ex_seconds } => {
            let expire_at_ms = ex_seconds.map(|s| now_ms + s * 1000);
            match handler.propose(Command::Set { key, value, expire_at_ms }).await {
                Ok(r) => response_to_frame(r),
                Err(ProposeFailure::NotLeader { hint }) => RespFrame::err(format!("MOVED 0 {}", hint.unwrap_or_default())),
                Err(e) => RespFrame::err(format!("ERR {e}")),
            }
        }
        ClientCommand::Del(keys) => match handler.propose(Command::Del { keys }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => RespFrame::err(format!("ERR {e}")),
        },
        ClientCommand::Incr(key) => match handler.propose(Command::Incr { key, delta: 1 }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => RespFrame::err(format!("ERR {e}")),
        },
        ClientCommand::Decr(key) => match handler.propose(Command::Incr { key, delta: -1 }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => RespFrame::err(format!("ERR {e}")),
        },
        ClientCommand::MSet(pairs) => match handler.propose(Command::MSet { pairs }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => RespFrame::err(format!("ERR {e}")),
        },
        ClientCommand::Expire { key, seconds } => {
            let expire_at_ms = now_ms + seconds * 1000;
            match handler.propose(Command::Expire { key, expire_at_ms }).await {
                Ok(r) => response_to_frame(r),
                Err(e) => RespFrame::err(format!("ERR {e}")),
            }
        }
        ClientCommand::Ttl(_key) => RespFrame::int(-1),
        ClientCommand::Persist(key) => match handler.propose(Command::Persist { key }).await {
            Ok(r) => response_to_frame(r),
            Err(e) => RespFrame::err(format!("ERR {e}")),
        },
        ClientCommand::FlushDb => match handler.propose(Command::FlushDb).await {
            Ok(r) => response_to_frame(r),
            Err(e) => RespFrame::err(format!("ERR {e}")),
        },
        ClientCommand::Unknown(c) => RespFrame::err(format!("ERR unknown command '{}'", c)),
    }
}
