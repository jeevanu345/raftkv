//! Outbound peer client.
//!
//! Each Raft RPC has exactly one synchronous reply. The peer client sends the
//! outbound request and feeds the response back into the local inbox so the
//! core can step on it. `*Response` variants in the message stream are no-ops
//! at this layer (they're already on the wire by the time we see them).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, Semaphore};
use tonic::transport::{Channel, Endpoint};

use crate::convert::{
    append_request_to_pb, append_response_from_pb, snapshot_to_pb, vote_request_to_pb,
    vote_response_from_pb,
};
use crate::pb::raft as pb;
use crate::pb::raft::raft_client::RaftClient;
use crate::InboxTx;
use raft_core::{Message, NodeId};

/// One-per-peer client that handles connection lifecycle and message dispatch.
#[derive(Clone)]
pub struct PeerClient {
    /// Peer id (informational).
    pub peer_id: NodeId,
    /// gRPC endpoint URL, e.g. `http://node2:7000`.
    pub endpoint: String,
    /// Inbox to deliver synchronous Raft responses into.
    pub inbox: InboxTx,
    inner: Arc<Mutex<Option<RaftClient<Channel>>>>,
    admission: Arc<Semaphore>,
}

const MAX_CONCURRENT_REQUESTS: usize = 32;

impl PeerClient {
    /// Create a client without connecting yet.
    pub fn new(peer_id: NodeId, endpoint: String, inbox: InboxTx) -> Self {
        Self {
            peer_id,
            endpoint,
            inbox,
            inner: Arc::new(Mutex::new(None)),
            admission: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
        }
    }

    async fn connect(&self) -> Result<RaftClient<Channel>, tonic::transport::Error> {
        let ep: Endpoint = Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(Duration::from_millis(500))
            .timeout(Duration::from_secs(2));
        let chan = ep.connect().await?;
        Ok(RaftClient::new(chan))
    }

    async fn ensure(&self) -> Option<RaftClient<Channel>> {
        let mut g = self.inner.lock().await;
        if g.is_none() {
            match self.connect().await {
                Ok(c) => *g = Some(c),
                Err(e) => {
                    tracing::debug!(peer = self.peer_id, error = %e, "peer connect failed");
                    return None;
                }
            }
        }
        g.clone()
    }

    /// Send a single Raft message to the peer; deliver any synchronous
    /// response into the local inbox.
    pub async fn send(&self, msg: Message) {
        // Drop excess attempts instead of accumulating suspended RPC tasks.
        // Raft retries through its regular heartbeat/replication mechanism.
        let Ok(_permit) = self.admission.try_acquire() else {
            tracing::debug!(peer = self.peer_id, "peer transport at concurrency limit");
            return;
        };
        let Some(mut client) = self.ensure().await else {
            return;
        };
        let dropped = match &msg {
            Message::AppendEntries { .. } => {
                let Some(req) = append_request_to_pb(&msg) else {
                    return;
                };
                match client.append_entries(req).await {
                    Ok(resp) => {
                        let resp_msg = append_response_from_pb(resp.into_inner(), self.peer_id);
                        let _ = self.inbox.send(resp_msg);
                        false
                    }
                    Err(e) => {
                        tracing::debug!(peer = self.peer_id, error = %e, "AE rpc failed");
                        true
                    }
                }
            }
            Message::RequestVote { pre_vote, .. } => {
                let Some(req) = vote_request_to_pb(&msg) else {
                    return;
                };
                let pre = *pre_vote;
                let r = if pre {
                    client.pre_vote(req).await
                } else {
                    client.request_vote(req).await
                };
                match r {
                    Ok(resp) => {
                        let resp_msg = vote_response_from_pb(resp.into_inner(), self.peer_id, pre);
                        let _ = self.inbox.send(resp_msg);
                        false
                    }
                    Err(e) => {
                        tracing::debug!(peer = self.peer_id, error = %e, "vote rpc failed");
                        true
                    }
                }
            }
            Message::AppendEntriesResponse { .. }
            | Message::RequestVoteResponse { .. }
            | Message::InstallSnapshotResponse { .. } => {
                // These responses already flowed back over the inbound RPC's
                // synchronous reply path. Nothing to do here.
                false
            }
            Message::InstallSnapshot { .. } => {
                let Some(chunk) = snapshot_to_pb(&msg) else {
                    return;
                };
                let stream = futures::stream::iter(vec![chunk]);
                match client.install_snapshot(stream).await {
                    Ok(_) => false,
                    Err(e) => {
                        tracing::debug!(peer = self.peer_id, error = %e, "snapshot rpc failed");
                        true
                    }
                }
            }
            Message::TimeoutNow { from, term } => {
                let req = pb::TimeoutNowRequest {
                    term: *term,
                    leader_id: *from,
                };
                match client.timeout_now(req).await {
                    Ok(_) => false,
                    Err(e) => {
                        tracing::debug!(peer = self.peer_id, error = %e, "timeout_now rpc failed");
                        true
                    }
                }
            }
        };
        if dropped {
            *self.inner.lock().await = None;
        }
    }

    /// Number of outbound tasks currently admitted for this peer.
    pub fn active_requests(&self) -> usize {
        MAX_CONCURRENT_REQUESTS - self.admission.available_permits()
    }

    /// Transport admission bound (independent of core progress accounting).
    pub const fn max_concurrent_requests(&self) -> usize {
        MAX_CONCURRENT_REQUESTS
    }
}
