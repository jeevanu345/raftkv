//! Outbound peer client.
//!
//! Each Raft RPC has exactly one synchronous reply. The peer client sends the
//! outbound request and feeds the response back into the local inbox so the
//! core can step on it. `*Response` variants in the message stream are no-ops
//! at this layer (they're already on the wire by the time we see them).

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
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
    tls: Option<tonic::transport::ClientTlsConfig>,
    stream: Arc<Mutex<Option<tokio::sync::mpsc::Sender<pb::Envelope>>>>,
}

impl PeerClient {
    /// Create a client without connecting yet.
    pub fn new(peer_id: NodeId, endpoint: String, inbox: InboxTx) -> Self {
        Self {
            peer_id,
            endpoint,
            inbox,
            inner: Arc::new(Mutex::new(None)),
            tls: None,
            stream: Arc::new(Mutex::new(None)),
        }
    }

    /// Configure mutually authenticated peer TLS.
    pub fn with_tls(mut self, tls: tonic::transport::ClientTlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    async fn connect(&self) -> Result<RaftClient<Channel>, tonic::transport::Error> {
        let mut ep: Endpoint = Endpoint::from_shared(self.endpoint.clone())?
            .connect_timeout(Duration::from_millis(500))
            .timeout(Duration::from_secs(2));
        if let Some(tls) = &self.tls {
            ep = ep.tls_config(tls.clone())?;
        }
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

    async fn stream_sender(&self) -> Option<tokio::sync::mpsc::Sender<pb::Envelope>> {
        let mut guard = self.stream.lock().await;
        if guard.as_ref().is_some_and(|tx| !tx.is_closed()) {
            return guard.clone();
        }
        let mut client = self.ensure().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(1024);
        let response = client
            .raft_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .ok()?;
        let mut responses = response.into_inner();
        let inbox = self.inbox.clone();
        let expected_peer = self.peer_id;
        let state = self.stream.clone();
        tokio::spawn(async move {
            while let Ok(Some(envelope)) = responses.message().await {
                if envelope.node_id != expected_peer {
                    break;
                }
                if let Some(message) = crate::stream::decode(envelope) {
                    if inbox.send(message).await.is_err() {
                        break;
                    }
                }
            }
            *state.lock().await = None;
        });
        *guard = Some(tx.clone());
        Some(tx)
    }
    /// Send a single Raft message to the peer; deliver any synchronous
    /// response into the local inbox.
    pub async fn send(&self, msg: Message) {
        if matches!(
            msg,
            Message::AppendEntries { .. }
                | Message::RequestVote { .. }
                | Message::TimeoutNow { .. }
        ) {
            if let (Some(tx), Some(envelope)) =
                (self.stream_sender().await, crate::stream::encode(&msg))
            {
                let _ = tx.try_send(envelope);
                return;
            }
        }
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
                        let _ = self.inbox.send(resp_msg).await;
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
                        let _ = self.inbox.send(resp_msg).await;
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
                let Some(head) = snapshot_to_pb(&msg) else {
                    return;
                };
                let chunks: Vec<_> = head
                    .data
                    .chunks(64 * 1024)
                    .enumerate()
                    .map(|(i, data)| pb::InstallSnapshotChunk {
                        data: data.to_vec(),
                        offset: (i * 64 * 1024) as u64,
                        done: (i + 1) * 64 * 1024 >= head.data.len(),
                        checksum: crc32c::crc32c(data),
                        ..head.clone()
                    })
                    .collect();
                let stream = futures::stream::iter(chunks);
                match client.install_snapshot(stream).await {
                    Ok(response) => {
                        let response = response.into_inner();
                        let mut bytes = [0; 8];
                        if response.request_id.len() == 8 {
                            bytes.copy_from_slice(&response.request_id);
                        }
                        let _ = self
                            .inbox
                            .send(Message::InstallSnapshotResponse {
                                from: self.peer_id,
                                term: response.term,
                                request_id: u64::from_le_bytes(bytes),
                            })
                            .await;
                        false
                    }
                    Err(e) => {
                        tracing::debug!(peer=self.peer_id,error=%e,"snapshot RPC failed");
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
}
