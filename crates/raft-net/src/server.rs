//! gRPC service implementation.
//!
//! The server is structured around a `MessageProcessor` trait: gRPC handlers
//! convert wire messages to `raft_core::Message`, hand them to the processor,
//! and return the response inline. The processor implementation lives in the
//! runtime — it locks the core, runs `step()`, and pulls the
//! `Action::SendMessage { to: <caller> }` whose body is the response.
//!
//! This is the *correct* mapping from Raft RPC semantics to gRPC: each Raft
//! RPC has exactly one synchronous reply, so it goes back over the same gRPC
//! call rather than through the outbound peer client.

use std::sync::Arc;

use async_trait::async_trait;
use tonic::{Request, Response, Status};

use crate::convert::{
    append_request_from_pb, snapshot_from_chunks, vote_request_from_pb,
};
use crate::pb::raft as pb;
use crate::pb::raft::raft_server::Raft as RaftService;
use raft_core::Message;

/// Synchronous message-processing hook the runtime implements.
#[async_trait]
pub trait MessageProcessor: Send + Sync + 'static {
    /// Step the core with `msg` and return the synchronous response message
    /// the core asked us to send back to `from`, if any.
    async fn process(&self, from: u64, msg: Message) -> Option<Message>;
}

/// gRPC service.
pub struct RaftServer {
    processor: Arc<dyn MessageProcessor>,
    /// Local node id.
    pub local_id: u64,
}

impl RaftServer {
    /// Construct a new server attached to a processor.
    pub fn new(local_id: u64, processor: Arc<dyn MessageProcessor>) -> Self {
        Self { processor, local_id }
    }
}

fn unwrap_append_response(msg: Option<Message>, term_fallback: u64, last_log_index: u64, request_id: Vec<u8>) -> pb::AppendEntriesResponse {
    if let Some(Message::AppendEntriesResponse { term, success, conflict_index, conflict_term, last_log_index, request_id: rid, .. }) = msg {
        pb::AppendEntriesResponse {
            term, success, conflict_index, conflict_term, last_log_index,
            request_id: rid.to_le_bytes().to_vec(),
        }
    } else {
        pb::AppendEntriesResponse {
            term: term_fallback,
            success: false,
            conflict_index: 0,
            conflict_term: 0,
            last_log_index,
            request_id,
        }
    }
}

fn unwrap_vote_response(msg: Option<Message>, term_fallback: u64, request_id: Vec<u8>) -> pb::RequestVoteResponse {
    if let Some(Message::RequestVoteResponse { term, vote_granted, request_id: rid, .. }) = msg {
        pb::RequestVoteResponse {
            term, vote_granted,
            request_id: rid.to_le_bytes().to_vec(),
        }
    } else {
        pb::RequestVoteResponse { term: term_fallback, vote_granted: false, request_id }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();
        let term = req.term;
        let last_log_index_hint = req.prev_log_index + req.entries.len() as u64;
        let from = req.leader_id;
        let msg = append_request_from_pb(req);
        let resp_msg = self.processor.process(from, msg).await;
        let pb_resp = unwrap_append_response(resp_msg, term, last_log_index_hint, request_id);
        Ok(Response::new(pb_resp))
    }

    async fn request_vote(
        &self,
        request: Request<pb::RequestVoteRequest>,
    ) -> Result<Response<pb::RequestVoteResponse>, Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();
        let term = req.term;
        let from = req.candidate_id;
        let msg = vote_request_from_pb(req);
        let resp_msg = self.processor.process(from, msg).await;
        Ok(Response::new(unwrap_vote_response(resp_msg, term, request_id)))
    }

    async fn pre_vote(
        &self,
        request: Request<pb::RequestVoteRequest>,
    ) -> Result<Response<pb::RequestVoteResponse>, Status> {
        self.request_vote(request).await
    }

    async fn install_snapshot(
        &self,
        request: Request<tonic::Streaming<pb::InstallSnapshotChunk>>,
    ) -> Result<Response<pb::InstallSnapshotResponse>, Status> {
        let mut stream = request.into_inner();
        let mut chunks = Vec::new();
        while let Some(c) = stream.message().await? {
            chunks.push(c);
        }
        let mut term = 0;
        let mut request_id = vec![];
        if let Some(msg) = snapshot_from_chunks(chunks) {
            if let Message::InstallSnapshot { from, term: t, request_id: rid, .. } = &msg {
                term = *t;
                request_id = rid.to_le_bytes().to_vec();
                let _ = self.processor.process(*from, msg).await;
            }
        }
        Ok(Response::new(pb::InstallSnapshotResponse { term, request_id }))
    }

    async fn timeout_now(
        &self,
        request: Request<pb::TimeoutNowRequest>,
    ) -> Result<Response<pb::TimeoutNowResponse>, Status> {
        let req = request.into_inner();
        let from = req.leader_id;
        let msg = Message::TimeoutNow { from, term: req.term };
        let _ = self.processor.process(from, msg).await;
        Ok(Response::new(pb::TimeoutNowResponse { accepted: true }))
    }
}
