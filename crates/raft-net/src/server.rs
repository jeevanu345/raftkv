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

use crate::convert::{append_request_from_pb, vote_request_from_pb};
use crate::pb::raft as pb;
use crate::pb::raft::raft_server::Raft as RaftService;
use raft_core::Message;

/// Synchronous message-processing hook the runtime implements.
#[async_trait]
pub trait MessageProcessor: Send + Sync + 'static {
    /// Validate dynamically registered mTLS node identities.
    fn validate_certificate(&self, _node: u64, _hash: &str) -> bool {
        false
    }
    /// Step the core with `msg` and return the synchronous response message
    /// the core asked us to send back to `from`, if any.
    async fn process(&self, from: u64, msg: Message) -> Option<Message>;
}

/// gRPC service.
pub struct RaftServer {
    processor: Arc<dyn MessageProcessor>,
    /// Local node id.
    pub local_id: u64,
    peer_certificates: Option<std::collections::BTreeMap<u64, String>>,
}

impl RaftServer {
    /// Construct a new server attached to a processor.
    pub fn new(local_id: u64, processor: Arc<dyn MessageProcessor>) -> Self {
        Self {
            processor,
            local_id,
            peer_certificates: None,
        }
    }
    /// Bind node IDs to certificate fingerprints in addition to CA validation.
    pub fn with_peer_certificates(
        mut self,
        certificates: std::collections::BTreeMap<u64, String>,
    ) -> Self {
        self.peer_certificates = Some(certificates);
        self
    }
    fn authenticate<T>(&self, request: &Request<T>, node: u64) -> Result<(), Status> {
        use sha2::{Digest, Sha256};
        if let Some(certificates) = &self.peer_certificates {
            let certs = request
                .peer_certs()
                .ok_or_else(|| Status::unauthenticated("peer certificate required"))?;
            let cert = certs
                .first()
                .ok_or_else(|| Status::unauthenticated("peer certificate required"))?;
            let fingerprint = hex::encode(Sha256::digest(cert.as_ref()));
            if !self.processor.validate_certificate(node, &fingerprint)
                && certificates.get(&node) != Some(&fingerprint)
            {
                return Err(Status::permission_denied(
                    "certificate does not match node id",
                ));
            }
        }
        Ok(())
    }
}

fn unwrap_append_response(
    msg: Option<Message>,
    term_fallback: u64,
    last_log_index: u64,
    request_id: Vec<u8>,
) -> pb::AppendEntriesResponse {
    if let Some(Message::AppendEntriesResponse {
        term,
        success,
        conflict_index,
        conflict_term,
        last_log_index,
        request_id: rid,
        ..
    }) = msg
    {
        pb::AppendEntriesResponse {
            term,
            success,
            conflict_index,
            conflict_term,
            last_log_index,
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

fn unwrap_vote_response(
    msg: Option<Message>,
    term_fallback: u64,
    request_id: Vec<u8>,
) -> pb::RequestVoteResponse {
    if let Some(Message::RequestVoteResponse {
        term,
        vote_granted,
        request_id: rid,
        ..
    }) = msg
    {
        pb::RequestVoteResponse {
            term,
            vote_granted,
            request_id: rid.to_le_bytes().to_vec(),
        }
    } else {
        pb::RequestVoteResponse {
            term: term_fallback,
            vote_granted: false,
            request_id,
        }
    }
}

#[tonic::async_trait]
impl RaftService for RaftServer {
    type RaftStreamStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<pb::Envelope, Status>> + Send>>;
    async fn raft_stream(
        &self,
        request: Request<tonic::Streaming<pb::Envelope>>,
    ) -> Result<Response<Self::RaftStreamStream>, Status> {
        let certs = request.peer_certs();
        let mut stream = request.into_inner();
        let processor = self.processor.clone();
        let certificates = self.peer_certificates.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            while let Ok(Some(envelope)) = stream.message().await {
                let from = envelope.node_id;
                if let Some(certificates) = &certificates {
                    use sha2::{Digest, Sha256};
                    let fingerprint = certs
                        .as_ref()
                        .and_then(|c| c.first())
                        .map(|cert| hex::encode(Sha256::digest(cert.as_ref())));
                    if !fingerprint
                        .as_ref()
                        .is_some_and(|hash| processor.validate_certificate(from, hash))
                        && fingerprint.as_ref() != certificates.get(&from)
                    {
                        let _ = tx
                            .send(Err(Status::permission_denied(
                                "certificate does not match node id",
                            )))
                            .await;
                        break;
                    }
                }
                let Some(message) = crate::stream::decode(envelope) else {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "invalid envelope identity/body",
                        )))
                        .await;
                    break;
                };
                if matches!(
                    message,
                    Message::AppendEntriesResponse { .. } | Message::RequestVoteResponse { .. }
                ) {
                    let _ = tx
                        .send(Err(Status::invalid_argument("requests required")))
                        .await;
                    break;
                }
                if let Some(response) = processor
                    .process(from, message)
                    .await
                    .and_then(|m| crate::stream::encode(&m))
                {
                    if tx.send(Ok(response)).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(
            tokio_stream::wrappers::ReceiverStream::new(rx),
        )))
    }
    async fn append_entries(
        &self,
        request: Request<pb::AppendEntriesRequest>,
    ) -> Result<Response<pb::AppendEntriesResponse>, Status> {
        self.authenticate(&request, request.get_ref().leader_id)?;
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
        self.authenticate(&request, request.get_ref().candidate_id)?;
        let req = request.into_inner();
        let request_id = req.request_id.clone();
        let term = req.term;
        let from = req.candidate_id;
        let msg = vote_request_from_pb(req);
        let resp_msg = self.processor.process(from, msg).await;
        Ok(Response::new(unwrap_vote_response(
            resp_msg, term, request_id,
        )))
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
        let certs = request.peer_certs();
        let mut stream = request.into_inner();
        use tokio::io::AsyncWriteExt;
        let temporary =
            tempfile::NamedTempFile::new().map_err(|e| Status::internal(e.to_string()))?;
        let mut file = tokio::fs::File::create(temporary.path())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mut first: Option<pb::InstallSnapshotChunk> = None;
        let mut total = 0u64;
        let mut done = false;
        while let Some(chunk) =
            tokio::time::timeout(std::time::Duration::from_secs(30), stream.message())
                .await
                .map_err(|_| Status::deadline_exceeded("snapshot stalled"))??
        {
            if done
                || chunk.offset != total
                || chunk.data.len() > 64 * 1024
                || (chunk.data.is_empty() && !chunk.done)
                || crc32c::crc32c(&chunk.data) != chunk.checksum
            {
                return Err(Status::invalid_argument(
                    "invalid snapshot chunk/offset/checksum",
                ));
            }
            if let Some(header) = &first {
                if chunk.term != header.term
                    || chunk.leader_id != header.leader_id
                    || chunk.last_included_index != header.last_included_index
                    || chunk.last_included_term != header.last_included_term
                    || chunk.request_id != header.request_id
                    || chunk.config != header.config
                {
                    return Err(Status::invalid_argument("snapshot metadata changed"));
                }
            }
            total += chunk.data.len() as u64;
            if total > 256 * 1024 * 1024 {
                return Err(Status::resource_exhausted("snapshot too large"));
            }
            file.write_all(&chunk.data)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            done = chunk.done;
            if first.is_none() {
                let mut header = chunk;
                header.data.clear();
                first = Some(header);
            }
        }
        if !done {
            return Err(Status::invalid_argument("incomplete snapshot"));
        }
        file.sync_all()
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        drop(file);
        let header = first.ok_or_else(|| Status::invalid_argument("empty snapshot stream"))?;
        let request_id = u64::from_le_bytes(
            header
                .request_id
                .as_slice()
                .try_into()
                .map_err(|_| Status::invalid_argument("invalid request id"))?,
        );
        let data = tokio::fs::read(temporary.path())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let msg = Message::InstallSnapshot {
            from: header.leader_id,
            term: header.term,
            last_included_index: header.last_included_index,
            last_included_term: header.last_included_term,
            data: data.into(),
            request_id,
        };
        let from = match &msg {
            Message::InstallSnapshot { from, .. } => *from,
            _ => unreachable!(),
        };
        if let Some(certificates) = &self.peer_certificates {
            use sha2::{Digest, Sha256};
            let cert = certs
                .as_ref()
                .and_then(|c| c.first())
                .ok_or_else(|| Status::unauthenticated("peer certificate required"))?;
            if !self
                .processor
                .validate_certificate(from, &hex::encode(Sha256::digest(cert.as_ref())))
                && certificates.get(&from) != Some(&hex::encode(Sha256::digest(cert.as_ref())))
            {
                return Err(Status::permission_denied(
                    "certificate does not match node id",
                ));
            }
        }
        match self.processor.process(from, msg).await {
            Some(Message::InstallSnapshotResponse {
                term, request_id, ..
            }) => Ok(Response::new(pb::InstallSnapshotResponse {
                term,
                request_id: request_id.to_le_bytes().to_vec(),
            })),
            _ => Err(Status::unavailable("snapshot installation failed")),
        }
    }

    async fn timeout_now(
        &self,
        request: Request<pb::TimeoutNowRequest>,
    ) -> Result<Response<pb::TimeoutNowResponse>, Status> {
        self.authenticate(&request, request.get_ref().leader_id)?;
        let req = request.into_inner();
        let from = req.leader_id;
        let msg = Message::TimeoutNow {
            from,
            term: req.term,
        };
        let _ = self.processor.process(from, msg).await;
        Ok(Response::new(pb::TimeoutNowResponse { accepted: true }))
    }
}
