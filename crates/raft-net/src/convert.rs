//! Conversions between protobuf wire types and `raft_core::Message`.

use bytes::Bytes;
use raft_core::log::{Entry, EntryKind};
use raft_core::Message;

use crate::pb::raft as pb;

fn entry_kind_to_pb(k: EntryKind) -> i32 {
    match k {
        EntryKind::Noop => pb::EntryType::Noop as i32,
        EntryKind::Normal => pb::EntryType::Normal as i32,
        EntryKind::ConfigJoint => pb::EntryType::ConfigJoint as i32,
        EntryKind::ConfigNew => pb::EntryType::ConfigNew as i32,
    }
}

fn entry_kind_from_pb(t: i32) -> EntryKind {
    match pb::EntryType::try_from(t).unwrap_or(pb::EntryType::Normal) {
        pb::EntryType::Noop => EntryKind::Noop,
        pb::EntryType::Normal => EntryKind::Normal,
        pb::EntryType::ConfigJoint => EntryKind::ConfigJoint,
        pb::EntryType::ConfigNew => EntryKind::ConfigNew,
    }
}

/// Convert a core entry to wire form.
pub fn entry_to_pb(e: &Entry) -> pb::LogEntry {
    pb::LogEntry { term: e.term, index: e.index, entry_type: entry_kind_to_pb(e.kind), data: e.data.clone() }
}

/// Convert a wire entry to core form.
pub fn entry_from_pb(e: pb::LogEntry) -> Entry {
    Entry { term: e.term, index: e.index, kind: entry_kind_from_pb(e.entry_type), data: e.data }
}

fn rid_to_bytes(rid: u64) -> Vec<u8> { rid.to_le_bytes().to_vec() }
fn rid_from_bytes(b: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    let n = b.len().min(8);
    buf[..n].copy_from_slice(&b[..n]);
    u64::from_le_bytes(buf)
}

/// Convert AppendEntries request from wire.
pub fn append_request_from_pb(r: pb::AppendEntriesRequest) -> Message {
    Message::AppendEntries {
        from: r.leader_id,
        term: r.term,
        prev_log_index: r.prev_log_index,
        prev_log_term: r.prev_log_term,
        entries: r.entries.into_iter().map(entry_from_pb).collect(),
        leader_commit: r.leader_commit,
        request_id: rid_from_bytes(&r.request_id),
    }
}

/// Convert AppendEntries response from wire.
pub fn append_response_from_pb(r: pb::AppendEntriesResponse, from: u64) -> Message {
    Message::AppendEntriesResponse {
        from,
        term: r.term,
        success: r.success,
        conflict_index: r.conflict_index,
        conflict_term: r.conflict_term,
        last_log_index: r.last_log_index,
        request_id: rid_from_bytes(&r.request_id),
    }
}

/// Convert RequestVote/PreVote request from wire.
pub fn vote_request_from_pb(r: pb::RequestVoteRequest) -> Message {
    Message::RequestVote {
        from: r.candidate_id,
        term: r.term,
        last_log_index: r.last_log_index,
        last_log_term: r.last_log_term,
        pre_vote: r.pre_vote,
        request_id: rid_from_bytes(&r.request_id),
    }
}

/// Convert vote response from wire.
pub fn vote_response_from_pb(r: pb::RequestVoteResponse, from: u64, pre_vote: bool) -> Message {
    Message::RequestVoteResponse {
        from,
        term: r.term,
        vote_granted: r.vote_granted,
        pre_vote,
        request_id: rid_from_bytes(&r.request_id),
    }
}

/// Build a wire AppendEntries request.
pub fn append_request_to_pb(msg: &Message) -> Option<pb::AppendEntriesRequest> {
    if let Message::AppendEntries { from, term, prev_log_index, prev_log_term, entries, leader_commit, request_id } = msg {
        Some(pb::AppendEntriesRequest {
            term: *term,
            leader_id: *from,
            prev_log_index: *prev_log_index,
            prev_log_term: *prev_log_term,
            entries: entries.iter().map(entry_to_pb).collect(),
            leader_commit: *leader_commit,
            request_id: rid_to_bytes(*request_id),
        })
    } else { None }
}

/// Build a wire AppendEntries response.
pub fn append_response_to_pb(msg: &Message) -> Option<pb::AppendEntriesResponse> {
    if let Message::AppendEntriesResponse { term, success, conflict_index, conflict_term, last_log_index, request_id, .. } = msg {
        Some(pb::AppendEntriesResponse {
            term: *term,
            success: *success,
            conflict_index: *conflict_index,
            conflict_term: *conflict_term,
            last_log_index: *last_log_index,
            request_id: rid_to_bytes(*request_id),
        })
    } else { None }
}

/// Build a wire RequestVote request.
pub fn vote_request_to_pb(msg: &Message) -> Option<pb::RequestVoteRequest> {
    if let Message::RequestVote { from, term, last_log_index, last_log_term, pre_vote, request_id } = msg {
        Some(pb::RequestVoteRequest {
            term: *term,
            candidate_id: *from,
            last_log_index: *last_log_index,
            last_log_term: *last_log_term,
            pre_vote: *pre_vote,
            request_id: rid_to_bytes(*request_id),
        })
    } else { None }
}

/// Build a wire RequestVote response.
pub fn vote_response_to_pb(msg: &Message) -> Option<pb::RequestVoteResponse> {
    if let Message::RequestVoteResponse { term, vote_granted, request_id, .. } = msg {
        Some(pb::RequestVoteResponse {
            term: *term,
            vote_granted: *vote_granted,
            request_id: rid_to_bytes(*request_id),
        })
    } else { None }
}

/// Build a wire InstallSnapshot single-chunk message.
pub fn snapshot_to_pb(msg: &Message) -> Option<pb::InstallSnapshotChunk> {
    if let Message::InstallSnapshot { from, term, last_included_index, last_included_term, data, request_id } = msg {
        Some(pb::InstallSnapshotChunk {
            term: *term,
            leader_id: *from,
            last_included_index: *last_included_index,
            last_included_term: *last_included_term,
            offset: 0,
            data: data.to_vec(),
            done: true,
            config: vec![],
            request_id: rid_to_bytes(*request_id),
        })
    } else { None }
}

/// Reconstruct a single InstallSnapshot message from a chunk stream.
pub fn snapshot_from_chunks(chunks: Vec<pb::InstallSnapshotChunk>) -> Option<Message> {
    let head = chunks.first()?;
    let from = head.leader_id;
    let term = head.term;
    let last_included_index = head.last_included_index;
    let last_included_term = head.last_included_term;
    let request_id = rid_from_bytes(&head.request_id);
    let mut buf = Vec::new();
    for c in chunks { buf.extend_from_slice(&c.data); }
    Some(Message::InstallSnapshot {
        from,
        term,
        last_included_index,
        last_included_term,
        data: Bytes::from(buf),
        request_id,
    })
}
