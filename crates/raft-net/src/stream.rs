//! Typed envelopes for the persistent peer stream. Snapshots use their own stream.
use crate::{convert::*, pb::raft as p};
use raft_core::Message;
pub fn encode(message: &Message) -> Option<p::Envelope> {
    use p::envelope::Body;
    let (node_id, body) = match message {
        Message::AppendEntries { from, .. } => {
            (*from, Body::Append(append_request_to_pb(message)?))
        }
        Message::AppendEntriesResponse { from, .. } => {
            (*from, Body::AppendResponse(append_response_to_pb(message)?))
        }
        Message::RequestVote { from, .. } => (*from, Body::Vote(vote_request_to_pb(message)?)),
        Message::RequestVoteResponse { from, .. } => {
            (*from, Body::VoteResponse(vote_response_to_pb(message)?))
        }
        Message::TimeoutNow { from, term } => (
            *from,
            Body::Timeout(p::TimeoutNowRequest {
                leader_id: *from,
                term: *term,
            }),
        ),
        _ => return None,
    };
    Some(p::Envelope {
        node_id,
        body: Some(body),
        pre_vote: matches!(
            message,
            Message::RequestVote { pre_vote: true, .. }
                | Message::RequestVoteResponse { pre_vote: true, .. }
        ),
    })
}
pub fn decode(envelope: p::Envelope) -> Option<Message> {
    use p::envelope::Body;
    let id = envelope.node_id;
    let pre_vote = envelope.pre_vote;
    Some(match envelope.body? {
        Body::Append(request) => {
            if request.leader_id != id {
                return None;
            }
            append_request_from_pb(request)
        }
        Body::AppendResponse(response) => append_response_from_pb(response, id),
        Body::Vote(request) => {
            if request.candidate_id != id {
                return None;
            }
            vote_request_from_pb(request)
        }
        Body::VoteResponse(response) => vote_response_from_pb(response, id, pre_vote),
        Body::Timeout(request) => {
            if request.leader_id != id {
                return None;
            }
            Message::TimeoutNow {
                from: id,
                term: request.term,
            }
        }
    })
}
