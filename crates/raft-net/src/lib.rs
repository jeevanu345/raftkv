#![deny(unsafe_code)]
//! gRPC transport for Raft RPCs.
//!
//! Wire types live in the generated module `pb` below. Conversion helpers
//! map between the generated types and `raft_core::Message`.

#[allow(clippy::all)]
#[allow(missing_docs)]
pub mod pb {
    pub mod raft {
        tonic::include_proto!("raftkv.raft.v1");
    }
    pub mod membership {
        tonic::include_proto!("raftkv.membership.v1");
    }
    pub mod admin {
        tonic::include_proto!("raftkv.admin.v1");
    }
}

pub mod convert;
pub mod client;
pub mod server;

pub use client::PeerClient;
pub use server::{MessageProcessor, RaftServer};

/// Outbox for Raft messages produced by the core (consumed by the network layer).
pub type Outbox = tokio::sync::mpsc::UnboundedSender<(raft_core::NodeId, raft_core::Message)>;
/// Inbox of incoming Raft messages from peers.
pub type Inbox = tokio::sync::mpsc::UnboundedReceiver<raft_core::Message>;
/// Inbox sender side.
pub type InboxTx = tokio::sync::mpsc::UnboundedSender<raft_core::Message>;
