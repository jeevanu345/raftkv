#![deny(unsafe_code)]
//! RESP2/RESP3 protocol server for a Raft-replicated KV.

pub mod codec;
pub mod commands;
pub mod handler;
pub mod server;

pub use codec::{RespCodec, RespFrame};
pub use handler::CommandHandler;
pub use server::serve;
