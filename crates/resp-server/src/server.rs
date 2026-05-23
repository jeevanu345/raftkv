//! RESP TCP server.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

use crate::codec::{RespCodec, RespFrame};
use crate::commands::parse;
use crate::handler::{dispatch, CommandHandler};

/// Run a RESP server on `addr`. Each connection is handled by a spawned task.
pub async fn serve<H: CommandHandler + 'static>(
    addr: &str,
    handler: Arc<H>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(addr = addr, "RESP server listening");
    loop {
        let (sock, peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => { tracing::warn!(error = %e, "accept failed"); continue; }
        };
        let handler = handler.clone();
        tokio::spawn(async move {
            sock.set_nodelay(true).ok();
            let mut framed = Framed::new(sock, RespCodec);
            tracing::debug!(client = %peer, "client connected");
            while let Some(frame_res) = framed.next().await {
                match frame_res {
                    Ok(frame) => {
                        let cmd = parse(&frame);
                        let now_ms = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        let resp: RespFrame = dispatch(handler.clone(), cmd, now_ms).await;
                        if let Err(e) = framed.send(resp).await {
                            tracing::debug!(error = %e, "send failed");
                            break;
                        }
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "client read error");
                        break;
                    }
                }
            }
        });
    }
}
