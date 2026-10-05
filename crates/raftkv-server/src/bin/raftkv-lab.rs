//! Standalone deterministic lab. It has no live-cluster runtime or data access.
#[path = "../lab.rs"]
mod lab;
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let addr = std::env::var("RAFTKV_LAB_LISTEN").unwrap_or_else(|_| "127.0.0.1:8090".into());
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("RaftKV deterministic lab at {addr}");
    axum::serve(listener, lab::router()).await?;
    Ok(())
}
