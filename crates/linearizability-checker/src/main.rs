//! Check a short JSON history exported by the integration harness.
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .ok_or("usage: linearizability-checker history.json")?;
    let history: Vec<linearizability_checker::Operation> =
        serde_json::from_slice(&std::fs::read(path)?)?;
    if history.len() > 256 {
        return Err("use a scalable external checker above 256 operations".into());
    }
    match linearizability_checker::check(&history)? {
        linearizability_checker::Verdict::Linearizable => {
            println!("Linearizable: {} operations", history.len())
        }
        verdict => {
            eprintln!("{verdict:?}");
            std::process::exit(1);
        }
    }
    Ok(())
}
