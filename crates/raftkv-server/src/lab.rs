//! Isolated deterministic simulation state; never routes faults to live nodes.
use axum::{
    extract::{Path, State},
    routing::{get, post},
    Json, Router,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sim_tests::Sim;
use std::sync::Arc;
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Config {
    seed: u64,
    node_count: u64,
    delay_min_ticks: u64,
    delay_max_ticks: u64,
    drop_probability: f64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            seed: 51966,
            node_count: 5,
            delay_min_ticks: 1,
            delay_max_ticks: 4,
            drop_probability: 0.0,
        }
    }
}
struct Lab {
    config: Config,
    sim: Sim,
    running: bool,
    actions: Vec<Value>,
}
impl Lab {
    fn new(config: Config) -> Self {
        let mut sim = Sim::new(&(1..=config.node_count).collect::<Vec<_>>(), config.seed);
        sim.delay_min = config.delay_min_ticks;
        sim.delay_max = config.delay_max_ticks;
        sim.drop_p = config.drop_probability;
        Self {
            config,
            sim,
            running: false,
            actions: vec![],
        }
    }
    fn state(&self) -> Value {
        json!({"running":self.running,"tick":self.sim.now,"leaderId":self.sim.leader(),"seed":self.sim.seed,"config":self.config,"nodes":self.sim.nodes.values().map(|n|json!({"id":n.id,"role":n.node.role().as_str(),"term":n.node.hard_state().current_term,"commitIndex":n.node.commit_index(),"lastLogIndex":n.node.last_log_index(),"partitioned":n.partitioned,"crashed":n.crashed})).collect::<Vec<_>>(),"events":self.sim.events.iter().rev().take(250).enumerate().map(|(i,(tick,id,action))|json!({"seq":self.sim.event_count-i as u64,"nodeId":id,"term":self.sim.nodes[id].node.hard_state().current_term,"type":match action{raft_core::Action::SendMessage{..}=>"MessageSent",raft_core::Action::ApplyCommitted{..}=>"EntryApplied",raft_core::Action::AppendEntries(_)=>"LogAppended",raft_core::Action::BecameLeader{..}|raft_core::Action::BecameFollower{..}=>"RoleChanged",_=>"ProtocolTransition"},"detail":format!("tick {tick}: {action:?}")})).collect::<Vec<_>>()})
    }
}
pub fn router() -> Router {
    let state = Arc::new(Mutex::new(Lab::new(Config::default())));
    let task = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            let mut lab = task.lock();
            if lab.running {
                lab.sim.step();
            }
        }
    });
    Router::new()
        .route("/lab/api/v1/state", get(state_get))
        .route("/lab/api/v1/configure", post(configure))
        .route("/lab/api/v1/replay", get(export).post(import))
        .route("/lab/api/v1/:action", post(action))
        .with_state(state)
}
async fn state_get(State(lab): State<Arc<Mutex<Lab>>>) -> Json<Value> {
    Json(lab.lock().state())
}
async fn configure(
    State(lab): State<Arc<Mutex<Lab>>>,
    Json(config): Json<Config>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    if !(1..=9).contains(&config.node_count)
        || config.delay_min_ticks > config.delay_max_ticks
        || config.delay_max_ticks > 1000
        || !config.drop_probability.is_finite()
        || !(0.0..=1.0).contains(&config.drop_probability)
    {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "invalid simulation config".into(),
        ));
    }
    let mut lab = lab.lock();
    *lab = Lab::new(config);
    Ok(Json(lab.state()))
}
fn apply(lab: &mut Lab, name: &str, body: &Value) -> Result<(), String> {
    let id = body["nodeId"].as_u64().unwrap_or(0);
    match name {
        "start" => lab.running = true,
        "pause" => lab.running = false,
        "step" => lab.sim.step(),
        "reset" => *lab = Lab::new(lab.config.clone()),
        "heal" => {
            for node in lab.sim.nodes.values_mut() {
                node.partitioned = false;
            }
            lab.sim.blocked.clear();
        }
        "partition" => {
            let node = lab.sim.nodes.get_mut(&id).ok_or("unknown node")?;
            node.partitioned = !node.partitioned;
        }
        "crash" => lab.sim.crash(id),
        "restart" => lab.sim.restart(id),
        "duplicate" => {
            if let Some(message) = lab.sim.network.first() {
                lab.sim.network.push(sim_tests::InFlight {
                    from: message.from,
                    to: message.to,
                    deliver_at: message.deliver_at + 1,
                    msg: message.msg.clone(),
                });
            }
        }
        "drop" => {
            if !lab.sim.network.is_empty() {
                lab.sim.network.remove(0);
            }
        }
        "reorder" => lab.sim.network.reverse(),
        "delay" => {
            let delay = body["ticks"].as_u64().unwrap_or(10).min(1000);
            for message in &mut lab.sim.network {
                message.deliver_at += delay;
            }
        }
        "asymmetric" => {
            let peer = body["peerId"].as_u64().ok_or("peerId required")?;
            lab.sim.blocked.insert((id, peer));
        }
        "disk-failure" | "fsync-failure" | "torn-write" | "snapshot-failure" => {
            if !lab.sim.nodes.contains_key(&id) {
                return Err("unknown node".into());
            }
            use sim_tests::StorageFault;
            let fault = match name {
                "fsync-failure" => StorageFault::AfterPersistence,
                "torn-write" => StorageFault::TornAppend,
                "snapshot-failure" => StorageFault::SnapshotPublication,
                _ => StorageFault::BeforePersistence,
            };
            lab.sim.storage_fault = Some((id, fault));
        }
        "clock-stall" => lab
            .sim
            .stall(id, body["ticks"].as_u64().unwrap_or(20).min(1000)),
        "slow-follower" => {
            if !lab.sim.nodes.contains_key(&id) {
                return Err("unknown node".into());
            }
            lab.sim.slow_until.insert(
                id,
                lab.sim.now + body["ticks"].as_u64().unwrap_or(20).min(1000),
            );
        }
        "snapshot" => lab.sim.snapshot(id)?,
        "propose" => {
            let id = if id == 0 {
                lab.sim.leader().ok_or("no leader")?
            } else {
                id
            };
            lab.sim.propose(
                id,
                body["value"].as_str().unwrap_or("lab").as_bytes().to_vec(),
            )?;
        }
        "remove-member" => {
            let leader = lab.sim.leader().ok_or("no leader")?;
            lab.sim
                .change_membership(leader, raft_core::ConfigChange::RemoveServer(id))?;
        }
        _ => return Err("unknown simulation action".into()),
    };
    Ok(())
}
async fn action(
    State(lab): State<Arc<Mutex<Lab>>>,
    Path(name): Path<String>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let mut lab = lab.lock();
    if lab.actions.len() >= 10_000 {
        return Err((
            axum::http::StatusCode::CONFLICT,
            "reset before recording more actions".into(),
        ));
    }
    let tick = lab.sim.now;
    apply(&mut lab, &name, &body).map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e))?;
    lab.actions
        .push(json!({"tick":tick,"action":name,"payload":body}));
    Ok(Json(lab.state()))
}
async fn export(State(lab): State<Arc<Mutex<Lab>>>) -> Json<Value> {
    let lab = lab.lock();
    Json(json!({"config":lab.config,"tick":lab.sim.now,"actions":lab.actions}))
}
async fn import(
    State(lab): State<Arc<Mutex<Lab>>>,
    Json(body): Json<Value>,
) -> Result<Json<Value>, (axum::http::StatusCode, String)> {
    let config: Config = serde_json::from_value(body["config"].clone())
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e.to_string()))?;
    if !(1..=9).contains(&config.node_count)
        || config.delay_min_ticks > config.delay_max_ticks
        || config.delay_max_ticks > 1000
        || !(0.0..=1.0).contains(&config.drop_probability)
    {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "invalid replay config".into(),
        ));
    }
    let actions = body["actions"].as_array().ok_or((
        axum::http::StatusCode::BAD_REQUEST,
        "actions required".into(),
    ))?;
    let end = body["tick"].as_u64().unwrap_or(0);
    if actions.len() > 10_000 || end > 100_000 {
        return Err((
            axum::http::StatusCode::BAD_REQUEST,
            "replay too large".into(),
        ));
    }
    let mut replay = Lab::new(config);
    for event in actions {
        let tick = event["tick"].as_u64().unwrap_or(0);
        if tick > end {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "invalid replay tick".into(),
            ));
        }
        while replay.sim.now < tick {
            replay.sim.step();
        }
        apply(
            &mut replay,
            event["action"].as_str().unwrap_or(""),
            &event["payload"],
        )
        .map_err(|e| (axum::http::StatusCode::BAD_REQUEST, e))?;
    }
    while replay.sim.now < end {
        replay.sim.step();
    }
    replay.running = false;
    replay.actions = actions.clone();
    let mut lab = lab.lock();
    *lab = replay;
    Ok(Json(lab.state()))
}
