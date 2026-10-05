//! Browser-facing typed HTTP/SSE facade. Reads and writes use the runtime.
use crate::runtime::{directory_size, ClientHandler, Runtime};
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures::StreamExt;
use hmac::{Hmac, Mac};
use resp_server::{
    codec::{RespCodec, RespFrame},
    handler::{dispatch, CommandHandler},
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

type ApiResult = Result<Json<Value>, (StatusCode, Json<Value>)>;
fn error(status: StatusCode, message: impl ToString) -> (StatusCode, Json<Value>) {
    (status, Json(json!({"message":message.to_string()})))
}
pub(crate) fn timestamp() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .to_string()
}
fn http_client(rt: &Runtime) -> reqwest::Client {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(6));
    if let Some(tls) = &rt.cfg.admin_tls {
        if let Ok(pem) = std::fs::read(&tls.ca) {
            if let Ok(cert) = reqwest::Certificate::from_pem(&pem) {
                builder = builder.add_root_certificate(cert);
            }
        }
    }
    builder.build().expect("HTTP client")
}

fn session_mac(token: &str, issued: u64) -> Hmac<Sha256> {
    let mut mac = Hmac::<Sha256>::new_from_slice(token.as_bytes())
        .expect("HMAC accepts arbitrary key length");
    mac.update(b"raftkv-session-v1");
    mac.update(&issued.to_le_bytes());
    mac
}
fn session(token: &str, issued: u64) -> String {
    format!(
        "{issued}.{}",
        hex::encode(session_mac(token, issued).finalize().into_bytes())
    )
}
fn valid_session(token: &str, value: &str) -> bool {
    let Some((issued, signature)) = value.split_once('.') else {
        return false;
    };
    let Ok(issued) = issued.parse::<u64>() else {
        return false;
    };
    let Ok(signature) = hex::decode(signature) else {
        return false;
    };
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    issued <= now
        && now - issued <= 43200
        && session_mac(token, issued).verify_slice(&signature).is_ok()
}
fn identity(rt: &Runtime, bearer: Option<&str>, cookies: Option<&str>) -> Option<(String, String)> {
    let matches = |token: &str| {
        bearer == Some(token)
            || cookies.is_some_and(|s| {
                s.split(';').any(|p| {
                    p.trim()
                        .strip_prefix("raftkv_session=")
                        .is_some_and(|v| valid_session(token, v))
                })
            })
    };
    if let Some(token) = &rt.cfg.admin_token {
        if matches(token) {
            return Some(("admin".into(), "admin".into()));
        }
    }
    rt.cfg
        .admin_users
        .iter()
        .find(|user| matches(&user.token))
        .map(|u| (u.name.clone(), u.role.clone()))
}
async fn authorize(
    State(rt): State<Arc<Runtime>>,
    request: axum::extract::Request,
    next: Next,
) -> Response {
    let request_id = rt.trace_id();
    let bearer = request
        .headers()
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "));
    let secured = rt.cfg.admin_token.is_some() || !rt.cfg.admin_users.is_empty();
    let principal = identity(
        &rt,
        bearer,
        request
            .headers()
            .get("cookie")
            .and_then(|h| h.to_str().ok()),
    );
    if secured && principal.is_none() {
        return error(StatusCode::UNAUTHORIZED, "authentication required").into_response();
    }
    let (name, role) = principal.unwrap_or_else(|| ("local-development".into(), "admin".into()));
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    let mutation = method != axum::http::Method::GET;
    let required = if path.starts_with("/api/v1/keys/")
        || path == "/api/v1/commands"
        || path == "/api/v1/admin/backup"
        || (mutation && path.contains("/members"))
    {
        "admin"
    } else if mutation {
        "operator"
    } else {
        "viewer"
    };
    let rank = |role: &str| match role {
        "admin" => 3,
        "operator" => 2,
        "viewer" => 1,
        _ => 0,
    };
    if rank(&role) < rank(required) {
        return error(StatusCode::FORBIDDEN, "role does not permit this operation").into_response();
    }
    if mutation && bearer.is_none() {
        if let Some(origin) = request
            .headers()
            .get("origin")
            .and_then(|h| h.to_str().ok())
        {
            let host = request
                .headers()
                .get("host")
                .and_then(|h| h.to_str().ok())
                .unwrap_or("");
            let same = url::Url::parse(origin)
                .ok()
                .and_then(|u| {
                    u.host_str().map(|h| {
                        format!(
                            "{h}{}",
                            u.port().map(|p| format!(":{p}")).unwrap_or_default()
                        )
                    })
                })
                .as_deref()
                == Some(host);
            if !same
                && !rt
                    .cfg
                    .allowed_origins
                    .iter()
                    .any(|allowed| allowed == origin)
            {
                return error(StatusCode::FORBIDDEN, "origin is not allowed").into_response();
            }
        }
    }
    let audit_action = mutation || path == "/api/v1/admin/backup";
    if audit_action {
        if let Err(e) = rt.audit(&name, method.as_str(), &path, "attempt", request_id) {
            return error(StatusCode::SERVICE_UNAVAILABLE, e).into_response();
        }
    }
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        "x-request-id",
        request_id.to_string().parse().expect("numeric request id"),
    );
    if audit_action {
        if let Err(error) = rt.audit(
            &name,
            method.as_str(),
            &path,
            &response.status().as_u16().to_string(),
            request_id,
        ) {
            tracing::error!(%error,"audit outcome write failed");
        }
    }
    response
}
async fn login(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Response {
    if let Some(origin) = headers.get("origin").and_then(|v| v.to_str().ok()) {
        let host = headers
            .get("host")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let same = origin
            .strip_prefix("https://")
            .or_else(|| origin.strip_prefix("http://"))
            .is_some_and(|s| s == host);
        if !same && !rt.cfg.allowed_origins.iter().any(|o| o == origin) {
            return error(StatusCode::FORBIDDEN, "origin not allowed").into_response();
        }
    }
    if rt.cfg.admin_token.is_none() && rt.cfg.admin_users.is_empty() {
        return Json(json!({"authenticated":true,"role":"admin"})).into_response();
    }
    let supplied = body["token"].as_str().unwrap_or("");
    let Some((_, role)) = identity(&rt, Some(supplied), None) else {
        return error(StatusCode::UNAUTHORIZED, "invalid token").into_response();
    };
    (
        [(
            "set-cookie",
            format!(
                "raftkv_session={}; HttpOnly; SameSite=Strict; Path=/; Max-Age=43200{}",
                session(
                    supplied,
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs()
                ),
                if rt.cfg.admin_tls.is_some() {
                    "; Secure"
                } else {
                    ""
                }
            ),
        )],
        Json(json!({"authenticated":true,"role":role})),
    )
        .into_response()
}

pub fn metrics_router(rt: Arc<Runtime>) -> Router {
    Router::new()
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .with_state(rt)
}
pub fn router(rt: Arc<Runtime>) -> Router {
    let api = Router::new()
        .route("/api/v1/status", get(status))
        .route("/api/v1/cluster", get(cluster))
        .route("/api/v1/nodes", get(nodes))
        .route("/api/v1/nodes/:id", get(node))
        .route("/api/v1/events", get(events))
        .route("/api/v1/keys", get(keys))
        .route(
            "/api/v1/keys/:key",
            get(key).put(put_key).delete(delete_key),
        )
        .route("/api/v1/commands", post(commands))
        .route("/api/v1/snapshots", get(snapshots))
        .route("/api/v1/admin/backup", get(backup))
        .route("/api/v1/admin/snapshot", post(snapshot))
        .route("/api/v1/admin/leadership", post(leadership))
        .route("/api/v1/admin/members", post(add_member))
        .route(
            "/api/v1/admin/members/:id",
            axum::routing::delete(remove_member),
        )
        .route_layer(middleware::from_fn_with_state(rt.clone(), authorize));
    Router::new()
        .merge(api)
        .route("/api/v1/auth", post(login))
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(ready))
        .route("/metrics", get(metrics))
        .layer(DefaultBodyLimit::max(2 * 1024 * 1024))
        .with_state(rt)
}
async fn ready(State(rt): State<Arc<Runtime>>) -> StatusCode {
    if rt.ready() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn metrics(State(rt): State<Arc<Runtime>>) -> Response {
    match prometheus::TextEncoder::new().encode_to_string(&rt.metrics.registry.gather()) {
        Ok(text) => ([("content-type", "text/plain; version=0.0.4")], text).into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}
async fn status(State(rt): State<Arc<Runtime>>) -> Json<Value> {
    Json(rt.status())
}
async fn collect_nodes(rt: &Arc<Runtime>) -> Vec<Value> {
    let members: Vec<_> = rt.members.lock().values().cloned().collect();
    let client = http_client(rt);
    let token = rt.cfg.admin_token.clone().or_else(|| {
        rt.cfg
            .admin_users
            .iter()
            .find(|u| u.role == "viewer")
            .or_else(|| rt.cfg.admin_users.first())
            .map(|u| u.token.clone())
    });
    futures::stream::iter(members)
        .map(|member| {
            let client = client.clone();
            let token = token.clone();
            let rt = rt.clone();
            async move {
                if member.id == rt.local_id() {
                    return rt.status();
                }
                let mut request = client
                    .get(format!(
                        "{}/api/v1/status",
                        member.admin_addr.trim_end_matches('/')
                    ))
                    .timeout(Duration::from_millis(700));
                if let Some(token) = token {
                    request = request.bearer_auth(token);
                }
                match request.send().await {
                    Ok(resp) if resp.status().is_success() => {
                        resp.json().await.unwrap_or_else(|_| unknown_node(&member))
                    }
                    _ => unknown_node(&member),
                }
            }
        })
        .buffer_unordered(8)
        .collect()
        .await
}
fn unknown_node(member: &raft_core::config::Member) -> Value {
    json!({"nodeId":member.id,"role":"unknown","health":"unreachable","term":null,"leaderId":null,"commitIndex":null,"appliedIndex":null,"lastLogIndex":null,"snapshotIndex":null,"stateHash":"Unavailable","uptimeSeconds":null,"raftAddress":member.raft_addr,"clientAddress":member.client_addr,"adminAddress":member.admin_addr,"peers":[],"logEntries":[]})
}
async fn cluster(State(rt): State<Arc<Runtime>>) -> Json<Value> {
    let nodes = collect_nodes(&rt).await;
    let local = rt.status();
    let voters = local["voters"].as_array().cloned().unwrap_or_default();
    let available: std::collections::BTreeSet<_> = nodes
        .iter()
        .filter(|n| n["health"] == "healthy")
        .filter_map(|n| n["nodeId"].as_u64())
        .collect();
    let config: raft_core::ConfigState =
        serde_json::from_value(local["configuration"].clone()).expect("local config");
    let leader = nodes
        .iter()
        .filter(|n| n["role"] == "leader")
        .max_by_key(|n| n["term"].as_u64())
        .unwrap_or(&local);
    Json(
        json!({"clusterId":"raftkv","health":if config.has_quorum(&available){"healthy"}else{"degraded"},"leaderId":leader["leaderId"],"term":leader["term"],"commitIndex":leader["commitIndex"],"appliedIndex":leader["appliedIndex"],"snapshotIndex":leader["snapshotIndex"],"quorumSize":voters.len()/2+1,"voterCount":voters.len(),"configurationState":local["configurationState"],"totalKeys":leader["keyCount"],"nodes":nodes,"metrics":rt.metrics.summary(directory_size(&rt.cfg.data_dir.join("log")),directory_size(&rt.cfg.data_dir.join("snapshots"))),"members":rt.members.lock().values().map(|m|json!({"id":m.id,"role":if m.learner{"learner"}else{"voter"},"raftAddress":m.raft_addr,"clientAddress":m.client_addr,"adminAddress":m.admin_addr,"certificateSha256":m.certificate_sha256})).collect::<Vec<_>>()}),
    )
}
async fn nodes(State(rt): State<Arc<Runtime>>) -> Json<Value> {
    Json(json!(collect_nodes(&rt).await))
}
async fn node(State(rt): State<Arc<Runtime>>, Path(id): Path<u64>) -> ApiResult {
    collect_nodes(&rt)
        .await
        .into_iter()
        .find(|n| n["nodeId"] == id)
        .map(Json)
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "unknown member"))
}
async fn forward(
    rt: &Arc<Runtime>,
    headers: &HeaderMap,
    method: reqwest::Method,
    path: &str,
    body: Option<Value>,
) -> Result<Option<Value>, (StatusCode, Json<Value>)> {
    if rt.is_leader() {
        return Ok(None);
    }
    if headers.contains_key("x-raftkv-forwarded") {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "leader changed; retry",
        ));
    }
    let leader = rt
        .leader_id()
        .and_then(|id| rt.members.lock().get(&id).cloned())
        .ok_or_else(|| error(StatusCode::SERVICE_UNAVAILABLE, "leader unavailable"))?;
    if leader.admin_addr.is_empty() {
        return Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "leader admin address unavailable",
        ));
    }
    let mut request = http_client(rt)
        .request(
            method,
            format!("{}{path}", leader.admin_addr.trim_end_matches('/')),
        )
        .header("x-raftkv-forwarded", "1");
    if let Some(auth) = headers.get("authorization") {
        request = request.header("authorization", auth);
    } else if let Some(cookie) = headers.get("cookie") {
        request = request.header("cookie", cookie);
    } else if let Some(token) = &rt.cfg.admin_token {
        request = request.bearer_auth(token);
    }
    if let Some(body) = body {
        request = request.json(&body);
    }
    let response = request
        .send()
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e))?;
    let status = response.status();
    let value: Value = response
        .json()
        .await
        .map_err(|e| error(StatusCode::BAD_GATEWAY, e))?;
    if !status.is_success() {
        return Err((status, Json(value)));
    }
    Ok(Some(value))
}
async fn read_json<F>(rt: &Arc<Runtime>, f: F) -> ApiResult
where
    F: FnOnce(&kv_state_machine::KvStateMachine) -> Result<Value, String> + Send + 'static,
{
    let handler = ClientHandler::new(rt.clone());
    match handler
        .linearizable_read(move |sm| match f(sm) {
            Ok(v) => RespFrame::Bulk(Some(v.to_string().into_bytes())),
            Err(e) => RespFrame::err(e),
        })
        .await
    {
        Ok(RespFrame::Bulk(Some(data))) => {
            Ok(Json(serde_json::from_slice(&data).map_err(|e| {
                error(StatusCode::INTERNAL_SERVER_ERROR, e)
            })?))
        }
        Ok(RespFrame::Error(e)) => Err(error(StatusCode::BAD_REQUEST, e)),
        _ => Err(error(
            StatusCode::SERVICE_UNAVAILABLE,
            "read failed or quorum unavailable",
        )),
    }
}
#[derive(Deserialize, Default)]
struct KeyQuery {
    cursor: Option<String>,
    limit: Option<usize>,
    pattern: Option<String>,
}
fn key_name(key: &[u8]) -> String {
    std::str::from_utf8(key)
        .ok()
        .filter(|s| !s.starts_with("~base64:"))
        .map(str::to_owned)
        .unwrap_or_else(|| format!("~base64:{}", STANDARD.encode(key)))
}
fn key_bytes(key: &str) -> Result<Vec<u8>, String> {
    match key.strip_prefix("~base64:") {
        Some(k) => STANDARD.decode(k).map_err(|e| e.to_string()),
        None => Ok(key.as_bytes().to_vec()),
    }
}
fn key_info(
    sm: &kv_state_machine::KvStateMachine,
    key: &[u8],
    detail: bool,
) -> Result<Value, String> {
    let value = sm
        .get(key)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "key does not exist".to_owned())?;
    let ttl = sm.ttl_ms(key).map_err(|e| e.to_string())?;
    let encoding = if std::str::from_utf8(&value).is_ok() {
        "utf8"
    } else {
        "base64"
    };
    let mut info = json!({"key":key_name(key),"type":if encoding=="utf8"{"string"}else{"binary"},"sizeBytes":value.len(),"ttlMs":if ttl<0{None}else{Some(ttl)}});
    if detail {
        info["encoding"] = json!(encoding);
        info["value"] = json!(if encoding == "utf8" {
            String::from_utf8(value).unwrap()
        } else {
            STANDARD.encode(value)
        });
    }
    Ok(info)
}
async fn keys(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Query(query): Query<KeyQuery>,
) -> ApiResult {
    let cursor = query.cursor.unwrap_or_else(|| "0".into());
    let limit = query.limit.unwrap_or(100);
    let pattern = query
        .pattern
        .filter(|p| !p.is_empty())
        .unwrap_or_else(|| "*".into());
    let path = format!(
        "/api/v1/keys?{}",
        url::form_urlencoded::Serializer::new(String::new())
            .append_pair("cursor", &cursor)
            .append_pair("limit", &limit.to_string())
            .append_pair("pattern", &pattern)
            .finish()
    );
    if let Some(value) = forward(&rt, &headers, reqwest::Method::GET, &path, None).await? {
        return Ok(Json(value));
    }
    read_json(&rt,move|sm|{let(next,keys)=sm.scan(&cursor,limit,&pattern).map_err(|e|e.to_string())?;let items=keys.iter().map(|key|key_info(sm,key,false)).collect::<Result<Vec<_>,_>>()?;Ok(json!({"cursor":if next=="0"{None}else{Some(next)},"items":items,"totalApproximate":sm.len()}))}).await
}
async fn key(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> ApiResult {
    if let Some(value) = forward(
        &rt,
        &headers,
        reqwest::Method::GET,
        &format!("/api/v1/keys/{}", urlencoding::encode(&key)),
        None,
    )
    .await?
    {
        return Ok(Json(value));
    }
    let key = key_bytes(&key).map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    read_json(&rt, move |sm| key_info(sm, &key, true)).await
}
async fn put_key(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Path(key): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult {
    if let Some(value) = forward(
        &rt,
        &headers,
        reqwest::Method::PUT,
        &format!("/api/v1/keys/{}", urlencoding::encode(&key)),
        Some(body.clone()),
    )
    .await?
    {
        return Ok(Json(value));
    }
    let key = key_bytes(&key).map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    let value = body["value"]
        .as_str()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "value must be a string"))?;
    let value = match body["encoding"].as_str().unwrap_or("utf8") {
        "utf8" => value.as_bytes().to_vec(),
        "hex" => hex::decode(value).map_err(|e| error(StatusCode::BAD_REQUEST, e))?,
        "base64" => STANDARD
            .decode(value)
            .map_err(|e| error(StatusCode::BAD_REQUEST, e))?,
        _ => return Err(error(StatusCode::BAD_REQUEST, "invalid encoding")),
    };
    let handler = ClientHandler::new(rt.clone());
    let expire_at_ms = body["ttlMs"]
        .as_u64()
        .map(|ttl| rt.logical_now().saturating_add(ttl));
    let result = handler
        .propose(kv_state_machine::Command::Set {
            key,
            value,
            expire_at_ms,
        })
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e))?;
    if let kv_state_machine::Response::Error(e) = result {
        return Err(error(StatusCode::BAD_REQUEST, e));
    }
    Ok(Json(json!({"ok":true})))
}
async fn delete_key(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> ApiResult {
    if let Some(value) = forward(
        &rt,
        &headers,
        reqwest::Method::DELETE,
        &format!("/api/v1/keys/{}", urlencoding::encode(&key)),
        None,
    )
    .await?
    {
        return Ok(Json(value));
    }
    let key = key_bytes(&key).map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    let result = ClientHandler::new(rt)
        .propose(kv_state_machine::Command::Del { keys: vec![key] })
        .await
        .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e))?;
    Ok(Json(json!({"result":result})))
}
async fn commands(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult {
    if body["command"].as_str().is_none() {
        return Err(error(StatusCode::BAD_REQUEST, "command is required"));
    }
    if let Some(target) = body["targetNodeId"].as_u64() {
        if target != rt.local_id() {
            if headers.contains_key("x-raftkv-targeted") {
                return Err(error(StatusCode::BAD_REQUEST, "invalid command target"));
            }
            let member = rt
                .members
                .lock()
                .get(&target)
                .cloned()
                .ok_or_else(|| error(StatusCode::NOT_FOUND, "unknown command target"))?;
            let mut request = http_client(&rt)
                .post(format!(
                    "{}/api/v1/commands",
                    member.admin_addr.trim_end_matches('/')
                ))
                .header("x-raftkv-targeted", "1")
                .json(&body);
            if let Some(auth) = headers.get("authorization") {
                request = request.header("authorization", auth);
            } else if let Some(cookie) = headers.get("cookie") {
                request = request.header("cookie", cookie);
            } else if let Some(token) = &rt.cfg.admin_token {
                request = request.bearer_auth(token);
            }
            let response = request
                .send()
                .await
                .map_err(|e| error(StatusCode::SERVICE_UNAVAILABLE, e))?;
            let status = response.status();
            let value = response
                .json::<Value>()
                .await
                .map_err(|e| error(StatusCode::BAD_GATEWAY, e))?;
            return if status.is_success() {
                Ok(Json(value))
            } else {
                Err((status, Json(value)))
            };
        }
    } else if let Some(value) = forward(
        &rt,
        &headers,
        reqwest::Method::POST,
        "/api/v1/commands",
        Some(body.clone()),
    )
    .await?
    {
        return Ok(Json(value));
    }
    let args = shell_words::split(body["command"].as_str().unwrap())
        .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    if args
        .first()
        .is_some_and(|s| s.eq_ignore_ascii_case("FLUSHDB"))
        && body["confirmed"] != true
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "FLUSHDB requires confirmed=true",
        ));
    }
    let frame = RespFrame::Array(
        args.iter()
            .map(|s| RespFrame::Bulk(Some(s.as_bytes().to_vec())))
            .collect(),
    );
    let parsed = resp_server::commands::parse(&frame);
    let start = Instant::now();
    let handler = Arc::new(ClientHandler::new(rt.clone()));
    let response = dispatch(handler.clone(), parsed, rt.logical_now()).await;
    let mut bytes = bytes::BytesMut::new();
    tokio_util::codec::Encoder::encode(&mut RespCodec, response.clone(), &mut bytes)
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    let after = rt.status();
    Ok(Json(
        json!({"raw":String::from_utf8_lossy(&bytes),"display":display_frame(&response),"success":!matches!(response,RespFrame::Error(_)),"durationMs":start.elapsed().as_secs_f64()*1000.0,"execution":handler.execution.lock().clone().unwrap_or_else(||json!({"receivedByNodeId":rt.local_id(),"leaderId":after["leaderId"],"term":after["term"]}))}),
    ))
}
fn display_frame(frame: &RespFrame) -> String {
    match frame {
        RespFrame::Simple(s) | RespFrame::Error(s) => s.clone(),
        RespFrame::Integer(n) => n.to_string(),
        RespFrame::Bulk(None) => "(nil)".into(),
        RespFrame::Bulk(Some(b)) => std::str::from_utf8(b)
            .map(str::to_owned)
            .unwrap_or_else(|_| format!("base64:{}", STANDARD.encode(b))),
        RespFrame::Array(items) => items
            .iter()
            .enumerate()
            .map(|(i, f)| format!("{}) {}", i + 1, display_frame(f)))
            .collect::<Vec<_>>()
            .join("\n"),
    }
}
async fn backup(State(rt): State<Arc<Runtime>>, headers: HeaderMap) -> Response {
    if !rt.is_leader() {
        if headers.contains_key("x-raftkv-forwarded") {
            return error(StatusCode::SERVICE_UNAVAILABLE, "leader changed; retry").into_response();
        }
        let Some(member) = rt
            .leader_id()
            .and_then(|id| rt.members.lock().get(&id).cloned())
        else {
            return error(StatusCode::SERVICE_UNAVAILABLE, "leader unavailable").into_response();
        };
        let mut request = http_client(&rt)
            .get(format!(
                "{}/api/v1/admin/backup",
                member.admin_addr.trim_end_matches('/')
            ))
            .header("x-raftkv-forwarded", "1");
        if let Some(auth) = headers.get("authorization") {
            request = request.header("authorization", auth);
        } else if let Some(cookie) = headers.get("cookie") {
            request = request.header("cookie", cookie);
        } else if let Some(token) = &rt.cfg.admin_token {
            request = request.bearer_auth(token);
        }
        return match request.send().await {
            Ok(response) => {
                let status = response.status();
                match response.bytes().await {
                    Ok(bytes) => (
                        status,
                        [
                            (
                                "content-type",
                                if status.is_success() {
                                    "application/octet-stream"
                                } else {
                                    "application/json"
                                },
                            ),
                            (
                                "content-disposition",
                                "attachment; filename=raftkv-backup.rkv",
                            ),
                        ],
                        bytes,
                    )
                        .into_response(),
                    Err(e) => error(StatusCode::BAD_GATEWAY, e).into_response(),
                }
            }
            Err(e) => error(StatusCode::SERVICE_UNAVAILABLE, e).into_response(),
        };
    }
    if let Err(e) = read_json(&rt, |_| Ok(json!(null))).await {
        return e.into_response();
    }
    match rt.backup() {
        Ok(bytes) => (
            [
                (axum::http::header::CONTENT_TYPE, "application/octet-stream"),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    "attachment; filename=backup.rkv",
                ),
            ],
            bytes,
        )
            .into_response(),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e).into_response(),
    }
}
async fn snapshots(State(rt): State<Arc<Runtime>>) -> Json<Value> {
    Json(rt.snapshots())
}
async fn snapshot(State(rt): State<Arc<Runtime>>) -> ApiResult {
    rt.trigger_snapshot()
        .map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, e))?;
    Ok(Json(json!({"accepted":true})))
}
async fn leadership(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult {
    if let Some(value) = forward(
        &rt,
        &headers,
        reqwest::Method::POST,
        "/api/v1/admin/leadership",
        Some(body.clone()),
    )
    .await?
    {
        return Ok(Json(value));
    }
    rt.transfer(
        body["targetId"]
            .as_u64()
            .ok_or_else(|| error(StatusCode::BAD_REQUEST, "targetId is required"))?,
    )
    .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(json!({"accepted":true})))
}
async fn add_member(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> ApiResult {
    if let Some(value) = forward(
        &rt,
        &headers,
        reqwest::Method::POST,
        "/api/v1/admin/members",
        Some(body.clone()),
    )
    .await?
    {
        return Ok(Json(value));
    }
    let member = raft_core::config::Member {
        id: body["id"].as_u64().unwrap_or(0),
        raft_addr: body["raftAddress"].as_str().unwrap_or("").into(),
        client_addr: body["clientAddress"].as_str().unwrap_or("").into(),
        admin_addr: body["adminAddress"].as_str().unwrap_or("").into(),
        learner: body["role"] == "learner",
        certificate_sha256: body["certificateSha256"].as_str().map(str::to_owned),
    };
    rt.member_change(member)
        .await
        .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(json!({"accepted":true})))
}
async fn remove_member(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
    Path(id): Path<u64>,
) -> ApiResult {
    if let Some(value) = forward(
        &rt,
        &headers,
        reqwest::Method::DELETE,
        &format!("/api/v1/admin/members/{id}"),
        None,
    )
    .await?
    {
        return Ok(Json(value));
    }
    rt.remove_member(id)
        .await
        .map_err(|e| error(StatusCode::BAD_REQUEST, e))?;
    Ok(Json(json!({"accepted":true})))
}
async fn events(
    State(rt): State<Arc<Runtime>>,
    headers: HeaderMap,
) -> Sse<impl futures::Stream<Item = Result<Event, Infallible>>> {
    let receiver = rt.events.subscribe();
    let after = headers
        .get("last-event-id")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let history = rt
        .event_history
        .lock()
        .iter()
        .filter(|e| e["seq"].as_u64().unwrap_or(0) > after)
        .cloned()
        .collect::<Vec<_>>();
    let initial = futures::stream::iter(history.into_iter().map(|e| {
        Ok(Event::default()
            .id(e["seq"].to_string())
            .data(e.to_string()))
    }));
    let live = futures::stream::unfold(receiver, |mut receiver| async move {
        match receiver.recv().await {
            Ok(e) => Some((
                Ok(Event::default()
                    .id(e["seq"].to_string())
                    .data(e.to_string())),
                receiver,
            )),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(count)) => Some((
                Ok(Event::default()
                    .event("gap")
                    .data(json!({"dropped":count}).to_string())),
                receiver,
            )),
            Err(_) => None,
        }
    });
    Sse::new(initial.chain(live)).keep_alive(KeepAlive::new().interval(Duration::from_secs(15)))
}
