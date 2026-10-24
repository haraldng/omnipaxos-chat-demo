//! Minimal HTTP API. Every replica serves reads from its own folded transcript.

use std::{convert::Infallible, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    extract::State,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use futures::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::chat::{now_millis, Node, PostOutcome};

#[derive(Clone)]
struct App {
    node: Arc<Node>,
    /// Present only in single-binary mode, where "crash" is simulated.
    crash: Option<Arc<dyn Fn() + Send + Sync>>,
}

#[derive(Deserialize)]
struct ChatRequest {
    content: String,
    client_msg_id: Option<String>,
    /// How long to wait for the assistant reply before returning 202.
    wait_ms: Option<u64>,
}

pub fn router(node: Arc<Node>, crash: Option<Arc<dyn Fn() + Send + Sync>>) -> Router {
    let mut r = Router::new()
        .route("/v1/chat", post(post_chat).get(get_chat))
        .route("/v1/stream", get(stream_draft))
        .route("/admin/status", get(status));
    if crash.is_some() {
        r = r.route("/admin/crash", post(crash_node));
    }
    // The UI is served by a separate (non-replica) process, so it is cross-origin.
    r.with_state(App { node, crash })
        .layer(CorsLayer::permissive())
}

/// Serve until the node crashes.
pub async fn serve(
    addr: SocketAddr,
    node: Arc<Node>,
    crash: Option<Arc<dyn Fn() + Send + Sync>>,
) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(node = node.pid(), "HTTP API on http://{addr}");
    // A crash is a fail-stop: stop listening immediately (not gracefully), so
    // the port is free for a restarted node.
    let crashed = node.crashed();
    tokio::select! {
        r = axum::serve(listener, router(node, crash)) => r?,
        _ = crashed => {}
    }
    Ok(())
}

async fn post_chat(State(app): State<App>, Json(req): Json<ChatRequest>) -> Response {
    if req.content.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "empty content"})),
        )
            .into_response();
    }
    let s = &app.node.shared;
    let wait = Duration::from_millis(req.wait_ms.unwrap_or(30_000));
    match s.post(req.content, req.client_msg_id, wait).await {
        Ok(PostOutcome::Replied { user, reply }) => Json(json!({
            "node_id": s.pid,
            "msg_id": user.msg_id,
            "user": user,
            "reply": reply,
        }))
        .into_response(),
        Ok(PostOutcome::Pending { msg_id }) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "node_id": s.pid,
                "msg_id": msg_id,
                "status": "pending",
                "hint": "poll GET /v1/chat",
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"node_id": s.pid, "error": format!("{e:#}")})),
        )
            .into_response(),
    }
}

async fn get_chat(State(app): State<App>) -> Json<serde_json::Value> {
    let s = &app.node.shared;
    let leader = s.leader_state();
    let (messages, decided_len) = s.read(|t| (t.messages(), t.applied_len()));
    Json(json!({
        "node_id": s.pid,
        "leader": leader.leader,
        "decided_len": decided_len,
        "messages": messages,
    }))
}

async fn status(State(app): State<App>) -> Json<serde_json::Value> {
    let s = &app.node.shared;
    let leader = s.leader_state();
    let decided_idx = s.log.decided_idx().await;
    let (applied_len, turns, unanswered, dups) = s.read(|t| {
        (
            t.applied_len(),
            t.turns().len(),
            t.turns().iter().filter(|x| x.reply.is_none()).count(),
            t.duplicates_ignored(),
        )
    });
    Json(json!({
        "node_id": s.pid,
        "leader": leader.leader,
        "is_leader": leader.is_ready_leader(s.pid),
        "accepted": leader.accepted,
        "leader_since_ms": leader.since_ms,
        "decided_idx": decided_idx,
        "decided_len": applied_len,
        "msg_count": turns + (turns - unanswered),
        "unanswered": unanswered,
        "duplicates_ignored": dups,
        "model": s.engine.model(),
    }))
}

/// Server-sent events with this node's in-progress reply (JSON `Draft` or
/// `null`). Only the leader ever has one. Ends when the node crashes.
async fn stream_draft(
    State(app): State<App>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let drafts = tokio_stream::wrappers::WatchStream::new(app.node.shared.draft.subscribe())
        .map(|d| Ok(Event::default().json_data(&d).expect("draft serializes")))
        .take_until(app.node.crashed());
    Sse::new(drafts).keep_alive(KeepAlive::default())
}

async fn crash_node(State(app): State<App>) -> Json<serde_json::Value> {
    let pid = app.node.pid();
    // Reply first, then die. `aborting_at_ms` lets clients time the failover.
    const GRACE_MS: u64 = 50;
    let aborting_at_ms = now_millis() + GRACE_MS;
    if let Some(crash) = &app.crash {
        let crash = crash.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(GRACE_MS)).await;
            crash();
        });
    }
    Json(json!({"node_id": pid, "status": "crashing", "aborting_at_ms": aborting_at_ms}))
}
