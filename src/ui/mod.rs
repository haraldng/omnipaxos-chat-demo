//! Serves the browser UI. Deliberately *not* a replica: killing any node must
//! not take the page down. The browser talks to the nodes directly (CORS);
//! kill/restart go through this server's [`Supervisor`] when there is one.

use std::{net::SocketAddr, sync::Arc};

use axum::{
    extract::{Path, State},
    http::{header::CONTENT_TYPE, StatusCode},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde_json::json;
use tracing::info;

use crate::server::supervisor::Supervisor;

// The page, embedded so the binary is self-contained.
const INDEX: &str = include_str!("index.html");
const STYLE: &str = include_str!("style.css");
const APP: &str = include_str!("app.js");

#[derive(Clone)]
struct App {
    config: serde_json::Value,
    supervisor: Option<Arc<dyn Supervisor>>,
}

/// `nodes` = (node id, HTTP base URL) for every replica.
pub async fn serve(
    addr: SocketAddr,
    nodes: Vec<(u64, String)>,
    supervisor: Option<Arc<dyn Supervisor>>,
) -> anyhow::Result<()> {
    let config = json!({
        "nodes": nodes.iter().map(|(id, url)| json!({"id": id, "url": url})).collect::<Vec<_>>(),
        "supervisor": supervisor.is_some(),
    });
    let app = Router::new()
        .route("/", get(|| async { Html(INDEX) }))
        .route(
            "/style.css",
            get(|| async { ([(CONTENT_TYPE, "text/css")], STYLE) }),
        )
        .route(
            "/app.js",
            get(|| async { ([(CONTENT_TYPE, "text/javascript")], APP) }),
        )
        .route(
            "/config.json",
            get(|State(app): State<App>| async move { Json(app.config) }),
        )
        .route("/api/nodes/:id/kill", post(kill))
        .route("/api/nodes/:id/restart", post(restart))
        .with_state(App { config, supervisor });
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!("UI on http://{addr}");
    axum::serve(listener, app).await?;
    Ok(())
}

fn no_supervisor() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "this UI server does not manage the nodes"})),
    )
        .into_response()
}

async fn kill(State(app): State<App>, Path(id): Path<u64>) -> Response {
    let Some(s) = app.supervisor else {
        return no_supervisor();
    };
    match s.kill(id).await {
        Ok(at) => {
            Json(json!({"node_id": id, "status": "killed", "aborting_at_ms": at})).into_response()
        }
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("{e:#}")})),
        )
            .into_response(),
    }
}

async fn restart(State(app): State<App>, Path(id): Path<u64>) -> Response {
    let Some(s) = app.supervisor else {
        return no_supervisor();
    };
    match s.restart(id).await {
        Ok(()) => Json(json!({"node_id": id, "status": "restarting"})).into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": format!("{e:#}")})),
        )
            .into_response(),
    }
}
