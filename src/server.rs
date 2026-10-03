//! REST + WebSocket API.
//!
//! Reads never touch the engine thread: `/v1/state` and `/v1/oracle` clone an
//! `Arc` out of a `watch` channel. Writes go through the bounded engine
//! queue, and a full queue is a fast `503`, not an unbounded wait — under
//! load, shedding at the edge is what keeps the engine's latency flat.

use crate::engine::Command;
use crate::service::{EngineHandle, OracleView};
use crate::types::AccountId;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};

#[derive(Clone)]
pub struct AppState {
    pub engine: EngineHandle,
    pub oracle: watch::Receiver<Arc<OracleView>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/v1/state", get(engine_state))
        .route("/v1/oracle", get(oracle_state))
        .route("/v1/accounts/{id}", get(account))
        .route("/v1/commands", post(command))
        .route("/v1/stream", get(stream))
        .with_state(state)
}

async fn engine_state(State(s): State<AppState>) -> Response {
    let snap = s.engine.snapshot.borrow().clone();
    Json(snap.as_ref()).into_response()
}

async fn oracle_state(State(s): State<AppState>) -> Response {
    let view = s.oracle.borrow().clone();
    Json(view.as_ref()).into_response()
}

async fn account(State(s): State<AppState>, Path(id): Path<u64>) -> Response {
    match tokio::time::timeout(Duration::from_secs(1), s.engine.account(AccountId(id))).await {
        Ok(Some(v)) => Json(v).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(_) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
    }
}

async fn command(State(s): State<AppState>, Json(cmd): Json<Command>) -> Response {
    // Markets and marks are operator/oracle inputs, never client inputs.
    if matches!(cmd, Command::CreateMarket(_) | Command::Mark { .. }) {
        return (StatusCode::FORBIDDEN, "command not accepted from clients").into_response();
    }
    match tokio::time::timeout(Duration::from_secs(1), s.engine.submit(cmd)).await {
        Ok(Some(events)) => Json(events).into_response(),
        Ok(None) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
        Err(_) => (StatusCode::SERVICE_UNAVAILABLE, "engine queue full").into_response(),
    }
}

async fn stream(State(s): State<AppState>, ws: WebSocketUpgrade) -> Response {
    let rx = s.engine.events.subscribe();
    ws.on_upgrade(move |socket| pump(socket, rx))
}

/// A slow client must not hold back anyone else: the broadcast channel is
/// bounded, and a receiver that falls behind is told how many batches it
/// missed rather than buffering without limit.
async fn pump(mut socket: WebSocket, mut rx: broadcast::Receiver<Arc<str>>) {
    loop {
        let msg = match rx.recv().await {
            Ok(json) => Message::text(json.as_ref()),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                Message::text(format!(r#"{{"type":"lagged","skipped_batches":{n}}}"#))
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        if socket.send(msg).await.is_err() {
            return;
        }
    }
}
