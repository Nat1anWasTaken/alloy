use crate::app_state::AppState;
use crate::document::get_or_create_doc;
use crate::error::AppError;
use crate::persistence::{DocumentId, UserId};
use crate::session::peer;
use axum::extract::ws::WebSocketUpgrade;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::Level;

/// Request payload for issuing a document session ticket.
#[derive(Debug, Deserialize, Serialize)]
pub struct IssueTicketRequest {
    pub user_id: String,
}

/// Response payload containing the issued ticket.
#[derive(Debug, Deserialize, Serialize)]
pub struct IssueTicketResponse {
    pub ticket: String,
    pub expires_at: i64,
}

/// Query parameters expected by the WebSocket entrypoint.
#[derive(Debug, Deserialize)]
pub struct TicketQuery {
    pub ticket: Option<String>,
}

/// Build the HTTP router with all public endpoints.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/documents/{doc_id}/ticket", post(issue_ticket))
        .route("/edit", get(ws_handler))
        .layer(
            TraceLayer::new_for_http()
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state)
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Query(params): Query<TicketQuery>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let ticket = params
        .ticket
        .ok_or_else(|| AppError::InvalidTicket("ticket query parameter required".to_string()))?;

    let subject = state.ticketing.validate(&ticket)?;
    let doc_id = subject.doc_id;
    let user = subject.user_id;

    let doc = get_or_create_doc(state.clone(), doc_id).await?;
    Ok(ws.on_upgrade(move |socket| peer(socket, doc, doc_id, state, user)))
}

async fn issue_ticket(
    Path(doc_id): Path<DocumentId>,
    State(state): State<Arc<AppState>>,
    Json(payload): Json<IssueTicketRequest>,
) -> Result<Json<IssueTicketResponse>, AppError> {
    let user = UserId(payload.user_id);
    let issued = state.ticketing.issue(doc_id, &user)?;

    Ok(Json(IssueTicketResponse {
        ticket: issued.token,
        expires_at: issued.expires_at,
    }))
}
