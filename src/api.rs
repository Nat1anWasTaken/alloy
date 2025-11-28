use crate::app_state::AppState;
use crate::document::{close_document, get_or_create_doc};
use crate::error::AppError;
use crate::persistence::{ClientId, DocumentId, SessionRecord, SnapshotRecord, UserId};
use crate::session::peer;
use axum::extract::ws::WebSocketUpgrade;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::Level;
use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, Transact, Update};

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
        .route(
            "/api/documents/{doc_id}",
            get(get_document_content).delete(delete_document),
        )
        .route(
            "/api/documents/{doc_id}/snapshots",
            get(list_snapshots_handler),
        )
        .route(
            "/api/documents/{doc_id}/snapshots/latest",
            get(get_latest_snapshot_handler),
        )
        .route(
            "/api/documents/{doc_id}/snapshots/{base_seq}",
            get(get_snapshot_handler),
        )
        .route(
            "/api/documents/{doc_id}/sessions",
            get(list_sessions_handler),
        )
        .route(
            "/api/documents/{doc_id}/sessions/{client_id}",
            get(get_session_handler),
        )
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

// ---------- Snapshot APIs ----------

#[derive(Debug, Deserialize)]
struct SnapshotListQuery {
    cursor: Option<i64>,
    limit: Option<usize>,
    since_seq: Option<i64>,
    max_seq: Option<i64>,
    #[serde(default)]
    tag: OneOrMany<String>,
}

#[derive(Debug, Serialize)]
struct SnapshotResponse {
    base_seq: i64,
    tags: Vec<String>,
    snapshot: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
#[serde(untagged)]
enum OneOrMany<T> {
    One(T),
    Many(Vec<T>),
    #[serde(skip)]
    #[default]
    Empty,
}

impl<T> OneOrMany<T> {
    fn into_vec(self) -> Vec<T> {
        match self {
            OneOrMany::One(v) => vec![v],
            OneOrMany::Many(v) => v,
            OneOrMany::Empty => Vec::new(),
        }
    }
}

#[derive(Debug, Serialize)]
struct SnapshotListResponse {
    snapshots: Vec<SnapshotResponse>,
    next_cursor: Option<i64>,
}

async fn list_snapshots_handler(
    Path(doc_id): Path<DocumentId>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<SnapshotListQuery>,
) -> Result<Json<SnapshotListResponse>, AppError> {
    let desired = query.limit.unwrap_or(20);
    if desired == 0 {
        return Ok(Json(SnapshotListResponse {
            snapshots: Vec::new(),
            next_cursor: None,
        }));
    }

    let tags: HashSet<String> = query.tag.into_vec().into_iter().collect();
    let mut collected = Vec::new();
    let mut scan_cursor = query.cursor;
    let mut next_cursor = None;

    while collected.len() < desired {
        let page = state
            .store
            .list_snapshots(doc_id, scan_cursor, desired)
            .await?;

        for snap in &page.snapshots {
            if snapshot_matches(snap, query.since_seq, query.max_seq, &tags) {
                collected.push(to_snapshot_response(snap));
                if collected.len() == desired {
                    break;
                }
            }
        }

        if let Some(cursor) = page.next_cursor {
            scan_cursor = Some(cursor);
            next_cursor = Some(cursor);
        } else {
            next_cursor = None;
            break;
        }
    }

    Ok(Json(SnapshotListResponse {
        snapshots: collected,
        next_cursor,
    }))
}

fn snapshot_matches(
    snap: &SnapshotRecord,
    since_seq: Option<i64>,
    max_seq: Option<i64>,
    tags: &HashSet<String>,
) -> bool {
    if let Some(min) = since_seq && snap.base_seq < min {
        return false;
    }
    if let Some(max) = max_seq && snap.base_seq > max {
        return false;
    }

    if tags.is_empty() {
        return true;
    }

    let snap_tags: HashSet<&str> = snap.tags.iter().map(|t| t.0.as_str()).collect();
    tags.iter().all(|t| snap_tags.contains(t.as_str()))
}

fn to_snapshot_response(snap: &SnapshotRecord) -> SnapshotResponse {
    SnapshotResponse {
        base_seq: snap.base_seq,
        tags: snap.tags.iter().map(|t| t.0.clone()).collect(),
        snapshot: BASE64.encode(&snap.snapshot.0),
    }
}

#[derive(Debug, Deserialize)]
struct SnapshotPath {
    doc_id: DocumentId,
    base_seq: i64,
}

async fn get_snapshot_handler(
    Path(path): Path<SnapshotPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<SnapshotResponse>, AppError> {
    let snap = state
        .store
        .load_snapshot(path.doc_id, path.base_seq)
        .await?;
    match snap {
        Some(s) => Ok(Json(to_snapshot_response(&s))),
        None => Err(AppError::NotFound("snapshot not found".to_string())),
    }
}

#[derive(Debug, Deserialize)]
struct LatestSnapshotQuery {
    #[serde(default)]
    tag: OneOrMany<String>,
}

async fn get_latest_snapshot_handler(
    Path(doc_id): Path<DocumentId>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<LatestSnapshotQuery>,
) -> Result<Json<SnapshotResponse>, AppError> {
    let tags: HashSet<String> = query.tag.into_vec().into_iter().collect();

    // Walk pages to find the latest snapshot that matches tags (ascending storage).
    let mut cursor = None;
    let mut latest_match: Option<SnapshotRecord> = None;
    loop {
        let page = state.store.list_snapshots(doc_id, cursor, 50).await?;
        for snap in &page.snapshots {
            if snapshot_matches(snap, None, None, &tags) {
                latest_match = Some(snap.clone());
            }
        }
        if let Some(next) = page.next_cursor {
            cursor = Some(next);
        } else {
            break;
        }
    }

    if let Some(snap) = latest_match {
        Ok(Json(to_snapshot_response(&snap)))
    } else {
        Err(AppError::NotFound("snapshot not found".to_string()))
    }
}

// ---------- Session APIs ----------

#[derive(Debug, Deserialize)]
struct SessionListQuery {
    cursor: Option<i64>,
    limit: Option<usize>,
    user_id: Option<String>,
    client_id: Option<u64>,
    min_seq: Option<i64>,
    max_seq: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SessionResponse {
    seq: i64,
    client_id: u64,
    user_id: String,
}

#[derive(Debug, Serialize)]
struct SessionListResponse {
    sessions: Vec<SessionResponse>,
    next_cursor: Option<i64>,
}

async fn list_sessions_handler(
    Path(doc_id): Path<DocumentId>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<SessionListQuery>,
) -> Result<Json<SessionListResponse>, AppError> {
    let desired = query.limit.unwrap_or(20);
    if desired == 0 {
        return Ok(Json(SessionListResponse {
            sessions: Vec::new(),
            next_cursor: None,
        }));
    }

    let mut collected = Vec::new();
    let mut scan_cursor = query.cursor;
    let mut next_cursor = None;

    while collected.len() < desired {
        let page = state
            .store
            .list_sessions(doc_id, scan_cursor, desired)
            .await?;

        for session in &page.sessions {
            if session_matches(session, &query) {
                collected.push(SessionResponse {
                    seq: session.seq,
                    client_id: session.client.0,
                    user_id: session.user.0.clone(),
                });
                if collected.len() == desired {
                    break;
                }
            }
        }

        if let Some(cursor) = page.next_cursor {
            scan_cursor = Some(cursor);
            next_cursor = Some(cursor);
        } else {
            next_cursor = None;
            break;
        }
    }

    Ok(Json(SessionListResponse {
        sessions: collected,
        next_cursor,
    }))
}

fn session_matches(session: &SessionRecord, query: &SessionListQuery) -> bool {
    if let Some(user) = &query.user_id && &session.user.0 != user {
        return false;
    }
    if let Some(client) = query.client_id && session.client.0 != client {
        return false;
    }
    if let Some(min) = query.min_seq && session.seq < min {
        return false;
    }
    if let Some(max) = query.max_seq && session.seq > max {
        return false;
    }
    true
}

#[derive(Debug, Deserialize)]
struct SessionPath {
    doc_id: DocumentId,
    client_id: u64,
}

async fn get_session_handler(
    Path(path): Path<SessionPath>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<SessionResponse>, AppError> {
    let client = ClientId(path.client_id);
    let user = state.store.get_session(path.doc_id, client).await?;

    if let Some(user_id) = user {
        // Try to surface sequence if available by scanning sessions list.
        let seq = find_session_seq(state.clone(), path.doc_id, client).await?;
        return Ok(Json(SessionResponse {
            seq: seq.unwrap_or(0),
            client_id: path.client_id,
            user_id: user_id.0,
        }));
    }

    Err(AppError::NotFound("session not found".to_string()))
}

async fn find_session_seq(
    state: Arc<AppState>,
    doc_id: DocumentId,
    client: ClientId,
) -> Result<Option<i64>, AppError> {
    let mut cursor = None;
    loop {
        let page = state.store.list_sessions(doc_id, cursor, 50).await?;
        for s in &page.sessions {
            if s.client == client {
                return Ok(Some(s.seq));
            }
        }
        if let Some(next) = page.next_cursor {
            cursor = Some(next);
        } else {
            return Ok(None);
        }
    }
}

// ---------- Document content & deletion ----------

#[derive(Debug, Serialize)]
struct DocumentContentResponse {
    doc_id: u64,
    content: String,
    seq: i64,
}

async fn get_document_content(
    Path(doc_id): Path<DocumentId>,
    State(state): State<Arc<AppState>>,
) -> Result<Json<DocumentContentResponse>, AppError> {
    let (doc, last_seq) = load_document_from_store(state.clone(), doc_id).await?;
    let text = doc.get_or_insert_text("content");
    let txn = doc.transact();
    let content = text.get_string(&txn);

    Ok(Json(DocumentContentResponse {
        doc_id: doc_id.into(),
        content,
        seq: last_seq,
    }))
}

async fn load_document_from_store(
    state: Arc<AppState>,
    doc_id: DocumentId,
) -> Result<(Doc, i64), AppError> {
    let doc = Doc::new();
    let mut last_seq = 0;

    if let Some(snapshot) = state.store.load_latest_snapshot(doc_id).await? {
        let update = Update::decode_v1(&snapshot.snapshot.0)?;
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
        last_seq = snapshot.base_seq;
    }

    let updates = state.store.load_updates_since(doc_id, last_seq).await?;
    for upd in updates {
        let update = Update::decode_v1(&upd.bytes.0)?;
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
        last_seq = upd.seq;
    }

    Ok((doc, last_seq))
}

async fn delete_document(
    Path(doc_id): Path<DocumentId>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    if let Some(active) = state
        .docs
        .read()
        .await
        .get(&doc_id)
        .and_then(|w| w.upgrade())
        && active.peers.load(Ordering::SeqCst) > 0
    {
        return Err(AppError::InvalidInput(
            "cannot delete document with active peers".to_string(),
        ));
    }

    // Stop recorder and remove from in-memory map if present.
    close_document(state.clone(), doc_id).await?;

    // Delete persisted state atomically.
    state.store.delete_document(doc_id).await?;

    Ok(StatusCode::NO_CONTENT)
}
