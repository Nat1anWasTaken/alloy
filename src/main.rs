use alloy::api::{IssueTicketRequest, IssueTicketResponse, TicketQuery};
use alloy::app_state::AppState;
use alloy::document::get_or_create_doc;
use alloy::error::AppError;
use alloy::persistence::{DocumentId, MemoryStore, UserId};
use alloy::session::{TicketIssuer, peer};
use axum::extract::ws::WebSocketUpgrade;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::{get, post},
};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{Level, info};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    setup_tracing();

    let addr: SocketAddr = "0.0.0.0:3000".parse()?;
    let listener = TcpListener::bind(addr).await?;
    let bound_addr = listener.local_addr()?;

    let store = Arc::new(MemoryStore::default());
    let ticketing = TicketIssuer::from_env_or_generate();
    let state = Arc::new(AppState::with_components(store, ticketing));

    let app = Router::new()
        .route("/api/documents/{doc_id}/ticket", post(issue_ticket))
        .route("/edit", get(ws_handler))
        .layer(
            TraceLayer::new_for_http()
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state);

    info!(
        "starting yrs-axum server on {}; websocket endpoint /edit",
        bound_addr
    );
    info!("logging is configured and working - connect a client to see document logs");

    axum::serve(listener, app).await?;

    Ok(())
}

fn setup_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| "debug,alloy=trace,axum::rejection=trace".into());

    eprintln!("Tracing filter: {}", filter);

    tracing_subscriber::fmt().with_env_filter(filter).init();
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
