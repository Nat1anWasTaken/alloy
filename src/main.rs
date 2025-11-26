use alloy::app_state::AppState;
use alloy::document::{get_or_create_doc, peer};
use alloy::error::AppError;
use alloy::persistence::{DocumentId, MemoryStore, UserId};
use axum::extract::ws::WebSocketUpgrade;
use axum::{
    Router,
    extract::{Path, Query, State},
    response::IntoResponse,
    routing::get,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::trace::{DefaultOnRequest, DefaultOnResponse, TraceLayer};
use tracing::{Level, info};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), AppError> {
    setup_tracing();

    let store = Arc::new(MemoryStore::default());
    let state = Arc::new(AppState::with_store(store));

    let app = Router::new()
        .route("/ws/{doc_id}", get(ws_handler))
        .layer(
            TraceLayer::new_for_http()
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO)),
        )
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:3000".parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("starting yrs-axum server on ws://{addr}/ws/{{doc_id}}");
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
    Path(doc_id): Path<Uuid>,
    Query(params): Query<WsParams>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let doc_id = DocumentId(doc_id);
    let user = UserId(params.user.unwrap_or_else(|| "anonymous".to_string()));
    let doc = get_or_create_doc(state.clone(), doc_id).await?;
    Ok(ws.on_upgrade(move |socket| peer(socket, doc, doc_id, state, user)))
}

#[derive(Deserialize, Default)]
struct WsParams {
    user: Option<String>,
}
