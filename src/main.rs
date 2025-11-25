use alloy::document::{ActiveDocument, AppState, get_or_create_doc};
use alloy::error::AppError;
use axum::{
    Router,
    extract::{
        Path, State,
        ws::{WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower_http::trace::{TraceLayer, DefaultOnRequest, DefaultOnResponse};
use tracing::{info, Level};
use uuid::Uuid;
use yrs_axum::ws::{AxumSink, AxumStream};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    setup_tracing();

    let state = Arc::new(AppState::new());

    let app = Router::new()
        .route("/ws/{doc_id}", get(ws_handler))
        .layer(
            TraceLayer::new_for_http()
                .on_request(DefaultOnRequest::new().level(Level::INFO))
                .on_response(DefaultOnResponse::new().level(Level::INFO))
        )
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:3000".parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("starting yrs-axum server on ws://{addr}/ws/{{doc_id}}");

    axum::serve(listener, app).await?;

    Ok(())
}

fn setup_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,axum::rejection=trace".into()),
        )
        .init();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(doc_id): Path<Uuid>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, AppError> {
    let doc = get_or_create_doc(state.clone(), doc_id).await?;
    Ok(ws.on_upgrade(move |socket| peer(socket, doc, doc_id)))
}

async fn peer(ws: WebSocket, active_doc: Arc<ActiveDocument>, doc_id: Uuid) {
    info!("Peer connected to {}", doc_id);

    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);

    let sub = active_doc.bcast.subscribe(sink, stream);
    match sub.completed().await {
        Ok(_) => info!("Peer finished for {}", doc_id),
        Err(err) => tracing::error!("Peer aborted for {}: {}", doc_id, err),
    }
}
