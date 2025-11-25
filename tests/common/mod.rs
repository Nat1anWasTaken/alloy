use alloy::document::{get_or_create_doc, ActiveDocument, AppState};
use axum::extract::ws::{WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use futures_util::StreamExt;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message, WebSocketStream};
use uuid::Uuid;
use yrs::{Doc, GetString, ReadTxn, StateVector, Transact};
use yrs::types::Text;
use yrs_axum::ws::{AxumSink, AxumStream};

/// Spawn a test server on a random port and return the address and app state
pub async fn spawn_test_server() -> (SocketAddr, Arc<AppState>) {
    init_test_tracing();

    let state = Arc::new(AppState::new());
    let app = create_test_router(state.clone());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    (addr, state)
}

/// Create the test router with the same structure as main.rs
fn create_test_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/ws/{doc_id}", get(test_ws_handler))
        .with_state(state)
}

/// WebSocket handler for tests (mirrors main.rs implementation)
async fn test_ws_handler(
    ws: WebSocketUpgrade,
    Path(doc_id): Path<Uuid>,
    State(state): State<Arc<AppState>>,
) -> Result<impl IntoResponse, alloy::error::AppError> {
    let doc = get_or_create_doc(state, doc_id).await?;
    Ok(ws.on_upgrade(move |socket| test_peer(socket, doc, doc_id)))
}

/// Peer handler for tests (mirrors main.rs implementation)
async fn test_peer(ws: WebSocket, active_doc: Arc<ActiveDocument>, doc_id: Uuid) {
    tracing::info!("Test peer connected to {}", doc_id);

    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);

    let sub = active_doc.bcast.subscribe(sink, stream);
    match sub.completed().await {
        Ok(_) => tracing::info!("Test peer finished for {}", doc_id),
        Err(err) => tracing::error!("Test peer aborted for {}: {}", doc_id, err),
    }
}

/// Connect to a document and return the WebSocket stream
pub async fn connect_to_doc(
    addr: SocketAddr,
    doc_id: Uuid,
) -> WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>> {
    let url = format!("ws://{}/ws/{}", addr, doc_id);
    let (ws_stream, _response) = connect_async(&url).await.unwrap();
    ws_stream
}

/// Create a Yrs update with the given text content
pub fn create_yrs_update(text: &str) -> Vec<u8> {
    let doc = Doc::new();
    let text_type = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text_type.insert(&mut txn, 0, text);
    }

    let txn = doc.transact();
    txn.encode_state_as_update_v1(&StateVector::default())
}

/// Create a Yrs Doc with the given text content
pub fn create_text_doc(content: &str) -> Doc {
    let doc = Doc::new();
    let text_type = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text_type.insert(&mut txn, 0, content);
    }
    doc
}

/// Get the text content from a Yrs Doc
pub fn get_doc_text(doc: &Doc) -> String {
    let text = doc.get_or_insert_text("content");
    let txn = doc.transact();
    text.get_string(&txn)
}

/// Assert that two documents have the same text content
pub fn assert_docs_equal(doc1: &Doc, doc2: &Doc) {
    let text1 = get_doc_text(doc1);
    let text2 = get_doc_text(doc2);
    assert_eq!(
        text1, text2,
        "Documents diverged:\n  Doc1: {:?}\n  Doc2: {:?}",
        text1, text2
    );
}

/// Initialize tracing for tests (only initializes once)
pub fn init_test_tracing() {
    use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_filter(
                    EnvFilter::try_from_default_env()
                        .unwrap_or_else(|_| EnvFilter::new("info"))
                ),
        )
        .try_init();
}
