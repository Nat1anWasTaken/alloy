use alloy::api::{IssueTicketRequest, IssueTicketResponse};
use alloy::document::AppState;
use alloy::persistence::{DocumentId, IdError, MemoryStore};
use alloy::session::TicketIssuer;
use axum::Router;
use std::net::SocketAddr;
use std::sync::Arc;
use thiserror::Error;
use tokio::net::TcpListener;
use tokio_tungstenite::{WebSocketStream, connect_async};
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact};

#[derive(Debug, Error)]
pub enum TestError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("websocket: {0}")]
    WebSocket(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("app: {0}")]
    App(#[from] alloy::error::AppError),
    #[allow(dead_code)]
    #[error("document {0} missing from state")]
    MissingDocument(DocumentId),
    #[allow(dead_code)]
    #[error("connection succeeded unexpectedly")]
    UnexpectedConnection,
    #[error("id generation failed: {0}")]
    Id(#[from] IdError),
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),
    #[allow(dead_code)]
    #[error("unexpected status {0}")]
    UnexpectedStatus(reqwest::StatusCode),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[allow(dead_code)]
    #[error("unexpected response: {0}")]
    UnexpectedResponse(String),
}

impl From<tokio_tungstenite::tungstenite::Error> for TestError {
    fn from(err: tokio_tungstenite::tungstenite::Error) -> Self {
        TestError::WebSocket(Box::new(err))
    }
}

pub type TestResult<T> = Result<T, TestError>;

pub async fn spawn_test_server() -> TestResult<(SocketAddr, Arc<AppState>)> {
    init_test_tracing();

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let store = Arc::new(MemoryStore::default());
    let ticketing = TicketIssuer::development();
    let state = Arc::new(AppState::with_components(store, ticketing));
    let app = create_test_router(state.clone());

    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::error!("axum server failed: {err}");
        }
    });

    // Wait for server to start
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    Ok((addr, state))
}

/// Create the test router with the same structure as main.rs
fn create_test_router(state: Arc<AppState>) -> Router {
    alloy::api::router(state)
}

/// Connect to a document and return the WebSocket stream
#[allow(dead_code)]
pub async fn connect_to_doc(
    addr: SocketAddr,
    doc_id: DocumentId,
) -> TestResult<WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>> {
    let ticket = request_ticket(addr, doc_id, "test").await?;
    let ws_url = format!("ws://{addr}/edit?ticket={}", ticket.ticket);
    let (ws_stream, _response) = connect_async(&ws_url).await?;
    Ok(ws_stream)
}

#[allow(dead_code)]
pub async fn request_ticket(
    addr: SocketAddr,
    doc_id: DocumentId,
    user: &str,
) -> TestResult<IssueTicketResponse> {
    let client = reqwest::Client::new();
    let url = format!("http://{}/api/documents/{}/ticket", addr, doc_id);
    let response = client
        .post(url)
        .json(&IssueTicketRequest {
            user_id: user.to_string(),
        })
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(TestError::UnexpectedStatus(response.status()));
    }

    let body = response.json::<IssueTicketResponse>().await?;
    Ok(body)
}

/// Create a Yrs update with the given text content
#[allow(dead_code)]
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
#[allow(dead_code)]
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
#[allow(dead_code)]
pub fn get_doc_text(doc: &Doc) -> String {
    let text = doc.get_or_insert_text("content");
    let txn = doc.transact();
    text.get_string(&txn)
}

/// Assert that two documents have the same text content
#[allow(dead_code)]
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
    use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

    let _ = tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_filter(
                    EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
                ),
        )
        .try_init();
}
