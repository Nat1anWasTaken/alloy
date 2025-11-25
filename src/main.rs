mod database;
mod document;
mod error;
mod persistence;

use axum::{
    extract::{
        ws::{WebSocket, WebSocketUpgrade},
        State, Path,
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use error::AppError;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock};
use tracing::info;
use uuid::Uuid;
use yrs::updates::decoder::Decode;
use yrs::{sync::Awareness, Doc, Transact, Update, Subscription};
use yrs_axum::{
    broadcast::BroadcastGroup,
    ws::{AxumSink, AxumStream},
};
use futures_util::StreamExt;

struct ActiveDocument {
    bcast: Arc<BroadcastGroup>,
    _persistence: persistence::DocumentPersistence,
    _observer_sub: Subscription,
}

struct AppState {
    docs: RwLock<HashMap<Uuid, Arc<ActiveDocument>>>,
    db: Pool<Postgres>,
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    setup_tracing();

    // 1. Initialize Database
    let db_url = std::env::var("DATABASE_URL")?;
    let db = sqlx::postgres::PgPoolOptions::new()
        .max_connections(20)
        .connect(&db_url)
        .await?;

    database::init_schema(&db).await?;

    let state = Arc::new(AppState {
        docs: RwLock::new(HashMap::new()),
        db: db.clone(),
    });

    let app = Router::new()
        .route("/ws/:doc_id", get(ws_handler))
        .with_state(state);

    let addr: SocketAddr = "0.0.0.0:3000".parse()?;
    let listener = TcpListener::bind(addr).await?;
    info!("starting yrs-axum server on ws://{addr}/ws/:doc_id");

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
    let active_doc = get_or_create_doc(&state, doc_id).await?;
    Ok(ws.on_upgrade(move |socket| peer(socket, active_doc, doc_id, state)))
}

async fn get_or_create_doc(state: &AppState, doc_id: Uuid) -> Result<Arc<ActiveDocument>, AppError> {
    // 1. Fast path
    {
        let docs = state.docs.read().await;
        if let Some(doc) = docs.get(&doc_id) {
            return Ok(doc.clone());
        }
    }

    // 2. Slow path
    let mut docs = state.docs.write().await;
    if let Some(doc) = docs.get(&doc_id) {
        return Ok(doc.clone());
    }

    info!("Loading document {}", doc_id);

    let (_snapshot_name, snapshot, _last_id, _tags, updates) = document::load_doc_state(&state.db, doc_id).await?;

    let doc = Doc::new();
    {
        let mut txn = doc.transact_mut();
        if let Some(snap) = snapshot {
            txn.apply_update(Update::decode_v1(&snap)?);
        }
        for u in updates {
            txn.apply_update(Update::decode_v1(&u)?);
        }
    }

    // Setup persistence: DocumentPersistence handles both updates and snapshots
    let doc_persistence = persistence::DocumentPersistence::new(doc.clone(), state.db.clone(), doc_id);

    // Observe document updates and send them to persistence worker
    let update_sender = doc_persistence.sender();
    let observer_sub = doc
        .observe_update_v1(move |_txn, event| {
            let update = event.update.to_vec();
            if let Err(e) = update_sender.send(update) {
                tracing::error!("Failed to send update to persistence worker: {}", e);
            }
        })
        .map_err(|e| AppError::YrsObserve(format!("{:?}", e)))?;

    let awareness = Arc::new(RwLock::new(Awareness::new(doc.clone())));
    let bcast = Arc::new(BroadcastGroup::new(awareness, 32).await);

    let active_doc = Arc::new(ActiveDocument {
        bcast,
        _persistence: doc_persistence,
        _observer_sub: observer_sub,
    });

    docs.insert(doc_id, active_doc.clone());

    Ok(active_doc)
}

async fn peer(ws: WebSocket, active_doc: Arc<ActiveDocument>, doc_id: Uuid, state: Arc<AppState>) {
    info!("Peer connected to {}", doc_id);

    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);

    let sub = active_doc.bcast.subscribe(sink, stream);
    match sub.completed().await {
        Ok(_) => info!("Peer finished for {}", doc_id),
        Err(err) => tracing::error!("Peer aborted for {}: {}", doc_id, err),
    }

    // After peer disconnects, check if we should cleanup
    // The BroadcastGroup holds references to active subscribers
    // If Arc::strong_count is 2 (one in HashMap, one in this function), no other peers exist
    if Arc::strong_count(&active_doc) == 2 {
        info!("Last peer disconnected from {}, removing from memory", doc_id);
        let mut docs = state.docs.write().await;
        docs.remove(&doc_id);
        // When removed from HashMap and this function exits, Drop::drop will be called
    }
}
