mod database;
mod error;

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
use yrs::{sync::Awareness, Doc, ReadTxn, Transact, Update, Subscription};
use yrs_axum::{
    broadcast::BroadcastGroup,
    ws::{AxumSink, AxumStream},
};
use futures_util::StreamExt;

struct ActiveDocument {
    bcast: Arc<BroadcastGroup>,
    _persistence_sub: Subscription,
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

    let addr: SocketAddr = "0.0.0.0:3000".parse().expect("valid bind address");
    let listener = TcpListener::bind(addr).await.expect("bind failed");
    info!("starting yrs-axum server on ws://{addr}/ws/:doc_id");

    axum::serve(listener, app)
        .await
        .expect("server error");

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
    let doc = get_or_create_doc(&state, doc_id).await?;
    Ok(ws.on_upgrade(move |socket| peer(socket, doc.bcast.clone())))
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

    let (_snapshot_name, snapshot, _last_id, updates) = database::load_doc_state(&state.db, doc_id).await?;

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

    let db_clone = state.db.clone();
    let persistence_sub = doc.observe_update_v1(move |_txn, event| {
        let update = event.update.to_vec();
        let db = db_clone.clone();
        tokio::spawn(async move {
            if let Err(e) = database::append_update(&db, doc_id, &update).await {
                tracing::error!("Failed to append update for {}: {}", doc_id, e);
            }
        });
    })
    .expect("Failed to observe update"); 
    // Note: yrs::BorrowMutError doesn't map easily to AppError::YrsDecode, 
    // but for now I just used a placeholder error or I should add a new variant.
    // Actually, I should just unwrap or expect because if we can't observe a fresh doc, it's a critical logic error.
    // However, map_err is safer than unwrap.
    // Let's use expect since Doc::new() should be clean.

    spawn_snapshotter(doc.clone(), state.db.clone(), doc_id);

    let awareness = Arc::new(RwLock::new(Awareness::new(doc)));
    let bcast = Arc::new(BroadcastGroup::new(awareness, 32).await);

    let active_doc = Arc::new(ActiveDocument {
        bcast,
        _persistence_sub: persistence_sub,
    });

    docs.insert(doc_id, active_doc.clone());

    Ok(active_doc)
}

async fn peer(ws: WebSocket, bcast: Arc<BroadcastGroup>) {
    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);

    let sub = bcast.subscribe(sink, stream);
    match sub.completed().await {
        Ok(_) => info!("peer finished"),
        Err(err) => tracing::error!("peer aborted: {err}"),
    }
}

fn compute_snapshot(doc: &Doc) -> Vec<u8> {
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&Default::default())
}

fn spawn_snapshotter(doc: Doc, db: Pool<Postgres>, doc_id: Uuid) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

            match database::get_last_update_id(&db, doc_id).await {
                Ok(last_id) => {
                    let snapshot = compute_snapshot(&doc);
                    let snapshot_name = format!("Auto Snapshot [{}]", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S"));

                    if let Err(e) = database::upsert_snapshot(&db, doc_id, &snapshot_name, &snapshot, last_id).await {
                        tracing::error!("Snapshot failed for {}: {}", doc_id, e);
                    } else {
                         // info!("Snapshot created for {} up to {}", doc_id, last_id);
                    }
                }
                Err(e) => tracing::error!("Could not get last update id for {}: {}", doc_id, e),
            }
        }
    });
}
