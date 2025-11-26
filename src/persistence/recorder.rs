use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use tokio::select;
use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_util::sync::PollSender;
use tracing::warn;
use yrs::sync::{Message, SyncMessage};
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Transact};
use yrs_axum::broadcast::BroadcastGroup;

use crate::error::AppError;
use crate::persistence::{DocumentId, SharedStore};
use crate::persistence::{SnapshotBytes, Tag, UpdateBytes};

#[derive(Debug, Clone)]
pub struct RecorderConfig {
    pub channel_capacity: usize,
    pub snapshot_interval: Duration,
    pub max_bytes_since_snapshot: usize,
}

impl Default for RecorderConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 2048,
            snapshot_interval: Duration::from_secs(60),
            max_bytes_since_snapshot: 512 * 1024,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RecorderState {
    pub last_seq: i64,
    pub last_snapshot_seq: i64,
}

pub struct RecorderHandle {
    shutdown: Option<oneshot::Sender<()>>,
    join: JoinHandle<Result<(), AppError>>,
    pub state: Arc<RwLock<RecorderState>>,
}

impl RecorderHandle {
    pub async fn shutdown(self) -> Result<(), AppError> {
        if let Some(tx) = self.shutdown {
            let _ = tx.send(());
        }
        self.join
            .await
            .map_err(|e| AppError::Store(e.to_string()))??;
        Ok(())
    }
}

pub fn spawn_recorder(
    doc_id: DocumentId,
    doc: Doc,
    store: SharedStore,
    bcast: Arc<BroadcastGroup>,
    config: RecorderConfig,
    initial_seq: i64,
) -> RecorderHandle {
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(config.channel_capacity);
    let sink = Arc::new(tokio::sync::Mutex::new(PollSender::new(tx)));

    let stream = futures_util::stream::empty::<Result<Vec<u8>, Infallible>>();

    // Keep subscription alive inside the task scope.
    let subscription = bcast.subscribe(sink, stream);

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
    let state = Arc::new(RwLock::new(RecorderState {
        last_seq: initial_seq,
        last_snapshot_seq: initial_seq,
    }));
    let shared_state = state.clone();

    let join = tokio::spawn(async move {
        let mut pending_bytes: usize = 0;
        let mut ticker = tokio::time::interval(config.snapshot_interval);
        // prevent immediate tick
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        loop {
            select! {
                _ = &mut shutdown_rx => {
                    tracing::info!(doc=%doc_id, "recorder shutdown requested; draining queue");
                    drain_queue(&mut rx, &store, doc_id, &mut pending_bytes, &state).await?;
                    maybe_snapshot(&doc, doc_id, &store, &state, &mut pending_bytes).await?;
                    tracing::info!(doc=%doc_id, "recorder shutdown completed");
                    break;
                }
                _ = ticker.tick() => {
                    tracing::trace!(doc=%doc_id, "recorder ticker fired; draining and snapshotting if needed");
                    drain_queue(&mut rx, &store, doc_id, &mut pending_bytes, &state).await?;
                    maybe_snapshot(&doc, doc_id, &store, &state, &mut pending_bytes).await?;
                }
                Some(msg) = rx.recv() => {
                    if let Err(e) = handle_update(msg, doc_id, &store, &state, &mut pending_bytes).await {
                        warn!("recorder failed to persist update: {e}");
                    }

                    if pending_bytes >= config.max_bytes_since_snapshot {
                        tracing::trace!(doc=%doc_id, pending_bytes, "snapshot threshold reached; draining and snapshotting");
                        drain_queue(&mut rx, &store, doc_id, &mut pending_bytes, &state).await?;
                        maybe_snapshot(&doc, doc_id, &store, &state, &mut pending_bytes).await?;
                    }
                }
                else => {
                    // channel closed unexpectedly: still try to snapshot if needed
                    tracing::warn!(doc=%doc_id, "recorder channel closed unexpectedly; attempting final snapshot");
                    maybe_snapshot(&doc, doc_id, &store, &state, &mut pending_bytes).await?;
                    break;
                }
            }
        }

        drop(subscription);
        Ok(())
    });

    RecorderHandle {
        shutdown: Some(shutdown_tx),
        join,
        state: shared_state,
    }
}

async fn handle_update(
    msg: Vec<u8>,
    doc_id: DocumentId,
    store: &SharedStore,
    state: &Arc<RwLock<RecorderState>>,
    pending_bytes: &mut usize,
) -> Result<(), AppError> {
    match Message::decode_v1(&msg) {
        Ok(Message::Sync(SyncMessage::Update(update_bytes))) => {
            let seq = store
                .append_update(doc_id, UpdateBytes(update_bytes.clone()))
                .await?;
            {
                let mut guard = state.write().await;
                guard.last_seq = seq;
            }
            *pending_bytes = pending_bytes.saturating_add(update_bytes.len());
        }
        Ok(_) => {
            // ignore awareness/auth/custom
        }
        Err(e) => return Err(AppError::YrsDecode(e)),
    }
    Ok(())
}

async fn drain_queue(
    rx: &mut mpsc::Receiver<Vec<u8>>,
    store: &SharedStore,
    doc_id: DocumentId,
    pending_bytes: &mut usize,
    state: &Arc<RwLock<RecorderState>>,
) -> Result<(), AppError> {
    let mut drained = 0usize;
    while let Ok(msg) = rx.try_recv() {
        let _ = handle_update(msg, doc_id, store, state, pending_bytes).await;
        drained += 1;
    }
    if drained > 0 {
        tracing::trace!(doc=%doc_id, drained, pending_bytes, "recorder drained queued updates");
    }
    Ok(())
}

async fn maybe_snapshot(
    doc: &Doc,
    doc_id: DocumentId,
    store: &SharedStore,
    state: &Arc<RwLock<RecorderState>>,
    pending_bytes: &mut usize,
) -> Result<(), AppError> {
    let (last_seq, last_snapshot_seq) = {
        let guard = state.read().await;
        (guard.last_seq, guard.last_snapshot_seq)
    };

    if last_seq == last_snapshot_seq {
        return Ok(());
    }

    let snapshot = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };

    tracing::trace!(doc=%doc_id, base_seq=last_seq, snapshot_size=snapshot.len(), "recorder storing snapshot");
    store
        .store_snapshot(
            doc_id,
            SnapshotBytes(snapshot),
            vec![Tag("system".to_string())],
            last_seq,
        )
        .await?;

    {
        let mut guard = state.write().await;
        guard.last_snapshot_seq = last_seq;
    }
    *pending_bytes = 0;
    tracing::debug!(doc=%doc_id, base_seq=last_seq, "recorder snapshot stored");
    Ok(())
}
