use sqlx::{Pool, Postgres};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;
use yrs::{Doc, ReadTxn, Transact};

use crate::document;

/// DocumentPersistence manages all storage operations for a single document.
/// It handles both real-time update persistence and periodic snapshots.
///
/// Architecture:
/// - Updates are sent through a channel (non-blocking for observers)
/// - Worker task processes updates sequentially, writing to database
/// - Snapshots are taken periodically (every 5 minutes OR after 100 updates)
/// - On drop, all pending updates are flushed and final snapshot is taken
pub struct DocumentPersistence {
    update_tx: Arc<mpsc::UnboundedSender<Vec<u8>>>,
    #[allow(dead_code)] // Kept alive to prevent worker task from being dropped
    worker_handle: JoinHandle<()>,
    doc_id: Uuid,
}

impl DocumentPersistence {
    pub fn new(doc: Doc, db: Pool<Postgres>, doc_id: Uuid) -> Self {
        let (update_tx, update_rx) = mpsc::unbounded_channel();

        let worker_handle = spawn_worker(
            doc.clone(),
            db.clone(),
            doc_id,
            update_rx,
        );

        Self {
            update_tx: Arc::new(update_tx),
            worker_handle,
            doc_id,
        }
    }

    /// Send an update to be persisted. Non-blocking.
    /// The update will be written to the database by the worker task.
    pub fn send_update(&self, update: Vec<u8>) {
        if let Err(e) = self.update_tx.send(update) {
            tracing::error!("Failed to send update for {}: {}", self.doc_id, e);
        }
    }

    /// Get a sender handle that can be cloned and used in closures
    pub fn sender(&self) -> Arc<mpsc::UnboundedSender<Vec<u8>>> {
        self.update_tx.clone()
    }
}

impl Drop for DocumentPersistence {
    fn drop(&mut self) {
        info!("DocumentPersistence dropping for {}, signaling worker to finish", self.doc_id);

        // Close the channel - this signals the worker to finish after processing remaining updates
        // The worker will:
        // 1. Process all queued updates
        // 2. Take a final snapshot
        // 3. Exit cleanly
        //
        // We replace the sender with a dummy one to close the original, which will cause
        // the receiver in the worker to return None after draining the queue.
        drop(std::mem::replace(&mut self.update_tx, Arc::new(mpsc::unbounded_channel().0)));

        // Note: We don't abort the worker - we let it finish processing and take final snapshot
        // The worker task will complete on its own after the channel is fully drained
    }
}

fn spawn_worker(
    doc: Doc,
    db: Pool<Postgres>,
    doc_id: Uuid,
    mut update_rx: mpsc::UnboundedReceiver<Vec<u8>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut last_snapshot = Instant::now();
        let mut updates_since_snapshot = 0u32;

        info!("Persistence worker started for {}", doc_id);

        // Process updates until channel is closed
        while let Some(update) = update_rx.recv().await {
            // Write update to database immediately
            match document::append_update(&db, doc_id, &update).await {
                Ok(id) => {
                    updates_since_snapshot += 1;
                    tracing::debug!("Update {} written for {}", id, doc_id);
                }
                Err(e) => {
                    tracing::error!("Failed to write update for {}: {}", doc_id, e);
                    continue;
                }
            }

            // Check if we should take a periodic snapshot
            // Criteria: 5 minutes elapsed OR 100 updates accumulated
            let should_snapshot = last_snapshot.elapsed() > Duration::from_secs(300)
                || updates_since_snapshot >= 100;

            if should_snapshot {
                take_snapshot(&doc, &db, doc_id, false).await;
                last_snapshot = Instant::now();
                updates_since_snapshot = 0;
            }
        }

        // Channel closed - all updates processed
        info!("All updates flushed for {}, taking final snapshot", doc_id);
        take_snapshot(&doc, &db, doc_id, true).await;
        info!("Persistence worker finished for {}", doc_id);
    })
}

async fn take_snapshot(doc: &Doc, db: &Pool<Postgres>, doc_id: Uuid, is_final: bool) {
    let snapshot_type = if is_final { "Final" } else { "Auto" };

    match document::get_last_update_id(db, doc_id).await {
        Ok(last_id) => {
            let snapshot = compute_snapshot(doc);
            let snapshot_name = format!(
                "{} Snapshot [{}]",
                snapshot_type,
                chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
            );
            let tags = vec!["system".to_string()];

            if let Err(e) = document::upsert_snapshot(db, doc_id, &snapshot_name, &snapshot, last_id, &tags).await {
                tracing::error!("Snapshot failed for {}: {}", doc_id, e);
            } else {
                info!("{} snapshot saved for {} (up to update {})", snapshot_type, doc_id, last_id);
            }
        }
        Err(e) => tracing::error!("Could not get last update id for snapshot of {}: {}", doc_id, e),
    }
}

fn compute_snapshot(doc: &Doc) -> Vec<u8> {
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&Default::default())
}
