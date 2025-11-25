use sqlx::{Pool, Postgres};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::info;
use uuid::Uuid;
use yrs::{Doc, ReadTxn, Transact};

use crate::document::DocumentRecord;

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
        let doc_record = DocumentRecord::new(doc_id);

        info!("Persistence worker started for {}", doc_id);

        // Process updates until channel is closed
        while let Some(update_data) = update_rx.recv().await {
            // Write update to database immediately
            match doc_record.save_update(&db, update_data).await {
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
                take_snapshot(&doc, &doc_record, &db, false).await;
                last_snapshot = Instant::now();
                updates_since_snapshot = 0;
            }
        }

        // Channel closed - all updates processed
        info!("All updates flushed for {}, taking final snapshot", doc_id);
        take_snapshot(&doc, &doc_record, &db, true).await;
        info!("Persistence worker finished for {}", doc_id);
    })
}

async fn take_snapshot(doc: &Doc, doc_record: &DocumentRecord, db: &Pool<Postgres>, is_final: bool) {
    let snapshot_type = if is_final { "Final" } else { "Auto" };

    let snapshot_data = compute_snapshot(doc);
    let snapshot_name = format!(
        "{} Snapshot [{}]",
        snapshot_type,
        chrono::Utc::now().format("%Y-%m-%d %H:%M:%S")
    );

    let tags = vec!["system".to_string()];

    match doc_record.create_snapshot(db, snapshot_name, snapshot_data, tags).await {
        Ok(()) => {
            info!("{} snapshot saved for {}", snapshot_type, doc_record.doc_id);
        }
        Err(e) => {
            tracing::error!("Snapshot failed for {}: {}", doc_record.doc_id, e);
        }
    }
}

fn compute_snapshot(doc: &Doc) -> Vec<u8> {
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&Default::default())
}
