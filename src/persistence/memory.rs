use std::collections::HashMap;

use tokio::sync::RwLock;
use tracing::{debug, instrument, trace};

use crate::error::AppError;

use super::store::DocumentStore;
use super::types::{ClientId, DocumentId, SnapshotRecord, Tag, UpdateBytes, UpdateRecord, UserId};

#[derive(Default)]
pub struct MemoryStore {
    inner: RwLock<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    updates: HashMap<DocumentId, Vec<UpdateRecord>>,
    snapshots: HashMap<DocumentId, SnapshotRecord>,
    sessions: HashMap<(DocumentId, ClientId), UserId>,
}

#[async_trait::async_trait]
impl DocumentStore for MemoryStore {
    #[instrument(skip(self), fields(doc = ?doc))]
    async fn load_latest_snapshot(
        &self,
        doc: DocumentId,
    ) -> Result<Option<SnapshotRecord>, AppError> {
        trace!("Loading latest snapshot");
        let state = self.inner.read().await;
        let snapshot = state.snapshots.get(&doc).cloned();

        if let Some(ref snap) = snapshot {
            debug!(
                base_seq = snap.base_seq,
                tags_count = snap.tags.len(),
                "Snapshot found"
            );
        } else {
            debug!("No snapshot found");
        }

        Ok(snapshot)
    }

    #[instrument(skip(self), fields(doc = ?doc, seq_inclusive))]
    async fn load_updates_since(
        &self,
        doc: DocumentId,
        seq_inclusive: i64,
    ) -> Result<Vec<UpdateRecord>, AppError> {
        trace!("Loading updates since sequence");
        let state = self.inner.read().await;
        let total_updates = state.updates.get(&doc).map(|v| v.len()).unwrap_or(0);

        let updates: Vec<UpdateRecord> = state
            .updates
            .get(&doc)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|u| u.seq > seq_inclusive)
            .collect();

        debug!(
            total_updates,
            filtered_updates = updates.len(),
            "Loaded updates"
        );

        Ok(updates)
    }

    #[instrument(skip(self, update), fields(doc = ?doc, update_size = update.0.len()))]
    async fn append_update(&self, doc: DocumentId, update: UpdateBytes) -> Result<i64, AppError> {
        trace!("Appending update");
        let mut state = self.inner.write().await;
        let seq = state
            .updates
            .get(&doc)
            .and_then(|v| v.last())
            .map(|u| u.seq + 1)
            .unwrap_or(1);
        let entry = state.updates.entry(doc).or_default();
        entry.push(UpdateRecord { seq, bytes: update });

        debug!(seq, total_updates = entry.len(), "Update appended");

        Ok(seq)
    }

    #[instrument(skip(self, snapshot, tags), fields(doc = ?doc, base_seq, tags_count = tags.len(), snapshot_size = snapshot.0.len()))]
    async fn store_snapshot(
        &self,
        doc: DocumentId,
        snapshot: super::types::SnapshotBytes,
        tags: Vec<Tag>,
        base_seq: i64,
    ) -> Result<(), AppError> {
        trace!("Storing snapshot");
        let mut state = self.inner.write().await;
        state.snapshots.insert(
            doc,
            SnapshotRecord {
                snapshot,
                tags,
                base_seq,
            },
        );

        debug!("Snapshot stored successfully");

        Ok(())
    }

    #[instrument(skip(self), fields(doc = ?doc, up_to_seq))]
    async fn cleanup_updates_until(&self, doc: DocumentId, up_to_seq: i64) -> Result<(), AppError> {
        trace!("Cleaning up updates");
        let mut state = self.inner.write().await;
        if let Some(existing) = state.updates.get_mut(&doc) {
            let before_count = existing.len();
            existing.retain(|u| u.seq > up_to_seq);
            let after_count = existing.len();
            let removed_count = before_count - after_count;

            debug!(
                before_count,
                after_count, removed_count, "Updates cleaned up"
            );
        } else {
            debug!("No updates found to clean up");
        }
        Ok(())
    }

    #[instrument(skip(self), fields(doc = ?doc, client = ?client, user = ?user))]
    async fn record_session(
        &self,
        doc: DocumentId,
        client: ClientId,
        user: UserId,
    ) -> Result<(), AppError> {
        trace!("Recording session");
        let mut state = self.inner.write().await;
        state.sessions.insert((doc, client), user);

        debug!(total_sessions = state.sessions.len(), "Session recorded");

        Ok(())
    }

    #[instrument(skip(self), fields(doc = ?doc, client = ?client))]
    async fn get_session(
        &self,
        doc: DocumentId,
        client: ClientId,
    ) -> Result<Option<UserId>, AppError> {
        trace!("Getting session");
        let state = self.inner.read().await;
        let user = state.sessions.get(&(doc, client)).cloned();

        if let Some(ref user_id) = user {
            debug!(user = ?user_id, "Session found");
        } else {
            debug!("No session found");
        }

        Ok(user)
    }
}
