use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;

use tokio::sync::RwLock;
use tracing::{debug, instrument, trace};

use crate::error::AppError;

use super::store::DocumentStore;
use super::types::{
    ClientId, DocumentId, SnapshotPage, SnapshotRecord, Tag, UpdateBytes, UpdateRecord, UserId,
};

#[derive(Default)]
pub struct MemoryStore {
    inner: RwLock<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    updates: HashMap<DocumentId, Vec<UpdateRecord>>,
    snapshots: HashMap<DocumentId, BTreeMap<i64, SnapshotRecord>>,
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
        let snapshot = state
            .snapshots
            .get(&doc)
            .and_then(|snaps| snaps.iter().next_back().map(|(_, snap)| snap.clone()));

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

    #[instrument(skip(self), fields(doc = ?doc, base_seq))]
    async fn load_snapshot(
        &self,
        doc: DocumentId,
        base_seq: i64,
    ) -> Result<Option<SnapshotRecord>, AppError> {
        trace!("Loading specific snapshot");
        let state = self.inner.read().await;
        let snapshot = state
            .snapshots
            .get(&doc)
            .and_then(|snaps| snaps.get(&base_seq).cloned());

        if let Some(ref snap) = snapshot {
            debug!(base_seq = snap.base_seq, tags_count = snap.tags.len(), "Snapshot found");
        } else {
            debug!("Snapshot not found");
        }

        Ok(snapshot)
    }

    #[instrument(skip(self), fields(doc = ?doc, start_after, limit))]
    async fn list_snapshots(
        &self,
        doc: DocumentId,
        start_after: Option<i64>,
        limit: usize,
    ) -> Result<SnapshotPage, AppError> {
        trace!("Listing snapshots");
        let state = self.inner.read().await;

        if limit == 0 {
            debug!("Pagination limit is zero; returning empty page");
            return Ok(SnapshotPage::default());
        }

        let mut page = SnapshotPage::default();

        if let Some(snaps) = state.snapshots.get(&doc) {
            let range_start = match start_after {
                Some(cursor) => (Bound::Excluded(cursor), Bound::Unbounded),
                None => (Bound::Unbounded, Bound::Unbounded),
            };

            let mut iter = snaps.range(range_start);

            for (_, snapshot) in iter.by_ref().take(limit) {
                page.snapshots.push(snapshot.clone());
            }

            if let Some(last) = page.snapshots.last() {
                let mut remaining = snaps.range((Bound::Excluded(last.base_seq), Bound::Unbounded));
                if remaining.next().is_some() {
                    page.next_cursor = Some(last.base_seq);
                }
            }
        }

        debug!(count = page.snapshots.len(), next_cursor = ?page.next_cursor, "Snapshots listed");

        Ok(page)
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
        let entry = state.snapshots.entry(doc).or_default();
        entry.insert(
            base_seq,
            SnapshotRecord {
                snapshot,
                tags,
                base_seq,
            },
        );

        debug!("Snapshot stored successfully");

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

#[cfg(test)]
mod tests {
    use super::MemoryStore;
    use crate::error::AppError;
    use crate::persistence::store::DocumentStore;
    use crate::persistence::types::{ClientId, DocumentId, SnapshotBytes, Tag, UpdateBytes, UserId};

    type TestResult<T> = Result<T, AppError>;

    #[tokio::test]
    async fn paginates_snapshots_with_cursor() -> TestResult<()> {
        let store = MemoryStore::default();
        let doc = DocumentId::from(1_u64);

        for seq in 1_i64..=3 {
            store
                .store_snapshot(
                    doc,
                    SnapshotBytes(vec![seq as u8]),
                    vec![Tag(seq.to_string())],
                    seq,
                )
                .await?;
        }

        let first_page = store.list_snapshots(doc, None, 2).await?;
        assert_eq!(first_page.snapshots.len(), 2);
        assert_eq!(first_page.snapshots[0].base_seq, 1);
        assert_eq!(first_page.snapshots[1].base_seq, 2);
        assert_eq!(first_page.next_cursor, Some(2));

        let second_page = store
            .list_snapshots(doc, first_page.next_cursor, 2)
            .await?;
        assert_eq!(second_page.snapshots.len(), 1);
        assert_eq!(second_page.snapshots[0].base_seq, 3);
        assert_eq!(second_page.next_cursor, None);

        Ok(())
    }

    #[tokio::test]
    async fn zero_limit_returns_empty_page() -> TestResult<()> {
        let store = MemoryStore::default();
        let doc = DocumentId::from(42_u64);

        let page = store.list_snapshots(doc, None, 0).await?;
        assert!(page.snapshots.is_empty());
        assert!(page.next_cursor.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn loads_latest_and_specific_snapshot() -> TestResult<()> {
        let store = MemoryStore::default();
        let doc = DocumentId::from(7_u64);

        store
            .store_snapshot(
                doc,
                SnapshotBytes(vec![1]),
                vec![Tag("older".to_string())],
                5,
            )
            .await?;
        store
            .store_snapshot(
                doc,
                SnapshotBytes(vec![2]),
                vec![Tag("newer".to_string())],
                10,
            )
            .await?;

        let latest = store
            .load_latest_snapshot(doc)
            .await?
            .ok_or_else(|| AppError::Store("expected latest snapshot".to_string()))?;
        assert_eq!(latest.base_seq, 10);
        assert_eq!(latest.snapshot, SnapshotBytes(vec![2]));
        assert_eq!(latest.tags, vec![Tag("newer".to_string())]);

        let specific = store
            .load_snapshot(doc, 5)
            .await?
            .ok_or_else(|| AppError::Store("expected snapshot at base_seq 5".to_string()))?;
        assert_eq!(specific.base_seq, 5);
        assert_eq!(specific.snapshot, SnapshotBytes(vec![1]));
        assert_eq!(specific.tags, vec![Tag("older".to_string())]);

        let missing = store.load_snapshot(doc, 99).await?;
        assert!(missing.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn appends_updates_and_filters_by_sequence() -> TestResult<()> {
        let store = MemoryStore::default();
        let doc = DocumentId::from(11_u64);

        let first_seq = store.append_update(doc, UpdateBytes(vec![1])).await?;
        let second_seq = store.append_update(doc, UpdateBytes(vec![2])).await?;
        let third_seq = store.append_update(doc, UpdateBytes(vec![3])).await?;

        assert_eq!(first_seq, 1);
        assert_eq!(second_seq, 2);
        assert_eq!(third_seq, 3);

        let all_updates = store.load_updates_since(doc, 0).await?;
        assert_eq!(all_updates.len(), 3);
        assert_eq!(all_updates[0].seq, 1);
        assert_eq!(all_updates[1].seq, 2);
        assert_eq!(all_updates[2].seq, 3);

        let after_first = store.load_updates_since(doc, 1).await?;
        assert_eq!(after_first.len(), 2);
        assert!(after_first.iter().all(|u| u.seq > 1));

        let none_after_third = store.load_updates_since(doc, 3).await?;
        assert!(none_after_third.is_empty());

        Ok(())
    }

    #[tokio::test]
    async fn records_and_reads_sessions_per_client() -> TestResult<()> {
        let store = MemoryStore::default();
        let doc_one = DocumentId::from(21_u64);
        let doc_two = DocumentId::from(22_u64);

        store
            .record_session(doc_one, ClientId(1), UserId("alice".to_string()))
            .await?;
        store
            .record_session(doc_one, ClientId(2), UserId("bob".to_string()))
            .await?;
        store
            .record_session(doc_two, ClientId(1), UserId("carol".to_string()))
            .await?;

        let alice = store
            .get_session(doc_one, ClientId(1))
            .await?
            .ok_or_else(|| AppError::Store("missing session for alice".to_string()))?;
        assert_eq!(alice, UserId("alice".to_string()));

        let bob = store
            .get_session(doc_one, ClientId(2))
            .await?
            .ok_or_else(|| AppError::Store("missing session for bob".to_string()))?;
        assert_eq!(bob, UserId("bob".to_string()));

        let carol = store
            .get_session(doc_two, ClientId(1))
            .await?
            .ok_or_else(|| AppError::Store("missing session for carol".to_string()))?;
        assert_eq!(carol, UserId("carol".to_string()));

        let missing = store.get_session(doc_one, ClientId(99)).await?;
        assert!(missing.is_none());

        Ok(())
    }
}
