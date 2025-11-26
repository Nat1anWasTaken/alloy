use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::RwLock;
use uuid::Uuid;
use yrs::{Doc, ReadTxn, StateVector, Transact};
use yrs_axum::broadcast::BroadcastGroup;

use crate::error::AppError;

pub struct AppState {
    pub docs: RwLock<HashMap<Uuid, Weak<ActiveDocument>>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            docs: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ActiveDocument {
    pub id: Uuid,
    /// Holds the live Yrs document so it stays alive as long as the group exists.
    _doc: Doc,
    pub bcast: Arc<BroadcastGroup>,
}

impl Drop for ActiveDocument {
    fn drop(&mut self) {
        tracing::info!("Document {} dropped. Saving state...", self.id);
        let doc = self._doc.clone();
        let id = self.id;

        tokio::spawn(async move {
            if let Err(e) = save_document_snapshot(id, &doc).await {
                tracing::error!("Failed to save document {}: {}", id, e);
            }
        });
    }
}

pub async fn save_document_snapshot(id: Uuid, doc: &Doc) -> Result<(), AppError> {
    let txn = doc.transact();
    let state_vector = StateVector::default();
    let update = txn.encode_state_as_update_v1(&state_vector);
    
    tracing::info!("Saved snapshot for document {}. Size: {} bytes", id, update.len());
    // TODO: Write `update` to a database or file here.
    
    Ok(())
}

pub async fn get_or_create_doc(
    state: Arc<AppState>,
    doc_id: Uuid,
) -> Result<Arc<ActiveDocument>, AppError> {
    // Prefer a read lock for the common case where the doc already exists.
    if let Some(doc) = state
        .docs
        .read()
        .await
        .get(&doc_id)
        .and_then(|weak| weak.upgrade())
    {
        return Ok(doc);
    }

    // Write lock only when creation might be needed; re-check to avoid races.
    let mut docs = state.docs.write().await;
    if let Some(doc) = docs.get(&doc_id).and_then(|weak| weak.upgrade()) {
        return Ok(doc);
    }

    tracing::info!("Creating in-memory document {}", doc_id);

    let doc = Doc::new();
    let awareness = Arc::new(tokio::sync::RwLock::new(yrs::sync::Awareness::new(doc.clone())));
    let bcast = Arc::new(BroadcastGroup::new(awareness, 32).await);

    let active_doc = Arc::new(ActiveDocument {
        id: doc_id,
        _doc: doc,
        bcast,
    });

    docs.insert(doc_id, Arc::downgrade(&active_doc));

    Ok(active_doc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::future::try_join_all;
    use yrs::types::Text;

    type TestResult<T> = Result<T, AppError>;

    #[tokio::test]
    async fn test_document_cleanup() -> TestResult<()> {
        let state = Arc::new(AppState::new());
        let id = Uuid::new_v4();

        {
            let _doc = get_or_create_doc(state.clone(), id).await?;
            assert_eq!(state.docs.read().await.len(), 1);
            assert!(state
                .docs
                .read()
                .await
                .get(&id)
                .and_then(|doc| doc.upgrade())
                .is_some());
        } // _doc dropped here

        // The map entry remains but should be weak/dead.
        assert_eq!(state.docs.read().await.len(), 1);
        assert!(state
            .docs
            .read()
            .await
            .get(&id)
            .and_then(|doc| doc.upgrade())
            .is_none());

        // Re-create
        let _doc2 = get_or_create_doc(state.clone(), id).await?;
        assert!(state
            .docs
            .read()
            .await
            .get(&id)
            .and_then(|doc| doc.upgrade())
            .is_some());

        Ok(())
    }

    #[test]
    fn test_app_state_new() {
        let state = AppState::new();
        let docs = state.docs.blocking_read();
        assert_eq!(docs.len(), 0);
    }

    #[tokio::test]
    async fn test_get_or_create_doc_creates_new() -> TestResult<()> {
        let state = Arc::new(AppState::new());
        let doc_id = Uuid::new_v4();

        let doc = get_or_create_doc(state.clone(), doc_id).await?;

        assert_eq!(doc.id, doc_id);
        assert_eq!(state.docs.read().await.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_get_or_create_doc_retrieves_existing() -> TestResult<()> {
        let state = Arc::new(AppState::new());
        let doc_id = Uuid::new_v4();

        let doc1 = get_or_create_doc(state.clone(), doc_id).await?;
        let doc2 = get_or_create_doc(state.clone(), doc_id).await?;

        // Verify same Arc instance (pointer equality)
        assert!(Arc::ptr_eq(&doc1, &doc2));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_get_or_create_doc_concurrent_creation() -> TestResult<()> {
        let state = Arc::new(AppState::new());
        let doc_id = Uuid::new_v4();

        // Spawn 100 tasks simultaneously trying to create same doc
        let doc_futures = (0..100).map(|_| {
            let state = state.clone();
            async move { get_or_create_doc(state, doc_id).await }
        });

        let docs: Vec<_> = try_join_all(doc_futures).await?;

        // Verify all tasks got the SAME Arc instance
        for i in 1..docs.len() {
            assert!(Arc::ptr_eq(&docs[0], &docs[i]));
        }

        // Verify only ONE document in state
        assert_eq!(state.docs.read().await.len(), 1);

        Ok(())
    }

    #[tokio::test]
    async fn test_save_document_snapshot_with_content() -> TestResult<()> {
        let doc = Doc::new();
        let text = doc.get_or_insert_text("content");
        {
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, "test");
        }

        save_document_snapshot(Uuid::new_v4(), &doc).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_save_document_snapshot_empty_doc() -> TestResult<()> {
        let doc = Doc::new();
        save_document_snapshot(Uuid::new_v4(), &doc).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_documents_in_state() -> TestResult<()> {
        let state = Arc::new(AppState::new());

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let _doc1 = get_or_create_doc(state.clone(), id1).await?;
        let _doc2 = get_or_create_doc(state.clone(), id2).await?;
        let _doc3 = get_or_create_doc(state.clone(), id3).await?;

        assert_eq!(state.docs.read().await.len(), 3);

        Ok(())
    }

    #[tokio::test]
    async fn test_document_recreation_after_drop() -> TestResult<()> {
        let state = Arc::new(AppState::new());
        let id = Uuid::new_v4();

        {
            let _doc = get_or_create_doc(state.clone(), id).await?;
        } // Drop all strong references

        // Weak reference exists but can't upgrade
        assert!(state
            .docs
            .read()
            .await
            .get(&id)
            .and_then(|doc| doc.upgrade())
            .is_none());

        // Create new doc with same ID - should work
        let _doc2 = get_or_create_doc(state.clone(), id).await?;
        assert!(state
            .docs
            .read()
            .await
            .get(&id)
            .and_then(|doc| doc.upgrade())
            .is_some());

        Ok(())
    }
}
