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

    #[tokio::test]
    async fn test_document_cleanup() {
        let state = Arc::new(AppState::new());
        let id = Uuid::new_v4();

        {
            let _doc = get_or_create_doc(state.clone(), id).await.unwrap();
            assert_eq!(state.docs.read().await.len(), 1);
            assert!(state.docs.read().await.get(&id).unwrap().upgrade().is_some());
        } // _doc dropped here

        // The map entry remains but should be weak/dead.
        assert_eq!(state.docs.read().await.len(), 1);
        assert!(state.docs.read().await.get(&id).unwrap().upgrade().is_none());
        
        // Re-create
        let _doc2 = get_or_create_doc(state.clone(), id).await.unwrap();
        assert!(state.docs.read().await.get(&id).unwrap().upgrade().is_some());
    }
}