use std::sync::Arc;

use tracing::info;
use yrs::sync::Awareness;
use yrs::updates::decoder::Decode;
use yrs::{Doc, Transact, Update};
use yrs_axum::broadcast::BroadcastGroup;

use crate::app_state::AppState;
use crate::error::AppError;
use crate::persistence::{DocumentId, SharedStore, spawn_recorder};

use super::active_document::ActiveDocument;

pub async fn get_or_create_doc(
    state: Arc<AppState>,
    doc_id: DocumentId,
) -> Result<Arc<ActiveDocument>, AppError> {
    if let Some(doc) = state
        .docs
        .read()
        .await
        .get(&doc_id)
        .and_then(|weak| weak.upgrade())
    {
        return Ok(doc);
    }

    let mut docs = state.docs.write().await;
    if let Some(doc) = docs.get(&doc_id).and_then(|weak| weak.upgrade()) {
        return Ok(doc);
    }

    info!("creating in-memory document {}", doc_id.0);

    let doc = Doc::new();
    let last_seq = load_from_store(&doc, doc_id, state.store.clone()).await?;

    let awareness = Arc::new(tokio::sync::RwLock::new(Awareness::new(doc.clone())));
    let bcast = Arc::new(BroadcastGroup::new(awareness, 32).await);

    let recorder = spawn_recorder(
        doc_id,
        doc.clone(),
        state.store.clone(),
        bcast.clone(),
        state.recorder_config.clone(),
        last_seq,
    );

    let active_doc = Arc::new(ActiveDocument::new(doc_id, doc, bcast, recorder));

    docs.insert(doc_id, Arc::downgrade(&active_doc));

    Ok(active_doc)
}

async fn load_from_store(
    doc: &Doc,
    doc_id: DocumentId,
    store: SharedStore,
) -> Result<i64, AppError> {
    let mut last_seq = 0;

    if let Some(snapshot) = store.load_latest_snapshot(doc_id).await? {
        let update = Update::decode_v1(&snapshot.snapshot.0)?;
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
        last_seq = snapshot.base_seq;
    }

    let updates = store.load_updates_since(doc_id, last_seq).await?;
    for upd in updates {
        let update = Update::decode_v1(&upd.bytes.0)?;
        let mut txn = doc.transact_mut();
        txn.apply_update(update);
        last_seq = upd.seq;
    }

    Ok(last_seq)
}

pub async fn close_document(state: Arc<AppState>, doc_id: DocumentId) -> Result<(), AppError> {
    let maybe_doc = {
        let guard = state.docs.read().await;
        guard.get(&doc_id).and_then(|w| w.upgrade())
    };

    if let Some(doc) = maybe_doc {
        if doc.peers.load(std::sync::atomic::Ordering::SeqCst) != 0 {
            return Ok(());
        }

        if let Some(handle) = doc.take_recorder().await {
            handle.shutdown().await?;
        }

        let mut guard = state.docs.write().await;
        guard.remove(&doc_id);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use uuid::Uuid;

    use super::get_or_create_doc;
    use crate::app_state::AppState;
    use crate::document::lifecycle::close_document;
    use crate::error::AppError;
    use crate::persistence::DocumentId;

    type TestResult<T> = Result<T, AppError>;

    #[tokio::test]
    async fn get_or_create_inserts_document() -> TestResult<()> {
        let state = Arc::new(AppState::default());
        let id = DocumentId(Uuid::new_v4());
        let doc = get_or_create_doc(state.clone(), id).await?;
        assert_eq!(doc.id, id);
        assert!(
            state
                .docs
                .read()
                .await
                .get(&id)
                .and_then(|w| w.upgrade())
                .is_some()
        );
        Ok(())
    }

    #[tokio::test]
    async fn close_document_removes_from_state() -> TestResult<()> {
        let state = Arc::new(AppState::default());
        let id = DocumentId(Uuid::new_v4());
        let _ = get_or_create_doc(state.clone(), id).await?;

        close_document(state.clone(), id).await?;

        assert!(
            state
                .docs
                .read()
                .await
                .get(&id)
                .and_then(|w| w.upgrade())
                .is_none()
        );
        Ok(())
    }
}
