use std::sync::Arc;

use tokio::sync::RwLock;
use uuid::Uuid;
use yrs::Doc;
use yrs_axum::broadcast::BroadcastGroup;

use crate::error::AppError;

pub struct AppState {
    pub docs: RwLock<Vec<Arc<ActiveDocument>>>,
}

impl AppState {
    pub fn new() -> Self {
        Self {
            docs: RwLock::new(Vec::new()),
        }
    }
}

pub struct ActiveDocument {
    pub id: Uuid,
    /// Holds the live Yrs document so it stays alive as long as the group exists.
    _doc: Doc,
    pub bcast: Arc<BroadcastGroup>,
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
        .iter()
        .find(|d| d.id == doc_id)
        .cloned()
    {
        return Ok(doc);
    }

    // Write lock only when creation might be needed; re-check to avoid races.
    let mut docs = state.docs.write().await;
    if let Some(doc) = docs.iter().find(|d| d.id == doc_id) {
        return Ok(doc.clone());
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

    docs.push(active_doc.clone());

    Ok(active_doc)
}
