use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use futures_util::StreamExt;
use tokio::sync::{Mutex, OnceCell, RwLock};
use tracing::{info, warn};
use yrs::sync::protocol::DefaultProtocol;
use yrs::sync::{Awareness, Message, Protocol};
use yrs::updates::decoder::Decode;
use yrs::{Doc, StateVector, Transact, Update};
use yrs_axum::broadcast::BroadcastGroup;
use yrs_axum::ws::{AxumSink, AxumStream};

use crate::error::AppError;
use crate::persistence::{ClientId, DocumentId, SharedStore, UserId};
use crate::recorder::{RecorderConfig, RecorderHandle, spawn_recorder};

pub struct AppState {
    pub docs: RwLock<HashMap<DocumentId, Weak<ActiveDocument>>>,
    pub store: SharedStore,
    pub recorder_config: RecorderConfig,
}

impl AppState {
    pub fn with_store(store: SharedStore) -> Self {
        Self {
            docs: RwLock::new(HashMap::new()),
            store,
            recorder_config: RecorderConfig::default(),
        }
    }

    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::with_store(Arc::new(crate::persistence::MemoryStore::default()))
    }
}

pub struct ActiveDocument {
    pub id: DocumentId,
    pub doc: Doc,
    pub bcast: Arc<BroadcastGroup>,
    pub peers: AtomicUsize,
    recorder: Mutex<Option<RecorderHandle>>,
}

impl ActiveDocument {
    pub fn peer_connected(&self) {
        self.peers.fetch_add(1, Ordering::SeqCst);
    }

    pub fn peer_disconnected(&self) -> usize {
        self.peers.fetch_sub(1, Ordering::SeqCst)
    }
}

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

    let active_doc = Arc::new(ActiveDocument {
        id: doc_id,
        doc,
        bcast,
        peers: AtomicUsize::new(0),
        recorder: Mutex::new(Some(recorder)),
    });

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
        if doc.peers.load(Ordering::SeqCst) != 0 {
            return Ok(());
        }

        if let Some(handle) = doc.recorder.lock().await.take() {
            handle.shutdown().await?;
        }

        let mut guard = state.docs.write().await;
        guard.remove(&doc_id);
    }

    Ok(())
}

/// Protocol wrapper that ties a client_id to a user_id per document and rejects mismatches.
struct SessionProtocol {
    doc_id: DocumentId,
    user: UserId,
    store: SharedStore,
    expected: OnceCell<ClientId>,
    inner: DefaultProtocol,
}

impl SessionProtocol {
    fn new(doc_id: DocumentId, user: UserId, store: SharedStore) -> Self {
        Self {
            doc_id,
            user,
            store,
            expected: OnceCell::new(),
            inner: DefaultProtocol,
        }
    }

    fn validate_update(&self, update: &Update) -> Result<(), yrs::sync::Error> {
        let sv = update.state_vector();
        let mut iter = sv.iter();
        let client = match iter.next() {
            Some((client, _)) => *client,
            None => return Ok(()),
        };

        if iter.any(|(other, _)| *other != client) {
            return Err(yrs::sync::Error::PermissionDenied {
                reason: "mixed client ids in update".to_string(),
            });
        }

        if let Some(expected) = self.expected.get() {
            if expected.0 != client {
                return Err(yrs::sync::Error::PermissionDenied {
                    reason: "client id mismatch".to_string(),
                });
            }
        } else {
            let _ = self.expected.set(ClientId(client));
            let store = self.store.clone();
            let doc_id = self.doc_id;
            let user = self.user.clone();
            tokio::spawn(async move {
                if let Err(e) = store.record_session(doc_id, ClientId(client), user).await {
                    warn!("record_session failed: {e}");
                }
            });
        }

        Ok(())
    }
}

impl Protocol for SessionProtocol {
    fn start<E: yrs::updates::encoder::Encoder>(
        &self,
        awareness: &Awareness,
        encoder: &mut E,
    ) -> Result<(), yrs::sync::Error> {
        self.inner.start(awareness, encoder)
    }

    fn handle_sync_step1(
        &self,
        awareness: &Awareness,
        sv: StateVector,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.inner.handle_sync_step1(awareness, sv)
    }

    fn handle_sync_step2(
        &self,
        awareness: &mut Awareness,
        update: Update,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.validate_update(&update)?;
        self.inner.handle_sync_step2(awareness, update)
    }

    fn handle_update(
        &self,
        awareness: &mut Awareness,
        update: Update,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.validate_update(&update)?;
        self.inner.handle_update(awareness, update)
    }

    fn handle_auth(
        &self,
        awareness: &Awareness,
        deny_reason: Option<String>,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.inner.handle_auth(awareness, deny_reason)
    }

    fn handle_awareness_query(
        &self,
        awareness: &Awareness,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.inner.handle_awareness_query(awareness)
    }

    fn handle_awareness_update(
        &self,
        awareness: &mut Awareness,
        update: yrs::sync::awareness::AwarenessUpdate,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.inner.handle_awareness_update(awareness, update)
    }

    fn missing_handle(
        &self,
        awareness: &mut Awareness,
        tag: u8,
        data: Vec<u8>,
    ) -> Result<Option<Message>, yrs::sync::Error> {
        self.inner.missing_handle(awareness, tag, data)
    }
}

pub async fn peer(
    ws: axum::extract::ws::WebSocket,
    active_doc: Arc<ActiveDocument>,
    doc_id: DocumentId,
    state: Arc<AppState>,
    user: UserId,
) {
    info!("peer connected to {}", doc_id.0);
    active_doc.peer_connected();

    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);

    let protocol = SessionProtocol::new(doc_id, user, state.store.clone());
    tracing::trace!(doc=%doc_id.0, "subscribing peer to broadcast group");
    let sub = active_doc
        .bcast
        .subscribe_with(sink.clone(), stream, protocol);

    match sub.completed().await {
        Ok(_) => info!("peer finished for {}", doc_id.0),
        Err(err) => warn!("peer aborted for {}: {}", doc_id.0, err),
    }

    let remaining = active_doc.peer_disconnected();
    tracing::debug!(doc=%doc_id.0, remaining_peers=remaining.saturating_sub(1), "peer disconnected");
    if remaining == 1
        && let Err(e) = close_document(state.clone(), doc_id).await
    {
        warn!("failed to close document {}: {}", doc_id.0, e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestResult<T> = Result<T, AppError>;

    #[test]
    fn app_state_new_empty() {
        let state = AppState::default();
        let guard = state.docs.blocking_read();
        assert!(guard.is_empty());
    }

    #[tokio::test]
    async fn get_or_create_inserts_document() -> TestResult<()> {
        let state = Arc::new(AppState::default());
        let id = DocumentId(uuid::Uuid::new_v4());
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
        let id = DocumentId(uuid::Uuid::new_v4());
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
