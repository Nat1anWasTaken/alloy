use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::Mutex;
use yrs::Doc;
use yrs_axum::broadcast::BroadcastGroup;

use crate::persistence::DocumentId;
use crate::recorder::RecorderHandle;

pub struct ActiveDocument {
    pub id: DocumentId,
    pub doc: Doc,
    pub bcast: Arc<BroadcastGroup>,
    pub peers: AtomicUsize,
    recorder: Mutex<Option<RecorderHandle>>, // holds recorder shutdown handle
}

impl ActiveDocument {
    pub fn new(
        id: DocumentId,
        doc: Doc,
        bcast: Arc<BroadcastGroup>,
        recorder: RecorderHandle,
    ) -> Self {
        Self {
            id,
            doc,
            bcast,
            peers: AtomicUsize::new(0),
            recorder: Mutex::new(Some(recorder)),
        }
    }

    pub fn peer_connected(&self) {
        self.peers.fetch_add(1, Ordering::SeqCst);
    }

    pub fn peer_disconnected(&self) -> usize {
        self.peers.fetch_sub(1, Ordering::SeqCst)
    }

    pub async fn take_recorder(&self) -> Option<RecorderHandle> {
        self.recorder.lock().await.take()
    }
}
