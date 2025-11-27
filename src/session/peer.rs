use std::sync::Arc;

use axum::extract::ws::WebSocket;
use futures_util::StreamExt;
use tokio::sync::Mutex;
use tracing::{info, warn};
use yrs_axum::ws::{AxumSink, AxumStream};

use crate::app_state::AppState;
use crate::document::{ActiveDocument, close_document};
use crate::persistence::{DocumentId, UserId};

use super::protocol::SessionProtocol;

pub async fn peer(
    ws: WebSocket,
    active_doc: Arc<ActiveDocument>,
    doc_id: DocumentId,
    state: Arc<AppState>,
    user: UserId,
) {
    info!("peer connected to {}", doc_id);
    active_doc.peer_connected();

    let (sink, stream) = ws.split();
    let sink = Arc::new(Mutex::new(AxumSink::from(sink)));
    let stream = AxumStream::from(stream);

    let protocol = SessionProtocol::new(doc_id, user, state.store.clone());
    tracing::trace!(doc=%doc_id, "subscribing peer to broadcast group");
    let sub = active_doc
        .bcast
        .subscribe_with(sink.clone(), stream, protocol);

    match sub.completed().await {
        Ok(_) => info!("peer finished for {}", doc_id),
        Err(err) => warn!("peer aborted for {}: {}", doc_id, err),
    }

    let remaining = active_doc.peer_disconnected();
    tracing::debug!(doc=%doc_id, remaining_peers=remaining.saturating_sub(1), "peer disconnected");
    if remaining == 1
        && let Err(e) = close_document(state.clone(), doc_id).await
    {
        warn!("failed to close document {}: {}", doc_id, e);
    }
}
