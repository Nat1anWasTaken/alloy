use std::collections::HashMap;
use std::sync::{Arc, Weak};

use tokio::sync::RwLock;

use crate::document::ActiveDocument;
use crate::persistence::{DocumentId, RecorderConfig, SharedStore};
use crate::session::TicketIssuer;

pub struct AppState {
    pub docs: RwLock<HashMap<DocumentId, Weak<ActiveDocument>>>,
    pub store: SharedStore,
    pub recorder_config: RecorderConfig,
    pub ticketing: TicketIssuer,
}

impl AppState {
    pub fn with_store(store: SharedStore) -> Self {
        Self::with_components(store, TicketIssuer::from_env_or_generate())
    }

    pub fn with_components(store: SharedStore, ticketing: TicketIssuer) -> Self {
        Self {
            docs: RwLock::new(HashMap::new()),
            store,
            recorder_config: RecorderConfig::default(),
            ticketing,
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

#[cfg(test)]
mod tests {
    use super::AppState;

    #[test]
    fn app_state_new_empty() {
        let state = AppState::default();
        let guard = state.docs.blocking_read();
        assert!(guard.is_empty());
    }
}
