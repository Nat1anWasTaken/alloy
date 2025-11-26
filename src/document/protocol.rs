use tokio::sync::OnceCell;
use tracing::warn;
use yrs::sync::protocol::DefaultProtocol;
use yrs::sync::{Awareness, Message, Protocol};
use yrs::{StateVector, Update};

use crate::persistence::{ClientId, DocumentId, SharedStore, UserId};

/// Protocol wrapper that ties a client_id to a user_id per document and rejects mismatches.
pub(crate) struct SessionProtocol {
    doc_id: DocumentId,
    user: UserId,
    store: SharedStore,
    expected: OnceCell<ClientId>,
    inner: DefaultProtocol,
}

impl SessionProtocol {
    pub fn new(doc_id: DocumentId, user: UserId, store: SharedStore) -> Self {
        Self {
            doc_id,
            user,
            store,
            expected: OnceCell::new(),
            inner: DefaultProtocol,
        }
    }

    fn ensure_expected_client(&self, client: u64) -> Result<(), yrs::sync::Error> {
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
        self.ensure_expected_client(client)
    }

    fn validate_awareness_update(
        &self,
        update: &yrs::sync::awareness::AwarenessUpdate,
    ) -> Result<(), yrs::sync::Error> {
        let mut iter = update.clients.keys();
        let client = match iter.next() {
            Some(client) => *client,
            None => return Ok(()),
        };

        if iter.any(|other| *other != client) {
            return Err(yrs::sync::Error::PermissionDenied {
                reason: "mixed client ids in awareness update".to_string(),
            });
        }

        self.ensure_expected_client(client)
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
        self.validate_awareness_update(&update)?;
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
