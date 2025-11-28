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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use anyhow::{ensure, Context, Result};
    use tokio::time::sleep;
    use yrs::sync::awareness::AwarenessUpdateEntry;
    use yrs::updates::decoder::Decode;
    use yrs::{Doc, ReadTxn, StateVector, Text, Transact, Update};

    use crate::persistence::{ClientId, DocumentId, DocumentStore, MemoryStore, UserId};

    use super::SessionProtocol;

    fn update_from_client(client_id: u64, text: &str) -> Result<Update> {
        let doc = Doc::with_client_id(client_id);
        let ytext = doc.get_or_insert_text("content");
        {
            let mut txn = doc.transact_mut();
            ytext.insert(&mut txn, 0, text);
        }
        let txn = doc.transact();
        let bytes = txn.encode_state_as_update_v1(&StateVector::default());
        Update::decode_v1(&bytes).map_err(anyhow::Error::from)
    }

    #[tokio::test]
    async fn accepts_single_client_and_records_session() -> Result<()> {
        let store = Arc::new(MemoryStore::default());
        let protocol = SessionProtocol::new(
            DocumentId::from(1_u64),
            UserId("alice".to_string()),
            store.clone(),
        );

        let update = update_from_client(1, "hello")?;
        protocol.validate_update(&update)?;

        // ensure the async session recorder had a chance to run
        sleep(Duration::from_millis(10)).await;
        let recorded = store
            .get_session(DocumentId::from(1_u64), ClientId(1))
            .await
            .context("session should have been recorded")?;
        ensure!(
            recorded == Some(UserId("alice".to_string())),
            "recorded user does not match"
        );

        Ok(())
    }

    #[tokio::test]
    async fn rejects_mismatching_client_across_updates() -> Result<()> {
        let store = Arc::new(MemoryStore::default());
        let protocol = SessionProtocol::new(
            DocumentId::from(2_u64),
            UserId("bob".to_string()),
            store.clone(),
        );

        let first = update_from_client(1, "one")?;
        let second = update_from_client(2, "two")?;

        protocol
            .validate_update(&first)
            .context("first update should establish expected client")?;

        match protocol.validate_update(&second) {
            Err(yrs::sync::Error::PermissionDenied { reason }) => {
                ensure!(
                    reason == "client id mismatch",
                    "unexpected denial reason: {reason}"
                );
            }
            Ok(_) => ensure!(false, "second update with mismatched client should be rejected"),
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    #[tokio::test]
    async fn rejects_mixed_client_ids_in_single_update() -> Result<()> {
        let store = Arc::new(MemoryStore::default());
        let protocol = SessionProtocol::new(
            DocumentId::from(3_u64),
            UserId("carol".to_string()),
            store.clone(),
        );

        let merged_update = Update::merge_updates([update_from_client(5, "a")?, update_from_client(6, "b")?]);

        match protocol.validate_update(&merged_update) {
            Err(yrs::sync::Error::PermissionDenied { reason }) => {
                ensure!(
                    reason == "mixed client ids in update",
                    "unexpected denial reason: {reason}"
                );
            }
            Ok(_) => ensure!(false, "mixed-client update should be rejected"),
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    #[tokio::test]
    async fn rejects_mixed_client_ids_in_awareness_update() -> Result<()> {
        let store = Arc::new(MemoryStore::default());
        let protocol = SessionProtocol::new(
            DocumentId::from(4_u64),
            UserId("dave".to_string()),
            store.clone(),
        );

        let mut clients = HashMap::new();
        clients.insert(
            7,
            AwarenessUpdateEntry {
                clock: 1,
                json: r#"{"name":"eve"}"#.to_string(),
            },
        );
        clients.insert(
            8,
            AwarenessUpdateEntry {
                clock: 1,
                json: r#"{"name":"frank"}"#.to_string(),
            },
        );

        let mixed_update = yrs::sync::awareness::AwarenessUpdate { clients };

        match protocol.validate_awareness_update(&mixed_update) {
            Err(yrs::sync::Error::PermissionDenied { reason }) => {
                ensure!(
                    reason == "mixed client ids in awareness update",
                    "unexpected denial reason: {reason}"
                );
            }
            Ok(_) => ensure!(false, "awareness update with mixed clients should be rejected"),
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }
}
