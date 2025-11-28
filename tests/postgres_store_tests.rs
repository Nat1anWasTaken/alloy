use std::env;

use alloy::persistence::{
    ClientId, DocumentId, DocumentStore, PostgresStore, SessionRecord, SnapshotBytes,
    SnapshotRecord, Tag, UpdateBytes, UserId,
};
use anyhow::{anyhow, Result};
use rand::{thread_rng, Rng};

fn database_url() -> Option<String> {
    env::var("TEST_DATABASE_URL")
        .ok()
        .or_else(|| env::var("DATABASE_URL").ok())
}

async fn init_store() -> Result<Option<PostgresStore>> {
    let Some(url) = database_url() else {
        eprintln!("skipping postgres store tests: set TEST_DATABASE_URL or DATABASE_URL");
        return Ok(None);
    };

    // Keep the pool small for tests to reduce resource usage
    // set_var is unsafe in Rust 2024 because it mutates global process state that may be read
    // concurrently from other threads.
    unsafe {
        env::set_var("DATABASE_URL", url);
        env::set_var("DATABASE_MIN_CONNECTIONS", "1");
        env::set_var("DATABASE_MAX_CONNECTIONS", "2");
    }

    let store = PostgresStore::from_env().await.map_err(|e| anyhow!(e))?;
    Ok(Some(store))
}

fn new_doc_id() -> DocumentId {
    let mut rng = thread_rng();
    // Keep within i64::MAX to satisfy advisory lock requirement
    let raw: u64 = rng.gen_range(1..i64::MAX as u64);
    DocumentId::from(raw)
}

async fn cleanup(store: &PostgresStore, doc: DocumentId) -> Result<()> {
    store.delete_document(doc).await.map_err(|e| anyhow!(e))
}

#[tokio::test]
async fn append_update_assigns_monotonic_seq() -> Result<()> {
    let Some(store) = init_store().await? else {
        return Ok(());
    };

    let doc = new_doc_id();
    cleanup(&store, doc).await?;

    let seq1 = store
        .append_update(doc, UpdateBytes(vec![1, 2, 3]))
        .await
        .map_err(|e| anyhow!(e))?;
    let seq2 = store
        .append_update(doc, UpdateBytes(vec![4, 5]))
        .await
        .map_err(|e| anyhow!(e))?;

    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);

    let updates = store
        .load_updates_since(doc, 0)
        .await
        .map_err(|e| anyhow!(e))?;
    assert_eq!(updates.len(), 2);
    assert_eq!(updates[0].seq, 1);
    assert_eq!(updates[0].bytes.0, vec![1, 2, 3]);
    assert_eq!(updates[1].seq, 2);
    assert_eq!(updates[1].bytes.0, vec![4, 5]);

    cleanup(&store, doc).await?;
    Ok(())
}

#[tokio::test]
async fn store_and_load_snapshot_roundtrip() -> Result<()> {
    let Some(store) = init_store().await? else {
        return Ok(());
    };

    let doc = new_doc_id();
    cleanup(&store, doc).await?;

    let tags = vec![Tag("draft".to_string()), Tag("v1".to_string())];
    store
        .store_snapshot(doc, SnapshotBytes(vec![9, 9, 9]), tags.clone(), 7)
        .await
        .map_err(|e| anyhow!(e))?;

    let latest = store
        .load_latest_snapshot(doc)
        .await
        .map_err(|e| anyhow!(e))?;

    let specific = store
        .load_snapshot(doc, 7)
        .await
        .map_err(|e| anyhow!(e))?;

    assert_eq!(latest, specific);

    let SnapshotRecord {
        snapshot,
        tags: stored_tags,
        base_seq,
    } = latest.ok_or_else(|| anyhow!("snapshot missing"))?;

    assert_eq!(snapshot.0, vec![9, 9, 9]);
    assert_eq!(stored_tags, tags);
    assert_eq!(base_seq, 7);

    cleanup(&store, doc).await?;
    Ok(())
}

#[tokio::test]
async fn list_snapshots_paginates() -> Result<()> {
    let Some(store) = init_store().await? else {
        return Ok(());
    };

    let doc = new_doc_id();
    cleanup(&store, doc).await?;

    for base_seq in 1..=3 {
        store
            .store_snapshot(
                doc,
                SnapshotBytes(vec![base_seq as u8]),
                Vec::new(),
                base_seq,
            )
            .await
            .map_err(|e| anyhow!(e))?;
    }

    let first_page = store
        .list_snapshots(doc, None, 2)
        .await
        .map_err(|e| anyhow!(e))?;
    assert_eq!(first_page.snapshots.len(), 2);
    assert_eq!(first_page.snapshots[0].base_seq, 1);
    assert_eq!(first_page.snapshots[1].base_seq, 2);
    let cursor = first_page.next_cursor.expect("next cursor missing");

    let second_page = store
        .list_snapshots(doc, Some(cursor), 2)
        .await
        .map_err(|e| anyhow!(e))?;
    assert_eq!(second_page.snapshots.len(), 1);
    assert_eq!(second_page.snapshots[0].base_seq, 3);
    assert!(second_page.next_cursor.is_none());

    cleanup(&store, doc).await?;
    Ok(())
}

#[tokio::test]
async fn record_session_is_idempotent_per_client() -> Result<()> {
    let Some(store) = init_store().await? else {
        return Ok(());
    };

    let doc = new_doc_id();
    cleanup(&store, doc).await?;

    let client = ClientId(42);
    let user = UserId("alice".to_string());

    store
        .record_session(doc, client, user.clone())
        .await
        .map_err(|e| anyhow!(e))?;
    store
        .record_session(doc, client, user.clone())
        .await
        .map_err(|e| anyhow!(e))?;

    let session = store
        .get_session(doc, client)
        .await
        .map_err(|e| anyhow!(e))?;
    assert_eq!(session, Some(user.clone()));

    let page = store
        .list_sessions(doc, None, 10)
        .await
        .map_err(|e| anyhow!(e))?;

    assert_eq!(page.sessions.len(), 1);
    let SessionRecord { seq, client: c, user: u } = page.sessions[0].clone();
    assert_eq!(seq, 1);
    assert_eq!(c, client);
    assert_eq!(u, user);

    cleanup(&store, doc).await?;
    Ok(())
}

#[tokio::test]
async fn delete_document_clears_all_state() -> Result<()> {
    let Some(store) = init_store().await? else {
        return Ok(());
    };

    let doc = new_doc_id();
    cleanup(&store, doc).await?;

    store
        .append_update(doc, UpdateBytes(vec![7]))
        .await
        .map_err(|e| anyhow!(e))?;
    store
        .store_snapshot(doc, SnapshotBytes(vec![8]), Vec::new(), 1)
        .await
        .map_err(|e| anyhow!(e))?;
    store
        .record_session(doc, ClientId(1), UserId("bob".to_string()))
        .await
        .map_err(|e| anyhow!(e))?;

    store.delete_document(doc).await.map_err(|e| anyhow!(e))?;

    let updates = store
        .load_updates_since(doc, 0)
        .await
        .map_err(|e| anyhow!(e))?;
    let snapshots = store
        .load_latest_snapshot(doc)
        .await
        .map_err(|e| anyhow!(e))?;
    let sessions = store
        .list_sessions(doc, None, 10)
        .await
        .map_err(|e| anyhow!(e))?;

    assert!(updates.is_empty());
    assert!(snapshots.is_none());
    assert!(sessions.sessions.is_empty());

    Ok(())
}
