use alloy::persistence::{ClientId, DocumentId, SnapshotBytes, Tag, UpdateBytes, UserId};
use base64::Engine;
use common::{TestResult, create_yrs_update, spawn_test_server};
use reqwest::StatusCode;
use serde::Deserialize;

mod common;

#[tokio::test]
async fn snapshots_endpoints_support_filters() -> TestResult<()> {
    let (addr, state) = spawn_test_server().await?;
    let doc = DocumentId::from(101_u64);

    // Insert snapshots with different tags and seq
    state
        .store
        .store_snapshot(doc, SnapshotBytes(vec![1]), vec![Tag("draft".into())], 5)
        .await?;
    state
        .store
        .store_snapshot(
            doc,
            SnapshotBytes(vec![2]),
            vec![Tag("draft".into()), Tag("release".into())],
            10,
        )
        .await?;

    let client = reqwest::Client::new();

    // List filtered by tag=release should return only the second snapshot
    let url = format!(
        "http://{}/api/documents/{}/snapshots?tag=release",
        addr, doc
    );
    let resp = client.get(url).send().await?;
    let status = resp.status();
    let bytes = resp.bytes().await?;
    if status != StatusCode::OK {
        let body = String::from_utf8_lossy(&bytes);
        return Err(common::TestError::UnexpectedResponse(format!(
            "snapshots list failed: status {status}, body: {body}"
        )));
    }
    let body: SnapshotList = serde_json::from_slice(&bytes)?;
    assert_eq!(body.snapshots.len(), 1);
    assert_eq!(body.snapshots[0].base_seq, 10);

    // Latest with tag=draft should pick seq 10 (most recent matching)
    let url = format!(
        "http://{}/api/documents/{}/snapshots/latest?tag=draft",
        addr, doc
    );
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let snap: Snapshot = resp.json().await?;
    assert_eq!(snap.base_seq, 10);

    // Get by base_seq
    let url = format!("http://{}/api/documents/{}/snapshots/5", addr, doc);
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let snap: Snapshot = resp.json().await?;
    assert_eq!(snap.base_seq, 5);
    let expected = base64::engine::general_purpose::STANDARD.encode([1_u8]);
    assert_eq!(snap.snapshot, expected);

    // since_seq filter excludes base_seq below threshold
    let url = format!(
        "http://{}/api/documents/{}/snapshots?since_seq=9",
        addr, doc
    );
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: SnapshotList = resp.json().await?;
    assert!(body.snapshots.iter().all(|s| s.base_seq >= 9));

    Ok(())
}

#[tokio::test]
async fn sessions_endpoints_filter_and_lookup() -> TestResult<()> {
    let (addr, state) = spawn_test_server().await?;
    let doc = DocumentId::from(202_u64);

    state
        .store
        .record_session(doc, ClientId(1), UserId("alice".into()))
        .await?;
    state
        .store
        .record_session(doc, ClientId(2), UserId("bob".into()))
        .await?;

    let client = reqwest::Client::new();

    // List filtered by user_id
    let url = format!(
        "http://{}/api/documents/{}/sessions?user_id=alice",
        addr, doc
    );
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let list: SessionList = resp.json().await?;
    assert_eq!(list.sessions.len(), 1);
    assert_eq!(list.sessions[0].user_id, "alice");

    // Lookup by client id
    let url = format!("http://{}/api/documents/{}/sessions/2", addr, doc);
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let session: Session = resp.json().await?;
    assert_eq!(session.client_id, 2);
    assert_eq!(session.user_id, "bob");

    Ok(())
}

#[tokio::test]
async fn document_content_round_trip_and_delete() -> TestResult<()> {
    let (addr, state) = spawn_test_server().await?;
    let doc = DocumentId::from(303_u64);

    // Append an update with text content
    let update = create_yrs_update("hello world");
    state.store.append_update(doc, UpdateBytes(update)).await?;
    state
        .store
        .record_session(doc, ClientId(9), UserId("carol".into()))
        .await?;

    let client = reqwest::Client::new();

    // Fetch content
    let url = format!("http://{}/api/documents/{}", addr, doc);
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let content: DocContent = resp.json().await?;
    assert_eq!(content.content, "hello world");
    assert_eq!(content.seq, 1);

    // Delete the document
    let url = format!("http://{}/api/documents/{}", addr, doc);
    let resp = client.delete(url).send().await?;
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    // Subsequent content fetch should be empty (data removed)
    let url = format!("http://{}/api/documents/{}", addr, doc);
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let content: DocContent = resp.json().await?;
    assert_eq!(content.content, "");
    assert_eq!(content.seq, 0);

    // Snapshots list should now be empty
    let url = format!("http://{}/api/documents/{}/snapshots", addr, doc);
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let snaps: SnapshotList = resp.json().await?;
    assert!(snaps.snapshots.is_empty());

    // Sessions list should now be empty
    let url = format!("http://{}/api/documents/{}/sessions", addr, doc);
    let resp = client.get(url).send().await?;
    assert_eq!(resp.status(), StatusCode::OK);
    let sessions: SessionList = resp.json().await?;
    assert!(sessions.sessions.is_empty());

    Ok(())
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct Snapshot {
    base_seq: i64,
    tags: Vec<String>,
    snapshot: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SnapshotList {
    snapshots: Vec<Snapshot>,
    next_cursor: Option<i64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct Session {
    seq: i64,
    client_id: u64,
    user_id: String,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct SessionList {
    sessions: Vec<Session>,
    next_cursor: Option<i64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct DocContent {
    doc_id: u64,
    content: String,
    seq: i64,
}
