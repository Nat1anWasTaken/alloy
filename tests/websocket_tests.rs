use alloy::persistence::DocumentId;
use common::{TestError, TestResult};
use futures_util::StreamExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, frame::coding::CloseCode};

mod common;

#[tokio::test]
async fn test_websocket_connection_success() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut ws = common::connect_to_doc(addr, doc_id).await?;

    ws.close(None).await?;

    Ok(())
}

#[tokio::test]
async fn test_connection_with_valid_id() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let result =
        tokio::time::timeout(Duration::from_secs(5), common::connect_to_doc(addr, doc_id)).await;

    match result {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(err)) => Err(err),
        Err(_) => Err(TestError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connect_to_doc timed out",
        ))),
    }
}

#[tokio::test]
async fn test_connection_with_invalid_id_path() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;

    let url = format!("ws://{}/ws/not-a-valid-id", addr);
    let result = tokio_tungstenite::connect_async(&url).await;

    if result.is_err() {
        return Ok(());
    }

    let (mut ws, _) = result?;
    let recv_result = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
    ws.close(None).await.ok();

    match recv_result {
        Ok(Some(Ok(_))) => Err(TestError::UnexpectedConnection),
        Ok(Some(Err(err))) => Err(err.into()),
        _ => Ok(()),
    }
}

#[tokio::test]
async fn test_multiple_connections_same_document() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let ws1 = common::connect_to_doc(addr, doc_id).await?;
    let ws2 = common::connect_to_doc(addr, doc_id).await?;
    let ws3 = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 1, "Should have exactly 1 document");
    let doc = docs
        .get(&doc_id)
        .ok_or(TestError::MissingDocument(doc_id))?;
    assert!(doc.upgrade().is_some(), "Document should be alive");

    drop(ws1);
    drop(ws2);
    drop(ws3);

    Ok(())
}

#[tokio::test]
async fn test_multiple_connections_different_documents() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;

    let doc_id1 = DocumentId::new();
    let doc_id2 = DocumentId::new();
    let doc_id3 = DocumentId::new();

    let ws1 = common::connect_to_doc(addr, doc_id1).await?;
    let ws2 = common::connect_to_doc(addr, doc_id2).await?;
    let ws3 = common::connect_to_doc(addr, doc_id3).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 3, "Should have exactly 3 documents");
    let doc1 = docs
        .get(&doc_id1)
        .ok_or(TestError::MissingDocument(doc_id1))?;
    let doc2 = docs
        .get(&doc_id2)
        .ok_or(TestError::MissingDocument(doc_id2))?;
    let doc3 = docs
        .get(&doc_id3)
        .ok_or(TestError::MissingDocument(doc_id3))?;
    assert!(doc1.upgrade().is_some());
    assert!(doc2.upgrade().is_some());
    assert!(doc3.upgrade().is_some());

    drop(ws1);
    drop(ws2);
    drop(ws3);

    Ok(())
}

#[tokio::test]
async fn test_connection_cleanup_on_disconnect() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    {
        let mut ws = common::connect_to_doc(addr, doc_id).await?;

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(state.docs.read().await.len(), 1);

        ws.close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "test".into(),
        }))
        .await?;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    let docs = state.docs.read().await;
    if let Some(weak_doc) = docs.get(&doc_id) {
        assert!(
            weak_doc.upgrade().is_none(),
            "Document should be cleaned up"
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_sync_message_on_connection() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let ws = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(ws);

    Ok(())
}

#[tokio::test]
async fn test_graceful_shutdown() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut ws1 = common::connect_to_doc(addr, doc_id).await?;
    let mut ws2 = common::connect_to_doc(addr, doc_id).await?;

    ws1.close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "shutdown".into(),
    }))
    .await?;

    ws2.close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "shutdown".into(),
    }))
    .await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    Ok(())
}
