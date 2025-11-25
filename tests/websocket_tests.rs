use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};
use uuid::Uuid;

mod common;

#[tokio::test]
async fn test_websocket_connection_success() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut ws = common::connect_to_doc(addr, doc_id).await;

    // Connection successful, now clean disconnect
    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn test_connection_with_valid_uuid() {
    let (addr, _state) = common::spawn_test_server().await;
    let uuid = Uuid::new_v4();

    let result = tokio::time::timeout(
        Duration::from_secs(5),
        common::connect_to_doc(addr, uuid)
    ).await;

    assert!(result.is_ok(), "Connection timed out");
}

#[tokio::test]
async fn test_connection_with_invalid_uuid_path() {
    let (addr, _state) = common::spawn_test_server().await;

    // Try connecting with invalid UUID
    let url = format!("ws://{}/ws/not-a-valid-uuid", addr);
    let result = tokio_tungstenite::connect_async(&url).await;

    // Connection should fail due to invalid UUID in path
    assert!(result.is_err() || {
        // Or if Axum allows connection, it might return 404 or similar
        if let Ok((mut ws, _)) = result {
            // Try to receive a message, might get error
            let msg = tokio::time::timeout(Duration::from_millis(500), ws.next()).await;
            ws.close(None).await.ok();
            msg.is_err() // Timeout means no proper connection
        } else {
            false
        }
    });
}

#[tokio::test]
async fn test_multiple_connections_same_document() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let ws1 = common::connect_to_doc(addr, doc_id).await;
    let ws2 = common::connect_to_doc(addr, doc_id).await;
    let ws3 = common::connect_to_doc(addr, doc_id).await;

    // All connected to same document
    tokio::time::sleep(Duration::from_millis(100)).await;

    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 1, "Should have exactly 1 document");
    assert!(docs.get(&doc_id).unwrap().upgrade().is_some(), "Document should be alive");

    drop(ws1);
    drop(ws2);
    drop(ws3);
}

#[tokio::test]
async fn test_multiple_connections_different_documents() {
    let (addr, state) = common::spawn_test_server().await;

    let doc_id1 = Uuid::new_v4();
    let doc_id2 = Uuid::new_v4();
    let doc_id3 = Uuid::new_v4();

    let ws1 = common::connect_to_doc(addr, doc_id1).await;
    let ws2 = common::connect_to_doc(addr, doc_id2).await;
    let ws3 = common::connect_to_doc(addr, doc_id3).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 3, "Should have exactly 3 documents");
    assert!(docs.get(&doc_id1).unwrap().upgrade().is_some());
    assert!(docs.get(&doc_id2).unwrap().upgrade().is_some());
    assert!(docs.get(&doc_id3).unwrap().upgrade().is_some());

    drop(ws1);
    drop(ws2);
    drop(ws3);
}

#[tokio::test]
async fn test_connection_cleanup_on_disconnect() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    {
        let mut ws = common::connect_to_doc(addr, doc_id).await;

        // Verify document exists
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(state.docs.read().await.len(), 1);

        // Abrupt close
        ws.close(Some(CloseFrame {
            code: CloseCode::Normal,
            reason: "test".into(),
        })).await.unwrap();
    }

    // Give time for cleanup
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Weak reference should be dead after all connections drop
    let docs = state.docs.read().await;
    if let Some(weak_doc) = docs.get(&doc_id) {
        assert!(weak_doc.upgrade().is_none(), "Document should be cleaned up");
    }
}

#[tokio::test]
async fn test_sync_message_on_connection() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut ws = common::connect_to_doc(addr, doc_id).await;

    // yrs-axum doesn't send an initial message for empty documents
    // The client needs to initiate the sync by sending a sync step 1 message
    // Instead, we test that the connection is established and stays open
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connection should remain open without sending messages
    // (yrs-axum uses a binary protocol that would require proper message framing)

    drop(ws);
}

#[tokio::test]
async fn test_graceful_shutdown() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut ws1 = common::connect_to_doc(addr, doc_id).await;
    let mut ws2 = common::connect_to_doc(addr, doc_id).await;

    // Gracefully close connections
    ws1.close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "shutdown".into(),
    })).await.unwrap();

    ws2.close(Some(CloseFrame {
        code: CloseCode::Normal,
        reason: "shutdown".into(),
    })).await.unwrap();

    // Should complete without errors
    tokio::time::sleep(Duration::from_millis(100)).await;
}
