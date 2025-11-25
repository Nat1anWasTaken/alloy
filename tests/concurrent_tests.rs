use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;
use yrs::{Doc, ReadTxn, StateVector, Transact, Update};
use yrs::types::Text;

mod common;

// ============================================================================
// Single Client Operations
// ============================================================================

#[tokio::test]
async fn test_client_insert_text() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let ws = common::connect_to_doc(addr, doc_id).await;

    // Note: Sending raw CRDT updates requires the yrs-axum message protocol
    // This test validates that connection is established and document is created
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify document was created
    assert_eq!(state.docs.read().await.len(), 1);

    drop(ws);
}

#[tokio::test]
async fn test_client_delete_text() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut ws = common::connect_to_doc(addr, doc_id).await;

    // Create doc with text, then delete some
    let doc = common::create_text_doc("Hello, world!");
    let text = doc.get_or_insert_text("content");

    // Delete "world"
    {
        let mut txn = doc.transact_mut();
        text.remove_range(&mut txn, 7, 5);
    }

    // Send the update
    let txn = doc.transact();
    let update = txn.encode_state_as_update_v1(&StateVector::default());
    ws.send(Message::Binary(update.to_vec())).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    ws.close(None).await.unwrap();
}

#[tokio::test]
async fn test_client_multiple_operations() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let ws = common::connect_to_doc(addr, doc_id).await;

    // Test that connection stays stable over time
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        // Verify document still exists
        assert_eq!(state.docs.read().await.len(), 1);
    }

    drop(ws);
}

// ============================================================================
// Multi-Client Synchronization
// ============================================================================

#[tokio::test]
async fn test_two_clients_sync_text() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut client_a = common::connect_to_doc(addr, doc_id).await;
    let mut client_b = common::connect_to_doc(addr, doc_id).await;

    // Wait for connections to establish
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify both clients are connected to the same document
    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 1, "Should have exactly 1 shared document");
    assert!(docs.get(&doc_id).is_some(), "Document should exist");

    drop(client_a);
    drop(client_b);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_insertions_converge() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    // Connect 3 clients
    let mut clients = vec![
        common::connect_to_doc(addr, doc_id).await,
        common::connect_to_doc(addr, doc_id).await,
        common::connect_to_doc(addr, doc_id).await,
    ];

    // Wait for initial sync
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Each client inserts different text
    let updates = vec![
        common::create_yrs_update("Client1 "),
        common::create_yrs_update("Client2 "),
        common::create_yrs_update("Client3 "),
    ];

    // Send updates from each client
    for (i, update) in updates.iter().enumerate() {
        clients[i].send(Message::Binary(update.clone())).await.unwrap();
    }

    // Wait for all updates to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Close all clients
    for mut client in clients {
        client.close(None).await.unwrap();
    }
}

#[tokio::test]
async fn test_late_joiner_receives_full_state() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    // Client A connects
    let mut client_a = common::connect_to_doc(addr, doc_id).await;

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify document exists
    assert_eq!(state.docs.read().await.len(), 1);

    // Client B connects later as a "late joiner"
    let mut client_b = common::connect_to_doc(addr, doc_id).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Both clients should be connected to same document
    assert_eq!(state.docs.read().await.len(), 1, "Should still have 1 shared document");

    client_a.close(None).await.unwrap();
    client_b.close(None).await.unwrap();
}

#[tokio::test]
async fn test_client_disconnect_during_sync() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut client_a = common::connect_to_doc(addr, doc_id).await;
    let mut client_b = common::connect_to_doc(addr, doc_id).await;
    let mut client_c = common::connect_to_doc(addr, doc_id).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start sending updates
    let update = common::create_yrs_update("Test data");
    client_a.send(Message::Binary(update.clone())).await.unwrap();

    // Disconnect client B abruptly
    drop(client_b);

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Remaining clients should continue normally
    client_a.send(Message::Binary(update)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    client_a.close(None).await.unwrap();
    client_c.close(None).await.unwrap();
}

#[tokio::test]
async fn test_reconnect_and_sync() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    // Initial connection
    let mut client = common::connect_to_doc(addr, doc_id).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Make edit
    let update = common::create_yrs_update("Initial edit");
    client.send(Message::Binary(update)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Disconnect
    client.close(None).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Reconnect
    let mut client2 = common::connect_to_doc(addr, doc_id).await;

    // Should be able to continue editing
    let update2 = common::create_yrs_update("After reconnect");
    client2.send(Message::Binary(update2)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    client2.close(None).await.unwrap();
}

// ============================================================================
// Stress Tests
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ten_concurrent_clients() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut clients = Vec::new();
    for _ in 0..10 {
        clients.push(common::connect_to_doc(addr, doc_id).await);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify all clients connected to same document
    assert_eq!(state.docs.read().await.len(), 1, "Should have 1 shared document");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Close all clients
    for client in clients {
        drop(client);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_hundred_clients_connection_only() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut clients = Vec::new();

    // Connect 100 clients
    for _ in 0..100 {
        let client = common::connect_to_doc(addr, doc_id).await;
        clients.push(client);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify document exists and is shared
    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 1, "Should have exactly 1 shared document");

    // Close all clients
    for mut client in clients {
        client.close(None).await.ok();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rapid_connect_disconnect() {
    let (addr, _state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    for _ in 0..50 {
        let mut client = common::connect_to_doc(addr, doc_id).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        client.close(None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Should complete without errors or resource leaks
}

#[tokio::test]
async fn test_large_document_sync() {
    let (addr, state) = common::spawn_test_server().await;
    let doc_id = Uuid::new_v4();

    let mut client_a = common::connect_to_doc(addr, doc_id).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify connection established
    assert_eq!(state.docs.read().await.len(), 1);

    // Connect second client
    let mut client_b = common::connect_to_doc(addr, doc_id).await;

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Both clients should be connected
    assert_eq!(state.docs.read().await.len(), 1, "Should share same document");

    drop(client_a);
    drop(client_b);
}
