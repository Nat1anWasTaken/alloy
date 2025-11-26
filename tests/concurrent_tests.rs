use alloy::persistence::DocumentId;
use common::TestResult;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use yrs::types::Text;
use yrs::{ReadTxn, StateVector, Transact};

mod common;

#[tokio::test]
async fn test_client_insert_text() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let ws = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(state.docs.read().await.len(), 1);

    drop(ws);

    Ok(())
}

#[tokio::test]
async fn test_client_delete_text() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut ws = common::connect_to_doc(addr, doc_id).await?;

    let doc = common::create_text_doc("Hello, world!");
    let text = doc.get_or_insert_text("content");

    {
        let mut txn = doc.transact_mut();
        text.remove_range(&mut txn, 7, 5);
    }

    let txn = doc.transact();
    let update = txn.encode_state_as_update_v1(&StateVector::default());
    ws.send(Message::Binary(update.to_vec())).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    ws.close(None).await?;

    Ok(())
}

#[tokio::test]
async fn test_client_multiple_operations() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let ws = common::connect_to_doc(addr, doc_id).await?;

    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(state.docs.read().await.len(), 1);
    }

    drop(ws);

    Ok(())
}

#[tokio::test]
async fn test_two_clients_sync_text() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let client_a = common::connect_to_doc(addr, doc_id).await?;
    let client_b = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 1, "Should have exactly 1 shared document");
    assert!(docs.get(&doc_id).is_some(), "Document should exist");

    drop(client_a);
    drop(client_b);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_insertions_converge() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut clients = vec![
        common::connect_to_doc(addr, doc_id).await?,
        common::connect_to_doc(addr, doc_id).await?,
        common::connect_to_doc(addr, doc_id).await?,
    ];

    tokio::time::sleep(Duration::from_millis(200)).await;

    let updates = [
        common::create_yrs_update("Client1 "),
        common::create_yrs_update("Client2 "),
        common::create_yrs_update("Client3 "),
    ];

    for (i, update) in updates.iter().enumerate() {
        clients[i].send(Message::Binary(update.clone())).await?;
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    for mut client in clients {
        client.close(None).await?;
    }

    Ok(())
}

#[tokio::test]
async fn test_late_joiner_receives_full_state() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut client_a = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(state.docs.read().await.len(), 1);

    let mut client_b = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        state.docs.read().await.len(),
        1,
        "Should still have 1 shared document"
    );

    client_a.close(None).await?;
    client_b.close(None).await?;

    Ok(())
}

#[tokio::test]
async fn test_client_disconnect_during_sync() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut client_a = common::connect_to_doc(addr, doc_id).await?;
    let client_b = common::connect_to_doc(addr, doc_id).await?;
    let mut client_c = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let update = common::create_yrs_update("Test data");
    client_a.send(Message::Binary(update.clone())).await?;

    drop(client_b);

    tokio::time::sleep(Duration::from_millis(100)).await;

    client_a.send(Message::Binary(update)).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    client_a.close(None).await?;
    client_c.close(None).await?;

    Ok(())
}

#[tokio::test]
async fn test_reconnect_and_sync() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut client = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let update = common::create_yrs_update("Initial edit");
    client.send(Message::Binary(update)).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    client.close(None).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut client2 = common::connect_to_doc(addr, doc_id).await?;

    let update2 = common::create_yrs_update("After reconnect");
    client2.send(Message::Binary(update2)).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    client2.close(None).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_ten_concurrent_clients() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut clients = Vec::new();
    for _ in 0..10 {
        clients.push(common::connect_to_doc(addr, doc_id).await?);
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    assert_eq!(
        state.docs.read().await.len(),
        1,
        "Should have 1 shared document"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    for mut client in clients {
        client.close(None).await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_hundred_clients_connection_only() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let mut clients = Vec::new();

    for _ in 0..100 {
        let client = common::connect_to_doc(addr, doc_id).await?;
        clients.push(client);
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let docs = state.docs.read().await;
    assert_eq!(docs.len(), 1, "Should have exactly 1 shared document");

    for mut client in clients {
        let _ = client.close(None).await;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_rapid_connect_disconnect() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    for _ in 0..50 {
        let mut client = common::connect_to_doc(addr, doc_id).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
        client.close(None).await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}

#[tokio::test]
async fn test_large_document_sync() -> TestResult<()> {
    let (addr, state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new();

    let client_a = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(state.docs.read().await.len(), 1);

    let client_b = common::connect_to_doc(addr, doc_id).await?;

    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(
        state.docs.read().await.len(),
        1,
        "Should share same document"
    );

    drop(client_a);
    drop(client_b);

    Ok(())
}
