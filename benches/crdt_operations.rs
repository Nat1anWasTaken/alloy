use alloy::document::{AppState, get_or_create_doc};
use alloy::persistence::DocumentId;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use uuid::Uuid;
use yrs::updates::decoder::Decode;
use yrs::{Doc, ReadTxn, StateVector, Text, Transact};

fn runtime_single_thread() -> Option<tokio::runtime::Runtime> {
    tokio::runtime::Runtime::new()
        .map_err(|err| {
            eprintln!("failed to build Tokio runtime: {err}");
        })
        .ok()
}

fn runtime_multi_thread(worker_threads: usize) -> Option<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .build()
        .map_err(|err| {
            eprintln!("failed to build Tokio multi-thread runtime: {err}");
        })
        .ok()
}

fn decode_update(bytes: &[u8]) -> Option<yrs::Update> {
    yrs::Update::decode_v1(bytes)
        .map_err(|err| {
            eprintln!("failed to decode update: {err}");
        })
        .ok()
}

// ============================================================================
// Benchmark Group 1: Document Operations
// ============================================================================

fn bench_document_creation(c: &mut Criterion) {
    let Some(runtime) = runtime_single_thread() else {
        return;
    };

    c.bench_function("document_creation", |b| {
        b.to_async(&runtime).iter(|| async {
            let state = Arc::new(AppState::new());
            let doc_id = DocumentId(Uuid::new_v4());
            if let Err(err) = get_or_create_doc(state, black_box(doc_id)).await {
                eprintln!("document_creation iteration failed: {err}");
            }
        });
    });
}

fn bench_document_retrieval(c: &mut Criterion) {
    let Some(runtime) = runtime_single_thread() else {
        return;
    };
    let state = Arc::new(AppState::new());
    let doc_id = DocumentId(Uuid::new_v4());

    // Pre-create document
    runtime.block_on(async {
        if let Err(err) = get_or_create_doc(state.clone(), doc_id).await {
            eprintln!("failed to seed document for retrieval benchmark: {err}");
        }
    });

    c.bench_function("document_retrieval", |b| {
        b.to_async(&runtime).iter(|| async {
            if let Err(err) = get_or_create_doc(state.clone(), black_box(doc_id)).await {
                eprintln!("document_retrieval iteration failed: {err}");
            }
        });
    });
}

fn bench_document_concurrent_access(c: &mut Criterion) {
    let mut group = c.benchmark_group("document_concurrent_access");

    for num_clients in [1, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_clients),
            num_clients,
            |b, &num_clients| {
                let Some(runtime) = runtime_multi_thread(4) else {
                    return;
                };

                let state = Arc::new(AppState::new());
                let doc_id = DocumentId(Uuid::new_v4());

                // Pre-create document
                runtime.block_on(async {
                    if let Err(err) = get_or_create_doc(state.clone(), doc_id).await {
                        eprintln!("failed to seed document for concurrent access benchmark: {err}");
                    }
                });

                b.to_async(&runtime).iter(|| async {
                    let mut handles = Vec::new();
                    for _ in 0..num_clients {
                        let state = state.clone();
                        let handle =
                            tokio::spawn(async move { get_or_create_doc(state, doc_id).await });
                        handles.push(handle);
                    }

                    for result in futures_util::future::join_all(handles).await {
                        match result {
                            Ok(Ok(_)) => {}
                            Ok(Err(err)) => eprintln!("concurrent access task failed: {err}"),
                            Err(join_err) => {
                                eprintln!("concurrent access task join error: {join_err}")
                            }
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

// ============================================================================
// Benchmark Group 2: CRDT Operations
// ============================================================================

fn bench_text_insert(c: &mut Criterion) {
    c.bench_function("crdt_text_insert", |b| {
        b.iter(|| {
            let doc = Doc::new();
            let text = doc.get_or_insert_text("content");
            let mut txn = doc.transact_mut();
            text.insert(&mut txn, 0, black_box("Hello, world!"));
        });
    });
}

fn bench_text_delete(c: &mut Criterion) {
    c.bench_function("crdt_text_delete", |b| {
        b.iter_batched(
            || {
                // Setup: create doc with text
                let doc = Doc::new();
                let text = doc.get_or_insert_text("content");
                {
                    let mut txn = doc.transact_mut();
                    text.insert(&mut txn, 0, "Hello, world!");
                }
                doc
            },
            |doc| {
                // Benchmark: delete text
                let text = doc.get_or_insert_text("content");
                let mut txn = doc.transact_mut();
                text.remove_range(&mut txn, 0, 5);
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

fn bench_encode_state_vector(c: &mut Criterion) {
    let doc = Doc::new();
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        text.insert(&mut txn, 0, "Test content for encoding");
    }

    c.bench_function("crdt_encode_state_vector", |b| {
        b.iter(|| {
            let txn = doc.transact();
            let _update = txn.encode_state_as_update_v1(black_box(&StateVector::default()));
        });
    });
}

fn bench_apply_update(c: &mut Criterion) {
    // Create source doc with update
    let src_doc = Doc::new();
    let text = src_doc.get_or_insert_text("content");
    {
        let mut txn = src_doc.transact_mut();
        text.insert(&mut txn, 0, "Update content");
    }

    let update = {
        let txn = src_doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };

    c.bench_function("crdt_apply_update", |b| {
        b.iter_batched(
            Doc::new,
            |doc| {
                let mut txn = doc.transact_mut();
                if let Some(decoded) = decode_update(black_box(&update)) {
                    txn.apply_update(decoded);
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });
}

// ============================================================================
// Benchmark Group 3: Multi-Client Scenarios
// ============================================================================

fn bench_broadcast_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("broadcast_simulation");

    for num_clients in [1, 10, 50].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_clients),
            num_clients,
            |b, &num_clients| {
                b.iter(|| {
                    // Simulate broadcasting to N clients
                    let src_doc = Doc::new();
                    let text = src_doc.get_or_insert_text("content");
                    {
                        let mut txn = src_doc.transact_mut();
                        text.insert(&mut txn, 0, "Broadcast message");
                    }

                    let update = {
                        let txn = src_doc.transact();
                        txn.encode_state_as_update_v1(&StateVector::default())
                    };

                    // Simulate applying to N clients
                    for _ in 0..num_clients {
                        let client_doc = Doc::new();
                        let mut txn = client_doc.transact_mut();
                        if let Some(decoded) = decode_update(&update) {
                            txn.apply_update(decoded);
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_sync_full_state(c: &mut Criterion) {
    // Create doc with substantial content
    let doc = Doc::new();
    let text = doc.get_or_insert_text("content");
    {
        let mut txn = doc.transact_mut();
        for i in 0..100 {
            let len = text.len(&txn);
            text.insert(&mut txn, len, &format!("Line {} content ", i));
        }
    }

    let full_state = {
        let txn = doc.transact();
        txn.encode_state_as_update_v1(&StateVector::default())
    };

    c.bench_function("sync_full_state", |b| {
        b.iter(|| {
            let new_doc = Doc::new();
            let mut txn = new_doc.transact_mut();
            if let Some(decoded) = decode_update(black_box(&full_state)) {
                txn.apply_update(decoded);
            }
        });
    });
}

// ============================================================================
// Benchmark Group 4: Scalability Tests
// ============================================================================

fn bench_document_count_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("document_count_scaling");

    for num_docs in [1, 10, 100].iter() {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_docs),
            num_docs,
            |b, &num_docs| {
                let Some(runtime) = runtime_single_thread() else {
                    return;
                };

                b.to_async(&runtime).iter(|| async {
                    let state = Arc::new(AppState::new());

                    for _ in 0..num_docs {
                        let doc_id = DocumentId(Uuid::new_v4());
                        if let Err(err) = get_or_create_doc(state.clone(), doc_id).await {
                            eprintln!("document_count_scaling iteration failed: {err}");
                            break;
                        }
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_message_size_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_size_scaling");

    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let content = "x".repeat(size);

            b.iter(|| {
                let doc = Doc::new();
                let text = doc.get_or_insert_text("content");
                {
                    let mut txn = doc.transact_mut();
                    text.insert(&mut txn, 0, black_box(&content));
                }

                let txn = doc.transact();
                let _update = txn.encode_state_as_update_v1(&StateVector::default());
            });
        });
    }
    group.finish();
}

// ============================================================================
// Benchmark Group 5: Lock Contention
// ============================================================================

fn bench_read_lock_contention(c: &mut Criterion) {
    let Some(runtime) = runtime_multi_thread(4) else {
        return;
    };

    let state = Arc::new(AppState::new());
    let doc_id = DocumentId(Uuid::new_v4());

    // Pre-create document
    runtime.block_on(async {
        if let Err(err) = get_or_create_doc(state.clone(), doc_id).await {
            eprintln!("failed to seed document for read_lock_contention benchmark: {err}");
        }
    });

    c.bench_function("read_lock_contention", |b| {
        b.to_async(&runtime).iter(|| async {
            let mut handles = Vec::new();

            // 50 concurrent readers
            for _ in 0..50 {
                let state = state.clone();
                let handle = tokio::spawn(async move {
                    let _lock = state.docs.read().await;
                    // Simulate some read work
                    tokio::time::sleep(tokio::time::Duration::from_micros(10)).await;
                });
                handles.push(handle);
            }

            futures_util::future::join_all(handles).await;
        });
    });
}

fn bench_document_creation_race(c: &mut Criterion) {
    let Some(runtime) = runtime_multi_thread(4) else {
        return;
    };

    c.bench_function("document_creation_race", |b| {
        b.to_async(&runtime).iter(|| async {
            let state = Arc::new(AppState::new());
            let doc_id = DocumentId(Uuid::new_v4());
            let mut handles = Vec::new();

            // 20 tasks trying to create same document simultaneously
            for _ in 0..20 {
                let state = state.clone();
                let handle = tokio::spawn(async move { get_or_create_doc(state, doc_id).await });
                handles.push(handle);
            }

            for result in futures_util::future::join_all(handles).await {
                match result {
                    Ok(Ok(_)) => {}
                    Ok(Err(err)) => eprintln!("document_creation_race task failed: {err}"),
                    Err(join_err) => eprintln!("document_creation_race join error: {join_err}"),
                }
            }
        });
    });
}

// ============================================================================
// Criterion Configuration
// ============================================================================

criterion_group!(
    document_ops,
    bench_document_creation,
    bench_document_retrieval,
    bench_document_concurrent_access
);

criterion_group!(
    crdt_ops,
    bench_text_insert,
    bench_text_delete,
    bench_encode_state_vector,
    bench_apply_update
);

criterion_group!(
    multi_client,
    bench_broadcast_simulation,
    bench_sync_full_state
);

criterion_group!(
    scalability,
    bench_document_count_scaling,
    bench_message_size_scaling
);

criterion_group!(
    lock_contention,
    bench_read_lock_contention,
    bench_document_creation_race
);

criterion_main!(
    document_ops,
    crdt_ops,
    multi_client,
    scalability,
    lock_contention
);
