use proptest::prelude::*;
use proptest::test_runner::TestCaseError;
use yrs::{Doc, GetString, ReadTxn, StateVector, Transact, Update};
use yrs::types::Text;
use yrs::updates::decoder::Decode;
use yrs::encoding::read::Error as DecodeError;
use thiserror::Error;

#[derive(Debug, Clone)]
enum TextOperation {
    Insert(usize, String),
    Delete(usize, u32),
}

impl TextOperation {
    fn apply(&self, doc: &Doc) {
        let text = doc.get_or_insert_text("content");
        let mut txn = doc.transact_mut();

        match self {
            TextOperation::Insert(pos, s) => {
                let current_len = text.len(&txn);
                let safe_pos = (*pos).min(current_len as usize);
                text.insert(&mut txn, safe_pos as u32, s);
            }
            TextOperation::Delete(pos, len) => {
                let current_len = text.len(&txn);
                if *pos < current_len as usize {
                    let safe_len = (*len).min(current_len.saturating_sub(*pos as u32));
                    if safe_len > 0 {
                        text.remove_range(&mut txn, *pos as u32, safe_len);
                    }
                }
            }
        }
    }
}
fn text_operation() -> impl Strategy<Value = TextOperation> {
    prop_oneof![
        (0..100usize, "[a-z]{1,10}").prop_map(|(pos, s)| TextOperation::Insert(pos, s)),
        (0..100usize, 1..10u32).prop_map(|(pos, len)| TextOperation::Delete(pos, len)),
    ]
}

fn operation_sequence(min: usize, max: usize) -> impl Strategy<Value = Vec<TextOperation>> {
    prop::collection::vec(text_operation(), min..max)
}

fn apply_operations(doc: &Doc, ops: &[TextOperation]) {
    for op in ops {
        op.apply(doc);
    }
}

fn get_doc_content(doc: &Doc) -> String {
    let text = doc.get_or_insert_text("content");
    let txn = doc.transact();
    text.get_string(&txn)
}

#[derive(Debug, Error)]
enum CrdtTestError {
    #[error("decode update: {0}")]
    Decode(#[from] DecodeError),
}

fn merge_updates(doc_dst: &Doc, doc_src: &Doc) -> Result<(), CrdtTestError> {
    let txn_src = doc_src.transact();
    let update = txn_src.encode_state_as_update_v1(&StateVector::default());

    let mut txn_dst = doc_dst.transact_mut();
    let decoded = Update::decode_v1(&update)?;
    txn_dst.apply_update(decoded);
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn prop_operations_commutative(
        ops_a in operation_sequence(1, 5),
        ops_b in operation_sequence(1, 5)
    ) {
        let doc_a = Doc::new();
        let doc_b = Doc::new();

        let src_a = Doc::new();
        let src_b = Doc::new();
        apply_operations(&src_a, &ops_a);
        apply_operations(&src_b, &ops_b);

        prop_assert!(merge_updates(&doc_a, &src_a).is_ok());
        prop_assert!(merge_updates(&doc_a, &src_b).is_ok());

        prop_assert!(merge_updates(&doc_b, &src_b).is_ok());
        prop_assert!(merge_updates(&doc_b, &src_a).is_ok());

        let content_a = get_doc_content(&doc_a);
        let content_b = get_doc_content(&doc_b);

        prop_assert_eq!(
            content_a,
            content_b,
            "CRDT should be commutative: merging updates in different orders should produce same result"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn prop_updates_idempotent(
        ops in operation_sequence(1, 20)
    ) {
        let doc = Doc::new();
        apply_operations(&doc, &ops);

        let content1 = get_doc_content(&doc);

        let txn = doc.transact();
        let update = txn.encode_state_as_update_v1(&StateVector::default());
        drop(txn);

        let mut txn_mut = doc.transact_mut();
        let decoded = Update::decode_v1(&update)
            .map_err(|err| TestCaseError::fail(format!("decode failed: {err}")))?;
        txn_mut.apply_update(decoded);
        drop(txn_mut);

        let content2 = get_doc_content(&doc);

        prop_assert_eq!(
            content1,
            content2,
            "Re-applying updates should be idempotent"
        );
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_convergence_invariant(
        ops1 in operation_sequence(1, 15),
        ops2 in operation_sequence(1, 15),
        ops3 in operation_sequence(1, 15)
    ) {
        let doc1 = Doc::new();
        let doc2 = Doc::new();
        let doc3 = Doc::new();

        apply_operations(&doc1, &ops1);
        apply_operations(&doc2, &ops2);
        apply_operations(&doc3, &ops3);

        let replica_a = Doc::new();
        let replica_b = Doc::new();
        let replica_c = Doc::new();

        prop_assert!(merge_updates(&replica_a, &doc1).is_ok());
        prop_assert!(merge_updates(&replica_a, &doc2).is_ok());
        prop_assert!(merge_updates(&replica_a, &doc3).is_ok());

        prop_assert!(merge_updates(&replica_b, &doc2).is_ok());
        prop_assert!(merge_updates(&replica_b, &doc3).is_ok());
        prop_assert!(merge_updates(&replica_b, &doc1).is_ok());

        prop_assert!(merge_updates(&replica_c, &doc3).is_ok());
        prop_assert!(merge_updates(&replica_c, &doc1).is_ok());
        prop_assert!(merge_updates(&replica_c, &doc2).is_ok());

        let content_a = get_doc_content(&replica_a);
        let content_b = get_doc_content(&replica_b);
        let content_c = get_doc_content(&replica_c);

        prop_assert_eq!(
            &content_a,
            &content_b,
            "Replica A and B should converge"
        );

        prop_assert_eq!(
            &content_b,
            &content_c,
            "Replica B and C should converge"
        );

        prop_assert_eq!(
            &content_a,
            &content_c,
            "All replicas should converge to identical state regardless of merge order"
        );
    }
}
