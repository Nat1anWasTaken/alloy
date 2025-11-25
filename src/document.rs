use crate::error::AppError;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

// ============================================================================
// READ Operations
// ============================================================================

/// Retrieves the latest snapshot and any subsequent updates for a document.
/// Returns (SnapshotName, SnapshotData, LastUpdateId, Tags, Vec<UpdateData>)
///
/// WHY: This is the primary rehydration path - we load the latest compacted
/// snapshot and replay only incremental updates since that snapshot.
pub async fn load_doc_state(
    pool: &Pool<Postgres>,
    doc_id: Uuid,
) -> Result<(Option<String>, Option<Vec<u8>>, i64, Vec<String>, Vec<Vec<u8>>), AppError> {
    // 1. Get the latest snapshot
    let snapshot_row = sqlx::query(
        r#"
        SELECT name, snapshot_data, last_update_id, tags
        FROM yjs_snapshots
        WHERE doc_id = $1
        "#,
    )
    .bind(doc_id)
    .fetch_optional(pool)
    .await?;

    let (snapshot_name, snapshot_data, last_update_id, tags) = if let Some(row) = snapshot_row {
        (
            Some(row.get("name")),
            Some(row.get("snapshot_data")),
            row.get("last_update_id"),
            row.get("tags"),
        )
    } else {
        (None, None, 0i64, Vec::new())
    };

    // 2. Get all updates that happened *after* the snapshot
    let updates = sqlx::query(
        r#"
        SELECT update_data
        FROM yjs_updates
        WHERE doc_id = $1 AND id > $2
        ORDER BY id ASC
        "#,
    )
    .bind(doc_id)
    .bind(last_update_id)
    .fetch_all(pool)
    .await?;

    let update_data: Vec<Vec<u8>> = updates
        .into_iter()
        .map(|row| row.get("update_data"))
        .collect();

    Ok((snapshot_name, snapshot_data, last_update_id, tags, update_data))
}

/// Get the max update ID currently in the updates table for a document.
/// Returns 0 if no updates exist.
///
/// WHY: Used when creating snapshots to mark which update ID the snapshot includes.
pub async fn get_last_update_id(pool: &Pool<Postgres>, doc_id: Uuid) -> Result<i64, AppError> {
    let row = sqlx::query(
        r#"
        SELECT MAX(id) as max_id FROM yjs_updates WHERE doc_id = $1
        "#,
    )
    .bind(doc_id)
    .fetch_one(pool)
    .await?;

    Ok(row.get::<Option<i64>, _>("max_id").unwrap_or(0))
}

#[allow(dead_code)]
/// Lists all unique document IDs that have updates in the system.
/// Returns documents sorted by most recently updated.
pub async fn list_documents(pool: &Pool<Postgres>) -> Result<Vec<Uuid>, AppError> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT doc_id
        FROM yjs_updates
        ORDER BY doc_id
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows.into_iter().map(|row| row.get("doc_id")).collect())
}

/// Appends a new update to the document's append-only log.
/// Returns the generated update ID.
///
/// WHY: This is called on every document edit - updates are never modified,
/// only appended. The ID is monotonically increasing per document.
pub async fn append_update(
    pool: &Pool<Postgres>,
    doc_id: Uuid,
    update: &[u8],
) -> Result<i64, AppError> {
    let row = sqlx::query(
        r#"
        INSERT INTO yjs_updates (doc_id, update_data)
        VALUES ($1, $2)
        RETURNING id
        "#,
    )
    .bind(doc_id)
    .bind(update)
    .fetch_one(pool)
    .await?;

    Ok(row.get("id"))
}

/// Persists or updates a compacted snapshot of the document.
///
/// WHY: Snapshots prevent unbounded replay time. Instead of replaying 10,000
/// updates, we load 1 snapshot + recent deltas. last_update_id marks which
/// update the snapshot includes.
pub async fn upsert_snapshot(
    pool: &Pool<Postgres>,
    doc_id: Uuid,
    name: &str,
    snapshot: &[u8],
    last_update_id: i64,
    tags: &[String],
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        INSERT INTO yjs_snapshots (doc_id, name, snapshot_data, last_update_id, tags, created_at)
        VALUES ($1, $2, $3, $4, $5, NOW())
        ON CONFLICT (doc_id) DO UPDATE
        SET name = EXCLUDED.name,
            snapshot_data = EXCLUDED.snapshot_data,
            last_update_id = EXCLUDED.last_update_id,
            tags = EXCLUDED.tags,
            created_at = EXCLUDED.created_at
        "#,
    )
    .bind(doc_id)
    .bind(name)
    .bind(snapshot)
    .bind(last_update_id)
    .bind(tags)
    .execute(pool)
    .await?;

    Ok(())
}

#[allow(dead_code)]
/// Deletes a document and all associated data (snapshots and updates).
/// Returns the number of updates deleted.
///
/// WHY: Cascade delete ensures no orphaned data. Deletes snapshot first
/// (single row), then all updates (potentially many rows).
pub async fn delete_document(pool: &Pool<Postgres>, doc_id: Uuid) -> Result<u64, AppError> {
    // Delete snapshot (if exists)
    sqlx::query(
        r#"
        DELETE FROM yjs_snapshots WHERE doc_id = $1
        "#,
    )
    .bind(doc_id)
    .execute(pool)
    .await?;

    // Delete all updates and return count
    let result = sqlx::query(
        r#"
        DELETE FROM yjs_updates WHERE doc_id = $1
        "#,
    )
    .bind(doc_id)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}
