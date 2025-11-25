use crate::error::AppError;
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

/// Initialize the database schema.
pub async fn init_schema(pool: &Pool<Postgres>) -> Result<(), AppError> {
    // 1. The Append-Only Log
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS yjs_updates (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            doc_id UUID NOT NULL,
            update_data BYTEA NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_yjs_updates_rehydration ON yjs_updates (doc_id, id);
        "#,
    )
    .execute(pool)
    .await?;

    // 2. The Snapshot Store
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS yjs_snapshots (
            doc_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            snapshot_data BYTEA NOT NULL,
            last_update_id BIGINT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

/// Inserts a new update into the log.
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

/// Retrieves the latest snapshot and any subsequent updates.
/// Returns (SnapshotName, SnapshotData, LastUpdateId, Vec<UpdateData>)
pub async fn load_doc_state(
    pool: &Pool<Postgres>,
    doc_id: Uuid,
) -> Result<(Option<String>, Option<Vec<u8>>, i64, Vec<Vec<u8>>), AppError> {
    // 1. Get the latest snapshot
    let snapshot_row = sqlx::query(
        r#"
        SELECT name, snapshot_data, last_update_id
        FROM yjs_snapshots
        WHERE doc_id = $1
        "#,
    )
    .bind(doc_id)
    .fetch_optional(pool)
    .await?;

    let (snapshot_name, snapshot_data, last_update_id) = if let Some(row) = snapshot_row {
        (Some(row.get("name")), Some(row.get("snapshot_data")), row.get("last_update_id"))
    } else {
        (None, None, 0i64)
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

    let update_data: Vec<Vec<u8>> = updates.into_iter().map(|row| row.get("update_data")).collect();

    Ok((snapshot_name, snapshot_data, last_update_id, update_data))
}

/// Persists a compacted snapshot of the document.
pub async fn upsert_snapshot(
    pool: &Pool<Postgres>,
    doc_id: Uuid,
    name: &str,
    snapshot: &[u8],
    last_update_id: i64,
) -> Result<(), AppError> {
    sqlx::query(
        r#"
        INSERT INTO yjs_snapshots (doc_id, name, snapshot_data, last_update_id, created_at)
        VALUES ($1, $2, $3, $4, NOW())
        ON CONFLICT (doc_id) DO UPDATE
        SET name = EXCLUDED.name,
            snapshot_data = EXCLUDED.snapshot_data,
            last_update_id = EXCLUDED.last_update_id,
            created_at = EXCLUDED.created_at
        "#,
    )
    .bind(doc_id)
    .bind(name)
    .bind(snapshot)
    .bind(last_update_id)
    .execute(pool)
    .await?;

    Ok(())
}

/// Get the max ID currently in the updates table for a doc.
/// Useful for determining the 'last_update_id' when creating a snapshot.
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
