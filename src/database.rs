use crate::error::AppError;
use sqlx::{Pool, Postgres};

/// Initialize the database schema for Yjs document storage.
///
/// WHY: Two-table design optimizes for collaborative editing patterns:
/// 1. yjs_updates: Append-only log of all edits (never modified after insert)
/// 2. yjs_snapshots: Compacted state to prevent unbounded replay on load
pub async fn init_schema(pool: &Pool<Postgres>) -> Result<(), AppError> {
    // 1. The Append-Only Update Log
    // WHY: CRDT updates must be preserved in order for proper conflict resolution
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

    // WHY: Index optimizes the common query pattern: load all updates for doc_id since snapshot
    sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_yjs_updates_rehydration ON yjs_updates (doc_id, id);
        "#,
    )
    .execute(pool)
    .await?;

    // 2. The Snapshot Store
    // WHY: One snapshot per document (UPSERT pattern). Snapshots are full document state
    // at a specific update ID, allowing fast rehydration without replaying all history.
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS yjs_snapshots (
            doc_id UUID PRIMARY KEY,
            name TEXT NOT NULL,
            snapshot_data BYTEA NOT NULL,
            last_update_id BIGINT NOT NULL,
            tags TEXT[] NOT NULL DEFAULT '{}',
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        "#,
    )
    .execute(pool)
    .await?;

    Ok(())
}
