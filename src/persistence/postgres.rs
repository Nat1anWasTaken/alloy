use async_trait::async_trait;
use sqlx::postgres::{PgPoolOptions, PgRow};
use sqlx::{Pool, Postgres, Row};
use tracing::{debug, instrument, trace};

use crate::error::AppError;

use super::store::DocumentStore;
use super::types::{
    ClientId, DocumentId, SessionPage, SnapshotBytes, SnapshotPage, SnapshotRecord, Tag,
    UpdateBytes, UpdateRecord, UserId,
};

/// PostgreSQL-backed implementation of [`DocumentStore`].
pub struct PostgresStore {
    pool: Pool<Postgres>,
}

impl PostgresStore {
    pub async fn from_env() -> Result<Self, AppError> {
        let database_url = std::env::var("DATABASE_URL")?;
        let max_connections = std::env::var("DATABASE_MAX_CONNECTIONS")
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .unwrap_or(10);
        let min_connections = std::env::var("DATABASE_MIN_CONNECTIONS")
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .unwrap_or(1);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::query("SET application_name = 'alloy'")
                        .execute(conn)
                        .await
                        .map(|_| ())
                })
            })
            .connect(&database_url)
            .await
            .map_err(|e| AppError::Store(format!("failed to connect to postgres: {e}")))?;

        Self::ensure_schema(&pool).await?;

        Ok(Self { pool })
    }

    async fn ensure_schema(pool: &Pool<Postgres>) -> Result<(), AppError> {
        let statements = [
            r#"
            CREATE TABLE IF NOT EXISTS updates (
                doc_id BIGINT NOT NULL,
                seq BIGINT NOT NULL,
                bytes BYTEA NOT NULL,
                PRIMARY KEY (doc_id, seq)
            );
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS snapshots (
                doc_id BIGINT NOT NULL,
                base_seq BIGINT NOT NULL,
                snapshot BYTEA NOT NULL,
                tags TEXT[] NOT NULL DEFAULT '{}',
                PRIMARY KEY (doc_id, base_seq)
            );
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS sessions (
                doc_id BIGINT NOT NULL,
                seq BIGINT NOT NULL,
                client_id BIGINT NOT NULL,
                user_id TEXT NOT NULL,
                PRIMARY KEY (doc_id, client_id),
                UNIQUE (doc_id, seq)
            );
            "#,
            "CREATE INDEX IF NOT EXISTS idx_updates_doc_seq ON updates(doc_id, seq);",
            "CREATE INDEX IF NOT EXISTS idx_snapshots_doc_seq ON snapshots(doc_id, base_seq);",
            "CREATE INDEX IF NOT EXISTS idx_sessions_doc_seq ON sessions(doc_id, seq);",
        ];

        for statement in statements {
            sqlx::query(statement)
                .execute(pool)
                .await
                .map_err(map_sqlx_err)?;
        }

        Ok(())
    }

    fn doc_id_as_i64(doc: DocumentId) -> Result<i64, AppError> {
        let raw = doc.as_u64();
        if raw > i64::MAX as u64 {
            return Err(AppError::Store(
                "docxument id exceeds postgres advisory lock range".to_string(),
            ));
        }
        Ok(raw as i64)
    }

    fn doc_lock_key(doc: DocumentId) -> Result<i64, AppError> {
        Self::doc_id_as_i64(doc)
    }

    fn row_to_snapshot(row: &PgRow) -> SnapshotRecord {
        let tags: Vec<String> = row.get("tags");
        SnapshotRecord {
            snapshot: SnapshotBytes(row.get::<Vec<u8>, _>("snapshot")),
            tags: tags.into_iter().map(Tag).collect(),
            base_seq: row.get::<i64, _>("base_seq"),
        }
    }

    fn row_to_update(row: &PgRow) -> UpdateRecord {
        UpdateRecord {
            seq: row.get::<i64, _>("seq"),
            bytes: UpdateBytes(row.get::<Vec<u8>, _>("bytes")),
        }
    }
}

#[async_trait]
impl DocumentStore for PostgresStore {
    #[instrument(skip(self), fields(doc = ?doc))]
    async fn load_latest_snapshot(
        &self,
        doc: DocumentId,
    ) -> Result<Option<SnapshotRecord>, AppError> {
        trace!("Loading latest snapshot from postgres");
        let row = sqlx::query(
            r#"
            SELECT base_seq, snapshot, tags
            FROM snapshots
            WHERE doc_id = $1
            ORDER BY base_seq DESC
            LIMIT 1
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let snapshot = row.as_ref().map(Self::row_to_snapshot);
        if let Some(ref snap) = snapshot {
            debug!(
                base_seq = snap.base_seq,
                tags_count = snap.tags.len(),
                "Latest snapshot loaded"
            );
        } else {
            debug!("No snapshot found");
        }

        Ok(snapshot)
    }

    #[instrument(skip(self), fields(doc = ?doc, base_seq))]
    async fn load_snapshot(
        &self,
        doc: DocumentId,
        base_seq: i64,
    ) -> Result<Option<SnapshotRecord>, AppError> {
        trace!("Loading specific snapshot from postgres");
        let row = sqlx::query(
            r#"
            SELECT base_seq, snapshot, tags
            FROM snapshots
            WHERE doc_id = $1 AND base_seq = $2
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(base_seq)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let snapshot = row.as_ref().map(Self::row_to_snapshot);

        if let Some(ref snap) = snapshot {
            debug!(
                base_seq = snap.base_seq,
                tags_count = snap.tags.len(),
                "Snapshot found"
            );
        } else {
            debug!("Snapshot not found");
        }

        Ok(snapshot)
    }

    #[instrument(skip(self), fields(doc = ?doc, start_after, limit))]
    async fn list_snapshots(
        &self,
        doc: DocumentId,
        start_after: Option<i64>,
        limit: usize,
    ) -> Result<SnapshotPage, AppError> {
        trace!("Listing snapshots from postgres");

        if limit == 0 {
            debug!("Pagination limit is zero; returning empty page");
            return Ok(SnapshotPage::default());
        }

        let rows = sqlx::query(
            r#"
            SELECT base_seq, snapshot, tags
            FROM snapshots
            WHERE doc_id = $1 AND ($2::BIGINT IS NULL OR base_seq > $2)
            ORDER BY base_seq ASC
            LIMIT $3
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(start_after)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let snapshots = rows.iter().map(Self::row_to_snapshot).collect();
        let mut page = SnapshotPage {
            snapshots,
            ..SnapshotPage::default()
        };

        if let Some(last) = page.snapshots.last() {
            let has_more = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT base_seq
                FROM snapshots
                WHERE doc_id = $1 AND base_seq > $2
                ORDER BY base_seq ASC
                LIMIT 1
                "#,
            )
            .bind(Self::doc_id_as_i64(doc)?)
            .bind(last.base_seq)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?
            .is_some();

            if has_more {
                page.next_cursor = Some(last.base_seq);
            }
        }

        debug!(
            count = page.snapshots.len(),
            next_cursor = ?page.next_cursor,
            "Snapshots listed"
        );

        Ok(page)
    }

    #[instrument(skip(self), fields(doc = ?doc, seq_inclusive))]
    async fn load_updates_since(
        &self,
        doc: DocumentId,
        seq_inclusive: i64,
    ) -> Result<Vec<UpdateRecord>, AppError> {
        trace!("Loading updates from postgres");
        let rows = sqlx::query(
            r#"
            SELECT seq, bytes
            FROM updates
            WHERE doc_id = $1 AND seq > $2
            ORDER BY seq ASC
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(seq_inclusive)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let updates = rows.iter().map(Self::row_to_update).collect::<Vec<_>>();

        debug!(total = updates.len(), "Loaded updates");

        Ok(updates)
    }

    #[instrument(skip(self, update), fields(doc = ?doc, update_size = update.0.len()))]
    async fn append_update(&self, doc: DocumentId, update: UpdateBytes) -> Result<i64, AppError> {
        trace!("Appending update to postgres");
        let lock_key = Self::doc_lock_key(doc)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;

        let next_seq: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(seq), 0) + 1
            FROM updates
            WHERE doc_id = $1
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        sqlx::query(
            r#"
            INSERT INTO updates (doc_id, seq, bytes)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(next_seq)
        .bind(update.0)
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        tx.commit().await.map_err(map_sqlx_err)?;

        debug!(seq = next_seq, "Update appended");

        Ok(next_seq)
    }

    #[instrument(skip(self, snapshot, tags), fields(doc = ?doc, base_seq, tags_count = tags.len(), snapshot_size = snapshot.0.len()))]
    async fn store_snapshot(
        &self,
        doc: DocumentId,
        snapshot: super::types::SnapshotBytes,
        tags: Vec<Tag>,
        base_seq: i64,
    ) -> Result<(), AppError> {
        trace!("Storing snapshot to postgres");
        sqlx::query(
            r#"
            INSERT INTO snapshots (doc_id, base_seq, snapshot, tags)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (doc_id, base_seq) DO UPDATE
            SET snapshot = EXCLUDED.snapshot,
                tags = EXCLUDED.tags
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(base_seq)
        .bind(snapshot.0)
        .bind(tags.into_iter().map(|t| t.0).collect::<Vec<String>>())
        .execute(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        debug!("Snapshot stored successfully");

        Ok(())
    }

    #[instrument(skip(self), fields(doc = ?doc, client = ?client, user = ?user))]
    async fn record_session(
        &self,
        doc: DocumentId,
        client: ClientId,
        user: UserId,
    ) -> Result<(), AppError> {
        trace!("Recording session in postgres");
        let lock_key = Self::doc_lock_key(doc)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;

        let existing: Option<String> = sqlx::query_scalar(
            r#"
            SELECT user_id
            FROM sessions
            WHERE doc_id = $1 AND client_id = $2
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(client.0 as i64)
        .fetch_optional(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        if existing.is_some() {
            tx.rollback().await.map_err(map_sqlx_err)?;
            return Ok(());
        }

        let next_seq: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(seq), 0) + 1
            FROM sessions
            WHERE doc_id = $1
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .fetch_one(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        sqlx::query(
            r#"
            INSERT INTO sessions (doc_id, seq, client_id, user_id)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(next_seq)
        .bind(client.0 as i64)
        .bind(user.0.clone())
        .execute(&mut *tx)
        .await
        .map_err(map_sqlx_err)?;

        tx.commit().await.map_err(map_sqlx_err)?;

        debug!(seq = next_seq, "Session recorded");

        Ok(())
    }

    #[instrument(skip(self), fields(doc = ?doc, client = ?client))]
    async fn get_session(
        &self,
        doc: DocumentId,
        client: ClientId,
    ) -> Result<Option<UserId>, AppError> {
        trace!("Fetching session from postgres");
        let row = sqlx::query(
            r#"
            SELECT user_id
            FROM sessions
            WHERE doc_id = $1 AND client_id = $2
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(client.0 as i64)
        .fetch_optional(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let user = row.map(|r| UserId(r.get::<String, _>("user_id"))).map(|u| {
            debug!(user = ?u, "Session found");
            u
        });

        if user.is_none() {
            debug!("No session found");
        }

        Ok(user)
    }

    #[instrument(skip(self), fields(doc = ?doc, start_after, limit))]
    async fn list_sessions(
        &self,
        doc: DocumentId,
        start_after: Option<i64>,
        limit: usize,
    ) -> Result<SessionPage, AppError> {
        trace!("Listing sessions from postgres");

        if limit == 0 {
            debug!("Pagination limit is zero; returning empty page");
            return Ok(SessionPage::default());
        }

        let rows = sqlx::query(
            r#"
            SELECT seq, client_id, user_id
            FROM sessions
            WHERE doc_id = $1 AND ($2::BIGINT IS NULL OR seq > $2)
            ORDER BY seq ASC
            LIMIT $3
            "#,
        )
        .bind(Self::doc_id_as_i64(doc)?)
        .bind(start_after)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(map_sqlx_err)?;

        let sessions = rows
            .iter()
            .map(|row| super::types::SessionRecord {
                seq: row.get::<i64, _>("seq"),
                client: ClientId(row.get::<i64, _>("client_id") as u64),
                user: UserId(row.get::<String, _>("user_id")),
            })
            .collect();
        let mut page = SessionPage {
            sessions,
            ..SessionPage::default()
        };

        if let Some(last) = page.sessions.last() {
            let has_more = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT seq
                FROM sessions
                WHERE doc_id = $1 AND seq > $2
                ORDER BY seq ASC
                LIMIT 1
                "#,
            )
            .bind(Self::doc_id_as_i64(doc)?)
            .bind(last.seq)
            .fetch_optional(&self.pool)
            .await
            .map_err(map_sqlx_err)?
            .is_some();

            if has_more {
                page.next_cursor = Some(last.seq);
            }
        }

        debug!(
            count = page.sessions.len(),
            next_cursor = ?page.next_cursor,
            "Sessions listed"
        );

        Ok(page)
    }

    #[instrument(skip(self), fields(doc = ?doc))]
    async fn delete_document(&self, doc: DocumentId) -> Result<(), AppError> {
        trace!("Deleting document from postgres store");
        let lock_key = Self::doc_lock_key(doc)?;
        let mut tx = self.pool.begin().await.map_err(map_sqlx_err)?;

        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;

        sqlx::query("DELETE FROM updates WHERE doc_id = $1")
            .bind(Self::doc_id_as_i64(doc)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;

        sqlx::query("DELETE FROM snapshots WHERE doc_id = $1")
            .bind(Self::doc_id_as_i64(doc)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;

        sqlx::query("DELETE FROM sessions WHERE doc_id = $1")
            .bind(Self::doc_id_as_i64(doc)?)
            .execute(&mut *tx)
            .await
            .map_err(map_sqlx_err)?;

        tx.commit().await.map_err(map_sqlx_err)?;

        debug!("Document deleted from postgres store");

        Ok(())
    }
}

fn map_sqlx_err(err: sqlx::Error) -> AppError {
    AppError::Store(format!("database error: {err}"))
}
