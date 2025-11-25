use crate::error::AppError;
use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres, Row};
use uuid::Uuid;

pub trait DbEntity: Sized {
    async fn save(&self, pool: &Pool<Postgres>) -> Result<(), AppError>;
    async fn delete(&self, pool: &Pool<Postgres>) -> Result<(), AppError>;
}

#[derive(Debug, Clone)]
pub struct Snapshot {
    pub doc_id: Uuid,
    pub name: String,
    pub data: Vec<u8>,
    pub last_update_id: i64,
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
}

impl Snapshot {
    pub fn new(doc_id: Uuid, name: String, data: Vec<u8>, last_update_id: i64) -> Self {
        Self {
            doc_id,
            name,
            data,
            last_update_id,
            tags: Vec::new(),
            created_at: Utc::now(),
        }
    }

    pub fn with_tags(mut self, tags: Vec<String>) -> Self {
        self.tags = tags;
        self
    }

    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    pub async fn load(pool: &Pool<Postgres>, doc_id: Uuid) -> Result<Option<Self>, AppError> {
        let row = sqlx::query(
            r#"
            SELECT doc_id, name, snapshot_data, last_update_id, tags, created_at
            FROM yjs_snapshots
            WHERE doc_id = $1
            "#,
        )
        .bind(doc_id)
        .fetch_optional(pool)
        .await?;

        Ok(row.map(|r| Self {
            doc_id: r.get("doc_id"),
            name: r.get("name"),
            data: r.get("snapshot_data"),
            last_update_id: r.get("last_update_id"),
            tags: r.get("tags"),
            created_at: r.get("created_at"),
        }))
    }

    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }
}

impl DbEntity for Snapshot {
    async fn save(&self, pool: &Pool<Postgres>) -> Result<(), AppError> {
        sqlx::query(
            r#"
            INSERT INTO yjs_snapshots (doc_id, name, snapshot_data, last_update_id, tags, created_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (doc_id) DO UPDATE
            SET name = EXCLUDED.name,
                snapshot_data = EXCLUDED.snapshot_data,
                last_update_id = EXCLUDED.last_update_id,
                tags = EXCLUDED.tags,
                created_at = EXCLUDED.created_at
            "#,
        )
        .bind(self.doc_id)
        .bind(&self.name)
        .bind(&self.data)
        .bind(self.last_update_id)
        .bind(&self.tags)
        .bind(self.created_at)
        .execute(pool)
        .await?;

        Ok(())
    }

    async fn delete(&self, pool: &Pool<Postgres>) -> Result<(), AppError> {
        sqlx::query("DELETE FROM yjs_snapshots WHERE doc_id = $1")
            .bind(self.doc_id)
            .execute(pool)
            .await?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Update {
    pub id: Option<i64>,
    pub doc_id: Uuid,
    pub data: Vec<u8>,
    pub created_at: DateTime<Utc>,
}

impl Update {
    pub fn new(doc_id: Uuid, data: Vec<u8>) -> Self {
        Self {
            id: None,
            doc_id,
            data,
            created_at: Utc::now(),
        }
    }

    pub async fn load_since(
        pool: &Pool<Postgres>,
        doc_id: Uuid,
        since_id: i64,
    ) -> Result<Vec<Self>, AppError> {
        let rows = sqlx::query(
            r#"
            SELECT id, doc_id, update_data, created_at
            FROM yjs_updates
            WHERE doc_id = $1 AND id > $2
            ORDER BY id ASC
            "#,
        )
        .bind(doc_id)
        .bind(since_id)
        .fetch_all(pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|r| Self {
                id: Some(r.get("id")),
                doc_id: r.get("doc_id"),
                data: r.get("update_data"),
                created_at: r.get("created_at"),
            })
            .collect())
    }

    pub async fn get_last_id_for_doc(pool: &Pool<Postgres>, doc_id: Uuid) -> Result<i64, AppError> {
        let row = sqlx::query("SELECT MAX(id) as max_id FROM yjs_updates WHERE doc_id = $1")
            .bind(doc_id)
            .fetch_one(pool)
            .await?;

        Ok(row.get::<Option<i64>, _>("max_id").unwrap_or(0))
    }

    pub async fn delete_all_for_doc(pool: &Pool<Postgres>, doc_id: Uuid) -> Result<u64, AppError> {
        let result = sqlx::query("DELETE FROM yjs_updates WHERE doc_id = $1")
            .bind(doc_id)
            .execute(pool)
            .await?;

        Ok(result.rows_affected())
    }

    #[allow(dead_code)]
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

        Ok(rows.into_iter().map(|r| r.get("doc_id")).collect())
    }
}

impl DbEntity for Update {
    async fn save(&self, pool: &Pool<Postgres>) -> Result<(), AppError> {
        if self.id.is_some() {
            return Ok(());
        }

        let row = sqlx::query(
            r#"
            INSERT INTO yjs_updates (doc_id, update_data, created_at)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(self.doc_id)
        .bind(&self.data)
        .bind(self.created_at)
        .fetch_one(pool)
        .await?;

        let _id: i64 = row.get("id");

        Ok(())
    }

    async fn delete(&self, pool: &Pool<Postgres>) -> Result<(), AppError> {
        if let Some(id) = self.id {
            sqlx::query("DELETE FROM yjs_updates WHERE id = $1")
                .bind(id)
                .execute(pool)
                .await?;
        }
        Ok(())
    }
}

impl Update {
    pub async fn save_and_get_id(&self, pool: &Pool<Postgres>) -> Result<i64, AppError> {
        let row = sqlx::query(
            r#"
            INSERT INTO yjs_updates (doc_id, update_data, created_at)
            VALUES ($1, $2, $3)
            RETURNING id
            "#,
        )
        .bind(self.doc_id)
        .bind(&self.data)
        .bind(self.created_at)
        .fetch_one(pool)
        .await?;

        Ok(row.get("id"))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DocumentRecord {
    pub doc_id: Uuid,
}

impl DocumentRecord {
    pub fn new(doc_id: Uuid) -> Self {
        Self { doc_id }
    }

    pub async fn load_state(
        &self,
        pool: &Pool<Postgres>,
    ) -> Result<(Option<Snapshot>, Vec<Update>), AppError> {
        let snapshot = Snapshot::load(pool, self.doc_id).await?;

        let last_update_id = snapshot.as_ref().map(|s| s.last_update_id).unwrap_or(0);
        let updates = Update::load_since(pool, self.doc_id, last_update_id).await?;

        Ok((snapshot, updates))
    }

    pub async fn get_snapshot(&self, pool: &Pool<Postgres>) -> Result<Option<Snapshot>, AppError> {
        Snapshot::load(pool, self.doc_id).await
    }

    pub async fn save_update(&self, pool: &Pool<Postgres>, data: Vec<u8>) -> Result<i64, AppError> {
        let update = Update::new(self.doc_id, data);
        update.save_and_get_id(pool).await
    }

    pub async fn create_snapshot(
        &self,
        pool: &Pool<Postgres>,
        name: String,
        data: Vec<u8>,
        tags: Vec<String>,
    ) -> Result<(), AppError> {
        let last_update_id = Update::get_last_id_for_doc(pool, self.doc_id).await?;
        let snapshot = Snapshot::new(self.doc_id, name, data, last_update_id).with_tags(tags);
        snapshot.save(pool).await
    }

    pub async fn last_update_id(&self, pool: &Pool<Postgres>) -> Result<i64, AppError> {
        Update::get_last_id_for_doc(pool, self.doc_id).await
    }

    pub async fn load_updates_since(
        &self,
        pool: &Pool<Postgres>,
        since_id: i64,
    ) -> Result<Vec<Update>, AppError> {
        Update::load_since(pool, self.doc_id, since_id).await
    }

    pub async fn delete_all(&self, pool: &Pool<Postgres>) -> Result<u64, AppError> {
        if let Some(snapshot) = Snapshot::load(pool, self.doc_id).await? {
            snapshot.delete(pool).await?;
        }

        Update::delete_all_for_doc(pool, self.doc_id).await
    }

    pub async fn exists(&self, pool: &Pool<Postgres>) -> Result<bool, AppError> {
        if Snapshot::load(pool, self.doc_id).await?.is_some() {
            return Ok(true);
        }

        let last_id = Update::get_last_id_for_doc(pool, self.doc_id).await?;
        Ok(last_id > 0)
    }
}

pub async fn load_doc_state(
    pool: &Pool<Postgres>,
    doc_id: Uuid,
) -> Result<(Option<Snapshot>, Vec<Update>), AppError> {
    DocumentRecord::new(doc_id).load_state(pool).await
}

#[allow(dead_code)]
pub async fn delete_document(pool: &Pool<Postgres>, doc_id: Uuid) -> Result<u64, AppError> {
    DocumentRecord::new(doc_id).delete_all(pool).await
}
