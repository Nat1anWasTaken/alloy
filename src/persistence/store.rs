use std::sync::Arc;

use async_trait::async_trait;

use crate::error::AppError;

use super::types::{
    ClientId, DocumentId, SnapshotBytes, SnapshotRecord, Tag, UpdateBytes, UpdateRecord, UserId,
};

#[async_trait]
pub trait DocumentStore: Send + Sync {
    async fn load_latest_snapshot(
        &self,
        doc: DocumentId,
    ) -> Result<Option<SnapshotRecord>, AppError>;

    async fn load_updates_since(
        &self,
        doc: DocumentId,
        seq_inclusive: i64,
    ) -> Result<Vec<UpdateRecord>, AppError>;

    async fn append_update(&self, doc: DocumentId, update: UpdateBytes) -> Result<i64, AppError>;

    async fn store_snapshot(
        &self,
        doc: DocumentId,
        snapshot: SnapshotBytes,
        tags: Vec<Tag>,
        base_seq: i64,
    ) -> Result<(), AppError>;

    async fn record_session(
        &self,
        doc: DocumentId,
        client: ClientId,
        user: UserId,
    ) -> Result<(), AppError>;

    async fn get_session(
        &self,
        doc: DocumentId,
        client: ClientId,
    ) -> Result<Option<UserId>, AppError>;
}

pub type SharedStore = Arc<dyn DocumentStore>;
