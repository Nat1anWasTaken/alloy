use std::borrow::Borrow;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DocumentId(pub uuid::Uuid);

impl From<uuid::Uuid> for DocumentId {
    fn from(value: uuid::Uuid) -> Self {
        DocumentId(value)
    }
}

impl From<DocumentId> for uuid::Uuid {
    fn from(value: DocumentId) -> Self {
        value.0
    }
}

impl Borrow<uuid::Uuid> for DocumentId {
    fn borrow(&self) -> &uuid::Uuid {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct UpdateBytes(pub Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SnapshotBytes(pub Vec<u8>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag(pub String);

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotRecord {
    pub snapshot: SnapshotBytes,
    pub tags: Vec<Tag>,
    pub base_seq: i64,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateRecord {
    pub seq: i64,
    pub bytes: UpdateBytes,
}
