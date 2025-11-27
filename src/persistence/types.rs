use std::borrow::Borrow;
use std::fmt::{Display, Formatter};
use std::num::ParseIntError;
use std::str::FromStr;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use sonyflake::Sonyflake;
use thiserror::Error;

static SONYFLAKE: OnceLock<Mutex<Sonyflake>> = OnceLock::new();

#[derive(Debug, Error)]
pub enum IdError {
    #[error("sonyflake generator unavailable")]
    Unavailable,
    #[error("failed to acquire id lock")]
    Poisoned,
    #[error("sonyflake exhausted ids")]
    Exhausted,
}

#[derive(Debug, Error)]
pub enum ParseDocumentIdError {
    #[error("invalid document id: {0}")]
    Invalid(#[from] ParseIntError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DocumentId(u64);

impl DocumentId {
    pub fn new() -> Result<Self, IdError> {
        let generator = if let Some(generator) = SONYFLAKE.get() {
            generator
        } else {
            let candidate = Sonyflake::new().map_err(|_| IdError::Unavailable)?;
            match SONYFLAKE.set(Mutex::new(candidate)) {
                Ok(()) => SONYFLAKE.get().ok_or(IdError::Unavailable)?,
                Err(conflicting) => {
                    drop(conflicting);
                    SONYFLAKE.get().ok_or(IdError::Unavailable)?
                }
            }
        };

        let guard = generator.lock().map_err(|_| IdError::Poisoned)?;
        let id = guard.next_id().map_err(|_| IdError::Exhausted)?;
        Ok(DocumentId(id))
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl From<u64> for DocumentId {
    fn from(value: u64) -> Self {
        DocumentId(value)
    }
}

impl From<DocumentId> for u64 {
    fn from(value: DocumentId) -> Self {
        value.0
    }
}

impl Borrow<u64> for DocumentId {
    fn borrow(&self) -> &u64 {
        &self.0
    }
}

impl Display for DocumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for DocumentId {
    type Err = ParseDocumentIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value = s.parse::<u64>()?;
        Ok(DocumentId(value))
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SnapshotPage {
    pub snapshots: Vec<SnapshotRecord>,
    pub next_cursor: Option<i64>,
}

#[derive(Debug, Clone, Default)]
pub struct UpdateRecord {
    pub seq: i64,
    pub bytes: UpdateBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub seq: i64,
    pub client: ClientId,
    pub user: UserId,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SessionPage {
    pub sessions: Vec<SessionRecord>,
    pub next_cursor: Option<i64>,
}
