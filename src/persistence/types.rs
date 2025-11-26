use std::sync::LazyLock;

static SONYFLAKE: LazyLock<sonyflake::Sonyflake> = LazyLock::new(|| {
    sonyflake::Sonyflake::new().expect("failed to initialize Sonyflake ID generator")
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DocumentId(u64);

impl DocumentId {
    /// Generate a new unique document ID using Sonyflake
    pub fn new() -> Self {
        let id = SONYFLAKE
            .next_id()
            .expect("failed to generate Sonyflake ID");
        Self(id)
    }

    /// Parse a document ID from a string
    pub fn parse(s: &str) -> Result<Self, std::num::ParseIntError> {
        s.parse::<u64>().map(Self)
    }

    /// Get the raw u64 ID value
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for DocumentId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DocumentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::str::FromStr for DocumentId {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
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
