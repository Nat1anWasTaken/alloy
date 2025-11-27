use serde::{Deserialize, Serialize};

/// Request payload for issuing a document session ticket.
#[derive(Debug, Deserialize, Serialize)]
pub struct IssueTicketRequest {
    pub user_id: String,
}

/// Response payload containing the issued ticket.
#[derive(Debug, Deserialize, Serialize)]
pub struct IssueTicketResponse {
    pub ticket: String,
    pub expires_at: i64,
}

/// Query parameters expected by the WebSocket entrypoint.
#[derive(Debug, Deserialize)]
pub struct TicketQuery {
    pub ticket: Option<String>,
}
