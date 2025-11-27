use alloy::api::IssueTicketRequest;
use alloy::persistence::DocumentId;
use common::TestResult;
use reqwest::StatusCode;

mod common;

#[tokio::test]
async fn test_issue_ticket_with_empty_user_id() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new()?;

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/documents/{}/ticket", addr, doc_id);
    let response = client
        .post(&url)
        .json(&IssueTicketRequest {
            user_id: "".to_string(),
        })
        .send()
        .await?;

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for empty user_id"
    );

    Ok(())
}

#[tokio::test]
async fn test_issue_ticket_with_whitespace_user_id() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new()?;

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/documents/{}/ticket", addr, doc_id);

    // Test with spaces
    let response = client
        .post(&url)
        .json(&IssueTicketRequest {
            user_id: "   ".to_string(),
        })
        .send()
        .await?;

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for whitespace-only user_id"
    );

    Ok(())
}

#[tokio::test]
async fn test_issue_ticket_with_tabs_and_newlines() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new()?;

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/documents/{}/ticket", addr, doc_id);

    // Test with tabs and newlines
    let response = client
        .post(&url)
        .json(&IssueTicketRequest {
            user_id: "\t\n\r".to_string(),
        })
        .send()
        .await?;

    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Expected 400 Bad Request for user_id with only whitespace characters"
    );

    Ok(())
}

#[tokio::test]
async fn test_issue_ticket_with_valid_user_id() -> TestResult<()> {
    let (addr, _state) = common::spawn_test_server().await?;
    let doc_id = DocumentId::new()?;

    let client = reqwest::Client::new();
    let url = format!("http://{}/api/documents/{}/ticket", addr, doc_id);
    let response = client
        .post(&url)
        .json(&IssueTicketRequest {
            user_id: "valid_user".to_string(),
        })
        .send()
        .await?;

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "Expected 200 OK for valid user_id"
    );

    Ok(())
}
