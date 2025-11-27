# Alloy
The CRDT relaying node for the alloy project.

Responsible for:
- [x] Relaying CRDT messages between clients
- [x] Maintaining persistent storage of documents
  - [x] Temporary in-memory storage
  - [ ] Persistent PostgreSQL storage
- [ ] Histories
  - [x] Snapshots
  - [ ] Versioning
- [ ] API Endpoints
  - [ ] Document CRUD
  - [x] Ticket Generation (document session tickets via `/api/documents/{doc_id}/ticket`)

## Document session flow

1. Backend requests a ticket:
   - `POST /api/documents/{doc_id}/ticket`
   - Body: `{"user_id": "<user identifier>"}`.
   - Response: `{ "ticket": "<jwt>", "expires_at": <unix seconds> }`
2. Client connects to the shared document at `ws://<host>/edit?ticket=<jwt>` using the same host and port as the HTTP request.
3. Server validates the ticket and joins the user to the document session.

WebSocket endpoint is now `/edit` and requires the `ticket` query parameter. The ticket is a JWT binding the document id and user id with a short TTL.
