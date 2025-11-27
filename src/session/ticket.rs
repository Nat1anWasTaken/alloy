use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use rand::RngCore;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use time::{Duration, OffsetDateTime};
use tracing::warn;

use crate::error::AppError;
use crate::persistence::{DocumentId, UserId};

const DEFAULT_ISSUER: &str = "alloy";
const DEFAULT_TTL: Duration = Duration::minutes(15);
const CURRENT_VERSION: u8 = 1;

#[derive(Clone)]
pub struct TicketIssuer {
    encoding: EncodingKey,
    decoding: DecodingKey,
    validation: Validation,
    ttl: Duration,
    issuer: String,
}

#[derive(Debug, Clone)]
pub struct IssuedTicket {
    pub token: String,
    pub expires_at: i64,
}

#[derive(Debug, Clone)]
pub struct TicketSubject {
    pub doc_id: DocumentId,
    pub user_id: UserId,
    pub expires_at: i64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct SessionTicketClaims {
    sub: String,
    doc: u64,
    exp: i64,
    iat: i64,
    iss: String,
    ver: u8,
}

impl TicketIssuer {
    pub fn new(secret: &[u8], ttl: Duration, issuer: impl Into<String>) -> Self {
        let issuer = issuer.into();
        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = true;
        validation.leeway = 0; // No leeway for expiration - tickets expire exactly when they say they do
        validation.set_required_spec_claims(&["exp", "iat", "iss", "sub", "ver"]);
        validation.set_issuer(std::slice::from_ref(&issuer));

        Self {
            encoding: EncodingKey::from_secret(secret),
            decoding: DecodingKey::from_secret(secret),
            validation,
            ttl,
            issuer,
        }
    }

    pub fn from_env_or_generate() -> Self {
        let secret = match std::env::var("ALLOY_TICKET_SECRET") {
            Ok(value) => value.into_bytes(),
            Err(err) => {
                warn!(
                    error = %err,
                    "ALLOY_TICKET_SECRET not set; generating an ephemeral ticket secret. \
                     Tickets issued during this run will be invalid after the process restarts. \
                     Set ALLOY_TICKET_SECRET to a stable value in production."
                );
                Self::generate_secret()
            }
        };

        Self::new(&secret, DEFAULT_TTL, DEFAULT_ISSUER)
    }

    pub fn development() -> Self {
        Self::new(&Self::generate_secret(), DEFAULT_TTL, DEFAULT_ISSUER)
    }

    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    pub fn issue(&self, doc_id: DocumentId, user: &UserId) -> Result<IssuedTicket, AppError> {
        if user.0.trim().is_empty() {
            return Err(AppError::InvalidInput(
                "user_id cannot be empty".to_string(),
            ));
        }

        let now = OffsetDateTime::now_utc();
        let exp = now + self.ttl;

        let claims = SessionTicketClaims {
            sub: user.0.clone(),
            doc: doc_id.as_u64(),
            exp: exp.unix_timestamp(),
            iat: now.unix_timestamp(),
            iss: self.issuer.clone(),
            ver: CURRENT_VERSION,
        };

        let token = encode(&Header::new(Algorithm::HS256), &claims, &self.encoding)
            .map_err(AppError::from)?;

        Ok(IssuedTicket {
            token,
            expires_at: claims.exp,
        })
    }

    pub fn validate(&self, token: &str) -> Result<TicketSubject, AppError> {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return Err(AppError::InvalidTicket("missing ticket".to_string()));
        }

        let data = decode::<SessionTicketClaims>(trimmed, &self.decoding, &self.validation)
            .map_err(|err| {
                let reason = err.to_string();
                AppError::InvalidTicket(reason)
            })?;

        // Validate version
        if data.claims.ver != CURRENT_VERSION {
            return Err(AppError::InvalidTicket(format!(
                "unsupported ticket version: expected {}, got {}",
                CURRENT_VERSION, data.claims.ver
            )));
        }

        Ok(TicketSubject {
            doc_id: data.claims.doc.into(),
            user_id: UserId(data.claims.sub),
            expires_at: data.claims.exp,
        })
    }

    fn generate_secret() -> Vec<u8> {
        let mut key = vec![0_u8; 32];
        OsRng.fill_bytes(&mut key);
        key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::{DocumentId, UserId};

    #[test]
    fn issues_and_validates_ticket() -> Result<(), AppError> {
        let issuer = TicketIssuer::new(b"secret", Duration::minutes(5), "test-issuer");
        let doc = DocumentId::from(42_u64);
        let user = UserId("alice".to_string());

        let issued = issuer.issue(doc, &user)?;
        let subject = issuer.validate(&issued.token)?;

        assert_eq!(subject.doc_id, doc);
        assert_eq!(subject.user_id, user);
        assert!(subject.expires_at >= issued.expires_at);

        Ok(())
    }

    #[test]
    fn rejects_empty_user() {
        let issuer = TicketIssuer::development();
        let doc = DocumentId::from(1_u64);
        let user = UserId("   ".to_string());

        let result = issuer.issue(doc, &user);

        assert!(result.is_err());
    }

    #[test]
    fn rejects_wrong_issuer() -> Result<(), AppError> {
        let first = TicketIssuer::new(b"secret", Duration::minutes(5), "issuer-a");
        let second = TicketIssuer::new(b"secret", Duration::minutes(5), "issuer-b");

        let doc = DocumentId::from(5_u64);
        let user = UserId("bob".to_string());
        let issued = first.issue(doc, &user)?;

        let result = second.validate(&issued.token);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn rejects_expired_ticket() -> Result<(), AppError> {
        let issuer = TicketIssuer::new(b"secret", Duration::seconds(1), "test-issuer");
        let doc = DocumentId::from(99_u64);
        let user = UserId("charlie".to_string());

        let issued = issuer.issue(doc, &user)?;

        // Wait for the ticket to expire (JWT exp is in seconds)
        std::thread::sleep(std::time::Duration::from_secs(2));

        let result = issuer.validate(&issued.token);

        assert!(
            matches!(result, Err(AppError::InvalidTicket(ref reason)) if reason.contains("ExpiredSignature")),
            "Expected InvalidTicket error with ExpiredSignature, got: {:?}",
            result
        );

        Ok(())
    }

    #[test]
    fn rejects_wrong_version() -> Result<(), AppError> {
        let issuer = TicketIssuer::new(b"secret", Duration::minutes(5), "test-issuer");
        let doc = DocumentId::from(123_u64);
        let user = UserId("dave".to_string());
        let now = OffsetDateTime::now_utc();

        // Create a ticket with wrong version
        let claims = SessionTicketClaims {
            sub: user.0.clone(),
            doc: doc.as_u64(),
            exp: (now + Duration::minutes(5)).unix_timestamp(),
            iat: now.unix_timestamp(),
            iss: "test-issuer".to_string(),
            ver: 99, // Wrong version
        };

        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &issuer.encoding,
        )
        .map_err(AppError::from)?;

        let result = issuer.validate(&token);

        assert!(
            matches!(result, Err(AppError::InvalidTicket(ref reason))
                if reason.contains("unsupported ticket version")
                && reason.contains("expected 1, got 99")),
            "Expected InvalidTicket error with version mismatch, got: {:?}",
            result
        );

        Ok(())
    }
}
