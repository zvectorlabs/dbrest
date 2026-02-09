//! JWT-specific error types
//!
//! Maps to PGRST300-303 error codes. Each variant carries enough detail
//! to produce the correct HTTP status code, `WWW-Authenticate` header,
//! and JSON error body.

use std::fmt;

/// Top-level JWT authentication error.
///
/// Each variant maps to one PGRST3xx error code:
///
/// | Variant | Code | HTTP |
/// |---------|------|------|
/// | `SecretMissing` | PGRST300 | 500 |
/// | `Decode(_)` | PGRST301 | 401 |
/// | `TokenRequired` | PGRST302 | 401 |
/// | `Claims(_)` | PGRST303 | 401 |
#[derive(Debug, Clone)]
pub enum JwtError {
    /// PGRST300 — no JWT secret or JWKS is configured on the server.
    SecretMissing,

    /// PGRST301 — the token could not be decoded (structural or crypto error).
    Decode(JwtDecodeError),

    /// PGRST302 — no token was provided and anonymous access is disabled.
    TokenRequired,

    /// PGRST303 — the token was decoded but a claims check failed.
    Claims(JwtClaimsError),
}

/// Token decode errors (PGRST301).
#[derive(Debug, Clone)]
pub enum JwtDecodeError {
    /// `Authorization: Bearer ` with an empty token string.
    EmptyAuthHeader,
    /// Token does not have exactly 3 dot-separated parts.
    UnexpectedParts(usize),
    /// No suitable key found, or key type mismatch.
    KeyError(String),
    /// The `alg` header specifies an unsupported algorithm.
    BadAlgorithm(String),
    /// Cryptographic signature verification failed.
    BadCrypto,
    /// The decoded token type (e.g. JWE) is not supported.
    UnsupportedTokenType,
}

/// Claims validation errors (PGRST303).
#[derive(Debug, Clone)]
pub enum JwtClaimsError {
    /// `exp` claim is in the past (beyond the 30-second skew window).
    Expired,
    /// `nbf` claim is in the future (beyond the 30-second skew window).
    NotYetValid,
    /// `iat` claim is in the future (beyond the 30-second skew window).
    IssuedAtFuture,
    /// `aud` claim does not match the configured audience.
    NotInAudience,
    /// Claims JSON could not be parsed into the expected structure.
    ParsingFailed,
}

// ---------------------------------------------------------------------------
// Display impls
// ---------------------------------------------------------------------------

impl fmt::Display for JwtError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JwtError::SecretMissing => write!(f, "Server lacks JWT secret"),
            JwtError::Decode(e) => write!(f, "{e}"),
            JwtError::TokenRequired => write!(f, "Anonymous access is disabled"),
            JwtError::Claims(e) => write!(f, "{e}"),
        }
    }
}

impl fmt::Display for JwtDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JwtDecodeError::EmptyAuthHeader => {
                write!(f, "Empty JWT is sent in Authorization header")
            }
            JwtDecodeError::UnexpectedParts(n) => {
                write!(f, "Expected 3 parts in JWT; got {n}")
            }
            JwtDecodeError::KeyError(_) => {
                write!(f, "No suitable key or wrong key type")
            }
            JwtDecodeError::BadAlgorithm(_) => {
                write!(f, "Wrong or unsupported encoding algorithm")
            }
            JwtDecodeError::BadCrypto => {
                write!(f, "JWT cryptographic operation failed")
            }
            JwtDecodeError::UnsupportedTokenType => {
                write!(f, "Unsupported token type")
            }
        }
    }
}

impl fmt::Display for JwtClaimsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JwtClaimsError::Expired => write!(f, "JWT expired"),
            JwtClaimsError::NotYetValid => write!(f, "JWT not yet valid"),
            JwtClaimsError::IssuedAtFuture => write!(f, "JWT issued at future"),
            JwtClaimsError::NotInAudience => write!(f, "JWT not in audience"),
            JwtClaimsError::ParsingFailed => write!(f, "Parsing claims failed"),
        }
    }
}

impl std::error::Error for JwtError {}
impl std::error::Error for JwtDecodeError {}
impl std::error::Error for JwtClaimsError {}

// ---------------------------------------------------------------------------
// Convenience conversions
// ---------------------------------------------------------------------------

impl From<JwtDecodeError> for JwtError {
    fn from(e: JwtDecodeError) -> Self {
        JwtError::Decode(e)
    }
}

impl From<JwtClaimsError> for JwtError {
    fn from(e: JwtClaimsError) -> Self {
        JwtError::Claims(e)
    }
}

// ---------------------------------------------------------------------------
// Error metadata helpers
// ---------------------------------------------------------------------------

impl JwtError {
    /// PGRST error code string.
    pub fn code(&self) -> &'static str {
        match self {
            JwtError::SecretMissing => "PGRST300",
            JwtError::Decode(_) => "PGRST301",
            JwtError::TokenRequired => "PGRST302",
            JwtError::Claims(_) => "PGRST303",
        }
    }

    /// HTTP status code.
    pub fn status(&self) -> http::StatusCode {
        match self {
            JwtError::SecretMissing => http::StatusCode::INTERNAL_SERVER_ERROR,
            JwtError::Decode(_) => http::StatusCode::UNAUTHORIZED,
            JwtError::TokenRequired => http::StatusCode::UNAUTHORIZED,
            JwtError::Claims(_) => http::StatusCode::UNAUTHORIZED,
        }
    }

    /// Optional detail string for the error JSON body.
    pub fn details(&self) -> Option<String> {
        match self {
            JwtError::Decode(JwtDecodeError::KeyError(d)) => Some(d.clone()),
            JwtError::Decode(JwtDecodeError::BadAlgorithm(d)) => Some(d.clone()),
            _ => None,
        }
    }

    /// `WWW-Authenticate` header value, if applicable.
    pub fn www_authenticate(&self) -> Option<String> {
        match self {
            JwtError::TokenRequired => Some("Bearer".to_string()),
            JwtError::Decode(e) => {
                let msg = e.to_string();
                Some(format!(
                    "Bearer error=\"invalid_token\", error_description=\"{msg}\""
                ))
            }
            JwtError::Claims(e) => {
                let msg = e.to_string();
                Some(format!(
                    "Bearer error=\"invalid_token\", error_description=\"{msg}\""
                ))
            }
            JwtError::SecretMissing => None,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        assert_eq!(JwtError::SecretMissing.code(), "PGRST300");
        assert_eq!(
            JwtError::Decode(JwtDecodeError::EmptyAuthHeader).code(),
            "PGRST301"
        );
        assert_eq!(JwtError::TokenRequired.code(), "PGRST302");
        assert_eq!(
            JwtError::Claims(JwtClaimsError::Expired).code(),
            "PGRST303"
        );
    }

    #[test]
    fn test_error_status() {
        assert_eq!(
            JwtError::SecretMissing.status(),
            http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            JwtError::Decode(JwtDecodeError::BadCrypto).status(),
            http::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            JwtError::TokenRequired.status(),
            http::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            JwtError::Claims(JwtClaimsError::NotInAudience).status(),
            http::StatusCode::UNAUTHORIZED
        );
    }

    #[test]
    fn test_www_authenticate_headers() {
        // TokenRequired → plain Bearer
        let hdr = JwtError::TokenRequired.www_authenticate().unwrap();
        assert_eq!(hdr, "Bearer");

        // Decode error → Bearer with error_description
        let hdr = JwtError::Decode(JwtDecodeError::BadCrypto)
            .www_authenticate()
            .unwrap();
        assert!(hdr.contains("invalid_token"));
        assert!(hdr.contains("cryptographic"));

        // Claims error → Bearer with error_description
        let hdr = JwtError::Claims(JwtClaimsError::Expired)
            .www_authenticate()
            .unwrap();
        assert!(hdr.contains("expired"));

        // SecretMissing → no header
        assert!(JwtError::SecretMissing.www_authenticate().is_none());
    }

    #[test]
    fn test_display_messages() {
        assert_eq!(
            JwtError::SecretMissing.to_string(),
            "Server lacks JWT secret"
        );
        assert_eq!(
            JwtError::TokenRequired.to_string(),
            "Anonymous access is disabled"
        );
        assert_eq!(
            JwtDecodeError::UnexpectedParts(2).to_string(),
            "Expected 3 parts in JWT; got 2"
        );
        assert_eq!(JwtClaimsError::Expired.to_string(), "JWT expired");
    }

    #[test]
    fn test_details() {
        let err = JwtError::Decode(JwtDecodeError::KeyError(
            "None of the keys was able to decode the JWT".to_string(),
        ));
        assert!(err.details().unwrap().contains("keys"));

        assert!(JwtError::Claims(JwtClaimsError::Expired).details().is_none());
    }
}
