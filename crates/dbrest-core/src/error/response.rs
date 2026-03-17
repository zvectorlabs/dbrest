//! Error response types for HTTP responses
//!
//! JSON error response formatting.

use axum::{
    Json,
    response::{IntoResponse, Response},
};
use http::header;
use serde::Serialize;

use super::Error;

/// JSON error response format
///
/// # Example Response
///
/// ```json
/// {
///   "code": "DBRST200",
///   "message": "Table not found: users",
///   "details": null,
///   "hint": "Did you mean 'user'?"
/// }
/// ```
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    /// DBRST error code (e.g., "DBRST200")
    pub code: &'static str,

    /// Human-readable error message
    pub message: String,

    /// Additional details about the error (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,

    /// Hint for resolution (optional)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

impl From<&Error> for ErrorResponse {
    fn from(err: &Error) -> Self {
        let (details, hint) = err.details_and_hint();

        Self {
            code: err.code(),
            message: err.to_string(),
            details,
            hint,
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = ErrorResponse::from(&self);

        let mut response = (status, Json(body)).into_response();

        // Propagate WWW-Authenticate header for JWT errors
        if let Error::JwtAuth(jwt_err) = &self
            && let Some(www_auth) = jwt_err.www_authenticate()
            && let Ok(header_value) = http::HeaderValue::from_str(&www_auth)
        {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, header_value);
        }

        response
    }
}

/// Result type alias for handlers returning potential errors
pub type AppResult<T> = Result<T, Error>;

/// Extension trait for adding context to Results
pub trait ResultExt<T> {
    /// Add context message on error
    fn with_context<F>(self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> String;

    /// Add table context to error message
    fn table_context(self, table: &str) -> Result<T, Error>;

    /// Add table and column context to error message
    fn column_context(self, table: &str, column: &str) -> Result<T, Error>;
}

impl<T, E> ResultExt<T> for Result<T, E>
where
    E: std::fmt::Display,
{
    fn with_context<F>(self, f: F) -> Result<T, Error>
    where
        F: FnOnce() -> String,
    {
        self.map_err(|e| Error::Internal(format!("{}: {}", f(), e)))
    }

    fn table_context(self, table: &str) -> Result<T, Error> {
        self.map_err(|e| Error::Internal(format!("[{}] {}", table, e)))
    }

    fn column_context(self, table: &str, column: &str) -> Result<T, Error> {
        self.map_err(|e| Error::Internal(format!("[{}.{}] {}", table, column, e)))
    }
}

/// Early return with an error.
///
/// # Example
///
/// ```rust
/// use dbrest::bail;
/// use dbrest::Error;
///
/// fn validate(x: i32) -> Result<(), Error> {
///     if x < 0 {
///         bail!(Error::InvalidQueryParam {
///             param: "x".to_string(),
///             message: "must be non-negative".to_string(),
///         });
///     }
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! bail {
    ($err:expr) => {
        return Err($err.into());
    };
}

/// Ensure a condition is true, otherwise return an error.
///
/// # Example
///
/// ```rust
/// use dbrest::ensure;
/// use dbrest::Error;
///
/// fn validate(x: i32) -> Result<(), Error> {
///     ensure!(x >= 0, Error::InvalidQueryParam {
///         param: "x".to_string(),
///         message: "must be non-negative".to_string(),
///     });
///     Ok(())
/// }
/// ```
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $err:expr) => {
        if !$cond {
            return Err($err.into());
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_response_serialization() {
        let err = Error::InvalidQueryParam {
            param: "select".to_string(),
            message: "Unknown column".to_string(),
        };

        let response = ErrorResponse::from(&err);
        let json = serde_json::to_string(&response).unwrap();

        assert!(json.contains("DBRST100"));
        assert!(json.contains("select"));
    }

    #[test]
    fn test_error_response_skips_null_fields() {
        let err = Error::MissingPayload;
        let response = ErrorResponse::from(&err);
        let json = serde_json::to_string(&response).unwrap();

        // details and hint should be omitted when None
        assert!(!json.contains("details"));
        assert!(!json.contains("hint"));
    }

    #[test]
    fn test_error_response_includes_hint() {
        let err = Error::TableNotFound {
            name: "usrs".to_string(),
            suggestion: Some("users".to_string()),
        };

        let response = ErrorResponse::from(&err);
        let json = serde_json::to_string(&response).unwrap();

        assert!(json.contains("hint"));
        assert!(json.contains("users"));
    }

    #[test]
    fn test_www_authenticate_header_propagation() {
        use crate::auth::error::{JwtDecodeError, JwtError};

        // Test that JwtAuth errors include WWW-Authenticate header
        let jwt_err = JwtError::TokenRequired;
        let err = Error::JwtAuth(jwt_err);
        let response = err.into_response();

        assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
        let www_auth = response.headers().get(header::WWW_AUTHENTICATE).unwrap();
        assert_eq!(www_auth, "Bearer");

        // Test decode error
        let jwt_err = JwtError::Decode(JwtDecodeError::BadCrypto);
        let err = Error::JwtAuth(jwt_err);
        let response = err.into_response();

        assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
        let www_auth = response.headers().get(header::WWW_AUTHENTICATE).unwrap();
        assert!(www_auth.to_str().unwrap().contains("invalid_token"));
    }

    #[test]
    fn test_result_ext_column_context() {
        let result: Result<i32, String> = Err("test error".to_string());
        let err = result.column_context("users", "email").unwrap_err();

        match err {
            Error::Internal(msg) => {
                assert!(msg.contains("users"));
                assert!(msg.contains("email"));
                assert!(msg.contains("test error"));
            }
            _ => panic!("Expected Internal error"),
        }
    }

    #[test]
    fn test_bail_macro() {
        fn test_bail() -> Result<(), Error> {
            crate::bail!(Error::InvalidQueryParam {
                param: "test".to_string(),
                message: "bail test".to_string(),
            });
        }

        let err = test_bail().unwrap_err();
        assert!(matches!(err, Error::InvalidQueryParam { .. }));
    }

    #[test]
    fn test_ensure_macro() {
        fn test_ensure(x: i32) -> Result<(), Error> {
            crate::ensure!(
                x >= 0,
                Error::InvalidQueryParam {
                    param: "x".to_string(),
                    message: "must be non-negative".to_string(),
                }
            );
            Ok(())
        }

        assert!(test_ensure(5).is_ok());
        let err = test_ensure(-1).unwrap_err();
        assert!(matches!(err, Error::InvalidQueryParam { .. }));
    }
}
