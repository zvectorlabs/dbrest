//! Error handling for PgREST
//!
//! This module provides:
//! - [`Error`] - Main error enum with PGRST-compatible error codes
//! - [`ErrorResponse`] - JSON response format for errors
//!
//! # Error Codes
//!
//! Error codes follow PGRST conventions:
//! - PGRST000-099: Configuration errors
//! - PGRST100-199: API request errors
//! - PGRST200-299: Schema cache errors
//! - PGRST300-399: Authentication errors
//! - PGRST400-499: Request/action errors
//! - PGRST500-599: Database errors

pub mod codes;
pub mod response;

pub use response::ErrorResponse;

use thiserror::Error;

/// Main error type for PgREST
///
/// Each variant maps to a specific PGRST error code and HTTP status code.
#[derive(Debug, Error)]
pub enum Error {
    // =========================================
    // Configuration Errors (PGRST000-099)
    // =========================================
    #[error("Database connection failed: {0}")]
    DbConnection(String),

    #[error("Unsupported PostgreSQL version: {major}.{minor} (minimum: 12.0)")]
    UnsupportedPgVersion { major: u32, minor: u32 },

    #[error("Invalid configuration: {message}")]
    InvalidConfig { message: String },

    #[error("Database connection retry timeout")]
    ConnectionRetryTimeout,

    // =========================================
    // API Request Errors (PGRST100-199)
    // =========================================
    #[error("Invalid query parameter '{param}': {message}")]
    InvalidQueryParam { param: String, message: String },

    #[error("Parse error in {location}: {message}")]
    ParseError { location: String, message: String },

    #[error("Invalid range: {0}")]
    InvalidRange(String),

    #[error("Invalid content-type: {0}")]
    InvalidContentType(String),

    #[error("Invalid preference: {0}")]
    InvalidPreference(String),

    #[error("Invalid filter operator '{op}' on column '{column}'")]
    InvalidFilterOperator { column: String, op: String },

    #[error("Ambiguous embedding: multiple relationships found for '{0}'")]
    AmbiguousEmbedding(String),

    #[error("Invalid embedding: {0}")]
    InvalidEmbedding(String),

    #[error("Invalid request body: {0}")]
    InvalidBody(String),

    #[error("Schema not found: {0}")]
    SchemaNotFound(String),

    #[error("Invalid column in spread relationship: {0}")]
    InvalidSpreadColumn(String),

    #[error("Invalid content type for media handler: {0}")]
    InvalidMediaHandler(String),

    #[error("Media types mismatch: {0}")]
    MediaTypeMismatch(String),

    #[error("URI too long: {0}")]
    UriTooLong(String),

    #[error("Invalid aggregate: {0}")]
    InvalidAggregate(String),

    #[error("response.headers GUC must be a JSON array composed of objects with a single key and a string value")]
    GucHeadersError,

    #[error("response.status GUC must be a valid status code")]
    GucStatusError,

    #[error("PUT with limit/offset querystring parameters is not allowed")]
    PutLimitNotAllowed,

    #[error("Payload values do not match URL in primary key column(s)")]
    PutMatchingPkError,

    #[error("Cannot coerce the result to a single JSON object")]
    SingularityError { count: i64 },

    #[error("Unsupported HTTP method: {0}")]
    UnsupportedMethod(String),

    #[error("A related order on '{target}' is not possible")]
    RelatedOrderNotToOne { origin: String, target: String },

    #[error("Bad operator on the '{target}' embedded resource")]
    UnacceptableFilter { target: String },

    #[error("Could not parse JSON in the \"RAISE SQLSTATE 'PGRST'\" error: {0}")]
    PgrstParseError(String),

    #[error("Invalid preferences given with handling=strict: {0}")]
    InvalidPreferencesStrict(String),

    #[error("Use of aggregate functions is not allowed")]
    AggregatesNotAllowed,

    #[error("Query result exceeds max-affected preference constraint: {count} rows")]
    MaxAffectedViolation { count: i64 },

    #[error("Invalid path specified in request URL")]
    InvalidResourcePath,

    #[error("Root endpoint metadata is disabled")]
    OpenApiDisabled,

    #[error("Feature not implemented: {0}")]
    NotImplemented(String),

    #[error("Function must return SETOF or TABLE when max-affected preference is used with handling=strict")]
    MaxAffectedRpcViolation,

    // =========================================
    // Schema Cache Errors (PGRST200-299)
    // =========================================
    #[error("Table not found: {name}")]
    TableNotFound {
        name: String,
        suggestion: Option<String>,
    },

    #[error("Column '{column}' not found in table '{table}'")]
    ColumnNotFound { table: String, column: String },

    #[error("Function not found: {name}")]
    FunctionNotFound { name: String },

    #[error("Relationship not found between '{from_table}' and '{to_table}'")]
    RelationshipNotFound { from_table: String, to_table: String },

    #[error("Schema cache not ready")]
    SchemaCacheNotReady,

    #[error("Ambiguous relationship: multiple relationships found between '{from_table}' and '{to_table}'")]
    AmbiguousRelationship { from_table: String, to_table: String },

    #[error("Ambiguous function: multiple function overloads found for '{name}'")]
    AmbiguousFunction { name: String },

    // =========================================
    // JWT/Auth Errors (PGRST300-399)
    // =========================================
    #[error("{0}")]
    JwtAuth(#[from] crate::auth::error::JwtError),

    #[error("JWT error: {0}")]
    Jwt(String),

    #[error("No anonymous role configured")]
    NoAnonRole,

    #[error("Permission denied for role '{role}'")]
    PermissionDenied { role: String },

    // =========================================
    // Request/Action Errors (PGRST400-499)
    // =========================================
    #[error("Table '{table}' is not insertable")]
    NotInsertable { table: String },

    #[error("Table '{table}' is not updatable")]
    NotUpdatable { table: String },

    #[error("Table '{table}' is not deletable")]
    NotDeletable { table: String },

    #[error("Single object expected but multiple rows returned")]
    SingleObjectExpected,

    #[error("Missing required payload")]
    MissingPayload,

    #[error("Invalid payload: {0}")]
    InvalidPayload(String),

    #[error("No primary key found for table '{table}'")]
    NoPrimaryKey { table: String },

    #[error("PUT requires all primary key columns")]
    PutIncomplete,

    // =========================================
    // Database Errors (PGRST500-599)
    // =========================================
    #[error("Database error: {message}")]
    Database {
        code: Option<String>,
        message: String,
        detail: Option<String>,
        hint: Option<String>,
    },

    #[error("Foreign key violation: {0}")]
    ForeignKeyViolation(String),

    #[error("Unique constraint violation: {0}")]
    UniqueViolation(String),

    #[error("Check constraint violation: {0}")]
    CheckViolation(String),

    #[error("Not null violation: {0}")]
    NotNullViolation(String),

    #[error("Exclusion violation: {0}")]
    ExclusionViolation(String),

    #[error("Row count limit exceeded: {count} rows affected, max is {max}")]
    MaxRowsExceeded { count: i64, max: i64 },

    #[error("Raised exception: {message}")]
    RaisedException {
        message: String,
        status: Option<u16>,
    },

    #[error("PostgREST raise: {message}")]
    PgrstRaise {
        message: String,
        status: u16,
    },

    // =========================================
    // Internal Errors
    // =========================================
    #[error("Internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Get the PGRST error code for this error.
    pub fn code(&self) -> &'static str {
        match self {
            // Config errors
            Error::DbConnection(_) => codes::config::DB_CONNECTION,
            Error::UnsupportedPgVersion { .. } => codes::config::UNSUPPORTED_PG_VERSION,
            Error::InvalidConfig { .. } => codes::config::INVALID_CONFIG,
            Error::ConnectionRetryTimeout => codes::config::CONNECTION_RETRY_TIMEOUT,

            // Request errors
            Error::InvalidQueryParam { .. } => codes::request::INVALID_QUERY_PARAM,
            Error::ParseError { .. } => codes::request::PARSE_ERROR,
            Error::InvalidRange(_) => codes::request::INVALID_RANGE,
            Error::InvalidContentType(_) => codes::request::INVALID_CONTENT_TYPE,
            Error::InvalidPreference(_) => codes::request::INVALID_PREFERENCE,
            Error::InvalidFilterOperator { .. } => codes::request::INVALID_FILTER_OPERATOR,
            Error::SchemaNotFound(_) => codes::request::SCHEMA_NOT_FOUND,
            Error::InvalidSpreadColumn(_) => codes::request::INVALID_SPREAD_COLUMN,
            Error::AmbiguousEmbedding(_) => codes::request::AMBIGUOUS_EMBEDDING,
            Error::InvalidEmbedding(_) => codes::request::INVALID_EMBEDDING,
            Error::InvalidBody(_) => codes::request::INVALID_BODY,
            Error::InvalidMediaHandler(_) => codes::request::INVALID_MEDIA_HANDLER,
            Error::MediaTypeMismatch(_) => codes::request::MEDIA_TYPE_MISMATCH,
            Error::UriTooLong(_) => codes::request::URI_TOO_LONG,
            Error::InvalidAggregate(_) => codes::request::INVALID_AGGREGATE,
            Error::GucHeadersError => codes::request::GUC_HEADERS_ERROR,
            Error::GucStatusError => codes::request::GUC_STATUS_ERROR,
            Error::PutLimitNotAllowed => codes::request::PUT_LIMIT_NOT_ALLOWED,
            Error::PutMatchingPkError => codes::request::PUT_MATCHING_PK_ERROR,
            Error::SingularityError { .. } => codes::request::SINGULARITY_ERROR,
            Error::UnsupportedMethod(_) => codes::request::UNSUPPORTED_METHOD,
            Error::RelatedOrderNotToOne { .. } => codes::request::RELATED_ORDER_NOT_TO_ONE,
            Error::UnacceptableFilter { .. } => codes::request::UNACCEPTABLE_FILTER,
            Error::PgrstParseError(_) => codes::request::PGRST_PARSE_ERROR,
            Error::InvalidPreferencesStrict(_) => codes::request::INVALID_PREFERENCES,
            Error::AggregatesNotAllowed => codes::request::AGGREGATES_NOT_ALLOWED,
            Error::MaxAffectedViolation { .. } => codes::request::MAX_AFFECTED_VIOLATION,
            Error::InvalidResourcePath => codes::request::INVALID_RESOURCE_PATH,
            Error::OpenApiDisabled => codes::request::OPENAPI_DISABLED,
            Error::NotImplemented(_) => codes::request::NOT_IMPLEMENTED,
            Error::MaxAffectedRpcViolation => codes::request::MAX_AFFECTED_RPC_VIOLATION,

            // Schema errors
            Error::TableNotFound { .. } => codes::schema::TABLE_NOT_FOUND,
            Error::ColumnNotFound { .. } => codes::schema::COLUMN_NOT_FOUND,
            Error::FunctionNotFound { .. } => codes::schema::FUNCTION_NOT_FOUND,
            Error::RelationshipNotFound { .. } => codes::schema::RELATIONSHIP_NOT_FOUND,
            Error::AmbiguousRelationship { .. } => codes::schema::AMBIGUOUS_RELATIONSHIP,
            Error::AmbiguousFunction { .. } => codes::schema::AMBIGUOUS_FUNCTION,
            Error::SchemaCacheNotReady => codes::schema::SCHEMA_CACHE_NOT_READY,

            // Auth errors
            Error::JwtAuth(e) => e.code(),
            Error::Jwt(_) => codes::auth::JWT_ERROR,
            Error::NoAnonRole => codes::auth::NO_ANON_ROLE,
            Error::PermissionDenied { .. } => codes::auth::CLAIMS_ERROR,

            // Action errors
            Error::NotInsertable { .. } => codes::action::NOT_INSERTABLE,
            Error::NotUpdatable { .. } => codes::action::NOT_UPDATABLE,
            Error::NotDeletable { .. } => codes::action::NOT_DELETABLE,
            Error::SingleObjectExpected => codes::action::SINGLE_OBJECT_EXPECTED,
            Error::MissingPayload => codes::action::MISSING_PAYLOAD,
            Error::InvalidPayload(_) => codes::action::INVALID_PAYLOAD,
            Error::NoPrimaryKey { .. } => codes::action::NO_PRIMARY_KEY,
            Error::PutIncomplete => codes::action::PUT_INCOMPLETE,

            // Database errors
            Error::Database { .. } => codes::database::DB_ERROR,
            Error::ForeignKeyViolation(_) => codes::database::FK_VIOLATION,
            Error::UniqueViolation(_) => codes::database::UNIQUE_VIOLATION,
            Error::CheckViolation(_) => codes::database::CHECK_VIOLATION,
            Error::NotNullViolation(_) => codes::database::NOT_NULL_VIOLATION,
            Error::ExclusionViolation(_) => codes::database::EXCLUSION_VIOLATION,
            Error::MaxRowsExceeded { .. } => codes::database::MAX_ROWS_EXCEEDED,
            Error::RaisedException { .. } => codes::database::RAISED_EXCEPTION,
            Error::PgrstRaise { .. } => codes::database::PGRST_RAISE,

            // Internal
            Error::Internal(_) => codes::internal::INTERNAL_ERROR,
        }
    }

    /// Get the HTTP status code for this error.
    pub fn status(&self) -> http::StatusCode {
        use http::StatusCode;

        match self {
            // Config errors → 503 Service Unavailable
            Error::DbConnection(_)
            | Error::UnsupportedPgVersion { .. }
            | Error::InvalidConfig { .. }
            | Error::ConnectionRetryTimeout
            | Error::SchemaCacheNotReady => StatusCode::SERVICE_UNAVAILABLE,

            // Request parsing errors → 400 Bad Request
            Error::InvalidQueryParam { .. }
            | Error::ParseError { .. }
            | Error::InvalidRange(_)
            | Error::InvalidContentType(_)
            | Error::InvalidPreference(_)
            | Error::InvalidFilterOperator { .. }
            | Error::SchemaNotFound(_)
            | Error::InvalidSpreadColumn(_)
            | Error::AmbiguousEmbedding(_)
            | Error::InvalidEmbedding(_)
            | Error::InvalidBody(_)
            | Error::InvalidPayload(_)
            | Error::MissingPayload
            | Error::PutIncomplete
            | Error::InvalidMediaHandler(_)
            | Error::MediaTypeMismatch(_)
            | Error::UriTooLong(_)
            |             Error::InvalidAggregate(_)
            | Error::NotNullViolation(_)
            | Error::MaxRowsExceeded { .. }
            | Error::GucHeadersError
            | Error::GucStatusError
            | Error::PutLimitNotAllowed
            | Error::PutMatchingPkError
            | Error::SingularityError { .. }
            | Error::UnsupportedMethod(_)
            | Error::RelatedOrderNotToOne { .. }
            | Error::UnacceptableFilter { .. }
            | Error::PgrstParseError(_)
            | Error::InvalidPreferencesStrict(_)
            |             Error::AggregatesNotAllowed
            | Error::MaxAffectedViolation { .. }
            | Error::NotImplemented(_)
            | Error::MaxAffectedRpcViolation => StatusCode::BAD_REQUEST,

            // Not found → 404
            Error::TableNotFound { .. }
            | Error::ColumnNotFound { .. }
            | Error::FunctionNotFound { .. }
            | Error::RelationshipNotFound { .. }
            | Error::InvalidResourcePath
            | Error::OpenApiDisabled => StatusCode::NOT_FOUND,

            // Ambiguous errors → 300 Multiple Choices
            Error::AmbiguousRelationship { .. }
            | Error::AmbiguousFunction { .. } => StatusCode::MULTIPLE_CHOICES,

            // Auth errors → 401/403/500
            Error::JwtAuth(e) => e.status(),
            Error::Jwt(_) | Error::NoAnonRole => StatusCode::UNAUTHORIZED,
            Error::PermissionDenied { .. } => StatusCode::FORBIDDEN,

            // Permission/capability errors → 405
            Error::NotInsertable { .. }
            | Error::NotUpdatable { .. }
            | Error::NotDeletable { .. } => StatusCode::METHOD_NOT_ALLOWED,

            // Conflict errors → 409
            Error::NoPrimaryKey { .. }
            | Error::UniqueViolation(_)
            | Error::ForeignKeyViolation(_)
            | Error::CheckViolation(_)
            | Error::ExclusionViolation(_) => StatusCode::CONFLICT,

            // Single object expected → 406
            Error::SingleObjectExpected => StatusCode::NOT_ACCEPTABLE,

            // Raised exceptions → use status from error or default to 400
            Error::RaisedException { status, .. } => {
                status
                    .and_then(|s| StatusCode::from_u16(s).ok())
                    .unwrap_or(StatusCode::BAD_REQUEST)
            }

            // PGRST raise → use status from error
            Error::PgrstRaise { status, .. } => {
                StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_REQUEST)
            }

            // General database errors → 500
            Error::Database { .. } | Error::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Extract additional details and hint for error response.
    pub fn details_and_hint(&self) -> (Option<String>, Option<String>) {
        match self {
            Error::TableNotFound { suggestion, .. } => {
                (None, suggestion.as_ref().map(|s| format!("Did you mean '{}'?", s)))
            }
            Error::ColumnNotFound { table, column } => (
                Some(format!("Column '{}' does not exist in table '{}'", column, table)),
                Some(format!("Check the table schema for available columns")),
            ),
            Error::FunctionNotFound { name } => (
                Some(format!("Function '{}' does not exist", name)),
                Some(format!("Check the schema for available functions")),
            ),
            Error::RelationshipNotFound { from_table, to_table } => (
                Some(format!("No relationship found between '{}' and '{}'", from_table, to_table)),
                Some(format!("Ensure foreign key constraints exist between these tables")),
            ),
            Error::Database { detail, hint, .. } => (detail.clone(), hint.clone()),
            Error::InvalidQueryParam { message, .. } => (Some(message.clone()), None),
            Error::ParseError { location, message } => {
                (Some(format!("At {}: {}", location, message)), None)
            }
            Error::InvalidFilterOperator { column, op } => (
                Some(format!("Operator '{}' is not valid for column '{}'", op, column)),
                Some(format!("Use valid operators: eq, neq, gt, gte, lt, lte, like, ilike, is, in, cs, cd, ov, sl, sr, nxr, nxl, adj")),
            ),
            Error::AmbiguousEmbedding(rel) => (
                None,
                Some(format!(
                    "Use the hint parameter to disambiguate: ?select={}!hint_name(*)",
                    rel
                )),
            ),
            Error::InvalidBody(msg) => (
                Some(msg.clone()),
                Some(format!("Ensure the request body is valid JSON")),
            ),
            Error::InvalidPayload(msg) => (
                Some(msg.clone()),
                Some(format!("Check the payload format and required fields")),
            ),
            Error::MaxRowsExceeded { count, max } => (
                Some(format!("Affected {} rows, but maximum allowed is {}", count, max)),
                Some(format!("Reduce the scope of your request or increase the limit")),
            ),
            Error::NotInsertable { table } => (
                Some(format!("Table '{}' does not allow INSERT operations", table)),
                Some(format!("Check table permissions and RLS policies")),
            ),
            Error::NotUpdatable { table } => (
                Some(format!("Table '{}' does not allow UPDATE operations", table)),
                Some(format!("Check table permissions and RLS policies")),
            ),
            Error::NotDeletable { table } => (
                Some(format!("Table '{}' does not allow DELETE operations", table)),
                Some(format!("Check table permissions and RLS policies")),
            ),
            Error::UniqueViolation(msg) => (
                Some(msg.clone()),
                Some(format!("The value violates a unique constraint. Use a different value or update the existing record")),
            ),
            Error::ForeignKeyViolation(msg) => (
                Some(msg.clone()),
                Some(format!("The value references a non-existent record. Ensure the referenced record exists")),
            ),
            Error::CheckViolation(msg) => (
                Some(msg.clone()),
                Some(format!("The value violates a check constraint. Check the constraint requirements")),
            ),
            Error::NotNullViolation(msg) => (
                Some(msg.clone()),
                Some(format!("A required field is missing or null. Provide a value for all required fields")),
            ),
            Error::ExclusionViolation(msg) => (
                Some(msg.clone()),
                Some(format!("The value violates an exclusion constraint")),
            ),
            Error::RaisedException { message, .. } => (
                Some(message.clone()),
                None,
            ),
            Error::PgrstRaise { message, .. } => (
                Some(message.clone()),
                None,
            ),
            Error::SingularityError { count } => (
                Some(format!("The result contains {} rows", count)),
                None,
            ),
            Error::MaxAffectedViolation { count } => (
                Some(format!("The query affects {} rows", count)),
                None,
            ),
            Error::GucHeadersError => (
                Some("response.headers GUC must be a JSON array composed of objects with a single key and a string value".to_string()),
                None,
            ),
            Error::GucStatusError => (
                Some("response.status GUC must be a valid status code".to_string()),
                None,
            ),
            Error::AmbiguousRelationship { from_table, to_table } => (
                Some(format!("Multiple relationships found between '{}' and '{}'", from_table, to_table)),
                Some("Use the hint parameter to disambiguate".to_string()),
            ),
            Error::AmbiguousFunction { name } => (
                Some(format!("Multiple function overloads found for '{}'", name)),
                Some("Try renaming the parameters or the function itself in the database so function overloading can be resolved".to_string()),
            ),
            _ => (None, None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_codes() {
        let err = Error::TableNotFound {
            name: "users".to_string(),
            suggestion: None,
        };
        assert_eq!(err.code(), "PGRST205"); // TableNotFound uses PGRST205 per PostgREST
        assert_eq!(err.status(), http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn test_error_status_mapping() {
        assert_eq!(
            Error::DbConnection("test".to_string()).status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            Error::InvalidRange("test".to_string()).status(),
            http::StatusCode::BAD_REQUEST
        );
        assert_eq!(
            Error::Jwt("invalid".to_string()).status(),
            http::StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            Error::PermissionDenied {
                role: "test".to_string()
            }
            .status(),
            http::StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn test_suggestion_hint() {
        let err = Error::TableNotFound {
            name: "usrs".to_string(),
            suggestion: Some("users".to_string()),
        };
        let (_, hint) = err.details_and_hint();
        assert!(hint.unwrap().contains("users"));
    }

    #[test]
    fn test_new_error_codes() {
        // PGRST003
        assert_eq!(
            Error::ConnectionRetryTimeout.code(),
            codes::config::CONNECTION_RETRY_TIMEOUT
        );
        assert_eq!(
            Error::ConnectionRetryTimeout.status(),
            http::StatusCode::SERVICE_UNAVAILABLE
        );

        // PGRST106
        assert_eq!(
            Error::SchemaNotFound("test".to_string()).code(),
            codes::request::SCHEMA_NOT_FOUND
        );
        assert_eq!(
            Error::SchemaNotFound("test".to_string()).status(),
            http::StatusCode::BAD_REQUEST
        );

        // PGRST107
        assert_eq!(
            Error::InvalidSpreadColumn("col".to_string()).code(),
            codes::request::INVALID_SPREAD_COLUMN
        );

        // PGRST112
        assert_eq!(
            Error::InvalidMediaHandler("handler".to_string()).code(),
            codes::request::INVALID_MEDIA_HANDLER
        );

        // PGRST113
        assert_eq!(
            Error::MediaTypeMismatch("mismatch".to_string()).code(),
            codes::request::MEDIA_TYPE_MISMATCH
        );

        // PGRST114
        assert_eq!(
            Error::UriTooLong("uri".to_string()).code(),
            codes::request::URI_TOO_LONG
        );

        // PGRST115
        assert_eq!(
            Error::InvalidAggregate("agg".to_string()).code(),
            codes::request::INVALID_AGGREGATE
        );

        // PGRST505
        assert_eq!(
            Error::NotNullViolation("msg".to_string()).code(),
            codes::database::NOT_NULL_VIOLATION
        );
        assert_eq!(
            Error::NotNullViolation("msg".to_string()).status(),
            http::StatusCode::BAD_REQUEST
        );

        // PGRST506
        assert_eq!(
            Error::ExclusionViolation("msg".to_string()).code(),
            codes::database::EXCLUSION_VIOLATION
        );
        assert_eq!(
            Error::ExclusionViolation("msg".to_string()).status(),
            http::StatusCode::CONFLICT
        );

        // PGRST507
        assert_eq!(
            Error::RaisedException {
                message: "test".to_string(),
                status: None
            }
            .code(),
            codes::database::RAISED_EXCEPTION
        );
        assert_eq!(
            Error::RaisedException {
                message: "test".to_string(),
                status: Some(202)
            }
            .status(),
            http::StatusCode::ACCEPTED
        );

        // PGRST508
        assert_eq!(
            Error::PgrstRaise {
                message: "test".to_string(),
                status: 418
            }
            .code(),
            codes::database::PGRST_RAISE
        );
        assert_eq!(
            Error::PgrstRaise {
                message: "test".to_string(),
                status: 418
            }
            .status(),
            http::StatusCode::IM_A_TEAPOT
        );
    }

    #[test]
    fn test_error_serialization() {
        // Test that all new variants serialize correctly
        let errs = vec![
            Error::ConnectionRetryTimeout,
            Error::SchemaNotFound("test".to_string()),
            Error::InvalidSpreadColumn("col".to_string()),
            Error::InvalidMediaHandler("handler".to_string()),
            Error::MediaTypeMismatch("mismatch".to_string()),
            Error::UriTooLong("uri".to_string()),
            Error::InvalidAggregate("agg".to_string()),
            Error::NotNullViolation("msg".to_string()),
            Error::ExclusionViolation("msg".to_string()),
            Error::RaisedException {
                message: "test".to_string(),
                status: None,
            },
            Error::PgrstRaise {
                message: "test".to_string(),
                status: 400,
            },
        ];

        for err in errs {
            let response = crate::error::ErrorResponse::from(&err);
            let json = serde_json::to_string(&response).unwrap();
            assert!(json.contains(&err.code()));
            assert!(json.contains("code"));
            assert!(json.contains("message"));
        }
    }

    #[test]
    fn test_all_error_codes_have_status() {
        // Test that every error variant has a valid status code
        let errs = vec![
            Error::DbConnection("test".to_string()),
            Error::UnsupportedPgVersion { major: 11, minor: 0 },
            Error::InvalidConfig { message: "test".to_string() },
            Error::ConnectionRetryTimeout,
            Error::InvalidQueryParam { param: "test".to_string(), message: "test".to_string() },
            Error::ParseError { location: "test".to_string(), message: "test".to_string() },
            Error::InvalidRange("test".to_string()),
            Error::InvalidContentType("test".to_string()),
            Error::InvalidPreference("test".to_string()),
            Error::InvalidFilterOperator { column: "test".to_string(), op: "test".to_string() },
            Error::SchemaNotFound("test".to_string()),
            Error::InvalidSpreadColumn("test".to_string()),
            Error::AmbiguousEmbedding("test".to_string()),
            Error::InvalidEmbedding("test".to_string()),
            Error::InvalidBody("test".to_string()),
            Error::InvalidMediaHandler("test".to_string()),
            Error::MediaTypeMismatch("test".to_string()),
            Error::UriTooLong("test".to_string()),
            Error::InvalidAggregate("test".to_string()),
            Error::TableNotFound { name: "test".to_string(), suggestion: None },
            Error::ColumnNotFound { table: "test".to_string(), column: "test".to_string() },
            Error::FunctionNotFound { name: "test".to_string() },
            Error::RelationshipNotFound { from_table: "test".to_string(), to_table: "test".to_string() },
            Error::SchemaCacheNotReady,
            Error::Jwt("test".to_string()),
            Error::NoAnonRole,
            Error::PermissionDenied { role: "test".to_string() },
            Error::NotInsertable { table: "test".to_string() },
            Error::NotUpdatable { table: "test".to_string() },
            Error::NotDeletable { table: "test".to_string() },
            Error::SingleObjectExpected,
            Error::MissingPayload,
            Error::InvalidPayload("test".to_string()),
            Error::NoPrimaryKey { table: "test".to_string() },
            Error::PutIncomplete,
            Error::Database { code: None, message: "test".to_string(), detail: None, hint: None },
            Error::ForeignKeyViolation("test".to_string()),
            Error::UniqueViolation("test".to_string()),
            Error::CheckViolation("test".to_string()),
            Error::NotNullViolation("test".to_string()),
            Error::ExclusionViolation("test".to_string()),
            Error::MaxRowsExceeded { count: 10, max: 5 },
            Error::RaisedException { message: "test".to_string(), status: None },
            Error::PgrstRaise { message: "test".to_string(), status: 400 },
            Error::Internal("test".to_string()),
        ];

        for err in errs {
            let status = err.status();
            // All status codes should be valid (not panic)
            assert!(status.as_u16() >= 400 || status.as_u16() == 200 || status.as_u16() == 201);
        }
    }

    #[test]
    fn test_details_and_hints() {
        // Test details_and_hint for various error types
        let err = Error::ColumnNotFound {
            table: "users".to_string(),
            column: "email".to_string(),
        };
        let (details, hint) = err.details_and_hint();
        assert!(details.is_some());
        assert!(hint.is_some());
        assert!(details.unwrap().contains("email"));
        assert!(hint.unwrap().contains("schema"));

        let err = Error::MaxRowsExceeded { count: 100, max: 50 };
        let (details, hint) = err.details_and_hint();
        assert!(details.is_some());
        assert!(hint.is_some());
        let details_str = details.unwrap();
        assert!(details_str.contains("100"));
        assert!(details_str.contains("50"));

        let err = Error::NotInsertable { table: "users".to_string() };
        let (details, hint) = err.details_and_hint();
        assert!(details.is_some());
        assert!(hint.is_some());
        assert!(details.unwrap().contains("users"));
    }
}
