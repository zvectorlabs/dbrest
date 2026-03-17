//! PGRST error codes
//!
//! PGRST error codes used throughout PgREST.

/// Configuration errors (000-099)
pub mod config {
    pub const DB_CONNECTION: &str = "PGRST000";
    pub const UNSUPPORTED_PG_VERSION: &str = "PGRST001";
    pub const INVALID_CONFIG: &str = "PGRST002";
    pub const CONNECTION_RETRY_TIMEOUT: &str = "PGRST003";
}

/// API request errors (100-199)
pub mod request {
    pub const INVALID_QUERY_PARAM: &str = "PGRST100";
    pub const INVALID_RPC_METHOD: &str = "PGRST101";
    pub const INVALID_BODY: &str = "PGRST102";
    pub const INVALID_RANGE: &str = "PGRST103";
    // PGRST104 is no longer used (ParseRequestError)
    pub const INVALID_FILTERS: &str = "PGRST105";
    pub const UNACCEPTABLE_SCHEMA: &str = "PGRST106";
    pub const MEDIA_TYPE_ERROR: &str = "PGRST107";
    pub const NOT_EMBEDDED: &str = "PGRST108";
    // PGRST109 is no longer used (LimitNoOrderError)
    // PGRST110 is no longer used (OffLimitsChangesError)
    pub const GUC_HEADERS_ERROR: &str = "PGRST111";
    pub const GUC_STATUS_ERROR: &str = "PGRST112";
    // PGRST113 is no longer used (BinaryFieldError)
    pub const PUT_LIMIT_NOT_ALLOWED: &str = "PGRST114";
    pub const PUT_MATCHING_PK_ERROR: &str = "PGRST115";
    pub const SINGULARITY_ERROR: &str = "PGRST116";
    pub const UNSUPPORTED_METHOD: &str = "PGRST117";
    pub const RELATED_ORDER_NOT_TO_ONE: &str = "PGRST118";
    // PGRST119 is unused
    pub const UNACCEPTABLE_FILTER: &str = "PGRST120";
    pub const PGRST_PARSE_ERROR: &str = "PGRST121";
    pub const INVALID_PREFERENCES: &str = "PGRST122";
    pub const AGGREGATES_NOT_ALLOWED: &str = "PGRST123";
    pub const MAX_AFFECTED_VIOLATION: &str = "PGRST124";
    pub const INVALID_RESOURCE_PATH: &str = "PGRST125";
    pub const OPENAPI_DISABLED: &str = "PGRST126";
    pub const NOT_IMPLEMENTED: &str = "PGRST127";
    pub const MAX_AFFECTED_RPC_VIOLATION: &str = "PGRST128";

    // Legacy/alternative codes for compatibility
    pub const PARSE_ERROR: &str = "PGRST101"; // Alias for INVALID_RPC_METHOD
    pub const INVALID_CONTENT_TYPE: &str = "PGRST103"; // Alias for INVALID_RANGE
    pub const INVALID_PREFERENCE: &str = "PGRST104"; // Legacy
    pub const INVALID_FILTER_OPERATOR: &str = "PGRST105"; // Alias for INVALID_FILTERS
    pub const SCHEMA_NOT_FOUND: &str = "PGRST106"; // Alias for UNACCEPTABLE_SCHEMA
    pub const INVALID_SPREAD_COLUMN: &str = "PGRST107"; // Alias for MEDIA_TYPE_ERROR
    pub const AMBIGUOUS_EMBEDDING: &str = "PGRST108"; // Alias for NOT_EMBEDDED
    pub const INVALID_EMBEDDING: &str = "PGRST109"; // Legacy
    pub const INVALID_MEDIA_HANDLER: &str = "PGRST112"; // Alias for GUC_STATUS_ERROR
    pub const MEDIA_TYPE_MISMATCH: &str = "PGRST113"; // Legacy
    pub const URI_TOO_LONG: &str = "PGRST114"; // Alias for PUT_LIMIT_NOT_ALLOWED
    pub const INVALID_AGGREGATE: &str = "PGRST115"; // Alias for PUT_MATCHING_PK_ERROR
}

/// Schema cache errors (200-299)
pub mod schema {
    pub const RELATIONSHIP_NOT_FOUND: &str = "PGRST200"; // NoRelBetween
    pub const AMBIGUOUS_RELATIONSHIP: &str = "PGRST201"; // AmbiguousRelBetween
    pub const FUNCTION_NOT_FOUND: &str = "PGRST202"; // NoRpc
    pub const AMBIGUOUS_FUNCTION: &str = "PGRST203"; // AmbiguousRpc
    pub const COLUMN_NOT_FOUND: &str = "PGRST204"; // ColumnNotFound
    pub const TABLE_NOT_FOUND: &str = "PGRST205"; // TableNotFound
    pub const SCHEMA_CACHE_NOT_READY: &str = "PGRST204"; // Legacy alias
}

/// Authentication errors (300-399)
pub mod auth {
    pub const SECRET_MISSING: &str = "PGRST300";
    pub const JWT_ERROR: &str = "PGRST301";
    pub const NO_ANON_ROLE: &str = "PGRST302";
    pub const CLAIMS_ERROR: &str = "PGRST303";
}

/// Request/action errors (400-499)
pub mod action {
    pub const NOT_INSERTABLE: &str = "PGRST400";
    pub const NOT_UPDATABLE: &str = "PGRST401";
    pub const NOT_DELETABLE: &str = "PGRST402";
    pub const SINGLE_OBJECT_EXPECTED: &str = "PGRST405";
    pub const MISSING_PAYLOAD: &str = "PGRST406";
    pub const INVALID_PAYLOAD: &str = "PGRST407";
    pub const NO_PRIMARY_KEY: &str = "PGRST408";
    pub const PUT_INCOMPLETE: &str = "PGRST409";
}

/// Database errors (500-599)
pub mod database {
    pub const DB_ERROR: &str = "PGRST500";
    pub const FK_VIOLATION: &str = "PGRST501";
    pub const UNIQUE_VIOLATION: &str = "PGRST502";
    pub const CHECK_VIOLATION: &str = "PGRST503";
    pub const MAX_ROWS_EXCEEDED: &str = "PGRST504";
    pub const NOT_NULL_VIOLATION: &str = "PGRST505";
    pub const EXCLUSION_VIOLATION: &str = "PGRST506";
    pub const RAISED_EXCEPTION: &str = "PGRST507";
    pub const PGRST_RAISE: &str = "PGRST508";
}

/// Internal errors
pub mod internal {
    pub const INTERNAL_ERROR: &str = "PGRST999";
}
