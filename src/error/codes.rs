//! DBRST error codes
//!
//! DBRST error codes used throughout dbrest.

/// Configuration errors (000-099)
pub mod config {
    pub const DB_CONNECTION: &str = "DBRST000";
    pub const UNSUPPORTED_PG_VERSION: &str = "DBRST001";
    pub const INVALID_CONFIG: &str = "DBRST002";
    pub const CONNECTION_RETRY_TIMEOUT: &str = "DBRST003";
}

/// API request errors (100-199)
pub mod request {
    pub const INVALID_QUERY_PARAM: &str = "DBRST100";
    pub const INVALID_RPC_METHOD: &str = "DBRST101";
    pub const INVALID_BODY: &str = "DBRST102";
    pub const INVALID_RANGE: &str = "DBRST103";
    // DBRST104 is no longer used (ParseRequestError)
    pub const INVALID_FILTERS: &str = "DBRST105";
    pub const UNACCEPTABLE_SCHEMA: &str = "DBRST106";
    pub const MEDIA_TYPE_ERROR: &str = "DBRST107";
    pub const NOT_EMBEDDED: &str = "DBRST108";
    // DBRST109 is no longer used (LimitNoOrderError)
    // DBRST110 is no longer used (OffLimitsChangesError)
    pub const GUC_HEADERS_ERROR: &str = "DBRST111";
    pub const GUC_STATUS_ERROR: &str = "DBRST112";
    // DBRST113 is no longer used (BinaryFieldError)
    pub const PUT_LIMIT_NOT_ALLOWED: &str = "DBRST114";
    pub const PUT_MATCHING_PK_ERROR: &str = "DBRST115";
    pub const SINGULARITY_ERROR: &str = "DBRST116";
    pub const UNSUPPORTED_METHOD: &str = "DBRST117";
    pub const RELATED_ORDER_NOT_TO_ONE: &str = "DBRST118";
    // DBRST119 is unused
    pub const UNACCEPTABLE_FILTER: &str = "DBRST120";
    pub const DBRST_PARSE_ERROR: &str = "DBRST121";
    pub const INVALID_PREFERENCES: &str = "DBRST122";
    pub const AGGREGATES_NOT_ALLOWED: &str = "DBRST123";
    pub const MAX_AFFECTED_VIOLATION: &str = "DBRST124";
    pub const INVALID_RESOURCE_PATH: &str = "DBRST125";
    pub const OPENAPI_DISABLED: &str = "DBRST126";
    pub const NOT_IMPLEMENTED: &str = "DBRST127";
    pub const MAX_AFFECTED_RPC_VIOLATION: &str = "DBRST128";

    // Legacy/alternative codes for compatibility
    pub const PARSE_ERROR: &str = "DBRST101"; // Alias for INVALID_RPC_METHOD
    pub const INVALID_CONTENT_TYPE: &str = "DBRST103"; // Alias for INVALID_RANGE
    pub const INVALID_PREFERENCE: &str = "DBRST104"; // Legacy
    pub const INVALID_FILTER_OPERATOR: &str = "DBRST105"; // Alias for INVALID_FILTERS
    pub const SCHEMA_NOT_FOUND: &str = "DBRST106"; // Alias for UNACCEPTABLE_SCHEMA
    pub const INVALID_SPREAD_COLUMN: &str = "DBRST107"; // Alias for MEDIA_TYPE_ERROR
    pub const AMBIGUOUS_EMBEDDING: &str = "DBRST108"; // Alias for NOT_EMBEDDED
    pub const INVALID_EMBEDDING: &str = "DBRST109"; // Legacy
    pub const INVALID_MEDIA_HANDLER: &str = "DBRST112"; // Alias for GUC_STATUS_ERROR
    pub const MEDIA_TYPE_MISMATCH: &str = "DBRST113"; // Legacy
    pub const URI_TOO_LONG: &str = "DBRST114"; // Alias for PUT_LIMIT_NOT_ALLOWED
    pub const INVALID_AGGREGATE: &str = "DBRST115"; // Alias for PUT_MATCHING_PK_ERROR
}

/// Schema cache errors (200-299)
pub mod schema {
    pub const RELATIONSHIP_NOT_FOUND: &str = "DBRST200"; // NoRelBetween
    pub const AMBIGUOUS_RELATIONSHIP: &str = "DBRST201"; // AmbiguousRelBetween
    pub const FUNCTION_NOT_FOUND: &str = "DBRST202"; // NoRpc
    pub const AMBIGUOUS_FUNCTION: &str = "DBRST203"; // AmbiguousRpc
    pub const COLUMN_NOT_FOUND: &str = "DBRST204"; // ColumnNotFound
    pub const TABLE_NOT_FOUND: &str = "DBRST205"; // TableNotFound
    pub const SCHEMA_CACHE_NOT_READY: &str = "DBRST204"; // Legacy alias
}

/// Authentication errors (300-399)
pub mod auth {
    pub const SECRET_MISSING: &str = "DBRST300";
    pub const JWT_ERROR: &str = "DBRST301";
    pub const NO_ANON_ROLE: &str = "DBRST302";
    pub const CLAIMS_ERROR: &str = "DBRST303";
}

/// Request/action errors (400-499)
pub mod action {
    pub const NOT_INSERTABLE: &str = "DBRST400";
    pub const NOT_UPDATABLE: &str = "DBRST401";
    pub const NOT_DELETABLE: &str = "DBRST402";
    pub const SINGLE_OBJECT_EXPECTED: &str = "DBRST405";
    pub const MISSING_PAYLOAD: &str = "DBRST406";
    pub const INVALID_PAYLOAD: &str = "DBRST407";
    pub const NO_PRIMARY_KEY: &str = "DBRST408";
    pub const PUT_INCOMPLETE: &str = "DBRST409";
}

/// Database errors (500-599)
pub mod database {
    pub const DB_ERROR: &str = "DBRST500";
    pub const FK_VIOLATION: &str = "DBRST501";
    pub const UNIQUE_VIOLATION: &str = "DBRST502";
    pub const CHECK_VIOLATION: &str = "DBRST503";
    pub const MAX_ROWS_EXCEEDED: &str = "DBRST504";
    pub const NOT_NULL_VIOLATION: &str = "DBRST505";
    pub const EXCLUSION_VIOLATION: &str = "DBRST506";
    pub const RAISED_EXCEPTION: &str = "DBRST507";
    pub const DBRST_RAISE: &str = "DBRST508";
}

/// Internal errors
pub mod internal {
    pub const INTERNAL_ERROR: &str = "DBRST999";
}
