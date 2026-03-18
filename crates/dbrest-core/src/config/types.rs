//! Configuration types
//!
//! This module defines the main `AppConfig` struct and related types.

use crate::types::QualifiedIdentifier;
use std::collections::HashMap;
use std::path::PathBuf;

use super::jwt::JsPathExp;

/// Main application configuration
///
/// All dbrest configuration options.
#[derive(Debug, Clone)]
pub struct AppConfig {
    // =========================================
    // Database settings
    // =========================================
    /// Database connection URI
    pub db_uri: String,

    /// Schemas to expose (first is default)
    pub db_schemas: Vec<String>,

    /// Anonymous role for unauthenticated requests
    pub db_anon_role: Option<String>,

    /// Connection pool size
    pub db_pool_size: usize,

    /// Pool acquisition timeout (seconds)
    pub db_pool_acquisition_timeout: u64,

    /// Pool connection max lifetime (seconds)
    pub db_pool_max_lifetime: u64,

    /// Pool connection idle timeout (seconds)
    pub db_pool_max_idletime: u64,

    /// Enable automatic pool recovery
    pub db_pool_automatic_recovery: bool,

    /// Use prepared statements
    pub db_prepared_statements: bool,

    /// Pre-request function to call
    pub db_pre_request: Option<QualifiedIdentifier>,

    /// Root spec function (for / endpoint)
    pub db_root_spec: Option<QualifiedIdentifier>,

    /// Extra schemas for search_path
    pub db_extra_search_path: Vec<String>,

    /// Transaction settings to hoist
    pub db_hoisted_tx_settings: Vec<String>,

    /// Maximum rows to return
    pub db_max_rows: Option<i64>,

    /// Enable EXPLAIN output
    pub db_plan_enabled: bool,

    /// Rollback all transactions by default
    pub db_tx_rollback_all: bool,

    /// Allow transaction override via Prefer header
    pub db_tx_allow_override: bool,

    /// Isolation level for read-only transactions
    pub db_tx_read_isolation: IsolationLevel,

    /// Isolation level for write transactions
    pub db_tx_write_isolation: IsolationLevel,

    /// Enable aggregate functions
    pub db_aggregates_enabled: bool,

    /// Load config from database
    pub db_config: bool,

    /// Pre-config function
    pub db_pre_config: Option<QualifiedIdentifier>,

    /// NOTIFY channel name
    pub db_channel: String,

    /// Enable NOTIFY listener
    pub db_channel_enabled: bool,

    // =========================================
    // Server settings
    // =========================================
    /// Server bind host
    pub server_host: String,

    /// Server bind port
    pub server_port: u16,

    /// Unix socket path
    pub server_unix_socket: Option<PathBuf>,

    /// Unix socket file mode
    pub server_unix_socket_mode: u32,

    /// CORS allowed origins
    pub server_cors_allowed_origins: Option<Vec<String>>,

    /// Trace header name
    pub server_trace_header: Option<String>,

    /// Enable Server-Timing header
    pub server_timing_enabled: bool,

    /// Maximum request body size in bytes
    pub server_max_body_size: usize,

    // =========================================
    // Admin server settings
    // =========================================
    /// Admin server bind host
    pub admin_server_host: String,

    /// Admin server bind port (None = disabled)
    pub admin_server_port: Option<u16>,

    // =========================================
    // JWT settings
    // =========================================
    /// JWT secret (or JWKS JSON)
    pub jwt_secret: Option<String>,

    /// JWT secret is base64 encoded
    pub jwt_secret_is_base64: bool,

    /// Expected JWT audience
    pub jwt_aud: Option<String>,

    /// Path to role claim in JWT
    pub jwt_role_claim_key: Vec<JsPathExp>,

    /// JWT cache maximum entries
    pub jwt_cache_max_entries: u64,

    // =========================================
    // Logging settings
    // =========================================
    /// Log level
    pub log_level: LogLevel,

    /// Log SQL queries
    pub log_query: bool,

    // =========================================
    // OpenAPI settings
    // =========================================
    /// OpenAPI generation mode
    pub openapi_mode: OpenApiMode,

    /// Include security definitions in OpenAPI
    pub openapi_security_active: bool,

    /// OpenAPI server proxy URI
    pub openapi_server_proxy_uri: Option<String>,

    // =========================================
    // Streaming settings
    // =========================================
    /// Enable streaming responses for large result sets
    pub server_streaming_enabled: bool,

    /// Threshold in bytes for streaming (default: 10MB)
    /// Responses larger than this will be streamed
    pub server_streaming_threshold: u64,

    // =========================================
    // Metrics / Observability settings
    // =========================================
    /// Enable OTLP metrics export
    pub metrics_enabled: bool,

    /// OTLP endpoint URL
    pub metrics_otlp_endpoint: String,

    /// OTLP protocol: "grpc" or "http"
    pub metrics_otlp_protocol: String,

    /// Metrics export interval in seconds
    pub metrics_export_interval_secs: u64,

    /// Service name for OTLP resource attribute
    pub metrics_service_name: String,

    /// Enable OTLP distributed tracing export
    pub tracing_enabled: bool,

    /// Trace sampling ratio (0.0 to 1.0, default 1.0 = sample all)
    pub tracing_sampling_ratio: f64,

    // =========================================
    // App settings (custom key-value pairs)
    // =========================================
    /// Custom application settings
    pub app_settings: HashMap<String, String>,

    // =========================================
    // Runtime-only (not from config file)
    // =========================================
    /// Path to config file (set during loading)
    pub config_file_path: Option<PathBuf>,

    /// Role-specific settings (loaded from database)
    pub role_settings: HashMap<String, HashMap<String, String>>,

    /// Role isolation levels (loaded from database)
    pub role_isolation_level: HashMap<String, IsolationLevel>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            // Database settings
            db_uri: "postgresql://".to_string(),
            db_schemas: vec!["public".to_string()],
            db_anon_role: None,
            db_pool_size: 10,
            db_pool_acquisition_timeout: 10,
            db_pool_max_lifetime: 1800,
            db_pool_max_idletime: 30,
            db_pool_automatic_recovery: true,
            db_prepared_statements: true,
            db_pre_request: None,
            db_root_spec: None,
            db_extra_search_path: vec!["public".to_string()],
            db_hoisted_tx_settings: vec![
                "statement_timeout".to_string(),
                "plan_filter.statement_cost_limit".to_string(),
                "default_transaction_isolation".to_string(),
            ],
            db_max_rows: None,
            db_plan_enabled: false,
            db_tx_rollback_all: false,
            db_tx_allow_override: false,
            db_tx_read_isolation: IsolationLevel::ReadCommitted,
            db_tx_write_isolation: IsolationLevel::ReadCommitted,
            db_aggregates_enabled: false,
            db_config: true,
            db_pre_config: None,
            db_channel: "dbrst".to_string(),
            db_channel_enabled: true,

            // Server settings
            server_host: "!4".to_string(),
            server_port: 3000,
            server_unix_socket: None,
            server_unix_socket_mode: 0o660,
            server_cors_allowed_origins: None,
            server_trace_header: None,
            server_timing_enabled: false,
            server_max_body_size: 10 * 1024 * 1024,

            // Admin server
            admin_server_host: "!4".to_string(),
            admin_server_port: None,

            // JWT settings
            jwt_secret: None,
            jwt_secret_is_base64: false,
            jwt_aud: None,
            jwt_role_claim_key: vec![JsPathExp::Key("role".into())],
            jwt_cache_max_entries: 1000,

            // Logging
            log_level: LogLevel::Error,
            log_query: false,

            // OpenAPI
            openapi_mode: OpenApiMode::FollowPrivileges,
            openapi_security_active: false,
            openapi_server_proxy_uri: None,

            // Streaming
            server_streaming_enabled: true,
            server_streaming_threshold: 10 * 1024 * 1024, // 10MB

            // Metrics
            metrics_enabled: false,
            metrics_otlp_endpoint: "http://localhost:4317".to_string(),
            metrics_otlp_protocol: "grpc".to_string(),
            metrics_export_interval_secs: 60,
            metrics_service_name: "dbrest".to_string(),
            tracing_enabled: false,
            tracing_sampling_ratio: 1.0,

            // App settings
            app_settings: HashMap::new(),

            // Runtime
            config_file_path: None,
            role_settings: HashMap::new(),
            role_isolation_level: HashMap::new(),
        }
    }
}

/// Log level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogLevel {
    /// Critical errors only
    Crit,
    /// Errors (default)
    #[default]
    Error,
    /// Warnings and above
    Warn,
    /// Info and above
    Info,
    /// Debug (all messages)
    Debug,
}

impl LogLevel {
    /// Parse log level from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "crit" | "critical" => Some(LogLevel::Crit),
            "error" | "err" => Some(LogLevel::Error),
            "warn" | "warning" => Some(LogLevel::Warn),
            "info" => Some(LogLevel::Info),
            "debug" => Some(LogLevel::Debug),
            _ => None,
        }
    }

    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Crit => "crit",
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
        }
    }
}

/// OpenAPI generation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OpenApiMode {
    /// Follow database privileges (default)
    #[default]
    FollowPrivileges,
    /// Ignore privileges (show all)
    IgnorePrivileges,
    /// Disable OpenAPI generation
    Disabled,
}

impl OpenApiMode {
    /// Parse OpenAPI mode from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "follow-privileges" => Some(OpenApiMode::FollowPrivileges),
            "ignore-privileges" => Some(OpenApiMode::IgnorePrivileges),
            "disabled" => Some(OpenApiMode::Disabled),
            _ => None,
        }
    }

    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            OpenApiMode::FollowPrivileges => "follow-privileges",
            OpenApiMode::IgnorePrivileges => "ignore-privileges",
            OpenApiMode::Disabled => "disabled",
        }
    }
}

/// Transaction isolation level
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Read committed (default)
    #[default]
    ReadCommitted,
    /// Repeatable read
    RepeatableRead,
    /// Serializable
    Serializable,
}

impl IsolationLevel {
    /// Parse isolation level from string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "read committed" | "read_committed" | "readcommitted" => {
                Some(IsolationLevel::ReadCommitted)
            }
            "repeatable read" | "repeatable_read" | "repeatableread" => {
                Some(IsolationLevel::RepeatableRead)
            }
            "serializable" => Some(IsolationLevel::Serializable),
            _ => None,
        }
    }

    /// Get SQL representation
    pub fn as_sql(&self) -> &'static str {
        match self {
            IsolationLevel::ReadCommitted => "READ COMMITTED",
            IsolationLevel::RepeatableRead => "REPEATABLE READ",
            IsolationLevel::Serializable => "SERIALIZABLE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = AppConfig::default();
        assert_eq!(config.db_schemas, vec!["public"]);
        assert_eq!(config.server_port, 3000);
        assert_eq!(config.db_pool_size, 10);
        assert_eq!(config.db_channel, "dbrst");
        assert!(config.db_channel_enabled);
        assert_eq!(config.server_max_body_size, 10 * 1024 * 1024);
    }

    #[test]
    fn test_log_level_parse() {
        assert_eq!(LogLevel::parse("error"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("ERROR"), Some(LogLevel::Error));
        assert_eq!(LogLevel::parse("debug"), Some(LogLevel::Debug));
        assert_eq!(LogLevel::parse("invalid"), None);
    }

    #[test]
    fn test_openapi_mode_parse() {
        assert_eq!(
            OpenApiMode::parse("follow-privileges"),
            Some(OpenApiMode::FollowPrivileges)
        );
        assert_eq!(OpenApiMode::parse("disabled"), Some(OpenApiMode::Disabled));
        assert_eq!(OpenApiMode::parse("invalid"), None);
    }

    #[test]
    fn test_isolation_level_parse() {
        assert_eq!(
            IsolationLevel::parse("read committed"),
            Some(IsolationLevel::ReadCommitted)
        );
        assert_eq!(
            IsolationLevel::parse("serializable"),
            Some(IsolationLevel::Serializable)
        );
        assert_eq!(IsolationLevel::parse("invalid"), None);
    }

    #[test]
    fn test_isolation_level_sql() {
        assert_eq!(IsolationLevel::ReadCommitted.as_sql(), "READ COMMITTED");
        assert_eq!(IsolationLevel::Serializable.as_sql(), "SERIALIZABLE");
    }
}
