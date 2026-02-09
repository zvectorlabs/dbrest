//! Configuration file and environment parser
//!
//! Handles loading configuration from files and environment variables.

use std::collections::HashMap;
use std::env;
use std::path::Path;

use base64::Engine;

use crate::types::QualifiedIdentifier;

use super::error::ConfigError;
use super::jwt::parse_js_path;
use super::types::{AppConfig, IsolationLevel, LogLevel, OpenApiMode};

/// Load configuration from file and environment
///
/// Order of precedence (highest to lowest):
/// 1. Environment variables (PGRST_*)
/// 2. Config file values
/// 3. Default values
///
/// # Arguments
///
/// * `file_path` - Optional path to configuration file
/// * `db_settings` - Optional settings loaded from database (deferred to Phase 3)
///
/// # Examples
///
/// ```ignore
/// let config = load_config(Some(Path::new("config.toml")), HashMap::new()).await?;
/// ```
pub async fn load_config(
    file_path: Option<&Path>,
    db_settings: HashMap<String, String>,
) -> Result<AppConfig, ConfigError> {
    let mut config = AppConfig::default();

    // 1. Load from file if provided
    if let Some(path) = file_path {
        let file_contents = tokio::fs::read_to_string(path).await?;
        parse_config_file(&file_contents, &mut config)?;
        config.config_file_path = Some(path.to_path_buf());
    }

    // 2. Override with environment variables
    apply_env_overrides(&mut config)?;

    // 3. Override with database settings (if provided)
    for (key, value) in db_settings {
        let _ = apply_config_value(&mut config, &key, &value);
    }

    // 4. Post-process (decode JWT secret, etc.)
    post_process_config(&mut config)?;

    // 5. Validate
    validate_config(&config)?;

    Ok(config)
}

/// Parse configuration file contents
///
/// Supports a simple key=value format with comments.
fn parse_config_file(contents: &str, config: &mut AppConfig) -> Result<(), ConfigError> {
    for (line_num, line) in contents.lines().enumerate() {
        let line = line.trim();

        // Skip empty lines and comments
        if line.is_empty() || line.starts_with('#') || line.starts_with("--") {
            continue;
        }

        // Parse key = value
        if let Some((key, value)) = parse_key_value(line) {
            apply_config_value(config, &key, &value).map_err(|e| match e {
                ConfigError::InvalidValue { .. } => ConfigError::Parse {
                    line: Some(line_num + 1),
                    message: e.to_string(),
                },
                _ => e,
            })?;
        }
    }

    Ok(())
}

/// Parse a key=value line
fn parse_key_value(line: &str) -> Option<(String, String)> {
    let mut parts = line.splitn(2, '=');
    let key = parts.next()?.trim().to_string();
    let value = parts.next()?.trim();

    // Remove surrounding quotes
    let value = value
        .trim_start_matches('"')
        .trim_end_matches('"')
        .trim_start_matches('\'')
        .trim_end_matches('\'')
        .to_string();

    if key.is_empty() {
        return None;
    }

    Some((key, value))
}

/// Apply environment variable overrides
///
/// Environment variables with the PGRST_ prefix override config file values.
/// The prefix is stripped and underscores are converted to hyphens.
fn apply_env_overrides(config: &mut AppConfig) -> Result<(), ConfigError> {
    for (key, value) in env::vars() {
        if let Some(config_key) = key.strip_prefix("PGRST_") {
            let config_key = config_key.to_lowercase().replace('_', "-");
            // Ignore errors for unknown keys from environment
            let _ = apply_config_value(config, &config_key, &value);
        }
    }
    Ok(())
}

/// Apply a single configuration value
pub fn apply_config_value(
    config: &mut AppConfig,
    key: &str,
    value: &str,
) -> Result<(), ConfigError> {
    match key {
        // Database settings
        "db-uri" => config.db_uri = value.to_string(),
        "db-schemas" | "db-schema" => {
            config.db_schemas = parse_comma_list(value);
        }
        "db-anon-role" => {
            config.db_anon_role = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        "db-pool" => {
            config.db_pool_size = parse_int(key, value)?;
        }
        "db-pool-acquisition-timeout" => {
            config.db_pool_acquisition_timeout = parse_int(key, value)?;
        }
        "db-pool-max-lifetime" => {
            config.db_pool_max_lifetime = parse_int(key, value)?;
        }
        "db-pool-max-idletime" | "db-pool-timeout" => {
            config.db_pool_max_idletime = parse_int(key, value)?;
        }
        "db-pool-automatic-recovery" => {
            config.db_pool_automatic_recovery = parse_bool(value)?;
        }
        "db-prepared-statements" => {
            config.db_prepared_statements = parse_bool(value)?;
        }
        "db-pre-request" | "pre-request" => {
            config.db_pre_request = if value.is_empty() {
                None
            } else {
                Some(parse_qualified_identifier(key, value)?)
            };
        }
        "db-root-spec" | "root-spec" => {
            config.db_root_spec = if value.is_empty() {
                None
            } else {
                Some(parse_qualified_identifier(key, value)?)
            };
        }
        "db-extra-search-path" => {
            config.db_extra_search_path = parse_comma_list(value);
        }
        "db-hoisted-tx-settings" => {
            config.db_hoisted_tx_settings = parse_comma_list(value);
        }
        "db-max-rows" | "max-rows" => {
            config.db_max_rows = if value.is_empty() {
                None
            } else {
                Some(parse_int(key, value)?)
            };
        }
        "db-plan-enabled" => {
            config.db_plan_enabled = parse_bool(value)?;
        }
        "db-tx-end" => {
            match value {
                "commit" => {
                    config.db_tx_rollback_all = false;
                    config.db_tx_allow_override = false;
                }
                "commit-allow-override" => {
                    config.db_tx_rollback_all = false;
                    config.db_tx_allow_override = true;
                }
                "rollback" => {
                    config.db_tx_rollback_all = true;
                    config.db_tx_allow_override = false;
                }
                "rollback-allow-override" => {
                    config.db_tx_rollback_all = true;
                    config.db_tx_allow_override = true;
                }
                _ => {
                    return Err(ConfigError::InvalidValue {
                        key: key.to_string(),
                        value: value.to_string(),
                        expected: Some(
                            "commit, commit-allow-override, rollback, rollback-allow-override"
                                .to_string(),
                        ),
                    });
                }
            }
        }
        "db-tx-read-isolation" => {
            config.db_tx_read_isolation = parse_isolation_level(value)?;
        }
        "db-tx-write-isolation" => {
            config.db_tx_write_isolation = parse_isolation_level(value)?;
        }
        "db-aggregates-enabled" => {
            config.db_aggregates_enabled = parse_bool(value)?;
        }
        "db-config" => {
            config.db_config = parse_bool(value)?;
        }
        "db-pre-config" => {
            config.db_pre_config = if value.is_empty() {
                None
            } else {
                Some(parse_qualified_identifier(key, value)?)
            };
        }
        "db-channel" => {
            config.db_channel = value.to_string();
        }
        "db-channel-enabled" => {
            config.db_channel_enabled = parse_bool(value)?;
        }

        // Server settings
        "server-host" => config.server_host = value.to_string(),
        "server-port" => {
            config.server_port = parse_int(key, value)?;
        }
        "server-unix-socket" => {
            config.server_unix_socket = if value.is_empty() {
                None
            } else {
                Some(value.into())
            };
        }
        "server-unix-socket-mode" => {
            config.server_unix_socket_mode =
                u32::from_str_radix(value, 8).map_err(|_| ConfigError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                    expected: Some("octal number (e.g., 660)".to_string()),
                })?;
        }
        "server-cors-allowed-origins" => {
            config.server_cors_allowed_origins = if value.is_empty() {
                None
            } else {
                Some(parse_comma_list(value))
            };
        }
        "server-trace-header" => {
            config.server_trace_header = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        "server-timing-enabled" => {
            config.server_timing_enabled = parse_bool(value)?;
        }

        // Admin server
        "admin-server-host" => config.admin_server_host = value.to_string(),
        "admin-server-port" => {
            config.admin_server_port = if value.is_empty() {
                None
            } else {
                Some(parse_int(key, value)?)
            };
        }

        // JWT settings
        "jwt-secret" => {
            config.jwt_secret = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        "jwt-secret-is-base64" | "secret-is-base64" => {
            config.jwt_secret_is_base64 = parse_bool(value)?;
        }
        "jwt-aud" => {
            config.jwt_aud = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }
        "jwt-role-claim-key" | "role-claim-key" => {
            config.jwt_role_claim_key = parse_js_path(value)?;
        }
        "jwt-cache-max-entries" => {
            config.jwt_cache_max_entries = parse_int(key, value)?;
        }

        // Logging
        "log-level" => {
            config.log_level = LogLevel::parse(value).ok_or_else(|| ConfigError::InvalidValue {
                key: key.to_string(),
                value: value.to_string(),
                expected: Some("crit, error, warn, info, debug".to_string()),
            })?;
        }
        "log-query" => {
            config.log_query = parse_bool(value)?;
        }

        // OpenAPI
        "openapi-mode" => {
            config.openapi_mode =
                OpenApiMode::parse(value).ok_or_else(|| ConfigError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                    expected: Some("follow-privileges, ignore-privileges, disabled".to_string()),
                })?;
        }
        "openapi-security-active" => {
            config.openapi_security_active = parse_bool(value)?;
        }
        "openapi-server-proxy-uri" => {
            config.openapi_server_proxy_uri = if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            };
        }

        // Streaming
        "server-streaming-enabled" => {
            config.server_streaming_enabled = parse_bool(value)?;
        }
        "server-streaming-threshold" => {
            config.server_streaming_threshold = value
                .parse::<u64>()
                .map_err(|_| ConfigError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                    expected: Some("positive integer (bytes)".to_string()),
                })?;
        }

        // App settings (app.settings.*)
        key if key.starts_with("app.settings.") => {
            if let Some(setting_key) = key.strip_prefix("app.settings.") {
                config
                    .app_settings
                    .insert(setting_key.to_string(), value.to_string());
            }
        }

        // Unknown keys are logged but not errors (lenient parsing)
        _ => {
            tracing::debug!("Unknown config key: {}", key);
        }
    }

    Ok(())
}

/// Parse a boolean value (flexible)
pub fn parse_bool(value: &str) -> Result<bool, ConfigError> {
    match value.to_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Ok(true),
        "false" | "no" | "off" | "0" => Ok(false),
        _ => Err(ConfigError::InvalidBool(value.to_string())),
    }
}

/// Parse an integer value
fn parse_int<T: std::str::FromStr>(key: &str, value: &str) -> Result<T, ConfigError>
where
    T::Err: std::fmt::Display,
{
    value.parse().map_err(|e: T::Err| ConfigError::InvalidValue {
        key: key.to_string(),
        value: value.to_string(),
        expected: Some(format!("integer ({})", e)),
    })
}

/// Parse an isolation level
fn parse_isolation_level(value: &str) -> Result<IsolationLevel, ConfigError> {
    match value.to_lowercase().as_str() {
        "read-committed" | "readcommitted" => Ok(IsolationLevel::ReadCommitted),
        "repeatable-read" | "repeatableread" => Ok(IsolationLevel::RepeatableRead),
        "serializable" => Ok(IsolationLevel::Serializable),
        _ => Err(ConfigError::InvalidValue {
            key: "isolation-level".to_string(),
            value: value.to_string(),
            expected: Some("read-committed, repeatable-read, serializable".to_string()),
        }),
    }
}

/// Parse a comma-separated list
fn parse_comma_list(value: &str) -> Vec<String> {
    if value.is_empty() {
        vec![]
    } else {
        value.split(',').map(|s| s.trim().to_string()).collect()
    }
}

/// Parse a qualified identifier (schema.name)
fn parse_qualified_identifier(key: &str, value: &str) -> Result<QualifiedIdentifier, ConfigError> {
    QualifiedIdentifier::parse(value).map_err(|_| ConfigError::InvalidValue {
        key: key.to_string(),
        value: value.to_string(),
        expected: Some("qualified identifier (schema.name or name)".to_string()),
    })
}

/// Post-process configuration after loading
fn post_process_config(config: &mut AppConfig) -> Result<(), ConfigError> {
    // Decode base64 JWT secret if needed
    if config.jwt_secret_is_base64
        && let Some(ref secret) = config.jwt_secret
    {
        let decoded = base64::engine::general_purpose::STANDARD.decode(secret)?;
        config.jwt_secret = Some(String::from_utf8(decoded)?);
    }

    // Add fallback_application_name to db_uri if not present
    if !config.db_uri.contains("application_name") {
        let separator = if config.db_uri.contains('?') {
            "&"
        } else {
            "?"
        };
        config.db_uri = format!(
            "{}{}fallback_application_name=pgrest",
            config.db_uri, separator
        );
    }

    Ok(())
}

/// Validate configuration
pub fn validate_config(config: &AppConfig) -> Result<(), ConfigError> {
    // db_schemas must not be empty
    if config.db_schemas.is_empty() {
        return Err(ConfigError::Validation(
            "db-schemas cannot be empty".to_string(),
        ));
    }

    // db_schemas cannot include pg_catalog or information_schema
    for schema in &config.db_schemas {
        if schema == "pg_catalog" || schema == "information_schema" {
            return Err(ConfigError::Validation(format!(
                "db-schemas cannot include system schema: '{}'",
                schema
            )));
        }
    }

    // admin_server_port must differ from server_port
    if let Some(admin_port) = config.admin_server_port
        && admin_port == config.server_port
    {
        return Err(ConfigError::Validation(
            "admin-server-port cannot be the same as server-port".to_string(),
        ));
    }

    // JWT secret must be at least 32 characters (if present and not JWKS)
    if let Some(ref secret) = config.jwt_secret {
        // Check if it might be JWKS (starts with { )
        let is_jwks = secret.trim().starts_with('{');
        if !is_jwks && secret.len() < 32 {
            return Err(ConfigError::Validation(
                "jwt-secret must be at least 32 characters long".to_string(),
            ));
        }
    }

    // Pool size must be positive
    if config.db_pool_size == 0 {
        return Err(ConfigError::Validation(
            "db-pool must be greater than 0".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_key_value() {
        assert_eq!(
            parse_key_value("key=value"),
            Some(("key".to_string(), "value".to_string()))
        );
        assert_eq!(
            parse_key_value("key = value"),
            Some(("key".to_string(), "value".to_string()))
        );
        assert_eq!(
            parse_key_value("key=\"value\""),
            Some(("key".to_string(), "value".to_string()))
        );
        assert_eq!(
            parse_key_value("key='value'"),
            Some(("key".to_string(), "value".to_string()))
        );
        assert_eq!(parse_key_value("no_equals"), None);
        assert_eq!(parse_key_value("=value"), None);
    }

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("true").unwrap());
        assert!(parse_bool("TRUE").unwrap());
        assert!(parse_bool("yes").unwrap());
        assert!(parse_bool("on").unwrap());
        assert!(parse_bool("1").unwrap());

        assert!(!parse_bool("false").unwrap());
        assert!(!parse_bool("FALSE").unwrap());
        assert!(!parse_bool("no").unwrap());
        assert!(!parse_bool("off").unwrap());
        assert!(!parse_bool("0").unwrap());

        assert!(parse_bool("maybe").is_err());
    }

    #[test]
    fn test_parse_comma_list() {
        assert_eq!(parse_comma_list("a,b,c"), vec!["a", "b", "c"]);
        assert_eq!(parse_comma_list("a, b, c"), vec!["a", "b", "c"]);
        assert_eq!(parse_comma_list("single"), vec!["single"]);
        assert!(parse_comma_list("").is_empty());
    }

    #[test]
    fn test_apply_config_value() {
        let mut config = AppConfig::default();

        apply_config_value(&mut config, "server-port", "8080").unwrap();
        assert_eq!(config.server_port, 8080);

        apply_config_value(&mut config, "db-schemas", "api,public").unwrap();
        assert_eq!(config.db_schemas, vec!["api", "public"]);

        apply_config_value(&mut config, "db-pool", "20").unwrap();
        assert_eq!(config.db_pool_size, 20);

        apply_config_value(&mut config, "log-level", "debug").unwrap();
        assert_eq!(config.log_level, LogLevel::Debug);
    }

    #[test]
    fn test_apply_config_tx_end() {
        let mut config = AppConfig::default();

        apply_config_value(&mut config, "db-tx-end", "commit").unwrap();
        assert!(!config.db_tx_rollback_all);
        assert!(!config.db_tx_allow_override);

        apply_config_value(&mut config, "db-tx-end", "rollback-allow-override").unwrap();
        assert!(config.db_tx_rollback_all);
        assert!(config.db_tx_allow_override);
    }

    #[test]
    fn test_apply_config_app_settings() {
        let mut config = AppConfig::default();

        apply_config_value(&mut config, "app.settings.my-key", "my-value").unwrap();
        assert_eq!(config.app_settings.get("my-key"), Some(&"my-value".to_string()));
    }

    #[test]
    fn test_validate_config_empty_schemas() {
        let mut config = AppConfig::default();
        config.db_schemas = vec![];
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_system_schema() {
        let mut config = AppConfig::default();
        config.db_schemas = vec!["pg_catalog".to_string()];
        assert!(validate_config(&config).is_err());

        config.db_schemas = vec!["information_schema".to_string()];
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_same_ports() {
        let mut config = AppConfig::default();
        config.server_port = 3000;
        config.admin_server_port = Some(3000);
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn test_validate_config_short_jwt_secret() {
        let mut config = AppConfig::default();
        config.jwt_secret = Some("short".to_string());
        assert!(validate_config(&config).is_err());

        config.jwt_secret = Some("a".repeat(32));
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_validate_config_jwks_bypass() {
        let mut config = AppConfig::default();
        // JWKS secrets start with { and bypass length check
        config.jwt_secret = Some("{\"keys\":[]}".to_string());
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_parse_config_file() {
        let contents = r#"
# Comment line
server-port = 8080
db-schemas = api, public
log-level = debug

-- Another comment style
db-pool = 25
"#;

        let mut config = AppConfig::default();
        parse_config_file(contents, &mut config).unwrap();

        assert_eq!(config.server_port, 8080);
        assert_eq!(config.db_schemas, vec!["api", "public"]);
        assert_eq!(config.log_level, LogLevel::Debug);
        assert_eq!(config.db_pool_size, 25);
    }

    #[tokio::test]
    async fn test_load_config_defaults() {
        let config = load_config(None, HashMap::new()).await.unwrap();
        assert_eq!(config.server_port, 3000);
        assert_eq!(config.db_schemas, vec!["public"]);
    }
}
