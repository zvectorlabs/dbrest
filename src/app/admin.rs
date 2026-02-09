//! Admin server
//!
//! Provides health check, readiness, metrics, and config endpoints on a
//! separate port for operational monitoring.
//!
//! # Endpoints
//!
//! | Path        | Method | Description                          |
//! |-------------|--------|--------------------------------------|
//! | `/live`     | GET    | Liveness probe (always 200)          |
//! | `/ready`    | GET    | Readiness probe (200 if schema ready)|
//! | `/metrics`  | GET    | Basic metrics (JSON)                 |
//! | `/config`   | GET    | Current config (redacted secrets)    |

use std::sync::atomic::Ordering;

use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

use super::state::AppState;

/// Create the admin router.
pub fn create_admin_router(state: AppState) -> Router {
    Router::new()
        .route("/live", get(liveness))
        .route("/ready", get(readiness))
        .route("/metrics", get(metrics))
        .route("/config", get(config_handler))
        .with_state(state)
}

/// Liveness probe — always returns 200 OK.
async fn liveness() -> Response {
    StatusCode::OK.into_response()
}

/// Readiness probe — returns 200 if schema cache is loaded, 503 otherwise.
async fn readiness(State(state): State<AppState>) -> Response {
    let cache_guard = state.schema_cache.load();
    if cache_guard.is_some() {
        StatusCode::OK.into_response()
    } else {
        StatusCode::SERVICE_UNAVAILABLE.into_response()
    }
}

/// Current configuration (JSON, secrets redacted).
async fn config_handler(State(state): State<AppState>) -> Response {
    let config = state.config.load();
    let body = redacted_config(&config);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Body::from(serde_json::to_string_pretty(&body).unwrap()))
        .unwrap()
}

/// Serialize config to JSON with secrets redacted.
pub fn redacted_config(config: &crate::config::AppConfig) -> serde_json::Value {
    serde_json::json!({
        "db_uri": "***",
        "db_schemas": config.db_schemas,
        "db_anon_role": config.db_anon_role,
        "db_pool_size": config.db_pool_size,
        "db_channel": config.db_channel,
        "db_channel_enabled": config.db_channel_enabled,
        "db_max_rows": config.db_max_rows,
        "server_host": config.server_host,
        "server_port": config.server_port,
        "server_timing_enabled": config.server_timing_enabled,
        "jwt_secret": if config.jwt_secret.is_some() { "***" } else { "" },
        "jwt_secret_is_base64": config.jwt_secret_is_base64,
        "log_level": config.log_level.as_str(),
        "openapi_mode": config.openapi_mode.as_str(),
    })
}

/// Basic metrics endpoint (JSON).
async fn metrics(State(state): State<AppState>) -> Response {
    let m = &state.metrics;
    let body = serde_json::json!({
        "requests_total": m.requests_total.load(Ordering::Relaxed),
        "requests_success": m.requests_success.load(Ordering::Relaxed),
        "requests_error": m.requests_error.load(Ordering::Relaxed),
        "db_queries_total": m.db_queries_total.load(Ordering::Relaxed),
        "schema_cache_reloads": m.schema_cache_reloads.load(Ordering::Relaxed),
        "jwt_cache_hits": m.jwt_cache_hits.load(Ordering::Relaxed),
        "jwt_cache_misses": m.jwt_cache_misses.load(Ordering::Relaxed),
        "jwt_cache_entries": state.jwt_cache.entry_count(),
        "pg_version": format!("{}.{}.{}", state.pg_version.major, state.pg_version.minor, state.pg_version.patch),
    });

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
        .body(Body::from(serde_json::to_string_pretty(&body).unwrap()))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AppConfig;

    #[test]
    fn redacted_config_hides_db_uri() {
        let config = AppConfig::default();
        let json = redacted_config(&config);
        assert_eq!(json["db_uri"], "***");
    }

    #[test]
    fn redacted_config_hides_jwt_secret_when_set() {
        let mut config = AppConfig::default();
        config.jwt_secret = Some("super-secret-key".to_string());
        let json = redacted_config(&config);
        assert_eq!(json["jwt_secret"], "***");
    }

    #[test]
    fn redacted_config_empty_jwt_when_none() {
        let config = AppConfig::default();
        let json = redacted_config(&config);
        assert_eq!(json["jwt_secret"], "");
    }

    #[test]
    fn redacted_config_preserves_schemas() {
        let mut config = AppConfig::default();
        config.db_schemas = vec!["public".to_string(), "api".to_string()];
        let json = redacted_config(&config);
        let schemas = json["db_schemas"].as_array().unwrap();
        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0], "public");
        assert_eq!(schemas[1], "api");
    }

    #[test]
    fn redacted_config_preserves_server_fields() {
        let mut config = AppConfig::default();
        config.server_host = "0.0.0.0".to_string();
        config.server_port = 8080;
        config.server_timing_enabled = true;
        let json = redacted_config(&config);
        assert_eq!(json["server_host"], "0.0.0.0");
        assert_eq!(json["server_port"], 8080);
        assert_eq!(json["server_timing_enabled"], true);
    }

    #[test]
    fn redacted_config_preserves_db_pool_and_channel() {
        let mut config = AppConfig::default();
        config.db_pool_size = 20;
        config.db_channel = "pgrst".to_string();
        config.db_channel_enabled = true;
        config.db_max_rows = Some(1000);
        let json = redacted_config(&config);
        assert_eq!(json["db_pool_size"], 20);
        assert_eq!(json["db_channel"], "pgrst");
        assert_eq!(json["db_channel_enabled"], true);
        assert_eq!(json["db_max_rows"], 1000);
    }
}
