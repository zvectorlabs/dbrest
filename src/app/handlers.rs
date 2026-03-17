//! HTTP request handlers
//!
//! Each handler follows the same pipeline:
//!
//! 1. Extract HTTP request parts (method, path, query, headers, body).
//! 2. Parse `Preferences` from the `Prefer` header.
//! 3. Build an `ApiRequest` via `api_request::from_request`.
//! 4. Generate an `ActionPlan` via `plan::action_plan`.
//! 5. Build SQL via `query::main_query`.
//! 6. Execute the SQL within a transaction.
//! 7. Build the HTTP response from the result set.

use std::collections::HashSet;
use std::sync::Arc;

use axum::{
    Extension,
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;

use crate::api_request;
use crate::api_request::preferences::{PreferRepresentation, Preferences};
use crate::auth::types::AuthResult;
use crate::backend::StatementResult;
use crate::error::Error;
use crate::plan::{self, ActionPlan, CrudPlan, DbActionPlan};
use crate::query::{self};
use crate::schema_cache::SchemaCache;
use crate::types::media::MediaType;

use super::state::AppState;
use super::streaming::{should_stream, stream_json_response};

// ==========================================================================
// Shared helpers
// ==========================================================================

/// Finalize a response builder into a `Response`.
///
/// If the builder fails (e.g. due to invalid headers from GUC overrides),
/// returns a plain 500 Internal Server Error instead of panicking.
fn finalize_response(builder: http::response::Builder, body: Body) -> Response {
    builder.body(body).unwrap_or_else(|_| {
        Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("Internal Server Error"))
            .expect("static 500 response must be valid")
    })
}

/// Parse the `Prefer` header from the request headers using `from_headers`.
fn parse_prefer(headers: &HeaderMap) -> Preferences {
    let flat: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|val| (k.as_str().to_string(), val.to_string()))
        })
        .collect();
    Preferences::from_headers(
        false,           // allow_tx_override
        &HashSet::new(), // valid_timezones (empty for now)
        &flat,
    )
}

/// Flatten axum `HeaderMap` into `Vec<(String, String)>` for `from_request`.
fn flatten_headers(headers: &HeaderMap) -> Vec<(String, String)> {
    headers
        .iter()
        .filter_map(|(k, v)| {
            v.to_str()
                .ok()
                .map(|val| (k.as_str().to_string(), val.to_string()))
        })
        .collect()
}

/// Execute a `MainQuery` against the database backend inside a transaction.
///
/// Runs tx_vars, pre_req, and main query in order within a single
/// transaction, returning the CTE result set from the main query.
async fn execute_main_query(
    state: &AppState,
    mq: &query::MainQuery,
) -> Result<StatementResult, Error> {
    state
        .metrics
        .db_queries_total
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    state
        .db
        .exec_in_transaction(
            mq.tx_vars.as_ref(),
            mq.pre_req.as_ref(),
            mq.main.as_ref(),
        )
        .await
}

// Error mapping has been moved to the backend module.
// See crate::backend::postgres::executor::map_sqlx_error

/// Apply GUC overrides from `response.status` and `response.headers` to a response builder.
///
/// PostgREST format:
/// - `response.status`: Text containing status code (e.g., "202")
/// - `response.headers`: JSON array of objects, each with a single key-value pair
///   Example: `[{"X-Custom": "value"}, {"X-Another": "value2"}]`
///
/// If `response_status` is set, it overrides the HTTP status code.
/// If `response_headers` is set (as a JSON array), it adds those headers to the response.
///
/// Returns the builder, or an error response if GUC values are invalid.
#[allow(clippy::result_large_err)]
fn apply_guc_overrides(
    mut builder: http::response::Builder,
    result: &StatementResult,
) -> Result<http::response::Builder, Response> {
    // Apply response.status GUC override
    // PostgREST stores this as i32, but it should be parsed as Text then converted
    // For now, we'll use the i32 directly since that's what we get from the DB
    if let Some(status_code) = result.response_status {
        if let Ok(status) = http::StatusCode::from_u16(status_code as u16) {
            builder = builder.status(status);
        } else {
            // Invalid status code - return error response (DBRST112)
            return Err(Error::InvalidConfig {
                message: format!(
                    "response.status GUC must be a valid status code, got: {}",
                    status_code
                ),
            }
            .into_response());
        }
    }

    // Apply response.headers GUC override
    // PostgREST expects: [{"Header-Name": "value"}, {"Another": "value2"}]
    if let Some(ref headers_json) = result.response_headers {
        if let Some(headers_array) = headers_json.as_array() {
            for header_obj in headers_array {
                if let Some(obj) = header_obj.as_object() {
                    // Each object should have exactly one key-value pair
                    if obj.len() == 1 {
                        for (key, value) in obj {
                            if let Some(header_value) = value.as_str()
                                && let Ok(hv) = http::HeaderValue::from_str(header_value)
                            {
                                // Only add header if not already present (PostgREST behavior)
                                if builder
                                    .headers_ref()
                                    .map(|h| !h.contains_key(key.as_str()))
                                    .unwrap_or(true)
                                {
                                    builder = builder.header(key.as_str(), hv);
                                }
                            }
                        }
                    }
                }
            }
        } else {
            // If it's not an array, return error (PostgREST returns GucHeadersError DBRST111)
            return Err(Error::InvalidConfig {
                message: "response.headers GUC must be a JSON array composed of objects with a single key and a string value".to_string(),
            }
            .into_response());
        }
    }

    Ok(builder)
}


// ==========================================================================
// Core request processing pipeline
// ==========================================================================

/// Process a single API request through the full pipeline.
///
/// This is the shared core used by all resource handlers (read, mutate, rpc).
async fn process_request(
    state: &AppState,
    auth: &AuthResult,
    method: &str,
    path: &str,
    query_str: &str,
    headers: &HeaderMap,
    body: Bytes,
) -> Result<(StatementResult, Preferences, MediaType), Error> {
    let config = state.config();
    let cache_guard = state.schema_cache_guard();
    let cache_ref: &Option<SchemaCache> = &cache_guard;
    let cache = cache_ref.as_ref().ok_or(Error::SchemaCacheNotReady)?;

    let prefs = parse_prefer(headers);
    let flat_headers = flatten_headers(headers);

    // 1. Parse the API request
    let api_req = api_request::from_request(
        &config,
        &prefs,
        method,
        path,
        query_str,
        &flat_headers,
        body,
    )?;

    // 2. Build the action plan
    let action_plan = plan::action_plan(&config, &api_req, cache)?;

    // 3. Build the full SQL query bundle
    let role_name = auth.role.as_str();
    let headers_json = serde_json::to_string(
        &flat_headers
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<Vec<_>>(),
    )
    .ok();

    let claims_json = if auth.is_anonymous() {
        None
    } else {
        Some(auth.claims_json())
    };

    let mq = query::main_query(
        &action_plan,
        &config,
        state.dialect.as_ref(),
        method,
        path,
        Some(role_name),
        headers_json.as_deref(),
        None, // cookies
        claims_json.as_deref(),
    );

    // 4. Extract media type from action plan for response Content-Type
    let media_type = match &action_plan {
        ActionPlan::Db(DbActionPlan::DbCrud { plan, .. }) => match plan {
            CrudPlan::WrappedReadPlan { media, .. }
            | CrudPlan::MutateReadPlan { media, .. }
            | CrudPlan::CallReadPlan { media, .. } => media.clone(),
        },
        _ => MediaType::ApplicationJson,
    };

    // 5. Execute
    let result = execute_main_query(state, &mq).await?;

    Ok((result, prefs, media_type))
}

// ==========================================================================
// Read handler (GET / HEAD)
// ==========================================================================

/// Handle `GET /:resource` and `HEAD /:resource`.
pub async fn read_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    method: Method,
    headers: HeaderMap,
    Path(resource): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let path = format!("/{}", resource);
    let query_str = raw_query.as_deref().unwrap_or("");
    let is_head = method == Method::HEAD;

    match process_request(
        &state,
        &auth,
        method.as_str(),
        &path,
        query_str,
        &headers,
        Bytes::new(),
    )
    .await
    {
        Ok((result, prefs, media)) => {
            let config = state.config();
            build_read_response(&result, &prefs, is_head, &config, &media)
        }
        Err(e) => e.into_response(),
    }
}

// ==========================================================================
// Create handler (POST)
// ==========================================================================

/// Handle `POST /:resource`.
pub async fn create_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    headers: HeaderMap,
    Path(resource): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: Bytes,
) -> Response {
    let path = format!("/{}", resource);
    let query_str = raw_query.as_deref().unwrap_or("");

    match process_request(&state, &auth, "POST", &path, query_str, &headers, body).await {
        Ok((result, prefs, media)) => {
            let config = state.config();
            build_mutate_response(&result, &prefs, "POST", &path, &config, &media)
        }
        Err(e) => e.into_response(),
    }
}

// ==========================================================================
// Update handler (PATCH)
// ==========================================================================

/// Handle `PATCH /:resource`.
pub async fn update_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    headers: HeaderMap,
    Path(resource): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: Bytes,
) -> Response {
    let path = format!("/{}", resource);
    let query_str = raw_query.as_deref().unwrap_or("");

    match process_request(&state, &auth, "PATCH", &path, query_str, &headers, body).await {
        Ok((result, prefs, media)) => {
            let config = state.config();
            build_mutate_response(&result, &prefs, "PATCH", &path, &config, &media)
        }
        Err(e) => e.into_response(),
    }
}

// ==========================================================================
// Delete handler (DELETE)
// ==========================================================================

/// Handle `DELETE /:resource`.
pub async fn delete_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    headers: HeaderMap,
    Path(resource): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let path = format!("/{}", resource);
    let query_str = raw_query.as_deref().unwrap_or("");

    match process_request(
        &state,
        &auth,
        "DELETE",
        &path,
        query_str,
        &headers,
        Bytes::new(),
    )
    .await
    {
        Ok((result, prefs, media)) => {
            let config = state.config();
            build_mutate_response(&result, &prefs, "DELETE", &path, &config, &media)
        }
        Err(e) => e.into_response(),
    }
}

// ==========================================================================
// Upsert handler (PUT)
// ==========================================================================

/// Handle `PUT /:resource`.
pub async fn upsert_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    headers: HeaderMap,
    Path(resource): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: Bytes,
) -> Response {
    let path = format!("/{}", resource);
    let query_str = raw_query.as_deref().unwrap_or("");

    match process_request(&state, &auth, "PUT", &path, query_str, &headers, body).await {
        Ok((result, prefs, media)) => {
            let config = state.config();
            build_mutate_response(&result, &prefs, "PUT", &path, &config, &media)
        }
        Err(e) => e.into_response(),
    }
}

// ==========================================================================
// RPC handlers
// ==========================================================================

/// Handle `GET /rpc/:function`.
pub async fn rpc_get_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    headers: HeaderMap,
    Path(function): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
) -> Response {
    let path = format!("/rpc/{}", function);
    let query_str = raw_query.as_deref().unwrap_or("");

    match process_request(
        &state,
        &auth,
        "GET",
        &path,
        query_str,
        &headers,
        Bytes::new(),
    )
    .await
    {
        Ok((result, prefs, media)) => {
            let config = state.config();
            build_rpc_response(&result, &prefs, &config, &media)
        }
        Err(e) => e.into_response(),
    }
}

/// Handle `POST /rpc/:function`.
pub async fn rpc_post_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    headers: HeaderMap,
    Path(function): Path<String>,
    axum::extract::RawQuery(raw_query): axum::extract::RawQuery,
    body: Bytes,
) -> Response {
    let path = format!("/rpc/{}", function);
    let query_str = raw_query.as_deref().unwrap_or("");

    match process_request(&state, &auth, "POST", &path, query_str, &headers, body).await {
        Ok((result, prefs, media)) => {
            let config = state.config();
            build_rpc_response(&result, &prefs, &config, &media)
        }
        Err(e) => e.into_response(),
    }
}

// ==========================================================================
// Root / schema handler
// ==========================================================================

/// Handle `GET /openapi.json` — returns OpenAPI 3.0 spec (no Accept header required).
/// Use this URL when tools or agents need a single spec URL (e.g. Swagger UI, codegen).
pub async fn openapi_spec_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
) -> Response {
    let cache_guard = state.schema_cache_guard();
    generate_openapi_spec(&state, &auth, &cache_guard).await
}

/// Handle `GET /` — returns OpenAPI spec or JSON listing of available tables.
///
/// If `Accept: application/openapi+json` header is present, returns full OpenAPI 3.0 spec.
/// Otherwise, returns a simple JSON listing of table definitions.
pub async fn schema_root_handler(
    State(state): State<AppState>,
    Extension(auth): Extension<AuthResult>,
    headers: HeaderMap,
) -> Response {
    let config = state.config();
    let cache_guard = state.schema_cache_guard();

    // Check if OpenAPI is requested
    if let Some(accept) = headers.get(http::header::ACCEPT)
        && let Ok(accept_str) = accept.to_str()
        && accept_str.contains("application/openapi+json")
    {
        return generate_openapi_spec(&state, &auth, &cache_guard).await;
    }

    // Default: return table definitions
    match cache_guard.as_ref() {
        Some(cache) => {
            let tables: Vec<serde_json::Value> = config
                .db_schemas
                .iter()
                .flat_map(|schema| {
                    cache.tables_in_schema(schema).map(|t| {
                        serde_json::json!({
                            "schema": t.schema,
                            "name": t.name,
                            "description": t.description,
                            "insertable": t.insertable,
                        })
                    })
                })
                .collect();

            let body = serde_json::json!({ "definitions": tables });

            finalize_response(
                Response::builder()
                    .status(StatusCode::OK)
                    .header(header::CONTENT_TYPE, "application/json; charset=utf-8"),
                Body::from(serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string())),
            )
        }
        None => Error::SchemaCacheNotReady.into_response(),
    }
}

/// Generate OpenAPI 3.0 specification
async fn generate_openapi_spec(
    state: &AppState,
    auth: &AuthResult,
    cache_guard: &arc_swap::Guard<Arc<Option<SchemaCache>>>,
) -> Response {
    use crate::openapi::generator::OpenApiGenerator;

    match cache_guard.as_ref() {
        Some(cache) => {
            let config_guard = state.config();
            let config = config_guard.clone();
            let generator =
                OpenApiGenerator::new(config, Arc::new(cache.clone()), Some(auth.clone()));

            match generator.generate() {
                Ok(spec) => {
                    let body = serde_json::to_string(&spec).unwrap_or_else(|_| "{}".to_string());
                    finalize_response(
                        Response::builder()
                            .status(StatusCode::OK)
                            .header(
                                header::CONTENT_TYPE,
                                "application/openapi+json; charset=utf-8",
                            ),
                        Body::from(body),
                    )
                }
                Err(e) => e.into_response(),
            }
        }
        None => Error::SchemaCacheNotReady.into_response(),
    }
}

// ==========================================================================
// OPTIONS handler
// ==========================================================================

/// Handle `OPTIONS /:resource`.
pub async fn options_handler(Path(_resource): Path<String>) -> Response {
    build_options_response(true)
}

/// Handle `OPTIONS /` (root).
pub async fn root_options_handler() -> Response {
    build_options_response(false)
}

// ==========================================================================
// Response builders
// ==========================================================================

/// Build an HTTP response for a read result.
fn build_read_response(
    result: &StatementResult,
    prefs: &Preferences,
    headers_only: bool,
    config: &crate::config::AppConfig,
    media: &MediaType,
) -> Response {
    let content_type = format!("{}; charset=utf-8", media.as_str());
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type);

    // Content-Range header
    let range_end = if result.page_total > 0 {
        result.page_total - 1
    } else {
        0
    };
    let total_str = match result.total {
        Some(t) => t.to_string(),
        None => "*".to_string(),
    };
    let range_header = if result.page_total > 0 {
        format!("0-{}/{}", range_end, total_str)
    } else {
        format!("*/{}", total_str)
    };
    builder = builder.header("content-range", &range_header);

    // Preference-Applied
    if prefs.count.is_some() {
        builder = builder.header("preference-applied", "count=exact");
    }

    // Apply GUC overrides (response.status and response.headers)
    match apply_guc_overrides(builder, result) {
        Ok(b) => {
            if headers_only {
                finalize_response(b, Body::empty())
            } else {
                // Check if we should stream this response
                let body_size = result.body.len();
                if should_stream(
                    body_size,
                    config.server_streaming_enabled,
                    config.server_streaming_threshold,
                ) {
                    finalize_response(b, stream_json_response(result.body.clone()))
                } else {
                    finalize_response(b, Body::from(result.body.clone()))
                }
            }
        }
        Err(e) => e.into_response(),
    }
}

/// Build an HTTP response for a mutation result.
fn build_mutate_response(
    result: &StatementResult,
    prefs: &Preferences,
    method: &str,
    path: &str,
    config: &crate::config::AppConfig,
    media: &MediaType,
) -> Response {
    let status = if method == "POST" {
        StatusCode::CREATED
    } else {
        StatusCode::OK
    };

    let return_rep = matches!(prefs.representation, Some(PreferRepresentation::Full));

    let content_type = format!("{}; charset=utf-8", media.as_str());
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type);

    // Content-Range
    let range_header = format!("*/{}", result.page_total);
    builder = builder.header("content-range", &range_header);

    // Location header for POST/201 responses
    if method == "POST" {
        builder = builder.header(header::LOCATION, path);
    }

    // Preference-Applied for return
    if let Some(ref rep) = prefs.representation {
        let applied = match rep {
            PreferRepresentation::Full => "return=representation",
            PreferRepresentation::HeadersOnly => "return=headers-only",
            PreferRepresentation::None => "return=minimal",
        };
        builder = builder.header("preference-applied", applied);
    }

    // Apply GUC overrides (response.status and response.headers)
    match apply_guc_overrides(builder, result) {
        Ok(b) => {
            if return_rep {
                // Check if we should stream this response
                let body_size = result.body.len();
                if should_stream(
                    body_size,
                    config.server_streaming_enabled,
                    config.server_streaming_threshold,
                ) {
                    finalize_response(b, stream_json_response(result.body.clone()))
                } else {
                    finalize_response(b, Body::from(result.body.clone()))
                }
            } else if matches!(prefs.representation, Some(PreferRepresentation::None)) {
                finalize_response(b, Body::empty())
            } else {
                finalize_response(b, Body::from(""))
            }
        }
        Err(err_response) => err_response,
    }
}

/// Build an HTTP response for an RPC result.
fn build_rpc_response(
    result: &StatementResult,
    _prefs: &Preferences,
    config: &crate::config::AppConfig,
    media: &MediaType,
) -> Response {
    let content_type = format!("{}; charset=utf-8", media.as_str());
    let builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type);

    // Apply GUC overrides (response.status and response.headers)
    match apply_guc_overrides(builder, result) {
        Ok(b) => {
            // Check if we should stream this response
            let body_size = result.body.len();
            if should_stream(
                body_size,
                config.server_streaming_enabled,
                config.server_streaming_threshold,
            ) {
                finalize_response(b, stream_json_response(result.body.clone()))
            } else {
                finalize_response(b, Body::from(result.body.clone()))
            }
        }
        Err(err_response) => err_response,
    }
}

/// Build an OPTIONS response with allowed methods.
fn build_options_response(is_resource: bool) -> Response {
    let methods = if is_resource {
        "GET, HEAD, POST, PATCH, PUT, DELETE, OPTIONS"
    } else {
        "GET, OPTIONS"
    };

    Response::builder()
        .status(StatusCode::OK)
        .header("allow", methods)
        .header(
            "access-control-allow-methods",
            "GET, HEAD, POST, PATCH, PUT, DELETE, OPTIONS",
        )
        .header(
            "access-control-allow-headers",
            "Authorization, Content-Type, Accept, Prefer, Range, \
             Accept-Profile, Content-Profile",
        )
        .body(Body::empty())
        .expect("static OPTIONS response must be valid")
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn test_apply_guc_overrides_status() {
        let result = StatementResult {
            total: None,
            page_total: 1,
            body: "[]".to_string(),
            response_headers: None,
            response_status: Some(202), // Override to 202 Accepted
        };

        let builder = Response::builder().status(StatusCode::OK);
        match apply_guc_overrides(builder, &result) {
            Ok(b) => {
                let response = b.body(Body::empty()).unwrap();
                assert_eq!(response.status(), StatusCode::ACCEPTED);
            }
            Err(_) => panic!("GUC override should succeed"),
        }
    }

    #[test]
    fn test_apply_guc_overrides_headers() {
        // PostgREST format: array of objects with single key-value pairs
        let headers_json = serde_json::json!([
            {"X-Custom-Header": "custom-value"},
            {"X-Another-Header": "another-value"}
        ]);

        let result = StatementResult {
            total: None,
            page_total: 1,
            body: "[]".to_string(),
            response_headers: Some(headers_json),
            response_status: None,
        };

        let builder = Response::builder().status(StatusCode::OK);
        match apply_guc_overrides(builder, &result) {
            Ok(b) => {
                let response = b.body(Body::empty()).unwrap();
                assert_eq!(
                    response.headers().get("X-Custom-Header").unwrap(),
                    "custom-value"
                );
                assert_eq!(
                    response.headers().get("X-Another-Header").unwrap(),
                    "another-value"
                );
            }
            Err(_) => panic!("GUC override should succeed"),
        }
    }

    #[test]
    fn test_apply_guc_overrides_both() {
        let headers_json = serde_json::json!([
            {"X-Custom": "value"}
        ]);

        let result = StatementResult {
            total: None,
            page_total: 1,
            body: "[]".to_string(),
            response_headers: Some(headers_json),
            response_status: Some(418), // I'm a teapot
        };

        let builder = Response::builder().status(StatusCode::OK);
        match apply_guc_overrides(builder, &result) {
            Ok(b) => {
                let response = b.body(Body::empty()).unwrap();
                assert_eq!(response.status(), StatusCode::IM_A_TEAPOT);
                assert_eq!(response.headers().get("X-Custom").unwrap(), "value");
            }
            Err(_) => panic!("GUC override should succeed"),
        }
    }

    #[test]
    fn test_apply_guc_overrides_no_overrides() {
        let result = StatementResult {
            total: None,
            page_total: 1,
            body: "[]".to_string(),
            response_headers: None,
            response_status: None,
        };

        let builder = Response::builder().status(StatusCode::OK);
        match apply_guc_overrides(builder, &result) {
            Ok(b) => {
                let response = b.body(Body::empty()).unwrap();
                // Should keep original status
                assert_eq!(response.status(), StatusCode::OK);
            }
            Err(_) => panic!("GUC override should succeed"),
        }
    }

    #[test]
    fn test_apply_guc_overrides_invalid_headers_format() {
        // Invalid format: object instead of array
        let headers_json = serde_json::json!({
            "X-Custom": "value"
        });

        let result = StatementResult {
            total: None,
            page_total: 1,
            body: "[]".to_string(),
            response_headers: Some(headers_json),
            response_status: None,
        };

        let builder = Response::builder().status(StatusCode::OK);
        match apply_guc_overrides(builder, &result) {
            Ok(_) => panic!("Should return error for invalid headers format"),
            Err(err_response) => {
                // Should return error response (DBRST111)
                assert!(
                    err_response.status().is_client_error()
                        || err_response.status().is_server_error()
                );
            }
        }
    }
}
