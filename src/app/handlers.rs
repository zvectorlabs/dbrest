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
use sqlx::Row;

use crate::api_request;
use crate::api_request::preferences::{PreferRepresentation, Preferences};
use crate::auth::types::AuthResult;
use crate::error::Error;
use crate::plan::{self, ActionPlan, CrudPlan, DbActionPlan};
use crate::query::{self, SqlBuilder, SqlParam};
use crate::schema_cache::SchemaCache;
use crate::types::media::MediaType;

use super::state::AppState;
use super::streaming::{should_stream, stream_json_response};

// ==========================================================================
// Shared helpers
// ==========================================================================

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

/// Execute a `MainQuery` against the pool inside a transaction.
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

    let pool = &state.pool;
    let mut tx = pool.begin().await.map_err(|e| Error::Database {
        code: None,
        message: e.to_string(),
        detail: None,
        hint: None,
    })?;

    // 1. Set session variables
    if let Some(ref tv) = mq.tx_vars {
        exec_raw(&mut tx, tv).await?;
    }

    // 2. Call pre-request function
    if let Some(ref pr) = mq.pre_req {
        exec_raw(&mut tx, pr).await?;
    }

    // 3. Execute the main query
    let result = if let Some(ref main) = mq.main {
        exec_statement(&mut tx, main).await?
    } else {
        StatementResult::empty()
    };

    tx.commit().await.map_err(|e| Error::Database {
        code: None,
        message: e.to_string(),
        detail: None,
        hint: None,
    })?;

    Ok(result)
}

/// Execute a raw SQL builder (e.g. tx_vars, pre_req) — no result set.
async fn exec_raw(conn: &mut sqlx::PgConnection, builder: &SqlBuilder) -> Result<(), Error> {
    let sql = builder.sql();
    let params = builder.params();
    let mut q = sqlx::query(sql);
    for p in params {
        match p {
            SqlParam::Text(t) => q = q.bind(t.as_str()),
            SqlParam::Json(j) => q = q.bind(j.to_vec()),
            SqlParam::Binary(b) => q = q.bind(b.to_vec()),
            SqlParam::Null => q = q.bind(Option::<String>::None),
        }
    }
    q.execute(&mut *conn).await.map_err(map_db_error)?;
    Ok(())
}

/// Execute a CTE-wrapped statement and parse the five-column result set.
async fn exec_statement(
    conn: &mut sqlx::PgConnection,
    builder: &SqlBuilder,
) -> Result<StatementResult, Error> {
    let sql = builder.sql();
    let params = builder.params();
    let mut q = sqlx::query(sql);
    for p in params {
        match p {
            SqlParam::Text(t) => q = q.bind(t.as_str()),
            SqlParam::Json(j) => q = q.bind(j.to_vec()),
            SqlParam::Binary(b) => q = q.bind(b.to_vec()),
            SqlParam::Null => q = q.bind(Option::<String>::None),
        }
    }
    let rows = q.fetch_all(&mut *conn).await.map_err(map_db_error)?;

    if rows.is_empty() {
        return Ok(StatementResult::empty());
    }

    let row = &rows[0];

    // total_result_set — may be '' (empty string) or a number
    let total: Option<i64> = row
        .try_get::<String, _>("total_result_set")
        .ok()
        .and_then(|s| s.parse::<i64>().ok());

    let page_total: i64 = row.try_get("page_total").unwrap_or(0);

    // body — character varying containing JSON
    let body_str: String = row.try_get("body").unwrap_or_else(|_| "[]".to_string());

    // response_headers — text from current_setting('response.headers', true), parse as JSON
    // Column is aliased as "response_headers" in the SQL
    let response_headers: Option<serde_json::Value> = row
        .try_get::<Option<String>, _>("response_headers")
        .ok()
        .flatten()
        .and_then(|s| {
            if s.is_empty() {
                None
            } else {
                serde_json::from_str(&s).ok()
            }
        });

    // response_status — text from current_setting('response.status', true), parse as i32
    // Column is aliased as "response_status" in the SQL
    let response_status: Option<i32> = row
        .try_get::<Option<String>, _>("response_status")
        .ok()
        .flatten()
        .and_then(|s| {
            if s.is_empty() {
                None
            } else {
                s.parse::<i32>().ok()
            }
        });

    Ok(StatementResult {
        total,
        page_total,
        body: body_str,
        response_headers,
        response_status,
    })
}

/// Map a sqlx error to our Error type, detecting constraint violations and other PostgreSQL errors.
fn map_db_error(e: sqlx::Error) -> Error {
    // Try to extract PostgreSQL-specific error information first
    let (code, message, detail, hint) = match &e {
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().map(|c| c.to_string());
            let message = db_err.message().to_string();
            let detail = db_err.constraint().map(|c| c.to_string());

            // Try to downcast to PgDatabaseError to get hint
            // We need to use the concrete type, not the trait
            let hint = if let Some(pg_err) =
                db_err.try_downcast_ref::<sqlx::postgres::PgDatabaseError>()
            {
                pg_err.hint().map(|s| s.to_string())
            } else {
                None
            };

            (code, message, detail, hint)
        }
        _ => {
            // Non-database error, return generic error
            return Error::Database {
                code: None,
                message: e.to_string(),
                detail: None,
                hint: None,
            };
        }
    };

    if code.is_some() || !message.is_empty() {
        // Map PostgreSQL error codes to specific Error variants
        match code.as_deref() {
            // Constraint violations
            Some("23505") => return Error::UniqueViolation(message),
            Some("23503") => return Error::ForeignKeyViolation(message),
            Some("23514") => return Error::CheckViolation(message),
            Some("23502") => return Error::NotNullViolation(message),
            Some("23P01") => return Error::ExclusionViolation(message),

            // Permission errors
            Some("42501") => {
                // Extract role from message if possible, otherwise use "unknown"
                let role =
                    extract_role_from_message(&message).unwrap_or_else(|| "unknown".to_string());
                return Error::PermissionDenied { role };
            }

            // Not found errors
            Some("42883") => {
                // Error code 42883 can be either "undefined_function" or "operator does not exist"
                // Check if it's an operator error first
                if message.contains("operator") {
                    // This is an operator error, not a function error
                    // Return as a database error with appropriate detail
                    return Error::Database {
                        code: Some("42883".to_string()),
                        message: message.clone(),
                        detail: Some("Operator error: The requested operator is not available for the given data types.".to_string()),
                        hint: Some("Check that the filter operator and column types are compatible.".to_string()),
                    };
                }
                // Otherwise, it's a function error
                let func_name =
                    extract_name_from_message(&message, "function").unwrap_or_else(|| {
                        tracing::debug!(
                            "Could not extract function name from PostgreSQL error: {}",
                            message
                        );
                        "unknown".to_string()
                    });
                return Error::FunctionNotFound { name: func_name };
            }
            Some("42P01") => {
                // undefined_table
                let table_name = extract_name_from_message(&message, "relation")
                    .unwrap_or_else(|| "unknown".to_string());
                return Error::TableNotFound {
                    name: table_name,
                    suggestion: None,
                };
            }
            Some("42703") => {
                // undefined_column
                // PostgreSQL error format: "column users.nonexistent_field does not exist"
                // or "column nonexistent_field does not exist"
                // Find the part after "column" keyword
                if let Some(col_start) = message.find("column ") {
                    let after_col = &message[col_start + 7..]; // Skip "column "
                    // Find the next space or "does"
                    let col_end = after_col.find(" does").unwrap_or(after_col.len());
                    let col_ref = after_col[..col_end].trim();

                    // Parse "table.column" or just "column"
                    let (table_name, col_name) = if let Some(dot_pos) = col_ref.find('.') {
                        // Format: "table.column"
                        let table = col_ref[..dot_pos].trim_matches('"').to_string();
                        let col = col_ref[dot_pos + 1..].trim_matches('"').to_string();
                        (table, col)
                    } else {
                        // Format: just "column"
                        let col = col_ref.trim_matches('"').to_string();
                        ("unknown".to_string(), col)
                    };
                    return Error::ColumnNotFound {
                        table: table_name,
                        column: col_name,
                    };
                }
                return Error::InvalidQueryParam {
                    param: "column".to_string(),
                    message,
                };
            }

            // RAISE exceptions (P0001)
            Some("P0001") => {
                // Check if response.status was set via GUC (we'd need to check the result, but for now use default)
                return Error::RaisedException {
                    message,
                    status: None, // Will be overridden by response.status GUC if set
                };
            }

            // PostgREST custom codes (PT***)
            Some(code) if code.starts_with("PT") => {
                // Extract status code from PT code (e.g., PT400 -> 400)
                if let Some(status_str) = code.strip_prefix("PT")
                    && let Ok(status) = status_str.parse::<u16>()
                {
                    return Error::PgrstRaise { message, status };
                }
            }

            _ => {}
        }

        // Default: return generic Database error with all details
        return Error::Database {
            code,
            message,
            detail,
            hint,
        };
    }

    // Non-database errors (connection, pool, etc.)
    Error::Database {
        code: None,
        message: e.to_string(),
        detail: None,
        hint: None,
    }
}

/// Extract role name from PostgreSQL error message.
fn extract_role_from_message(msg: &str) -> Option<String> {
    // PostgreSQL messages often have format like "permission denied for role <role>"
    if let Some(start) = msg.find("role ") {
        let rest = &msg[start + 5..];
        if let Some(end) = rest.find([' ', '\n', '\r']) {
            return Some(rest[..end].to_string());
        }
        return Some(rest.to_string());
    }
    None
}

/// Extract name (function, table, column) from PostgreSQL error message.
fn extract_name_from_message(msg: &str, keyword: &str) -> Option<String> {
    // Look for patterns like "function <name>", "relation <name>", "column <name>"
    if let Some(start) = msg.find(keyword) {
        let rest = &msg[start + keyword.len()..];
        // Skip whitespace
        let rest = rest.trim_start();
        // Find the name (up to space, comma, or parenthesis)
        if let Some(end) = rest.find([' ', ',', '(', '\n', '\r']) {
            let name = rest[..end].trim_matches('"').to_string();
            if !name.is_empty() {
                return Some(name);
            }
        }
        // If no delimiter, try to extract quoted or unquoted name
        let name = rest
            .split_whitespace()
            .next()?
            .trim_matches('"')
            .to_string();
        if !name.is_empty() {
            return Some(name);
        }
    }
    None
}

/// Parsed result from a CTE-wrapped statement.
struct StatementResult {
    total: Option<i64>,
    page_total: i64,
    body: String,
    response_headers: Option<serde_json::Value>,
    response_status: Option<i32>,
}

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
            // Invalid status code - return error response (PGRST112)
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
            // If it's not an array, return error (PostgREST returns GucHeadersError PGRST111)
            return Err(Error::InvalidConfig {
                message: "response.headers GUC must be a JSON array composed of objects with a single key and a string value".to_string(),
            }
            .into_response());
        }
    }

    Ok(builder)
}

impl StatementResult {
    fn empty() -> Self {
        Self {
            total: None,
            page_total: 0,
            body: "[]".to_string(),
            response_headers: None,
            response_status: None,
        }
    }
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

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
                .body(Body::from(serde_json::to_string(&body).unwrap()))
                .unwrap()
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
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(
                            header::CONTENT_TYPE,
                            "application/openapi+json; charset=utf-8",
                        )
                        .body(Body::from(body))
                        .unwrap()
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
                b.body(Body::empty()).unwrap()
            } else {
                // Check if we should stream this response
                let body_size = result.body.len();
                if should_stream(
                    body_size,
                    config.server_streaming_enabled,
                    config.server_streaming_threshold,
                ) {
                    b.body(stream_json_response(result.body.clone())).unwrap()
                } else {
                    b.body(Body::from(result.body.clone())).unwrap()
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
                    b.body(stream_json_response(result.body.clone())).unwrap()
                } else {
                    b.body(Body::from(result.body.clone())).unwrap()
                }
            } else if matches!(prefs.representation, Some(PreferRepresentation::None)) {
                b.body(Body::empty()).unwrap()
            } else {
                b.body(Body::from("")).unwrap()
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
                b.body(stream_json_response(result.body.clone())).unwrap()
            } else {
                b.body(Body::from(result.body.clone())).unwrap()
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
        .unwrap()
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
                // Should return error response (PGRST111)
                assert!(
                    err_response.status().is_client_error()
                        || err_response.status().is_server_error()
                );
            }
        }
    }
}
