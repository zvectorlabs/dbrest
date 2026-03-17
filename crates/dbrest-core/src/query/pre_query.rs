//! Transaction and session variable setup queries.
//!
//! Before executing the main SQL query, dbrest sets PostgreSQL session
//! variables via `set_config()` to communicate request context to
//! database functions and triggers. This module generates those setup
//! queries.
//!
//! # Pipeline
//!
//! ```text
//! HTTP request ──▶ pre_req_query() ──▶ SET search_path, role, claims …
//!               ──▶ tx_var_query()  ──▶ SET method, path, headers, cookies …
//! ```
//!
//! # SQL Example
//!
//! ```sql
//! SELECT
//!   set_config('search_path', '"test_api", "public"', true),
//!   set_config('role', 'web_anon', true),
//!   set_config('request.method', 'GET', true),
//!   set_config('request.path', '/users', true),
//!   set_config('request.headers', '{"accept":"application/json"}', true),
//!   set_config('request.cookies', '{}', true)
//! ```

use crate::backend::SqlDialect;
use crate::config::AppConfig;
use crate::types::identifiers::QualifiedIdentifier;

use super::sql_builder::SqlBuilder;

// ==========================================================================
// tx_var_query — session variable setup
// ==========================================================================

/// Build the session variable setup query.
///
/// Generates a `SELECT set_config(...)` call for each session variable that
/// must be set before executing the main query. These variables are available
/// to PostgreSQL functions, triggers, and RLS policies via
/// `current_setting('variable.name')`.
///
/// # Behaviour
///
/// The following variables are set:
/// - `search_path` — from the configured schemas
/// - `role` — the anonymous role or authenticated role
/// - `request.method` — HTTP method (GET, POST, etc.)
/// - `request.path` — URL path
/// - `request.headers` — serialized request headers as JSON
/// - `request.cookies` — serialized cookies as JSON
/// - `request.jwt.claims` — JWT claims as JSON
///
/// All values are set with `is_local = true` so they only apply to the
/// current transaction.
///
/// # SQL Example
///
/// ```sql
/// SELECT
///   set_config('search_path', '"test_api", "public"', true),
///   set_config('role', 'web_anon', true),
///   set_config('request.method', 'GET', true),
///   set_config('request.path', '/users', true)
/// ```
#[allow(clippy::too_many_arguments)]
pub fn tx_var_query(
    config: &AppConfig,
    dialect: &dyn SqlDialect,
    method: &str,
    path: &str,
    role: Option<&str>,
    headers_json: Option<&str>,
    cookies_json: Option<&str>,
    claims_json: Option<&str>,
) -> SqlBuilder {
    // Collect all key-value pairs
    let search_path = config
        .db_schemas
        .iter()
        .map(|s| format!("\"{}\"", s))
        .collect::<Vec<_>>()
        .join(", ");

    let mut vars: Vec<(&str, String)> = Vec::new();
    vars.push(("search_path", search_path));

    let effective_role = role
        .map(|r| r.to_string())
        .or_else(|| config.db_anon_role.clone());
    if let Some(ref role_val) = effective_role {
        vars.push(("role", role_val.clone()));
    }

    vars.push(("request.method", method.to_string()));
    vars.push(("request.path", path.to_string()));

    if let Some(headers) = headers_json {
        vars.push(("request.headers", headers.to_string()));
    }
    if let Some(cookies) = cookies_json {
        vars.push(("request.cookies", cookies.to_string()));
    }
    if let Some(claims) = claims_json {
        vars.push(("request.jwt.claims", claims.to_string()));
    }

    let mut b = SqlBuilder::new();

    if dialect.session_vars_are_select_exprs() {
        // PostgreSQL-style: SELECT set_config(...), set_config(...), ...
        b.push("SELECT ");
        let mut first = true;
        for (key, value) in &vars {
            push_set_var(&mut b, dialect, key, value, &mut first);
        }
    } else {
        // Batch-style: dialect produces a single statement for all vars
        let refs: Vec<(&str, &str)> = vars.iter().map(|(k, v)| (*k, v.as_str())).collect();
        dialect.build_tx_vars_statement(&mut b, &refs);
    }

    b
}

/// Append a session variable assignment expression via the dialect.
///
/// # Behaviour
///
/// - Prepends a comma separator when this is not the first call
/// - Delegates to `dialect.set_session_var()` for database-specific syntax
///
/// # SQL Example (PostgreSQL)
///
/// ```sql
/// set_config('request.method', 'GET', true)
/// ```
fn push_set_var(
    b: &mut SqlBuilder,
    dialect: &dyn SqlDialect,
    key: &str,
    value: &str,
    first: &mut bool,
) {
    if !*first {
        b.push(", ");
    }
    *first = false;
    dialect.set_session_var(b, key, value);
}

// ==========================================================================
// pre_req_query — pre-request function call
// ==========================================================================

/// Build the pre-request function call query.
///
/// If the configuration specifies a `db_pre_request` function, this generates
/// a `SELECT` call to that function. The function is invoked after session
/// variables are set but before the main query, allowing it to perform
/// custom authorization checks or request validation.
///
/// Returns `None` if no pre-request function is configured.
///
/// # SQL Example
///
/// ```sql
/// SELECT "my_schema"."check_request"()
/// ```
pub fn pre_req_query(pre_request: &QualifiedIdentifier) -> SqlBuilder {
    let mut b = SqlBuilder::new();
    b.push("SELECT ");
    b.push_qi(pre_request);
    b.push("()");
    b
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::TestPgDialect;

    fn test_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.db_schemas = vec!["test_api".to_string(), "public".to_string()];
        config.db_anon_role = Some("web_anon".to_string());
        config
    }

    fn dialect() -> &'static dyn SqlDialect {
        &TestPgDialect
    }

    #[test]
    fn test_tx_var_query_basic() {
        let config = test_config();
        let b = tx_var_query(&config, dialect(), "GET", "/users", None, None, None, None);
        let sql = b.sql();

        assert!(sql.starts_with("SELECT set_config("));
        assert!(sql.contains("search_path"));
        assert!(sql.contains("request.method"));
        assert!(sql.contains("request.path"));
        assert!(sql.contains("'GET'"));
        assert!(sql.contains("'/users'"));
    }

    #[test]
    fn test_tx_var_query_with_role() {
        let config = test_config();
        let b = tx_var_query(
            &config,
            dialect(),
            "POST",
            "/items",
            Some("admin"),
            None,
            None,
            None,
        );
        let sql = b.sql();

        assert!(sql.contains("'role'"));
        assert!(sql.contains("'admin'"));
    }

    #[test]
    fn test_tx_var_query_with_headers() {
        let config = test_config();
        let b = tx_var_query(
            &config,
            dialect(),
            "GET",
            "/users",
            None,
            Some(r#"{"accept":"application/json"}"#),
            None,
            None,
        );
        let sql = b.sql();

        assert!(sql.contains("request.headers"));
        assert!(sql.contains("application/json"));
    }

    #[test]
    fn test_tx_var_query_with_claims() {
        let config = test_config();
        let b = tx_var_query(
            &config,
            dialect(),
            "GET",
            "/users",
            None,
            None,
            None,
            Some(r#"{"sub":"user123"}"#),
        );
        let sql = b.sql();

        assert!(sql.contains("request.jwt.claims"));
    }

    #[test]
    fn test_pre_req_query() {
        let qi = QualifiedIdentifier::new("my_schema", "check_request");
        let b = pre_req_query(&qi);
        assert_eq!(b.sql(), "SELECT \"my_schema\".\"check_request\"()");
    }

    #[test]
    fn test_pre_req_query_unqualified() {
        let qi = QualifiedIdentifier::unqualified("pre_request_check");
        let b = pre_req_query(&qi);
        assert_eq!(b.sql(), "SELECT \"pre_request_check\"()");
    }

    #[test]
    fn test_tx_var_query_search_path_format() {
        let config = test_config();
        let b = tx_var_query(&config, dialect(), "GET", "/", None, None, None, None);
        let sql = b.sql();

        // Should contain quoted schema names
        assert!(sql.contains("\"test_api\", \"public\""));
    }
}
