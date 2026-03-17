//! Database backend abstraction layer.
//!
//! This module defines the core traits that decouple dbrest from any
//! specific database engine. A concrete backend (e.g. PostgreSQL, MySQL,
//! SQLite) implements these traits to plug into the rest of the system.
//!
//! # Architecture
//!
//! ```text
//! +-----------------------+
//! |    dbrest-core         |
//! |  (backend traits)      |
//! +-----------+-----------+
//!             |
//!    +--------+--------+
//!    |                 |
//! +--v--+          +---v---+
//! | PG  |          | MySQL | (future)
//! +-----+          +-------+
//! ```

pub mod postgres;

use std::fmt;

use async_trait::async_trait;

use crate::error::Error;
use crate::query::sql_builder::{SqlBuilder, SqlParam};
use crate::schema_cache::db::DbIntrospector;

// ==========================================================================
// DbVersion — database-agnostic version info
// ==========================================================================

/// Database server version information.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
    /// Human-readable name of the database engine (e.g. "PostgreSQL", "MySQL").
    pub engine: String,
}

impl fmt::Display for DbVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}.{}.{}", self.engine, self.major, self.minor, self.patch)
    }
}

// ==========================================================================
// StatementResult — database-agnostic query result
// ==========================================================================

/// Parsed result from a CTE-wrapped statement.
///
/// This is the uniform shape that every backend must produce from the
/// main query execution. The handler layer reads these fields to build
/// the HTTP response.
#[derive(Debug, Clone)]
pub struct StatementResult {
    pub total: Option<i64>,
    pub page_total: i64,
    pub body: String,
    pub response_headers: Option<serde_json::Value>,
    pub response_status: Option<i32>,
}

impl StatementResult {
    pub fn empty() -> Self {
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
// DatabaseBackend — the main abstraction trait
// ==========================================================================

/// Core trait that a database backend must implement.
///
/// Covers connection management, query execution, introspection,
/// version detection, error mapping, and change notification.
#[async_trait]
pub trait DatabaseBackend: Send + Sync + 'static {
    /// Connect to the database and return a backend instance.
    ///
    /// The implementation should create a connection pool internally.
    async fn connect(uri: &str, pool_size: u32, acquire_timeout_secs: u64, max_lifetime_secs: u64, idle_timeout_secs: u64) -> Result<Self, Error>
    where
        Self: Sized;

    /// Query the database server version.
    async fn version(&self) -> Result<DbVersion, Error>;

    /// Return the minimum supported version for this backend.
    fn min_version(&self) -> (u32, u32);

    /// Execute a raw SQL statement (no result set expected).
    ///
    /// Used for session variable setup and pre-request function calls.
    async fn exec_raw(&self, sql: &str, params: &[SqlParam]) -> Result<(), Error>;

    /// Execute a CTE-wrapped statement and parse the standard result set.
    ///
    /// The query is expected to return columns:
    /// `total_result_set`, `page_total`, `body`, `response_headers`, `response_status`
    async fn exec_statement(&self, sql: &str, params: &[SqlParam]) -> Result<StatementResult, Error>;

    /// Begin a transaction and execute multiple statements within it.
    ///
    /// Runs tx_vars, pre_req, and main query in order within a single
    /// transaction. Returns the result from the main query.
    async fn exec_in_transaction(
        &self,
        tx_vars: Option<&SqlBuilder>,
        pre_req: Option<&SqlBuilder>,
        main: Option<&SqlBuilder>,
    ) -> Result<StatementResult, Error>;

    /// Get a database introspector for schema cache loading.
    fn introspector(&self) -> Box<dyn DbIntrospector + '_>;

    /// Start a background change listener (e.g. PostgreSQL NOTIFY).
    ///
    /// Returns `None` if the backend does not support change notifications.
    /// The listener should call `on_schema_reload` when a schema reload is
    /// requested and `on_config_reload` when a config reload is requested.
    async fn start_listener(
        &self,
        channel: &str,
        cancel: tokio::sync::watch::Receiver<bool>,
        on_event: std::sync::Arc<dyn Fn(String) + Send + Sync>,
    ) -> Result<(), Error>;

    /// Map a backend-specific database error into our Error type.
    fn map_error(&self, err: Box<dyn std::error::Error + Send + Sync>) -> Error;
}

// ==========================================================================
// SqlDialect — SQL syntax abstraction
// ==========================================================================

/// Trait abstracting database-specific SQL syntax.
///
/// Each backend provides an implementation that generates the correct SQL
/// for its engine. The query module calls these methods instead of
/// hardcoding PostgreSQL-specific functions.
pub trait SqlDialect: Send + Sync {
    // -- Aggregation --

    /// JSON array aggregation expression.
    ///
    /// PostgreSQL: `coalesce(json_agg(_pgrest_t), '[]')::text`
    /// MySQL:      `COALESCE(JSON_ARRAYAGG(JSON_OBJECT(...)), JSON_ARRAY())`
    fn json_agg(&self, b: &mut SqlBuilder, alias: &str);

    /// Single-row JSON expression.
    ///
    /// PostgreSQL: `row_to_json(_pgrest_t)::text`
    /// MySQL:      `JSON_OBJECT(...)`
    fn row_to_json(&self, b: &mut SqlBuilder, alias: &str);

    // -- Counting --

    /// COUNT function with schema qualification.
    ///
    /// PostgreSQL: `pg_catalog.count(expr)`
    /// MySQL:      `COUNT(expr)`
    fn count_expr(&self, b: &mut SqlBuilder, expr: &str);

    /// COUNT(*) for total counts.
    ///
    /// PostgreSQL: `SELECT COUNT(*) AS "pgrst_filtered_count"`
    fn count_star(&self, b: &mut SqlBuilder);

    // -- Session variables --

    /// Set a session/transaction-local variable.
    ///
    /// PostgreSQL: `set_config('key', 'value', true)`
    /// MySQL:      `SET @key = 'value'`
    fn set_session_var(&self, b: &mut SqlBuilder, key: &str, value: &str);

    /// Read a session variable in a SELECT expression.
    ///
    /// PostgreSQL: `nullif(current_setting('key', true), '')`
    /// MySQL:      `@key`
    fn get_session_var(&self, b: &mut SqlBuilder, key: &str, column_alias: &str);

    // -- Type casting --

    /// Cast an expression to a type.
    ///
    /// PostgreSQL: `expr::type`
    /// MySQL:      `CAST(expr AS type)`
    fn type_cast(&self, b: &mut SqlBuilder, expr: &str, ty: &str);

    // -- JSON body unpacking --

    /// FROM clause for unpacking a JSON body into rows.
    ///
    /// PostgreSQL: `json_to_recordset($1) AS _("col1" type1, "col2" type2)`
    /// MySQL:      `JSON_TABLE($1, '$[*]' COLUMNS(...))`
    #[allow(clippy::wrong_self_convention)]
    fn from_json_body(
        &self,
        b: &mut SqlBuilder,
        columns: &[crate::plan::types::CoercibleField],
        json_bytes: &[u8],
    );

    // -- Type cast suffix --

    /// Append a type cast suffix to the builder.
    ///
    /// PostgreSQL: `::type`
    /// MySQL:      (no-op, or uses CAST wrapping at a higher level)
    fn push_type_cast_suffix(&self, b: &mut SqlBuilder, ty: &str);

    /// Append an array type cast suffix to the builder.
    ///
    /// PostgreSQL: `::type[]`
    /// MySQL:      (no-op)
    fn push_array_type_cast_suffix(&self, b: &mut SqlBuilder, ty: &str);

    // -- Quoting --

    /// Quote an identifier (table, column, schema name).
    ///
    /// PostgreSQL: `"identifier"`
    /// MySQL:      `` `identifier` ``
    fn quote_ident(&self, ident: &str) -> String;

    /// Quote a string literal.
    ///
    /// PostgreSQL: `'literal'`
    fn quote_literal(&self, lit: &str) -> String;

    // -- Full-text search --

    /// Full-text search predicate.
    ///
    /// PostgreSQL: `to_tsvector('config', col) @@ to_tsquery('config', $1)`
    /// Returns false if the backend doesn't support FTS.
    fn supports_fts(&self) -> bool;

    /// Generate FTS predicate SQL.
    fn fts_predicate(
        &self,
        b: &mut SqlBuilder,
        config: Option<&str>,
        column: &str,
        operator: &str,
    );

    // -- Lateral joins --

    /// Whether the backend supports LATERAL joins.
    ///
    /// If false, the query builder must use correlated subqueries instead.
    fn supports_lateral_join(&self) -> bool;

    // -- Named parameters in function calls --

    /// Named parameter assignment syntax for function calls.
    ///
    /// PostgreSQL: `"param" := $1`
    /// Others may use different syntax or not support this.
    fn named_param_assign(&self) -> &str {
        " := "
    }
}
