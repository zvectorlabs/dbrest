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
        write!(
            f,
            "{} {}.{}.{}",
            self.engine, self.major, self.minor, self.patch
        )
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
// PoolStatus — connection pool metrics
// ==========================================================================

/// Connection pool status for metrics reporting.
#[derive(Debug, Clone)]
pub struct PoolStatus {
    /// Number of connections currently in use.
    pub active: u32,
    /// Number of idle connections in the pool.
    pub idle: u32,
    /// Maximum pool size.
    pub max_size: u32,
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
    /// `busy_timeout_ms` controls how long to wait for locks:
    /// - SQLite: `PRAGMA busy_timeout`
    /// - Postgres: `SET lock_timeout`
    async fn connect(
        uri: &str,
        pool_size: u32,
        acquire_timeout_secs: u64,
        max_lifetime_secs: u64,
        idle_timeout_secs: u64,
        busy_timeout_ms: u64,
    ) -> Result<Self, Error>
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
    async fn exec_statement(
        &self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<StatementResult, Error>;

    /// Begin a transaction and execute multiple statements within it.
    ///
    /// Runs tx_vars, pre_req, mutation, and main query in order within a single
    /// transaction. Returns the result from the main query.
    ///
    /// `mutation` is only set for backends that don't support DML in CTEs.
    /// When set, the executor must run the mutation, capture RETURNING rows
    /// into a temp table `_dbrst_mut`, then run `main` which aggregates from it.
    async fn exec_in_transaction(
        &self,
        tx_vars: Option<&SqlBuilder>,
        pre_req: Option<&SqlBuilder>,
        mutation: Option<&SqlBuilder>,
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

    /// Return current connection pool status for metrics reporting.
    ///
    /// Returns `None` if the backend does not track pool statistics.
    fn pool_status(&self) -> Option<PoolStatus> {
        None
    }
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
    /// `columns` is optionally provided for backends that cannot aggregate
    /// a whole row alias (e.g. SQLite needs explicit column names).
    /// PostgreSQL ignores `columns` and uses `json_agg(alias)`.
    ///
    /// PostgreSQL: `coalesce(json_agg(_dbrst_t), '[]')::text`
    /// SQLite:     `COALESCE(json_group_array(json_object('col', "col", ...)), '[]')`
    fn json_agg(&self, b: &mut SqlBuilder, alias: &str) {
        self.json_agg_with_columns(b, alias, &[]);
    }

    /// JSON array aggregation with explicit column names.
    ///
    /// Default implementation ignores columns and delegates to the alias-based form.
    fn json_agg_with_columns(&self, b: &mut SqlBuilder, alias: &str, columns: &[&str]);

    /// Single-row JSON expression.
    ///
    /// PostgreSQL: `row_to_json(_dbrst_t)::text`
    /// MySQL:      `JSON_OBJECT(...)`
    fn row_to_json(&self, b: &mut SqlBuilder, alias: &str) {
        self.row_to_json_with_columns(b, alias, &[]);
    }

    /// Single-row JSON with explicit column names.
    fn row_to_json_with_columns(&self, b: &mut SqlBuilder, alias: &str, columns: &[&str]);

    // -- Counting --

    /// COUNT function with schema qualification.
    ///
    /// PostgreSQL: `pg_catalog.count(expr)`
    /// MySQL:      `COUNT(expr)`
    fn count_expr(&self, b: &mut SqlBuilder, expr: &str);

    /// COUNT(*) for total counts.
    ///
    /// PostgreSQL: `SELECT COUNT(*) AS "dbrst_filtered_count"`
    fn count_star(&self, b: &mut SqlBuilder);

    // -- Session variables --

    /// Set a session/transaction-local variable.
    ///
    /// The generated expression must be usable as a SELECT column expression.
    ///
    /// PostgreSQL: `set_config('key', 'value', true)`
    /// MySQL:      `SET @key = 'value'`
    fn set_session_var(&self, b: &mut SqlBuilder, key: &str, value: &str);

    /// Read a session variable in a SELECT expression.
    ///
    /// PostgreSQL: `nullif(current_setting('key', true), '')`
    /// MySQL:      `@key`
    fn get_session_var(&self, b: &mut SqlBuilder, key: &str, column_alias: &str);

    /// Whether session variable setup uses SELECT-based expressions.
    ///
    /// If true (default), `tx_var_query` wraps calls in `SELECT expr1, expr2, ...`.
    /// If false, `set_session_var` is not called; instead `build_tx_vars_statement`
    /// is used to produce a single statement for all variables at once.
    fn session_vars_are_select_exprs(&self) -> bool {
        true
    }

    /// Build a single statement that sets all session/transaction variables.
    ///
    /// Only called when `session_vars_are_select_exprs()` returns false.
    /// The default implementation panics — backends that return false must override.
    fn build_tx_vars_statement(&self, _b: &mut SqlBuilder, _vars: &[(&str, &str)]) {
        unimplemented!(
            "backends with session_vars_are_select_exprs() == false must implement build_tx_vars_statement"
        )
    }

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
    fn fts_predicate(&self, b: &mut SqlBuilder, config: Option<&str>, column: &str, operator: &str);

    // -- Scalar row-to-json --

    /// Convert an entire CTE row to JSON text (for scalar function calls).
    ///
    /// PostgreSQL: `row_to_json(dbrst_source.*)::text`
    /// SQLite:     `json_object(...)` (requires column list — override if needed)
    fn row_to_json_star(&self, b: &mut SqlBuilder, source: &str) {
        // Default implementation uses PG-style syntax.
        // Override for databases that don't support `source.*`.
        b.push("row_to_json(");
        b.push(source);
        b.push(".*)::text");
    }

    /// COUNT(*) subquery for exact count from a CTE source.
    ///
    /// PostgreSQL: `(SELECT pg_catalog.count(*) FROM dbrst_source)`
    /// SQLite:     `(SELECT COUNT(*) FROM dbrst_source)`
    fn count_star_from(&self, b: &mut SqlBuilder, source: &str) {
        b.push("(SELECT pg_catalog.count(*) FROM ");
        b.push(source);
        b.push(")");
    }

    // -- Literal escaping --

    /// Push a single-quoted SQL literal with proper escaping.
    ///
    /// PostgreSQL uses `E'...'` for backslash escapes. SQLite uses plain `'...'`.
    /// The default implementation uses PostgreSQL E-string syntax.
    fn push_literal(&self, b: &mut SqlBuilder, s: &str) {
        let has_backslash = s.contains('\\');
        if has_backslash {
            b.push("E");
        }
        b.push("'");
        for ch in s.chars() {
            if ch == '\'' {
                b.push("'");
            }
            b.push_char(ch);
        }
        b.push("'");
    }

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

    /// Whether the backend supports DML (INSERT/UPDATE/DELETE) inside CTEs.
    ///
    /// PostgreSQL supports `WITH cte AS (INSERT ... RETURNING ...) SELECT ... FROM cte`.
    /// SQLite does NOT — DML is only allowed as the top-level statement.
    ///
    /// When false, write queries are split into a mutation statement and a
    /// separate aggregation SELECT, executed sequentially in the same transaction.
    fn supports_dml_cte(&self) -> bool {
        true
    }
}
