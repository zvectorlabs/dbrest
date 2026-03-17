//! SQLite backend executor — implements [`DatabaseBackend`] for `sqlx::SqlitePool`.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row};

use dbrest_core::backend::{DatabaseBackend, DbVersion, StatementResult};
use dbrest_core::error::Error;
use dbrest_core::query::sql_builder::{SqlBuilder, SqlParam};
use dbrest_core::schema_cache::db::DbIntrospector;

use crate::introspector::SqliteIntrospector;

/// SQLite backend backed by `sqlx::SqlitePool`.
pub struct SqliteBackend {
    pool: sqlx::SqlitePool,
}

impl SqliteBackend {
    /// Get a reference to the underlying pool.
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }

    /// Create from an existing pool (useful for tests).
    pub fn from_pool(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }

    /// Ensure the session vars temp table exists on a connection.
    async fn ensure_vars_table(conn: &mut sqlx::SqliteConnection) -> Result<(), Error> {
        sqlx::query("CREATE TEMP TABLE IF NOT EXISTS _dbrest_vars(key TEXT PRIMARY KEY, val TEXT)")
            .execute(&mut *conn)
            .await
            .map_err(map_sqlx_error)?;
        Ok(())
    }
}

// --------------------------------------------------------------------------
// Helper: bind SqlParam values to a sqlx query
// --------------------------------------------------------------------------

fn bind_params<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    params: &'q [SqlParam],
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for p in params {
        match p {
            SqlParam::Text(t) => q = q.bind(t.as_str()),
            SqlParam::Json(j) => q = q.bind(String::from_utf8_lossy(j).into_owned()),
            SqlParam::Binary(b) => q = q.bind(b.to_vec()),
            SqlParam::Null => q = q.bind(Option::<String>::None),
        }
    }
    q
}

/// Map a `sqlx::Error` to our `Error` type for SQLite.
pub fn map_sqlx_error(e: sqlx::Error) -> Error {
    let (code, message) = match &e {
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().map(|c| c.to_string());
            let message = db_err.message().to_string();
            (code, message)
        }
        _ => {
            return Error::Database {
                code: None,
                message: e.to_string(),
                detail: None,
                hint: None,
            };
        }
    };

    // Map common SQLite error codes
    match code.as_deref() {
        // UNIQUE constraint
        Some("2067") | Some("1555") => Error::UniqueViolation(message),
        // FOREIGN KEY constraint
        Some("787") => Error::ForeignKeyViolation(message),
        // CHECK constraint
        Some("275") => Error::CheckViolation(message),
        // NOT NULL constraint
        Some("1299") => Error::NotNullViolation(message),
        _ => Error::Database {
            code,
            message,
            detail: None,
            hint: None,
        },
    }
}

// --------------------------------------------------------------------------
// Parse the standard 5-column result set
// --------------------------------------------------------------------------

fn parse_statement_row(row: &sqlx::sqlite::SqliteRow) -> StatementResult {
    let total: Option<i64> = row
        .try_get::<String, _>("total_result_set")
        .ok()
        .and_then(|s| s.parse::<i64>().ok());

    let page_total: i64 = row.try_get("page_total").unwrap_or(0);

    let body_str: String = row.try_get("body").unwrap_or_else(|_| "[]".to_string());

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

    StatementResult {
        total,
        page_total,
        body: body_str,
        response_headers,
        response_status,
    }
}

// --------------------------------------------------------------------------
// DatabaseBackend implementation
// --------------------------------------------------------------------------

#[async_trait]
impl DatabaseBackend for SqliteBackend {
    async fn connect(
        uri: &str,
        pool_size: u32,
        acquire_timeout_secs: u64,
        max_lifetime_secs: u64,
        idle_timeout_secs: u64,
    ) -> Result<Self, Error> {
        let pool = SqlitePoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
            .max_lifetime(Duration::from_secs(max_lifetime_secs))
            .idle_timeout(Duration::from_secs(idle_timeout_secs))
            .connect(uri)
            .await
            .map_err(|e| Error::DbConnection(e.to_string()))?;

        // Enable WAL mode and foreign keys for better concurrency
        sqlx::query("PRAGMA journal_mode=WAL")
            .execute(&pool)
            .await
            .map_err(map_sqlx_error)?;
        sqlx::query("PRAGMA foreign_keys=ON")
            .execute(&pool)
            .await
            .map_err(map_sqlx_error)?;

        Ok(Self { pool })
    }

    async fn version(&self) -> Result<DbVersion, Error> {
        let row: (String,) = sqlx::query_as("SELECT sqlite_version()")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::DbConnection(format!("Failed to query SQLite version: {}", e)))?;

        let version_str = &row.0;
        let parts: Vec<&str> = version_str.split('.').collect();
        Ok(DbVersion {
            major: parts.first().and_then(|s| s.parse().ok()).unwrap_or(0),
            minor: parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
            patch: parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0),
            engine: "SQLite".to_string(),
        })
    }

    fn min_version(&self) -> (u32, u32) {
        // Require SQLite 3.35+ for RETURNING support
        (3, 35)
    }

    async fn exec_raw(&self, sql: &str, params: &[SqlParam]) -> Result<(), Error> {
        let q = sqlx::query(sql);
        let q = bind_params(q, params);
        q.execute(&self.pool).await.map_err(map_sqlx_error)?;
        Ok(())
    }

    async fn exec_statement(
        &self,
        sql: &str,
        params: &[SqlParam],
    ) -> Result<StatementResult, Error> {
        let q = sqlx::query(sql);
        let q = bind_params(q, params);
        let rows = q.fetch_all(&self.pool).await.map_err(map_sqlx_error)?;

        if rows.is_empty() {
            return Ok(StatementResult::empty());
        }

        Ok(parse_statement_row(&rows[0]))
    }

    async fn exec_in_transaction(
        &self,
        tx_vars: Option<&SqlBuilder>,
        pre_req: Option<&SqlBuilder>,
        mutation: Option<&SqlBuilder>,
        main: Option<&SqlBuilder>,
    ) -> Result<StatementResult, Error> {
        let mut tx = self.pool.begin().await.map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        // Ensure the session vars temp table exists
        Self::ensure_vars_table(&mut tx).await?;

        // 1. Set session variables
        if let Some(tv) = tx_vars {
            let q = sqlx::query(tv.sql());
            let q = bind_params(q, tv.params());
            q.execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }

        // 2. Call pre-request function
        if let Some(pr) = pre_req {
            let q = sqlx::query(pr.sql());
            let q = bind_params(q, pr.params());
            q.execute(&mut *tx).await.map_err(map_sqlx_error)?;
        }

        // 3. If there's a split mutation, execute it and bridge results via temp table
        if let Some(mut_q) = mutation {
            // Execute mutation with RETURNING and collect rows
            let q = sqlx::query(mut_q.sql());
            let q = bind_params(q, mut_q.params());
            let rows = q.fetch_all(&mut *tx).await.map_err(map_sqlx_error)?;

            // Create temp table and insert RETURNING rows
            // We need to know column names/count — extract from first row
            if !rows.is_empty() {
                let ncols = rows[0].len();
                // Build CREATE TEMP TABLE with generic column names
                // Then use the actual column names from the row metadata
                let columns: Vec<String> = (0..ncols)
                    .map(|i| rows[0].column(i).name().to_string())
                    .collect();

                let mut create_sql = String::from("CREATE TEMP TABLE IF NOT EXISTS _dbrst_mut(");
                for (i, col) in columns.iter().enumerate() {
                    if i > 0 {
                        create_sql.push_str(", ");
                    }
                    create_sql.push('"');
                    create_sql.push_str(col);
                    create_sql.push_str("\" TEXT");
                }
                create_sql.push(')');
                sqlx::query(&create_sql)
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;

                // Insert each row
                for row in &rows {
                    let mut insert_sql = String::from("INSERT INTO _dbrst_mut VALUES(");
                    for i in 0..ncols {
                        if i > 0 {
                            insert_sql.push_str(", ");
                        }
                        insert_sql.push('?');
                    }
                    insert_sql.push(')');

                    let mut q = sqlx::query(&insert_sql);
                    for i in 0..ncols {
                        // Try to get as string; fall back to NULL
                        let val: Option<String> = row.try_get(i).ok();
                        q = q.bind(val);
                    }
                    q.execute(&mut *tx).await.map_err(map_sqlx_error)?;
                }
            } else {
                // No rows returned — still create the temp table with a dummy schema
                sqlx::query("CREATE TEMP TABLE IF NOT EXISTS _dbrst_mut(__dummy TEXT)")
                    .execute(&mut *tx)
                    .await
                    .map_err(map_sqlx_error)?;
            }
        }

        // 4. Execute the main query (aggregation SELECT)
        let result = if let Some(main_q) = main {
            let q = sqlx::query(main_q.sql());
            let q = bind_params(q, main_q.params());
            let rows = q.fetch_all(&mut *tx).await.map_err(map_sqlx_error)?;

            if rows.is_empty() {
                StatementResult::empty()
            } else {
                parse_statement_row(&rows[0])
            }
        } else {
            StatementResult::empty()
        };

        // 5. Clean up temp table if we created one
        if mutation.is_some() {
            let _ = sqlx::query("DROP TABLE IF EXISTS _dbrst_mut")
                .execute(&mut *tx)
                .await;
        }

        tx.commit().await.map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        Ok(result)
    }

    fn introspector(&self) -> Box<dyn DbIntrospector + '_> {
        Box::new(SqliteIntrospector::new(&self.pool))
    }

    async fn start_listener(
        &self,
        _channel: &str,
        _cancel: tokio::sync::watch::Receiver<bool>,
        _on_event: std::sync::Arc<dyn Fn(String) + Send + Sync>,
    ) -> Result<(), Error> {
        // SQLite has no LISTEN/NOTIFY mechanism.
        // We simply return Ok — schema reload must be triggered differently
        // (e.g., by file watch or timer in the caller).
        tracing::info!("SQLite does not support LISTEN/NOTIFY — schema change listener disabled");
        Ok(())
    }

    fn map_error(&self, err: Box<dyn std::error::Error + Send + Sync>) -> Error {
        if let Ok(sqlx_err) = err.downcast::<sqlx::Error>() {
            map_sqlx_error(*sqlx_err)
        } else {
            Error::Internal("Unknown database error".to_string())
        }
    }
}
