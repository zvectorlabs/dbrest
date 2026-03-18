//! PostgreSQL backend executor — implements [`DatabaseBackend`] for `sqlx::PgPool`.

use std::time::Duration;

use async_trait::async_trait;
use sqlx::Row;
use sqlx::postgres::PgPoolOptions;

use crate::introspector::SqlxIntrospector;
use dbrest_core::backend::{DatabaseBackend, DbVersion, PoolStatus, StatementResult};
use dbrest_core::error::Error;
use dbrest_core::query::sql_builder::{SqlBuilder, SqlParam};
use dbrest_core::schema_cache::db::DbIntrospector;

/// PostgreSQL backend backed by `sqlx::PgPool`.
pub struct PgBackend {
    pool: sqlx::PgPool,
}

impl PgBackend {
    /// Get a reference to the underlying pool (for callers that still need it
    /// during the migration period).
    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Create from an existing pool (useful for tests and migration).
    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }
}

// --------------------------------------------------------------------------
// Helper: bind SqlParam values to a sqlx query
// --------------------------------------------------------------------------

fn bind_params<'q>(
    mut q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    params: &'q [SqlParam],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for p in params {
        match p {
            SqlParam::Text(t) => q = q.bind(t.as_str()),
            SqlParam::Json(j) => q = q.bind(j.to_vec()),
            SqlParam::Binary(b) => q = q.bind(b.to_vec()),
            SqlParam::Null => q = q.bind(Option::<String>::None),
        }
    }
    q
}

/// Map a `sqlx::Error` to our `Error` type, detecting PostgreSQL-specific
/// constraint violations and error codes.
pub fn map_sqlx_error(e: sqlx::Error) -> Error {
    let (code, message, detail, hint) = match &e {
        sqlx::Error::Database(db_err) => {
            let code = db_err.code().map(|c| c.to_string());
            let message = db_err.message().to_string();
            let detail = db_err.constraint().map(|c| c.to_string());

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
            return Error::Database {
                code: None,
                message: e.to_string(),
                detail: None,
                hint: None,
            };
        }
    };

    if code.is_some() || !message.is_empty() {
        match code.as_deref() {
            // Constraint violations
            Some("23505") => return Error::UniqueViolation(message),
            Some("23503") => return Error::ForeignKeyViolation(message),
            Some("23514") => return Error::CheckViolation(message),
            Some("23502") => return Error::NotNullViolation(message),
            Some("23P01") => return Error::ExclusionViolation(message),

            // Permission errors
            Some("42501") => {
                let role =
                    extract_role_from_message(&message).unwrap_or_else(|| "unknown".to_string());
                return Error::PermissionDenied { role };
            }

            // Not found errors
            Some("42883") => {
                if message.contains("operator") {
                    return Error::Database {
                        code: Some("42883".to_string()),
                        message: message.clone(),
                        detail: Some(
                            "Operator error: The requested operator is not available for the given data types."
                                .to_string(),
                        ),
                        hint: Some(
                            "Check that the filter operator and column types are compatible."
                                .to_string(),
                        ),
                    };
                }
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
                let table_name = extract_name_from_message(&message, "relation")
                    .unwrap_or_else(|| "unknown".to_string());
                return Error::TableNotFound {
                    name: table_name,
                    suggestion: None,
                };
            }
            Some("42703") => {
                if let Some(col_start) = message.find("column ")
                    && let Some(after_col) = message.get(col_start + 7..)
                {
                    let col_end = after_col.find(" does").unwrap_or(after_col.len());
                    let col_ref = &after_col[..col_end];
                    let col_ref = col_ref.trim();

                    let (table_name, col_name) = if let Some(dot_pos) = col_ref.find('.') {
                        let table = col_ref[..dot_pos].trim_matches('"').to_string();
                        let col = col_ref[dot_pos + 1..].trim_matches('"').to_string();
                        (table, col)
                    } else {
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

            // RAISE exceptions
            Some("P0001") => {
                return Error::RaisedException {
                    message,
                    status: None,
                };
            }

            // PostgREST custom codes (PT***)
            Some(code) if code.starts_with("PT") => {
                if let Some(status_str) = code.strip_prefix("PT")
                    && let Ok(status) = status_str.parse::<u16>()
                {
                    return Error::DbrstRaise { message, status };
                }
            }

            _ => {}
        }

        return Error::Database {
            code,
            message,
            detail,
            hint,
        };
    }

    Error::Database {
        code: None,
        message: e.to_string(),
        detail: None,
        hint: None,
    }
}

fn extract_role_from_message(msg: &str) -> Option<String> {
    if let Some(start) = msg.find("role ") {
        let rest = &msg[start + 5..];
        if let Some(end) = rest.find([' ', '\n', '\r']) {
            return Some(rest[..end].to_string());
        }
        return Some(rest.to_string());
    }
    None
}

fn extract_name_from_message(msg: &str, keyword: &str) -> Option<String> {
    if let Some(start) = msg.find(keyword) {
        let rest = &msg[start + keyword.len()..];
        let rest = rest.trim_start();
        if let Some(end) = rest.find([' ', ',', '(', '\n', '\r']) {
            let name = rest[..end].trim_matches('"').to_string();
            if !name.is_empty() {
                return Some(name);
            }
        }
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

// --------------------------------------------------------------------------
// Parse the standard 5-column result set
// --------------------------------------------------------------------------

fn parse_statement_row(row: &sqlx::postgres::PgRow) -> StatementResult {
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
impl DatabaseBackend for PgBackend {
    async fn connect(
        uri: &str,
        pool_size: u32,
        acquire_timeout_secs: u64,
        max_lifetime_secs: u64,
        idle_timeout_secs: u64,
    ) -> Result<Self, Error> {
        let pool = PgPoolOptions::new()
            .max_connections(pool_size)
            .acquire_timeout(Duration::from_secs(acquire_timeout_secs))
            .max_lifetime(Duration::from_secs(max_lifetime_secs))
            .idle_timeout(Duration::from_secs(idle_timeout_secs))
            .connect(uri)
            .await
            .map_err(|e| Error::DbConnection(e.to_string()))?;

        Ok(Self { pool })
    }

    async fn version(&self) -> Result<DbVersion, Error> {
        let row: (String,) = sqlx::query_as("SHOW server_version")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| Error::DbConnection(format!("Failed to query PG version: {}", e)))?;

        let version_str = &row.0;
        let parts: Vec<&str> = version_str.split('.').collect();
        Ok(DbVersion {
            major: parts.first().and_then(|s| s.parse().ok()).unwrap_or(0),
            minor: parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
            patch: parts
                .get(2)
                .and_then(|s| s.split_whitespace().next().and_then(|v| v.parse().ok()))
                .unwrap_or(0),
            engine: "PostgreSQL".to_string(),
        })
    }

    fn min_version(&self) -> (u32, u32) {
        (12, 0)
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
        _mutation: Option<&SqlBuilder>,
        main: Option<&SqlBuilder>,
    ) -> Result<StatementResult, Error> {
        let mut tx = self.pool.begin().await.map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

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

        // 3. Execute the main query
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

        tx.commit().await.map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        Ok(result)
    }

    fn introspector(&self) -> Box<dyn DbIntrospector + '_> {
        Box::new(SqlxIntrospector::new(&self.pool))
    }

    async fn start_listener(
        &self,
        channel: &str,
        cancel: tokio::sync::watch::Receiver<bool>,
        on_event: std::sync::Arc<dyn Fn(String) + Send + Sync>,
    ) -> Result<(), Error> {
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .map_err(|e| Error::Database {
                code: None,
                message: e.to_string(),
                detail: None,
                hint: None,
            })?;

        listener
            .listen(channel)
            .await
            .map_err(|e| Error::Database {
                code: None,
                message: e.to_string(),
                detail: None,
                hint: None,
            })?;

        tracing::info!(channel = channel, "Subscribed to NOTIFY channel");

        // Process events in a sub-function to avoid borrow checker issues
        // with on_event's drop order vs notification payload lifetime.
        loop {
            if *cancel.borrow() {
                return Ok(());
            }

            let notification = tokio::time::timeout(Duration::from_secs(30), listener.recv()).await;

            // Extract payload as an owned String before calling on_event,
            // so the PgNotification (which borrows from the listener) is
            // dropped before the closure is invoked.
            let maybe_payload: Option<Result<String, sqlx::Error>> = match notification {
                Ok(Ok(msg)) => Some(Ok(msg.payload().to_string())),
                Ok(Err(e)) => Some(Err(e)),
                Err(_) => None,
            };

            match maybe_payload {
                Some(Ok(payload)) => {
                    tracing::info!(payload = %payload, "Received NOTIFY");
                    on_event(payload);
                }
                Some(Err(e)) => {
                    return Err(Error::Database {
                        code: None,
                        message: e.to_string(),
                        detail: None,
                        hint: None,
                    });
                }
                None => continue,
            }
        }
    }

    fn map_error(&self, err: Box<dyn std::error::Error + Send + Sync>) -> Error {
        if let Ok(sqlx_err) = err.downcast::<sqlx::Error>() {
            map_sqlx_error(*sqlx_err)
        } else {
            Error::Internal("Unknown database error".to_string())
        }
    }

    fn pool_status(&self) -> Option<PoolStatus> {
        Some(PoolStatus {
            active: self.pool.size().saturating_sub(self.pool.num_idle() as u32),
            idle: self.pool.num_idle() as u32,
            max_size: self.pool.options().get_max_connections(),
        })
    }
}
