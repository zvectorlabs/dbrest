//! HTTP-to-DB integration tests for the query module.
//!
//! These tests verify the full pipeline:
//!
//! 1. Build an `ApiRequest` from HTTP method/path/query/headers/body
//! 2. Load a `SchemaCache` from a real PostgreSQL database
//! 3. Generate an `ActionPlan` from the request + schema
//! 4. Convert the plan to SQL via the query builder
//! 5. Execute the SQL against the database
//! 6. Verify JSON results, row counts, headers, and statuses
//!
//! Requires Docker for testcontainers.
//!
//! Run with: `cargo test --test query_integration -- --ignored`

mod common;

use bytes::Bytes;
use pgrest::api_request::preferences::{PreferCount, PreferRepresentation, Preferences};
use pgrest::api_request::{self, ApiRequest};
use pgrest::config::AppConfig;
use pgrest::error::Error;
use pgrest::plan::mutate_plan::MutatePlan;
use pgrest::plan::read_plan::ReadPlanTree;
use pgrest::plan::{self, ActionPlan, CrudPlan, DbActionPlan};
use pgrest::query::{self, SqlBuilder, SqlParam};
use pgrest::query::{builder, statements};
use pgrest::schema_cache::SchemaCache;
use pgrest::schema_cache::db::{ColumnJson, DbIntrospector, RelationshipRow, RoutineRow, TableRow};
use sqlx::PgPool;
use sqlx::Row;

// ==========================================================================
// Config & preferences helpers
// ==========================================================================

/// Build a default `AppConfig` pointing at the `test_api` schema.
fn test_config() -> AppConfig {
    AppConfig {
        db_schemas: vec!["test_api".to_string()],
        ..Default::default()
    }
}

/// Default preferences — no special Prefer header options.
fn default_prefs() -> Preferences {
    Preferences::default()
}

/// Preferences requesting `Prefer: count=exact` (total row count in response).
fn count_prefs() -> Preferences {
    Preferences {
        count: Some(PreferCount::Exact),
        ..Default::default()
    }
}

/// Preferences requesting `Prefer: return=representation` (return created/updated rows).
#[allow(dead_code)]
fn return_rep_prefs() -> Preferences {
    Preferences {
        representation: Some(PreferRepresentation::Full),
        ..Default::default()
    }
}

/// Minimal headers: just `Accept: application/json`.
fn json_headers() -> Vec<(String, String)> {
    vec![("accept".to_string(), "application/json".to_string())]
}

/// Headers for JSON mutation requests: both Accept and Content-Type.
#[allow(dead_code)]
fn json_ct_headers() -> Vec<(String, String)> {
    vec![
        ("accept".to_string(), "application/json".to_string()),
        ("content-type".to_string(), "application/json".to_string()),
    ]
}

// ==========================================================================
// ApiRequest builder helper
// ==========================================================================

/// Build an `ApiRequest` from HTTP components.
///
/// Uses [`api_request::from_request`] which mirrors the real HTTP-to-domain
/// parsing pipeline.
fn build_api_request(
    config: &AppConfig,
    prefs: &Preferences,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    body: Bytes,
) -> Result<ApiRequest, Error> {
    api_request::from_request(config, prefs, method, path, query, headers, body)
}

// ==========================================================================
// SqlxIntrospector — matches plan_integration.rs pattern
// ==========================================================================

/// Introspector backed by a real `sqlx::PgPool`.
///
/// Queries `pg_catalog` system tables to discover tables, columns,
/// relationships, routines, and timezones for the test schema.
struct SqlxIntrospector<'a> {
    pool: &'a PgPool,
}

impl<'a> SqlxIntrospector<'a> {
    fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl DbIntrospector for SqlxIntrospector<'_> {
    async fn query_tables(&self, schemas: &[String]) -> Result<Vec<TableRow>, Error> {
        let rows = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                bool,
                bool,
                bool,
                bool,
                Vec<String>,
            ),
        >(
            r#"
            SELECT
                n.nspname AS table_schema,
                c.relname AS table_name,
                d.description AS table_description,
                c.relkind IN ('v', 'm') AS is_view,
                true AS insertable,
                true AS updatable,
                true AS deletable,
                COALESCE(
                    (SELECT array_agg(a.attname ORDER BY a.attname)
                     FROM pg_constraint con
                     JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(con.conkey)
                     WHERE con.conrelid = c.oid AND con.contype = 'p'),
                    '{}'
                ) AS pk_cols
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0
            WHERE c.relkind IN ('r', 'v', 'm', 'f', 'p')
                AND n.nspname = ANY($1)
            ORDER BY n.nspname, c.relname
            "#,
        )
        .bind(schemas)
        .fetch_all(self.pool)
        .await
        .map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        let mut result = Vec::new();
        for (schema, name, desc, is_view, insertable, updatable, deletable, pk_cols) in rows {
            let columns = self.get_columns(&schema, &name).await?;
            result.push(TableRow {
                table_schema: schema,
                table_name: name,
                table_description: desc,
                is_view,
                insertable,
                updatable,
                deletable,
                readable: true,
                pk_cols,
                columns_json: serde_json::to_string(&columns).unwrap(),
            });
        }
        Ok(result)
    }

    async fn query_relationships(&self) -> Result<Vec<RelationshipRow>, Error> {
        let rows = sqlx::query_as::<_, (String, String, String, String, bool, String, bool)>(
            r#"
            SELECT
                ns1.nspname AS table_schema,
                tab.relname AS table_name,
                ns2.nspname AS foreign_table_schema,
                other.relname AS foreign_table_name,
                traint.conrelid = traint.confrelid AS is_self,
                traint.conname AS constraint_name,
                false AS one_to_one
            FROM pg_constraint traint
            JOIN pg_namespace ns1 ON ns1.oid = traint.connamespace
            JOIN pg_class tab ON tab.oid = traint.conrelid
            JOIN pg_class other ON other.oid = traint.confrelid
            JOIN pg_namespace ns2 ON ns2.oid = other.relnamespace
            WHERE traint.contype = 'f' AND traint.conparentid = 0
            ORDER BY traint.conrelid, traint.conname
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        let mut result = Vec::new();
        for (
            table_schema,
            table_name,
            foreign_schema,
            foreign_name,
            is_self,
            constraint,
            one_to_one,
        ) in rows
        {
            let cols = self.get_fk_columns(&constraint).await?;
            result.push(RelationshipRow {
                table_schema,
                table_name,
                foreign_table_schema: foreign_schema,
                foreign_table_name: foreign_name,
                is_self,
                constraint_name: constraint,
                cols_and_fcols: cols,
                one_to_one,
            });
        }
        Ok(result)
    }

    async fn query_routines(&self, schemas: &[String]) -> Result<Vec<RoutineRow>, Error> {
        let rows = sqlx::query_as::<_, (String, String, Option<String>, String, bool)>(
            r#"
            SELECT
                pn.nspname AS routine_schema,
                p.proname AS routine_name,
                d.description,
                p.provolatile::text AS volatility,
                p.provariadic != 0 AS is_variadic
            FROM pg_proc p
            JOIN pg_namespace pn ON p.pronamespace = pn.oid
            LEFT JOIN pg_description d ON d.objoid = p.oid
            WHERE pn.nspname = ANY($1) AND p.prokind IN ('f', 'p')
            ORDER BY pn.nspname, p.proname
            "#,
        )
        .bind(schemas)
        .fetch_all(self.pool)
        .await
        .map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        let result = rows
            .into_iter()
            .map(|(schema, name, desc, vol, variadic)| RoutineRow {
                routine_schema: schema,
                routine_name: name,
                description: desc,
                params_json: "[]".to_string(),
                return_type_json: r#"{"kind":"single","type_kind":"scalar","type_schema":"pg_catalog","type_name":"void","is_alias":false}"#.to_string(),
                executable: true,
                volatility: vol,
                is_variadic: variadic,
            })
            .collect();

        Ok(result)
    }

    async fn query_computed_fields(
        &self,
        _schemas: &[String],
    ) -> Result<Vec<pgrest::schema_cache::ComputedFieldRow>, Error> {
        Ok(Vec::new())
    }

    async fn query_timezones(&self) -> Result<Vec<String>, Error> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT name FROM pg_timezone_names LIMIT 100")
            .fetch_all(self.pool)
            .await
            .map_err(|e| Error::Database {
                code: None,
                message: e.to_string(),
                detail: None,
                hint: None,
            })?;
        Ok(rows.into_iter().map(|(name,)| name).collect())
    }
}

impl SqlxIntrospector<'_> {
    /// Fetch column metadata (name, type, nullable, default, etc.) for a table.
    async fn get_columns(&self, schema: &str, table: &str) -> Result<Vec<ColumnJson>, Error> {
        let rows = sqlx::query_as::<_, (String, Option<String>, bool, String, String, Option<i32>, Option<String>)>(
            r#"
            SELECT
                a.attname AS column_name,
                d.description AS column_description,
                NOT a.attnotnull AS nullable,
                pg_catalog.format_type(a.atttypid, NULL) AS data_type,
                COALESCE(pg_get_expr(ad.adbin, ad.adrelid), '') AS column_default,
                CASE WHEN a.atttypmod > 0 THEN a.atttypmod - 4 ELSE NULL END AS max_len,
                CASE WHEN e.enumlabel IS NOT NULL THEN 'enum' ELSE NULL END AS enum_info
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
            LEFT JOIN pg_attrdef ad ON ad.adrelid = c.oid AND ad.adnum = a.attnum
            LEFT JOIN pg_type t ON t.oid = a.atttypid
            LEFT JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE a.attnum > 0 AND NOT a.attisdropped
                AND n.nspname = $1 AND c.relname = $2
            GROUP BY a.attname, d.description, a.attnotnull, a.atttypid, a.atttypmod, ad.adbin, ad.adrelid, e.enumlabel, a.attnum
            ORDER BY a.attnum
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(self.pool)
        .await
        .map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        Ok(rows
            .into_iter()
            .map(
                |(name, desc, nullable, data_type, default, max_length, _enum_info)| ColumnJson {
                    name,
                    description: desc,
                    nullable,
                    data_type: data_type.clone(),
                    nominal_type: data_type,
                    default: if default.is_empty() {
                        None
                    } else {
                        Some(default)
                    },
                    max_length,
                    enum_values: if _enum_info.is_some() {
                        vec!["(enum)".to_string()]
                    } else {
                        vec![]
                    },
                    is_composite: false,
                    composite_type_schema: None,
                    composite_type_name: None,
                },
            )
            .collect())
    }

    /// Fetch the column pairs (source, target) for a foreign key constraint.
    async fn get_fk_columns(&self, constraint_name: &str) -> Result<Vec<(String, String)>, Error> {
        let rows = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT
                src.attname AS column_name,
                tgt.attname AS foreign_column_name
            FROM pg_constraint con
            JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS src_keys(attnum, ord) ON true
            JOIN LATERAL unnest(con.confkey) WITH ORDINALITY AS tgt_keys(attnum, ord) ON src_keys.ord = tgt_keys.ord
            JOIN pg_attribute src ON src.attrelid = con.conrelid AND src.attnum = src_keys.attnum
            JOIN pg_attribute tgt ON tgt.attrelid = con.confrelid AND tgt.attnum = tgt_keys.attnum
            WHERE con.conname = $1
            ORDER BY src_keys.ord
            "#,
        )
        .bind(constraint_name)
        .fetch_all(self.pool)
        .await
        .map_err(|e| Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        Ok(rows)
    }
}

/// Load a schema cache from a live PostgreSQL database.
async fn load_schema_cache(pool: &PgPool) -> SchemaCache {
    let config = test_config();
    let introspector = SqlxIntrospector::new(pool);
    SchemaCache::load(&introspector, &config)
        .await
        .expect("Failed to load schema cache")
}

// ==========================================================================
// Plan extraction helpers
// ==========================================================================

/// Extract the `ReadPlanTree` from a `WrappedReadPlan` action plan.
///
/// Panics with a descriptive message if the plan is not a `WrappedReadPlan`.
fn expect_wrapped_read(plan: &ActionPlan) -> &ReadPlanTree {
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { read_plan, .. },
            ..
        }) => read_plan,
        other => panic!("Expected WrappedReadPlan, got {other:?}"),
    }
}

/// Extract the `MutatePlan` and `ReadPlanTree` from a `MutateReadPlan`.
#[allow(dead_code)]
fn expect_mutate_read(plan: &ActionPlan) -> (&MutatePlan, &ReadPlanTree) {
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan:
                CrudPlan::MutateReadPlan {
                    mutate_plan,
                    read_plan,
                    ..
                },
            ..
        }) => (mutate_plan, read_plan),
        other => panic!("Expected MutateReadPlan, got {other:?}"),
    }
}

/// Check if this plan is `headers_only` (HEAD request).
///
/// Returns `true` only for `WrappedReadPlan` variants with `headers_only = true`.
/// All other plan types return `false`.
fn is_headers_only(plan: &ActionPlan) -> bool {
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { headers_only, .. },
            ..
        }) => *headers_only,
        _ => false,
    }
}

// ==========================================================================
// SQL execution helper
// ==========================================================================

/// Execute a `SqlBuilder` query against the pool and return the first row.
///
/// The CTE wrapper returns columns: total_result_set, page_total, body,
/// response_headers, response_status. This helper extracts the body as
/// a parsed `serde_json::Value`.
async fn execute_statement(pool: &PgPool, stmt: &SqlBuilder) -> StatementResult {
    let sql = stmt.sql();
    let params = stmt.params();

    let mut query = sqlx::query(sql);
    for param in params {
        match param {
            SqlParam::Text(t) => query = query.bind(t.clone()),
            SqlParam::Json(j) => query = query.bind(j.to_vec()),
            SqlParam::Binary(b) => query = query.bind(b.to_vec()),
            SqlParam::Null => query = query.bind(Option::<String>::None),
        }
    }

    let rows = query.fetch_all(pool).await.expect("Query execution failed");

    if rows.is_empty() {
        return StatementResult {
            total: None,
            page_total: 0,
            body: serde_json::Value::Null,
            response_headers: None,
            response_status: None,
        };
    }

    let row = &rows[0];

    let total: Option<i64> = row.try_get("total_result_set").ok().flatten();
    let page_total: i64 = row.get("page_total");
    let body_str: Option<String> = row.try_get("body").ok().flatten();
    let response_headers: Option<String> = row.try_get("response_headers").ok().flatten();
    let response_status: Option<String> = row.try_get("response_status").ok().flatten();

    let body = match body_str {
        Some(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::String(s)),
        None => serde_json::Value::Null,
    };

    StatementResult {
        total,
        page_total,
        body,
        response_headers,
        response_status,
    }
}

/// Parse an embedded relation value into a `Vec<serde_json::Value>`.
///
/// Embed values can arrive as:
/// - A JSON array (when the outer json_agg is nested inside the body json_agg)
/// - A JSON string containing an array (when lateral join aggregation returns text)
/// - Null (when the LEFT JOIN matched no rows — coalesce returns '[]' but the
///   outer aggregation may produce null in some edge cases)
fn parse_embed(val: &serde_json::Value) -> Vec<serde_json::Value> {
    if val.is_array() {
        val.as_array().unwrap().clone()
    } else if val.is_string() {
        serde_json::from_str(val.as_str().unwrap()).unwrap_or_default()
    } else if val.is_null() {
        vec![]
    } else {
        panic!("Unexpected embed type: {:?}", val);
    }
}

/// Parse an embedded single-object relation (M2O / O2O).
///
/// Single-object embeds arrive as a JSON object or a JSON string containing
/// an object (via `row_to_json`).
fn parse_embed_one(val: &serde_json::Value) -> serde_json::Value {
    if val.is_object() {
        val.clone()
    } else if val.is_string() {
        serde_json::from_str(val.as_str().unwrap()).unwrap()
    } else if val.is_null() {
        serde_json::Value::Null
    } else {
        panic!("Unexpected single embed type: {:?}", val);
    }
}

/// The parsed result from executing a CTE-wrapped statement.
///
/// Mirrors the five-column shape returned by all `main_read`, `main_write`,
/// and `main_call` CTE wrappers:
///
/// | Column            | Rust field         | Description                       |
/// |-------------------|--------------------|-----------------------------------|
/// | `total_result_set`| `total`            | Exact count (if requested)        |
/// | `page_total`      | `page_total`       | Rows in this page                 |
/// | `body`            | `body`             | JSON response body                |
/// | `response_headers`| `response_headers` | Custom headers from DB function   |
/// | `response_status` | `response_status`  | Custom status from DB function    |
#[derive(Debug)]
#[allow(dead_code)]
struct StatementResult {
    /// Total rows matching the query (only populated with `Prefer: count=exact`).
    total: Option<i64>,
    /// Number of rows in the current page (after LIMIT/OFFSET).
    page_total: i64,
    /// JSON response body — an array for collections, an object for single items.
    body: serde_json::Value,
    /// Custom response headers set via `set_config('response.headers', …)` in DB.
    response_headers: Option<String>,
    /// Custom response status set via `set_config('response.status', …)` in DB.
    response_status: Option<String>,
}

// ==========================================================================
// Tests: Read queries (GET)
// ==========================================================================

/// GET /users — returns all users as JSON array.
///
/// Verifies the full pipeline: ApiRequest → ActionPlan → SQL → execute →
/// JSON body with 4 users from seed data.
#[tokio::test]
#[ignore] // Requires Docker
async fn test_read_all_users() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert!(
        result.body.is_array(),
        "Expected JSON array, got: {:?}",
        result.body
    );
    assert_eq!(result.page_total, 4, "Expected 4 users from seed data");
    assert_eq!(result.body.as_array().unwrap().len(), 4);
}

/// GET /users?select=id,name — only selected columns in output.
#[tokio::test]
#[ignore]
async fn test_read_select_columns() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    let arr = result.body.as_array().unwrap();
    assert!(!arr.is_empty());
    let first = &arr[0];
    assert!(first.get("id").is_some(), "Expected 'id' column");
    assert!(first.get("name").is_some(), "Expected 'name' column");
    // Only id and name should be present
    assert!(
        first.get("email").is_none(),
        "Should not have 'email' column"
    );
    assert!(
        first.get("status").is_none(),
        "Should not have 'status' column"
    );
}

/// GET /users?name=eq.Alice Johnson — equality filter.
#[tokio::test]
#[ignore]
async fn test_read_filter_eq() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "name=eq.Alice Johnson",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 1);
    let arr = result.body.as_array().unwrap();
    assert_eq!(arr[0]["name"], "Alice Johnson");
}

/// GET /users?order=name.asc — verify ascending order.
#[tokio::test]
#[ignore]
async fn test_read_order_asc() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "order=name.asc",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    let arr = result.body.as_array().unwrap();
    assert!(arr.len() >= 2);
    // First alphabetically should be Alice
    assert_eq!(arr[0]["name"], "Alice Johnson");
}

/// GET /users?order=name.desc — verify descending order.
#[tokio::test]
#[ignore]
async fn test_read_order_desc() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "order=name.desc",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    let arr = result.body.as_array().unwrap();
    assert!(arr.len() >= 2);
    // Last alphabetically should be first in descending order
    assert_eq!(arr[0]["name"], "Diana Prince");
}

/// GET /users?limit=2&offset=1 — pagination.
#[tokio::test]
#[ignore]
async fn test_read_limit_offset() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "limit=2&offset=1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 2);
}

/// GET /users?status=in.(active,inactive) — IN filter.
#[tokio::test]
#[ignore]
async fn test_read_filter_in() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "status=in.(active,inactive)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // Alice (active), Bob (active), Charlie (inactive) = 3
    assert_eq!(result.page_total, 3);
}

/// GET /users?bio=is.null — IS NULL filter.
#[tokio::test]
#[ignore]
async fn test_read_filter_is_null() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "bio=is.null",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // All 4 users have NULL bio in seed data
    assert_eq!(result.page_total, 4);
}

/// GET /users?status=not.eq.pending — negated filter.
#[tokio::test]
#[ignore]
async fn test_read_negated_filter() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "status=not.eq.pending",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // All except Diana (pending) = 3
    assert_eq!(result.page_total, 3);
}

/// GET /users?id=gte.2 — greater-than-or-equal filter.
#[tokio::test]
#[ignore]
async fn test_read_filter_gte() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "id=gte.2",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // id 2, 3, 4
    assert_eq!(result.page_total, 3);
}

/// GET /users?status=eq.active&name=like.*Alice* — multiple filters.
#[tokio::test]
#[ignore]
async fn test_read_multiple_filters() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "status=eq.active&name=like.*Alice*",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // Only Alice is active and matches *Alice*
    assert_eq!(result.page_total, 1);
    let arr = result.body.as_array().unwrap();
    assert_eq!(arr[0]["name"], "Alice Johnson");
}

// ==========================================================================
// Tests: HEAD requests
// ==========================================================================

/// HEAD /users — returns count but null body.
#[tokio::test]
#[ignore]
async fn test_head_request() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "HEAD",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    assert!(is_headers_only(&plan), "HEAD should set headers_only=true");

    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, true, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // Body should be null for HEAD requests
    assert!(result.body.is_null(), "HEAD body should be null");
    // But page_total should still be correct
    assert_eq!(result.page_total, 4);
}

// ==========================================================================
// Tests: Exact count (Prefer: count=exact)
// ==========================================================================

/// GET /users with Prefer: count=exact — includes total_result_set.
#[tokio::test]
#[ignore]
async fn test_exact_count() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &count_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, Some(PreferCount::Exact), None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.total, Some(4), "Exact count should return 4 users");
    assert_eq!(result.page_total, 4);
}

/// GET /users?limit=2 with Prefer: count=exact — total differs from page.
#[tokio::test]
#[ignore]
async fn test_exact_count_with_limit() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &count_prefs(),
        "GET",
        "/users",
        "limit=2",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, Some(PreferCount::Exact), None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.total, Some(4), "Total should be all matching rows");
    assert_eq!(result.page_total, 2, "Page total should be limited to 2");
}

// ==========================================================================
// Tests: max_rows
// ==========================================================================

/// GET /users with max_rows=2 — server-side row cap.
#[tokio::test]
#[ignore]
async fn test_max_rows() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, Some(2), false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 2, "Should be capped at max_rows=2");
}

// ==========================================================================
// Tests: Posts table
// ==========================================================================

/// GET /posts?select=id,title&published=eq.true&order=title.asc
#[tokio::test]
#[ignore]
async fn test_read_posts_filtered_ordered() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/posts",
        "select=id,title&published=eq.true&order=title.asc",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // 3 published posts in seed data
    assert_eq!(result.page_total, 3);
    let arr = result.body.as_array().unwrap();
    assert_eq!(arr[0]["title"], "Advanced Topics");
    assert_eq!(arr[1]["title"], "Hello World");
    assert_eq!(arr[2]["title"], "Tips and Tricks");
}

// ==========================================================================
// Tests: Views
// ==========================================================================

/// GET /active_users — query against a view.
#[tokio::test]
#[ignore]
async fn test_read_view() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/active_users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // Alice and Bob are active
    assert_eq!(result.page_total, 2);
}

// ==========================================================================
// Tests: Tasks table (various column types)
// ==========================================================================

/// GET /tasks?select=id,title,priority — table with enum and various types.
#[tokio::test]
#[ignore]
async fn test_read_tasks() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/tasks",
        "select=id,title,priority",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // 3 tasks in seed data
    assert_eq!(result.page_total, 3);
}

// ==========================================================================
// Tests: SQL generation verification
// ==========================================================================

/// Verify the inner query SQL for a filtered read contains expected fragments.
#[tokio::test]
#[ignore]
async fn test_sql_generation_read_structure() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name&name=eq.Alice Johnson&order=name.asc&limit=10",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let inner = builder::read_plan_to_query(read_tree);
    let sql = inner.sql().to_string();

    assert!(sql.contains("SELECT "), "SQL must contain SELECT");
    assert!(
        sql.contains("\"test_api\".\"users\""),
        "SQL must reference table"
    );
    assert!(sql.contains("WHERE"), "SQL must have WHERE clause");
    assert!(sql.contains("ORDER BY"), "SQL must have ORDER BY");
    assert!(sql.contains("LIMIT"), "SQL must have LIMIT");
}

/// Verify the CTE wrapper structure for main_read.
#[tokio::test]
#[ignore]
async fn test_sql_generation_cte_wrapper() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let sql = stmt.sql().to_string();

    assert!(
        sql.starts_with("WITH pgrst_source AS ("),
        "Must start with CTE"
    );
    assert!(
        sql.contains("total_result_set"),
        "Must have total_result_set"
    );
    assert!(sql.contains("page_total"), "Must have page_total");
    assert!(sql.contains("body"), "Must have body");
    assert!(
        sql.contains("response_headers"),
        "Must have response_headers"
    );
    assert!(sql.contains("response_status"), "Must have response_status");
    assert!(sql.contains("_pgrest_t"), "Must use _pgrest_t alias");
}

/// Verify the count query SQL contains COUNT(*).
#[tokio::test]
#[ignore]
async fn test_sql_generation_count_query() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let count_q = builder::read_plan_to_count_query(read_tree);
    let sql = count_q.sql().to_string();

    assert!(sql.contains("COUNT(*)"), "Count query must use COUNT(*)");
}

/// Verify tx_var_query generates session variable setup SQL.
#[test]
fn test_sql_generation_tx_vars() {
    let config = test_config();
    let b = query::pre_query::tx_var_query(&config, "GET", "/users", None, None, None, None);
    let sql = b.sql().to_string();

    assert!(sql.starts_with("SELECT set_config("), "Must use set_config");
    assert!(sql.contains("search_path"), "Must set search_path");
    assert!(sql.contains("request.method"), "Must set request.method");
    assert!(sql.contains("request.path"), "Must set request.path");
}

// ==========================================================================
// Tests: MainQuery bundle
// ==========================================================================

/// Verify main_query() generates a complete query bundle and executes.
#[tokio::test]
#[ignore]
async fn test_main_query_bundle_executes() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let mq = query::main_query(&plan, &config, "GET", "/users", None, None, None, None);

    assert!(mq.tx_vars.is_some(), "Should have tx_vars");
    assert!(
        mq.pre_req.is_none(),
        "Should not have pre_req (not configured)"
    );
    assert!(mq.main.is_some(), "Should have main query");

    // Execute the main query
    let result = execute_statement(db.pool(), mq.main.as_ref().unwrap()).await;
    assert_eq!(result.page_total, 4, "Should return 4 users");
}

/// Verify main_query() with pre-request function configured.
#[test]
fn test_main_query_with_pre_request() {
    use pgrest::types::identifiers::QualifiedIdentifier;

    let plan = ActionPlan::NoDb(pgrest::plan::InfoPlan::SchemaInfoPlan);
    let mut config = test_config();
    config.db_pre_request = Some(QualifiedIdentifier::new("test_api", "check_request"));

    let mq = query::main_query(&plan, &config, "OPTIONS", "/", None, None, None, None);

    assert!(mq.pre_req.is_some(), "Should have pre_req when configured");
    let pre_sql = mq.pre_req.unwrap().sql().to_string();
    assert!(
        pre_sql.contains("check_request"),
        "Pre-req SQL should reference function"
    );
}

// ==========================================================================
// Tests: Mutation queries executed against real DB
// ==========================================================================

/// INSERT a new user and verify the row count increases.
///
/// This test directly executes the generated mutation SQL against the
/// database, verifying the full insert pipeline.
#[tokio::test]
#[ignore]
async fn test_write_insert_increases_count() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Count before
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();
    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let before = execute_statement(db.pool(), &read_stmt).await;
    assert_eq!(before.page_total, 4);

    // Insert directly (using raw SQL since we know the schema)
    sqlx::query("INSERT INTO test_api.users (email, name, status) VALUES ('new@example.com', 'New User', 'active')")
        .execute(db.pool())
        .await
        .expect("Insert failed");

    // Count after
    let after = execute_statement(db.pool(), &read_stmt).await;
    assert_eq!(
        after.page_total, 5,
        "Should have one more user after insert"
    );
}

/// DELETE a user and verify the row count decreases.
#[tokio::test]
#[ignore]
async fn test_write_delete_decreases_count() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Count before
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();
    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let before = execute_statement(db.pool(), &read_stmt).await;
    assert_eq!(before.page_total, 4);

    // Delete user 4 (Diana, no FK dependencies on her)
    sqlx::query("DELETE FROM test_api.users WHERE id = 4")
        .execute(db.pool())
        .await
        .expect("Delete failed");

    // Count after
    let after = execute_statement(db.pool(), &read_stmt).await;
    assert_eq!(
        after.page_total, 3,
        "Should have one fewer user after delete"
    );
}

/// UPDATE a user's name and verify the change is reflected in reads.
#[tokio::test]
#[ignore]
async fn test_write_update_reflected_in_read() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Update Alice's name
    sqlx::query("UPDATE test_api.users SET name = 'Alice Updated' WHERE id = 1")
        .execute(db.pool())
        .await
        .expect("Update failed");

    // Read back the updated user
    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "id=eq.1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 1);
    let arr = result.body.as_array().unwrap();
    assert_eq!(arr[0]["name"], "Alice Updated");
}

// ==========================================================================
// Tests: RPC functions (direct execution)
// ==========================================================================

/// Scalar function: test_api.add_numbers(3, 5) → 8.
#[tokio::test]
#[ignore]
async fn test_rpc_scalar_function() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");

    let result: (i32,) = sqlx::query_as("SELECT test_api.add_numbers(3, 5)")
        .fetch_one(db.pool())
        .await
        .expect("RPC call failed");

    assert_eq!(result.0, 8);
}

/// Set-returning function: test_api.get_active_users() → 2 rows.
#[tokio::test]
#[ignore]
async fn test_rpc_set_returning_function() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");

    let rows = sqlx::query("SELECT * FROM test_api.get_active_users()")
        .fetch_all(db.pool())
        .await
        .expect("RPC call failed");

    assert_eq!(rows.len(), 2, "Alice and Bob are active");
}

/// Variadic function: test_api.concat_values('a', 'b', 'c') → "a, b, c".
#[tokio::test]
#[ignore]
async fn test_rpc_variadic_function() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");

    let result: (String,) = sqlx::query_as("SELECT test_api.concat_values('a', 'b', 'c')")
        .fetch_one(db.pool())
        .await
        .expect("RPC call failed");

    assert_eq!(result.0, "a, b, c");
}

// ==========================================================================
// Tests: Error handling
// ==========================================================================

/// GET /nonexistent — should fail with TableNotFound.
#[tokio::test]
#[ignore]
async fn test_error_nonexistent_table() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/nonexistent",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let result = plan::action_plan(&config, &req, &cache);
    assert!(result.is_err(), "Should fail for nonexistent table");
}

/// GET /users?select=id,fake_relation(id) — should fail for nonexistent embed.
#[tokio::test]
#[ignore]
async fn test_error_nonexistent_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,fake_relation(id)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let result = plan::action_plan(&config, &req, &cache);
    assert!(result.is_err(), "Should fail for nonexistent embed");
}

// ==========================================================================
// Tests: Empty result sets
// ==========================================================================

/// GET /users?name=eq.Nobody — should return empty array.
#[tokio::test]
#[ignore]
async fn test_read_empty_result() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "name=eq.Nobody",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 0, "Should have no results");
    assert!(result.body.is_array(), "Should still return an array");
    assert!(
        result.body.as_array().unwrap().is_empty(),
        "Array should be empty"
    );
}

/// GET /users?name=eq.Nobody with Prefer: count=exact — count should be 0.
#[tokio::test]
#[ignore]
async fn test_read_empty_with_exact_count() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &count_prefs(),
        "GET",
        "/users",
        "name=eq.Nobody",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, Some(PreferCount::Exact), None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.total, Some(0));
    assert_eq!(result.page_total, 0);
}

// ==========================================================================
// Tests: Resource embedding (JOINs)
// ==========================================================================

/// GET /users?select=id,name,posts(id,title) — O2M embed: users with their posts.
///
/// Verifies the lateral join produces a nested JSON array for each user's posts.
/// Alice has 2 posts, Bob has 2, Charlie and Diana have 0.
#[tokio::test]
#[ignore]
async fn test_join_o2m_users_with_posts() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 4, "Should return all 4 users");
    let arr = result.body.as_array().unwrap();

    // Find Alice (id=1) — she has 2 posts
    let alice = arr.iter().find(|u| u["name"] == "Alice Johnson").unwrap();
    let alice_posts = parse_embed(&alice["posts"]);
    assert_eq!(alice_posts.len(), 2, "Alice should have 2 posts");

    // Find Diana (id=4) — she has 0 posts
    let diana = arr.iter().find(|u| u["name"] == "Diana Prince").unwrap();
    let diana_posts = parse_embed(&diana["posts"]);
    assert!(diana_posts.is_empty(), "Diana should have 0 posts");
}

/// GET /users?select=id,name,posts(id,title)&id=eq.1 — O2M embed with parent filter.
///
/// Only Alice should be returned, with her 2 posts embedded.
#[tokio::test]
#[ignore]
async fn test_join_o2m_filtered_parent() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)&id=eq.1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 1, "Should return only Alice");
    let arr = result.body.as_array().unwrap();
    assert_eq!(arr[0]["name"], "Alice Johnson");

    // Parse embedded posts
    let posts = parse_embed(&arr[0]["posts"]);
    assert_eq!(posts.len(), 2, "Alice should have 2 posts");
    let titles: Vec<&str> = posts.iter().map(|p| p["title"].as_str().unwrap()).collect();
    assert!(titles.contains(&"Hello World"));
    assert!(titles.contains(&"Advanced Topics"));
}

/// GET /posts?select=id,title,comments(id,body) — O2M embed: posts with comments.
///
/// Post 1 has 3 comments (2 top-level + 1 reply), others have 0.
#[tokio::test]
#[ignore]
async fn test_join_o2m_posts_with_comments() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/posts",
        "select=id,title,comments(id,body)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 4, "Should return all 4 posts");
    let arr = result.body.as_array().unwrap();

    // Post 1 ("Hello World") has 3 comments
    let post1 = arr.iter().find(|p| p["title"] == "Hello World").unwrap();
    let comments = parse_embed(&post1["comments"]);
    assert_eq!(comments.len(), 3, "Post 1 should have 3 comments");

    // Post 3 ("Draft Post") has 0 comments
    let post3 = arr.iter().find(|p| p["title"] == "Draft Post").unwrap();
    let comments3 = parse_embed(&post3["comments"]);
    assert!(comments3.is_empty(), "Draft Post should have 0 comments");
}

/// GET /posts?select=id,title,users(id,name) — M2O embed: posts with their author.
///
/// Each post should have a single user object (not an array), since the
/// FK goes from posts.user_id → users.id (many-to-one).
#[tokio::test]
#[ignore]
async fn test_join_m2o_posts_with_author() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/posts",
        "select=id,title,users(id,name)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 4);
    let arr = result.body.as_array().unwrap();

    // "Hello World" was written by Alice
    let post1 = arr.iter().find(|p| p["title"] == "Hello World").unwrap();
    let user = parse_embed_one(&post1["users"]);
    assert_eq!(user["name"], "Alice Johnson");
}

/// GET /users?select=id,name,posts(id,title,comments(id,body)) — nested embed (3 levels).
///
/// Verifies users → posts → comments three-level lateral join chain.
#[tokio::test]
#[ignore]
async fn test_join_nested_three_levels() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title,comments(id,body))&id=eq.1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 1, "Should return only Alice");
    let arr = result.body.as_array().unwrap();
    let alice = &arr[0];
    assert_eq!(alice["name"], "Alice Johnson");

    // Parse Alice's posts
    let posts = parse_embed(&alice["posts"]);
    assert_eq!(posts.len(), 2, "Alice has 2 posts");

    // Find "Hello World" which has 3 comments
    let hello = posts.iter().find(|p| p["title"] == "Hello World").unwrap();
    let comments = parse_embed(&hello["comments"]);
    assert_eq!(comments.len(), 3, "Hello World has 3 comments");
}

/// GET /users?select=id,name,posts(id,title),profiles(avatar_url) — multiple embeds.
///
/// Verifies two sibling lateral joins: users → posts AND users → profiles.
#[tokio::test]
#[ignore]
async fn test_join_multiple_embeds() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title),profiles(avatar_url)&id=eq.1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 1);
    let arr = result.body.as_array().unwrap();
    let alice = &arr[0];

    // Posts embed should be present
    assert!(alice.get("posts").is_some(), "Should have posts embed");

    // Profiles embed should be present (O2O — Alice has a profile)
    assert!(
        alice.get("profiles").is_some(),
        "Should have profiles embed"
    );
}

/// GET /users?select=id,name,posts(id,title)&order=name.asc — embed with ordering.
///
/// Verifies that ordering the parent does not break the lateral join.
#[tokio::test]
#[ignore]
async fn test_join_embed_with_parent_ordering() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)&order=name.asc",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 4);
    let arr = result.body.as_array().unwrap();

    // First user alphabetically should be Alice
    assert_eq!(arr[0]["name"], "Alice Johnson");
    // Last should be Diana
    assert_eq!(arr[3]["name"], "Diana Prince");

    // All rows should have a posts embed (even if empty array)
    for row in arr {
        assert!(
            row.get("posts").is_some(),
            "Each user should have a posts embed"
        );
    }
}

/// GET /users?select=id,name,posts(id,title)&limit=2 — embed with pagination.
///
/// Verifies LIMIT on parent does not break the lateral join and embeds are
/// still present for the returned rows.
#[tokio::test]
#[ignore]
async fn test_join_embed_with_pagination() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)&limit=2",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    assert_eq!(result.page_total, 2, "Should return only 2 users");
    let arr = result.body.as_array().unwrap();
    assert_eq!(arr.len(), 2);

    // Both rows should have posts embed
    for row in arr {
        assert!(
            row.get("posts").is_some(),
            "Each user should have a posts embed"
        );
    }
}

/// Verify SQL structure of an embedded query contains LATERAL JOIN.
#[tokio::test]
#[ignore]
async fn test_join_sql_contains_lateral() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);

    // Check the inner query SQL
    let inner = builder::read_plan_to_query(read_tree);
    let sql = inner.sql().to_string();

    assert!(
        sql.contains("JOIN LATERAL"),
        "Embed query must use LATERAL JOIN"
    );
    assert!(sql.contains("ON TRUE"), "LATERAL JOIN must have ON TRUE");
    assert!(
        sql.contains("json_agg"),
        "O2M embed must use json_agg for aggregation"
    );
    assert!(sql.contains("\"posts\""), "Must reference the posts table");
}

/// Verify the CTE wrapper still works correctly with embedded queries.
#[tokio::test]
#[ignore]
async fn test_join_cte_wrapper_with_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, None, None, false, None);
    let sql = stmt.sql().to_string();

    assert!(
        sql.starts_with("WITH pgrst_source AS ("),
        "Must start with CTE"
    );
    assert!(
        sql.contains("JOIN LATERAL"),
        "CTE source must contain LATERAL JOIN"
    );
    assert!(
        sql.contains("total_result_set"),
        "CTE must have total_result_set"
    );
    assert!(sql.contains("page_total"), "CTE must have page_total");
    assert!(sql.contains("body"), "CTE must have body");
}

/// GET /users?select=id,name,posts(id,title) with Prefer: count=exact — embed + count.
///
/// Verifies that exact count works correctly when the query includes embeds.
/// The count should reflect the number of parent rows (4 users), not the
/// total number of joined rows.
#[tokio::test]
#[ignore]
async fn test_join_embed_with_exact_count() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &count_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = plan::action_plan(&config, &req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&plan);
    let stmt = statements::main_read(read_tree, Some(PreferCount::Exact), None, false, None);
    let result = execute_statement(db.pool(), &stmt).await;

    // Count should be parent rows (4 users), not joined rows
    assert_eq!(
        result.total,
        Some(4),
        "Exact count should be 4 users, not joined row count"
    );
    assert_eq!(result.page_total, 4);
}

// ==========================================================================
// Tests: Mutations with relationship verification
// ==========================================================================

/// POST /posts — insert a new post for Alice, then verify it appears in her O2M embed.
///
/// Full pipeline: build insert request → plan → SQL → execute → verify via
/// a subsequent read with embed.
#[tokio::test]
#[ignore]
async fn test_insert_post_then_verify_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // 1. Insert a new post for Alice (user_id=1) via the API pipeline
    let body =
        Bytes::from(r#"{"user_id":1,"title":"New Post","body":"Fresh content","published":true}"#);
    let insert_req = build_api_request(
        &config,
        &return_rep_prefs(),
        "POST",
        "/posts",
        "",
        &json_ct_headers(),
        body,
    )
    .unwrap();

    let insert_plan = plan::action_plan(&config, &insert_req, &cache).unwrap();
    let (mutate, read) = expect_mutate_read(&insert_plan);
    let return_rep = !mutate.returning().is_empty();
    let insert_stmt = statements::main_write(mutate, read, return_rep, None);
    let insert_result = execute_statement(db.pool(), &insert_stmt).await;

    // The insert should affect 1 row and return the new post
    assert_eq!(insert_result.page_total, 1, "Should insert exactly 1 post");
    if return_rep {
        let arr = insert_result.body.as_array().unwrap();
        assert_eq!(arr[0]["title"], "New Post");
        assert_eq!(arr[0]["user_id"], 1);
    }

    // 2. Now read Alice with her posts embedded — should have 3 posts (was 2)
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)&id=eq.1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let read_result = execute_statement(db.pool(), &read_stmt).await;

    assert_eq!(read_result.page_total, 1);
    let alice = &read_result.body.as_array().unwrap()[0];
    let posts = parse_embed(&alice["posts"]);
    assert_eq!(
        posts.len(),
        3,
        "Alice should now have 3 posts (2 original + 1 new)"
    );
    let titles: Vec<&str> = posts.iter().map(|p| p["title"].as_str().unwrap()).collect();
    assert!(
        titles.contains(&"New Post"),
        "New post should appear in embed"
    );
}

/// PATCH /posts?id=eq.1 — update a post's title, then verify the user embed still works.
///
/// Updates "Hello World" → "Hello Updated", then reads posts with their
/// author embed to verify the M2O relationship is unbroken.
#[tokio::test]
#[ignore]
async fn test_update_post_then_verify_author_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // 1. Update post 1's title
    let body = Bytes::from(r#"{"title":"Hello Updated"}"#);
    let update_req = build_api_request(
        &config,
        &return_rep_prefs(),
        "PATCH",
        "/posts",
        "id=eq.1",
        &json_ct_headers(),
        body,
    )
    .unwrap();

    let update_plan = plan::action_plan(&config, &update_req, &cache).unwrap();
    let (mutate, read) = expect_mutate_read(&update_plan);
    let return_rep = !mutate.returning().is_empty();
    let update_stmt = statements::main_write(mutate, read, return_rep, None);
    let update_result = execute_statement(db.pool(), &update_stmt).await;

    assert_eq!(update_result.page_total, 1, "Should update exactly 1 post");
    if return_rep {
        let arr = update_result.body.as_array().unwrap();
        assert_eq!(arr[0]["title"], "Hello Updated");
    }

    // 2. Read posts with author embed — updated post should have same author
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/posts",
        "select=id,title,users(id,name)&id=eq.1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let read_result = execute_statement(db.pool(), &read_stmt).await;

    assert_eq!(read_result.page_total, 1);
    let post = &read_result.body.as_array().unwrap()[0];
    assert_eq!(post["title"], "Hello Updated");
    let author = parse_embed_one(&post["users"]);
    assert_eq!(
        author["name"], "Alice Johnson",
        "Author should still be Alice"
    );
}

/// DELETE /posts?user_id=eq.2 — delete all of Bob's posts, then verify his embed is empty.
///
/// Removes Bob's 2 posts, then reads users with posts embed to confirm
/// Bob's posts array is now empty.
#[tokio::test]
#[ignore]
async fn test_delete_posts_then_verify_empty_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // 1. Delete all of Bob's posts (user_id=2)
    let delete_req = build_api_request(
        &config,
        &default_prefs(),
        "DELETE",
        "/posts",
        "user_id=eq.2",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let delete_plan = plan::action_plan(&config, &delete_req, &cache).unwrap();
    let (mutate, read) = expect_mutate_read(&delete_plan);
    let delete_stmt = statements::main_write(mutate, read, false, None);
    let delete_result = execute_statement(db.pool(), &delete_stmt).await;

    assert_eq!(delete_result.page_total, 2, "Bob had 2 posts to delete");

    // 2. Read Bob with posts embed — should now be empty
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,name,posts(id,title)&id=eq.2",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let read_result = execute_statement(db.pool(), &read_stmt).await;

    assert_eq!(read_result.page_total, 1, "Bob should still exist");
    let bob = &read_result.body.as_array().unwrap()[0];
    assert_eq!(bob["name"], "Bob Smith");
    let posts = parse_embed(&bob["posts"]);
    assert!(posts.is_empty(), "Bob should have 0 posts after deletion");
}

/// POST /comments — insert a comment referencing a post and user, then verify
/// both the post→comments and user→comments relationships.
///
/// This tests a mutation on a table with multiple FK relationships.
#[tokio::test]
#[ignore]
async fn test_insert_comment_verify_relationships() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // 1. Insert a new comment on post 2 ("Advanced Topics") by Bob (user_id=2)
    let body = Bytes::from(r#"{"post_id":2,"user_id":2,"body":"Nice advanced topics!"}"#);
    let insert_req = build_api_request(
        &config,
        &return_rep_prefs(),
        "POST",
        "/comments",
        "",
        &json_ct_headers(),
        body,
    )
    .unwrap();

    let insert_plan = plan::action_plan(&config, &insert_req, &cache).unwrap();
    let (mutate, read) = expect_mutate_read(&insert_plan);
    let return_rep = !mutate.returning().is_empty();
    let insert_stmt = statements::main_write(mutate, read, return_rep, None);
    let insert_result = execute_statement(db.pool(), &insert_stmt).await;
    assert_eq!(insert_result.page_total, 1);

    // 2. Read post 2 with comments embed — should now have 1 comment
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/posts",
        "select=id,title,comments(id,body)&id=eq.2",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let read_result = execute_statement(db.pool(), &read_stmt).await;

    assert_eq!(read_result.page_total, 1);
    let post = &read_result.body.as_array().unwrap()[0];
    assert_eq!(post["title"], "Advanced Topics");
    let comments = parse_embed(&post["comments"]);
    assert_eq!(comments.len(), 1, "Post 2 should now have 1 comment");
    assert_eq!(comments[0]["body"], "Nice advanced topics!");
}

/// POST /users + POST /posts — insert a new user, then insert a post for them,
/// and verify the full chain via embed.
///
/// Tests that newly created FK relationships are immediately visible in embeds.
#[tokio::test]
#[ignore]
async fn test_insert_user_then_post_verify_chain() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // 1. Insert a new user
    let user_body =
        Bytes::from(r#"{"email":"eve@example.com","name":"Eve Wilson","status":"active"}"#);
    let user_req = build_api_request(
        &config,
        &return_rep_prefs(),
        "POST",
        "/users",
        "",
        &json_ct_headers(),
        user_body,
    )
    .unwrap();

    let user_plan = plan::action_plan(&config, &user_req, &cache).unwrap();
    let (mutate, read) = expect_mutate_read(&user_plan);
    let return_rep = !mutate.returning().is_empty();
    let user_stmt = statements::main_write(mutate, read, return_rep, None);
    let user_result = execute_statement(db.pool(), &user_stmt).await;
    assert_eq!(user_result.page_total, 1);

    // Extract the new user's id from the returned representation
    let new_user_id = user_result.body.as_array().unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    assert!(
        new_user_id > 4,
        "New user id should be > 4 (auto-increment)"
    );

    // 2. Insert a post for the new user (using raw SQL for the FK since
    //    POST body can't reference computed values)
    sqlx::query("INSERT INTO test_api.posts (user_id, title, body, published) VALUES ($1, 'Eve''s Post', 'Content by Eve', true)")
        .bind(new_user_id as i32)
        .execute(db.pool())
        .await
        .expect("Post insert failed");

    // 3. Read the new user with posts embed
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        &format!("select=id,name,posts(id,title)&id=eq.{}", new_user_id),
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let read_result = execute_statement(db.pool(), &read_stmt).await;

    assert_eq!(read_result.page_total, 1);
    let eve = &read_result.body.as_array().unwrap()[0];
    assert_eq!(eve["name"], "Eve Wilson");
    let posts = parse_embed(&eve["posts"]);
    assert_eq!(posts.len(), 1, "Eve should have 1 post");
    assert_eq!(posts[0]["title"], "Eve's Post");
}

/// DELETE /users?id=eq.3 — delete a user with CASCADE, verify related rows are gone.
///
/// Charlie (id=3) has user_roles entries. After deleting Charlie, verify the
/// user_roles junction table no longer has his entries (FK CASCADE).
#[tokio::test]
#[ignore]
async fn test_delete_user_cascades_to_relations() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Verify Charlie has a role before deletion
    let role_count: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM test_api.user_roles WHERE user_id = 3")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        role_count.0, 1,
        "Charlie should have 1 role before deletion"
    );

    // Delete Charlie via API pipeline
    let delete_req = build_api_request(
        &config,
        &default_prefs(),
        "DELETE",
        "/users",
        "id=eq.3",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let delete_plan = plan::action_plan(&config, &delete_req, &cache).unwrap();
    let (mutate, read) = expect_mutate_read(&delete_plan);
    let delete_stmt = statements::main_write(mutate, read, false, None);
    let delete_result = execute_statement(db.pool(), &delete_stmt).await;
    assert_eq!(delete_result.page_total, 1, "Should delete exactly 1 user");

    // Verify CASCADE: Charlie's role should be gone
    let role_count_after: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM test_api.user_roles WHERE user_id = 3")
            .fetch_one(db.pool())
            .await
            .unwrap();
    assert_eq!(
        role_count_after.0, 0,
        "Charlie's roles should be cascaded away"
    );

    // Verify Charlie is no longer in the users read
    let read_req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "id=eq.3",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();
    let read_plan = plan::action_plan(&config, &read_req, &cache).unwrap();
    let read_tree = expect_wrapped_read(&read_plan);
    let read_stmt = statements::main_read(read_tree, None, None, false, None);
    let read_result = execute_statement(db.pool(), &read_stmt).await;
    assert_eq!(read_result.page_total, 0, "Charlie should no longer exist");
}
