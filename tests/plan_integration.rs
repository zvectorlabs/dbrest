//! Deep integration tests for the plan module.
//!
//! These tests spin up a real PostgreSQL instance via testcontainers,
//! load the schema cache from it, and run `action_plan()` end-to-end
//! to verify that API requests are correctly planned against live schema data.
//!
//! Run with: `cargo test --test plan_integration -- --ignored`
//!
//! Requires Docker.

mod common;

use bytes::Bytes;
use pgrest::api_request::preferences::{PreferRepresentation, PreferResolution, Preferences};
use pgrest::api_request::{self, ApiRequest};
use pgrest::config::AppConfig;
use pgrest::error::Error;
use pgrest::plan::{
    ActionPlan, CallPlan, CrudPlan, DbActionPlan, InfoPlan, MutatePlan, ReadPlanTree, action_plan,
};
use pgrest::schema_cache::{
    SchemaCache,
    db::{ColumnJson, DbIntrospector, RelationshipRow, RoutineRow, TableRow},
};
use sqlx::PgPool;

// ==========================================================================
// Test infrastructure
// ==========================================================================

fn test_config() -> AppConfig {
    AppConfig {
        db_schemas: vec!["test_api".to_string()],
        ..Default::default()
    }
}

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

fn default_prefs() -> Preferences {
    Preferences::default()
}

fn return_rep_prefs() -> Preferences {
    Preferences {
        representation: Some(PreferRepresentation::Full),
        ..Default::default()
    }
}

fn upsert_prefs() -> Preferences {
    Preferences {
        representation: Some(PreferRepresentation::Full),
        resolution: Some(PreferResolution::MergeDuplicates),
        ..Default::default()
    }
}

fn json_headers() -> Vec<(String, String)> {
    vec![("accept".to_string(), "application/json".to_string())]
}

fn csv_headers() -> Vec<(String, String)> {
    vec![("accept".to_string(), "text/csv".to_string())]
}

fn json_ct_headers() -> Vec<(String, String)> {
    vec![
        ("accept".to_string(), "application/json".to_string()),
        ("content-type".to_string(), "application/json".to_string()),
    ]
}

// ==========================================================================
// SqlxIntrospector — reused from schema_cache_integration.rs
// ==========================================================================

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
// Helper: extract plan variant
// ==========================================================================

fn expect_wrapped_read(plan: ActionPlan) -> ReadPlanTree {
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { read_plan, .. },
            ..
        }) => read_plan,
        other => panic!("Expected WrappedReadPlan, got {other:?}"),
    }
}

fn expect_mutate_read(plan: ActionPlan) -> (MutatePlan, ReadPlanTree) {
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

#[allow(dead_code)]
fn expect_call_read(plan: ActionPlan) -> (CallPlan, ReadPlanTree) {
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan:
                CrudPlan::CallReadPlan {
                    call_plan,
                    read_plan,
                    ..
                },
            ..
        }) => (call_plan, read_plan),
        other => panic!("Expected CallReadPlan, got {other:?}"),
    }
}

// ==========================================================================
// Tests: Basic table read plans
// ==========================================================================

#[tokio::test]
#[ignore] // Requires Docker
async fn test_plan_simple_read() {
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
        "select=id,name,email",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    // Root node should target test_api.users
    assert_eq!(tree.node.from.name.as_str(), "users");
    assert_eq!(tree.node.from.schema.as_str(), "test_api");
    assert_eq!(tree.node.depth, 0);
    assert!(tree.children().is_empty());

    // Should have 3 select fields
    assert_eq!(tree.node.select.len(), 3);
    assert_eq!(tree.node.select[0].field.name.as_str(), "id");
    assert_eq!(tree.node.select[1].field.name.as_str(), "name");
    assert_eq!(tree.node.select[2].field.name.as_str(), "email");
}

#[tokio::test]
#[ignore]
async fn test_plan_read_all_columns() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Default select (no `select=`) returns all columns
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

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);
    assert_eq!(tree.node.from.name.as_str(), "users");
}

#[tokio::test]
#[ignore]
async fn test_plan_read_with_filter() {
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
        "select=id,name&status=eq.active",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    // Should have a filter in the WHERE clause
    assert!(!tree.node.where_.is_empty(), "Expected a WHERE filter");
}

#[tokio::test]
#[ignore]
async fn test_plan_read_with_order() {
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
        "select=id,name&order=name.asc,id.desc",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);
    assert_eq!(tree.node.order.len(), 2);
}

#[tokio::test]
#[ignore]
async fn test_plan_read_with_range() {
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
        "select=id&limit=10&offset=5",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);
    // limit_to is an inclusive upper-bound index: offset + limit - 1 = 5 + 10 - 1 = 14
    assert_eq!(tree.node.range.limit_to, Some(14));
    assert_eq!(tree.node.range.offset, 5);
}

#[tokio::test]
#[ignore]
async fn test_plan_read_max_rows_limit() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let mut config = test_config();
    config.db_max_rows = Some(50);

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);
    // Should cap at max_rows when range is unbounded
    assert_eq!(tree.node.range.limit_to, Some(50));
}

// ==========================================================================
// Tests: Embedded relations (joins)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_read_with_m2o_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Posts embedding the author (M2O via user_id → users.id)
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

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    assert_eq!(tree.node.from.name.as_str(), "posts");
    assert_eq!(tree.node.select.len(), 2); // id, title
    assert_eq!(tree.children().len(), 1); // users embed

    let child = &tree.children()[0];
    assert_eq!(child.node.from.name.as_str(), "users");
    assert_eq!(child.node.rel_name.as_str(), "users");
    assert_eq!(child.node.depth, 1);
    assert_eq!(child.node.select.len(), 2); // id, name
    assert!(child.node.rel_to_parent.is_some());
}

#[tokio::test]
#[ignore]
async fn test_plan_read_with_o2m_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Users embedding their posts (O2M via users.id ← posts.user_id)
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

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    assert_eq!(tree.node.from.name.as_str(), "users");
    assert_eq!(tree.children().len(), 1);

    let child = &tree.children()[0];
    assert_eq!(child.node.from.name.as_str(), "posts");
    assert_eq!(child.node.depth, 1);
}

#[tokio::test]
#[ignore]
async fn test_plan_read_with_nested_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Users → posts → comments (3-level nesting)
    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,posts(id,comments(id,body))",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    assert_eq!(tree.node_count(), 3);
    assert_eq!(tree.max_depth(), 2);
    assert_eq!(tree.children().len(), 1);
    assert_eq!(tree.children()[0].children().len(), 1);
    assert_eq!(
        tree.children()[0].children()[0].node.from.name.as_str(),
        "comments"
    );
}

#[tokio::test]
#[ignore]
async fn test_plan_read_self_referencing_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Comments embedding their parent comment (self-reference)
    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/comments",
        "select=id,body,comments(id,body)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache);
    // Self-referencing may produce an ambiguous embed or succeed — just verify no panic
    assert!(
        plan.is_ok() || matches!(plan, Err(Error::AmbiguousEmbedding(_))),
        "Expected either success or AmbiguousEmbedding, got: {plan:?}"
    );
}

// ==========================================================================
// Tests: Views
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_read_from_view() {
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
        "select=id,name",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);
    assert_eq!(tree.node.from.name.as_str(), "active_users");
}

// ==========================================================================
// Tests: Mutation plans (INSERT / UPDATE / DELETE)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_insert() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let body = Bytes::from(r#"{"email":"new@test.com","name":"New User"}"#);
    let req = build_api_request(
        &config,
        &return_rep_prefs(),
        "POST",
        "/users",
        "",
        &json_ct_headers(),
        body,
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let (mutate, _read) = expect_mutate_read(plan);

    assert!(matches!(mutate, MutatePlan::Insert(_)));
    if let MutatePlan::Insert(insert) = mutate {
        assert_eq!(insert.into.name.as_str(), "users");
        assert!(
            !insert.returning.is_empty(),
            "Prefer: return=representation should have returning"
        );
    }
}

#[tokio::test]
#[ignore]
async fn test_plan_update() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let body = Bytes::from(r#"{"name":"Updated Name"}"#);
    let req = build_api_request(
        &config,
        &return_rep_prefs(),
        "PATCH",
        "/users",
        "id=eq.1",
        &json_ct_headers(),
        body,
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let (mutate, read) = expect_mutate_read(plan);

    assert!(matches!(mutate, MutatePlan::Update(_)));
    if let MutatePlan::Update(update) = mutate {
        assert_eq!(update.into.name.as_str(), "users");
        assert!(
            !update.where_.is_empty(),
            "Expected a WHERE clause for UPDATE"
        );
    }
    assert!(
        !read.node.where_.is_empty(),
        "Read plan should also have filter"
    );
}

#[tokio::test]
#[ignore]
async fn test_plan_delete() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "DELETE",
        "/users",
        "id=eq.1",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let (mutate, _read) = expect_mutate_read(plan);

    assert!(matches!(mutate, MutatePlan::Delete(_)));
    if let MutatePlan::Delete(delete) = mutate {
        assert_eq!(delete.from.name.as_str(), "users");
        assert!(!delete.where_.is_empty(), "Expected WHERE for DELETE");
    }
}

// ==========================================================================
// Tests: Content negotiation
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_content_negotiation_csv() {
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
        &csv_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { media, .. },
            ..
        }) => {
            assert_eq!(media.as_str(), "text/csv");
        }
        other => panic!("Expected WrappedReadPlan, got {other:?}"),
    }
}

#[tokio::test]
#[ignore]
async fn test_plan_content_negotiation_json() {
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
        "select=id",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { media, .. },
            ..
        }) => {
            assert_eq!(media.as_str(), "application/json");
        }
        other => panic!("Expected WrappedReadPlan, got {other:?}"),
    }
}

// ==========================================================================
// Tests: INFO / OPTIONS plans
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_options_table() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "OPTIONS",
        "/users",
        "",
        &[],
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    assert!(
        matches!(plan, ActionPlan::NoDb(InfoPlan::RelInfoPlan(_))),
        "Expected RelInfoPlan, got {plan:?}"
    );
}

#[tokio::test]
#[ignore]
async fn test_plan_options_root() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "OPTIONS",
        "/",
        "",
        &[],
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    assert!(
        matches!(plan, ActionPlan::NoDb(InfoPlan::SchemaInfoPlan)),
        "Expected SchemaInfoPlan, got {plan:?}"
    );
}

// ==========================================================================
// Tests: Schema read (GET /)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_schema_read() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    match plan {
        ActionPlan::Db(DbActionPlan::MayUseDb(inspect)) => {
            assert_eq!(inspect.schema.as_str(), "test_api");
            assert!(!inspect.headers_only);
        }
        other => panic!("Expected MayUseDb InspectPlan, got {other:?}"),
    }
}

#[tokio::test]
#[ignore]
async fn test_plan_head_schema_read() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let req = build_api_request(
        &config,
        &default_prefs(),
        "HEAD",
        "/",
        "",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    match plan {
        ActionPlan::Db(DbActionPlan::MayUseDb(inspect)) => {
            assert!(inspect.headers_only);
        }
        other => panic!("Expected MayUseDb with headers_only, got {other:?}"),
    }
}

// ==========================================================================
// Tests: Error cases
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_nonexistent_table() {
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

    let result = action_plan(&config, &req, &cache);
    assert!(result.is_err());
    assert!(
        matches!(result.unwrap_err(), Error::TableNotFound { .. }),
        "Expected TableNotFound error"
    );
}

#[tokio::test]
#[ignore]
async fn test_plan_nonexistent_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Try to embed a relation that doesn't exist
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

    let result = action_plan(&config, &req, &cache);
    assert!(
        result.is_err(),
        "Expected error for nonexistent embed, got: {result:?}"
    );
}

// ==========================================================================
// Tests: Transaction mode
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_tx_mode_read() {
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
        "select=id",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { tx_mode, .. },
            ..
        }) => {
            assert!(!tx_mode.rollback);
        }
        other => panic!("Unexpected plan: {other:?}"),
    }
}

#[tokio::test]
#[ignore]
async fn test_plan_tx_mode_rollback_all() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let mut config = test_config();
    config.db_tx_rollback_all = true;

    let body = Bytes::from(r#"{"email":"a@b.com","name":"A"}"#);
    let req = build_api_request(
        &config,
        &return_rep_prefs(),
        "POST",
        "/users",
        "",
        &json_ct_headers(),
        body,
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::MutateReadPlan { tx_mode, .. },
            ..
        }) => {
            assert!(
                tx_mode.rollback,
                "Expected rollback when db_tx_rollback_all"
            );
        }
        other => panic!("Unexpected plan: {other:?}"),
    }
}

// ==========================================================================
// Tests: HEAD requests
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_head_table() {
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
        "select=id",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    match plan {
        ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { headers_only, .. },
            ..
        }) => {
            assert!(headers_only, "HEAD should set headers_only=true");
        }
        other => panic!("Unexpected plan: {other:?}"),
    }
}

// ==========================================================================
// Tests: Multiple filters
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_multiple_filters() {
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
        "select=id,name&status=eq.active&name=like.*Alice*",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);
    assert!(
        tree.node.where_.len() >= 2,
        "Expected at least 2 filters, got {}",
        tree.node.where_.len()
    );
}

// ==========================================================================
// Tests: Complex scenarios
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_plan_multi_table_embed() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Users embedding both posts and profiles simultaneously
    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/users",
        "select=id,posts(id,title),profiles(avatar_url)",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    assert_eq!(tree.node.from.name.as_str(), "users");
    assert_eq!(tree.children().len(), 2, "Expected two embedded relations");

    let child_names: Vec<&str> = tree
        .children()
        .iter()
        .map(|c| c.node.rel_name.as_str())
        .collect();
    assert!(child_names.contains(&"posts"), "Expected posts embed");
    assert!(child_names.contains(&"profiles"), "Expected profiles embed");
}

#[tokio::test]
#[ignore]
async fn test_plan_insert_with_on_conflict() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    let body = Bytes::from(r#"{"email":"alice@example.com","name":"Alice Updated"}"#);
    let req = build_api_request(
        &config,
        &upsert_prefs(),
        "POST",
        "/users",
        "on_conflict=email",
        &json_ct_headers(),
        body,
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let (mutate, _read) = expect_mutate_read(plan);

    if let MutatePlan::Insert(insert) = mutate {
        assert!(
            insert.on_conflict.is_some(),
            "Expected on_conflict for upsert"
        );
        let oc = insert.on_conflict.unwrap();
        assert!(oc.merge_duplicates, "Expected merge_duplicates=true");
    } else {
        panic!("Expected InsertPlan");
    }
}

#[tokio::test]
#[ignore]
async fn test_plan_read_iterator_depth_first() {
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
        "select=id,posts(id,comments(id))",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    // Verify depth-first iteration
    let node_names: Vec<&str> = tree.iter().map(|n| n.rel_name.as_str()).collect();
    assert_eq!(node_names.len(), 3);
    assert_eq!(node_names[0], "users");
    assert_eq!(node_names[1], "posts");
    assert_eq!(node_names[2], "comments");
}

#[tokio::test]
#[ignore]
async fn test_plan_read_with_tasks() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let cache = load_schema_cache(db.pool()).await;
    let config = test_config();

    // Tasks table with various column types
    let req = build_api_request(
        &config,
        &default_prefs(),
        "GET",
        "/tasks",
        "select=id,title,priority,is_completed&priority=eq.high&order=due_date.asc",
        &json_headers(),
        Bytes::new(),
    )
    .unwrap();

    let plan = action_plan(&config, &req, &cache).unwrap();
    let tree = expect_wrapped_read(plan);

    assert_eq!(tree.node.from.name.as_str(), "tasks");
    assert_eq!(tree.node.select.len(), 4);
    assert!(!tree.node.where_.is_empty());
    assert!(!tree.node.order.is_empty());
}
