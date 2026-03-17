//! Integration tests for schema cache
//!
//! These tests require Docker to be running as they use testcontainers
//! to spin up a PostgreSQL instance.
//!
//! Run with: `cargo test --test schema_cache_integration`
//!
//! Note: These tests are marked as #[ignore] by default because they require
//! Docker. Run with `cargo test --test schema_cache_integration -- --ignored`

#![allow(clippy::field_reassign_with_default)]

mod common;

use dbrest::config::AppConfig;
use dbrest::schema_cache::{
    SchemaCache,
    db::{ColumnJson, DbIntrospector, RelationshipRow, RoutineRow, TableRow},
};
use sqlx::PgPool;

/// Real database introspector using sqlx
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
    async fn query_tables(&self, schemas: &[String]) -> Result<Vec<TableRow>, dbrest::Error> {
        // Simplified query for testing
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
        .map_err(|e| dbrest::Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        // Get columns for each table
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

    async fn query_relationships(&self) -> Result<Vec<RelationshipRow>, dbrest::Error> {
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
        .map_err(|e| dbrest::Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        // Get column mappings for each relationship
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

    async fn query_routines(&self, schemas: &[String]) -> Result<Vec<RoutineRow>, dbrest::Error> {
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
            WHERE pn.nspname = ANY($1)
                AND p.prokind IN ('f', 'p')
            ORDER BY pn.nspname, p.proname
            "#,
        )
        .bind(schemas)
        .fetch_all(self.pool)
        .await
        .map_err(|e| dbrest::Error::Database {
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
                params_json: "[]".to_string(), // Simplified
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
        schemas: &[String],
    ) -> Result<Vec<dbrest::schema_cache::ComputedFieldRow>, dbrest::Error> {
        use dbrest::schema_cache::queries::computed_fields::COMPUTED_FIELDS_QUERY;

        let rows = sqlx::query_as::<_, (String, String, String, String, String, bool)>(
            COMPUTED_FIELDS_QUERY,
        )
        .bind(schemas)
        .fetch_all(self.pool)
        .await
        .map_err(|e| dbrest::Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    table_schema,
                    table_name,
                    function_schema,
                    function_name,
                    return_type,
                    returns_set,
                )| {
                    dbrest::schema_cache::ComputedFieldRow {
                        table_schema,
                        table_name,
                        function_schema,
                        function_name,
                        return_type,
                        returns_set,
                    }
                },
            )
            .collect())
    }

    async fn query_timezones(&self) -> Result<Vec<String>, dbrest::Error> {
        let rows: Vec<(String,)> = sqlx::query_as("SELECT name FROM pg_timezone_names LIMIT 100")
            .fetch_all(self.pool)
            .await
            .map_err(|e| dbrest::Error::Database {
                code: None,
                message: e.to_string(),
                detail: None,
                hint: None,
            })?;

        Ok(rows.into_iter().map(|(name,)| name).collect())
    }
}

impl SqlxIntrospector<'_> {
    async fn get_columns(
        &self,
        schema: &str,
        table: &str,
    ) -> Result<Vec<ColumnJson>, dbrest::Error> {
        let rows = sqlx::query_as::<
            _,
            (
                String,
                Option<String>,
                bool,
                String,
                String,
                Option<i32>,
                Option<String>,
            ),
        >(
            r#"
            SELECT
                a.attname AS name,
                d.description,
                NOT a.attnotnull AS nullable,
                format_type(a.atttypid, NULL) AS data_type,
                format_type(a.atttypid, a.atttypmod) AS nominal_type,
                information_schema._pg_char_max_length(a.atttypid, a.atttypmod)::int AS max_length,
                pg_get_expr(ad.adbin, ad.adrelid) AS default_value
            FROM pg_attribute a
            JOIN pg_class c ON a.attrelid = c.oid
            JOIN pg_namespace n ON c.relnamespace = n.oid
            LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum
            LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
            WHERE n.nspname = $1 AND c.relname = $2
                AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            "#,
        )
        .bind(schema)
        .bind(table)
        .fetch_all(self.pool)
        .await
        .map_err(|e| dbrest::Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None,
        })?;

        Ok(rows
            .into_iter()
            .map(
                |(name, desc, nullable, data_type, nominal_type, max_length, default)| ColumnJson {
                    name,
                    description: desc,
                    nullable,
                    data_type,
                    nominal_type,
                    max_length,
                    default,
                    enum_values: vec![],
                    is_composite: false,
                    composite_type_schema: None,
                    composite_type_name: None,
                },
            )
            .collect())
    }

    async fn get_fk_columns(
        &self,
        constraint: &str,
    ) -> Result<Vec<(String, String)>, dbrest::Error> {
        let rows = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT cols.attname, refs.attname
            FROM pg_constraint c
            CROSS JOIN LATERAL unnest(c.conkey, c.confkey) WITH ORDINALITY AS t(col_num, ref_num, ord)
            JOIN pg_attribute cols ON cols.attrelid = c.conrelid AND cols.attnum = t.col_num
            JOIN pg_attribute refs ON refs.attrelid = c.confrelid AND refs.attnum = t.ref_num
            WHERE c.conname = $1
            ORDER BY t.ord
            "#,
        )
        .bind(constraint)
        .fetch_all(self.pool)
        .await
        .map_err(|e| dbrest::Error::Database {
            code: None,
            message: e.to_string(),
            detail: None,
            hint: None
        })?;

        Ok(rows)
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_load_tables() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Should have tables from test_api schema
    assert!(cache.table_count() > 0);

    // Check specific tables exist
    assert!(cache.get_table_by_name("test_api", "users").is_some());
    assert!(cache.get_table_by_name("test_api", "posts").is_some());
    assert!(cache.get_table_by_name("test_api", "comments").is_some());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_load_columns() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    let users = cache.get_table_by_name("test_api", "users").unwrap();

    // Check columns exist
    assert!(users.get_column("id").is_some());
    assert!(users.get_column("email").is_some());
    assert!(users.get_column("name").is_some());
    assert!(users.get_column("status").is_some());

    // Check column types
    let id_col = users.get_column("id").unwrap();
    assert!(id_col.is_numeric_type());

    let email_col = users.get_column("email").unwrap();
    assert!(email_col.is_text_type());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_load_relationships() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Check relationships exist
    assert!(cache.relationship_count() > 0);

    // Posts should have relationship to users
    let posts_qi = dbrest::QualifiedIdentifier::new("test_api", "posts");
    let rels = cache.find_relationships(&posts_qi);
    assert!(!rels.is_empty());

    // At least one should point to users
    let to_users = cache.find_relationships_to(&posts_qi, "users");
    assert!(!to_users.is_empty());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_load_views() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Check views are loaded
    let active_users = cache.get_table_by_name("test_api", "active_users");
    assert!(active_users.is_some());

    let view = active_users.unwrap();
    assert!(view.is_view);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_load_routines() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Check routines are loaded
    assert!(cache.routine_count() > 0);

    // Check specific functions exist
    let add_func = cache.get_routines_by_name("test_api", "add_numbers");
    assert!(add_func.is_some());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_load_timezones() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let config = AppConfig::default();
    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Should have timezones
    assert!(!cache.timezones.is_empty());

    // UTC should always be valid
    assert!(cache.is_valid_timezone("UTC"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_table_primary_key() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Users table should have PK
    let users = cache.get_table_by_name("test_api", "users").unwrap();
    assert!(users.has_pk());
    assert!(users.is_pk_column("id"));

    // Profiles has composite-ish PK (just user_id)
    let profiles = cache.get_table_by_name("test_api", "profiles").unwrap();
    assert!(profiles.has_pk());
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_self_referencing_relationship() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Comments has self-referencing FK (parent_id)
    let comments_qi = dbrest::QualifiedIdentifier::new("test_api", "comments");
    let rels = cache.find_relationships(&comments_qi);

    // Should have a self-referencing relationship
    let self_rels: Vec<_> = rels.iter().filter(|r| r.is_self()).collect();
    assert!(
        !self_rels.is_empty(),
        "Comments should have self-referencing relationship"
    );
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_schema_cache_summary() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    let summary = cache.summary();
    assert!(summary.contains("tables"));
    assert!(summary.contains("relationships"));
    assert!(summary.contains("routines"));
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_tables_in_schema() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    let tables: Vec<_> = cache.tables_in_schema("test_api").collect();
    assert!(!tables.is_empty());

    // All should be in test_api schema
    for table in &tables {
        assert_eq!(table.schema.as_str(), "test_api");
    }
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_load_computed_fields() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Find users table
    let users_qi = dbrest::types::QualifiedIdentifier::new("test_api", "users");
    let users_table = cache
        .tables
        .get(&users_qi)
        .expect("users table should exist");

    // Check that computed fields are loaded
    assert!(users_table.computed_fields.contains_key("full_name"));
    assert!(users_table.computed_fields.contains_key("initials"));

    // Verify computed field structure
    let full_name_cf = users_table.get_computed_field("full_name").unwrap();
    assert_eq!(full_name_cf.function.schema.as_str(), "test_api");
    assert_eq!(full_name_cf.function.name.as_str(), "full_name");
    assert_eq!(full_name_cf.return_type.as_str(), "text");
    assert!(!full_name_cf.returns_set);

    let initials_cf = users_table.get_computed_field("initials").unwrap();
    assert_eq!(initials_cf.function.schema.as_str(), "test_api");
    assert_eq!(initials_cf.function.name.as_str(), "initials");
    assert_eq!(initials_cf.return_type.as_str(), "text");
    assert!(!initials_cf.returns_set);
}

#[tokio::test]
#[ignore = "requires Docker"]
async fn test_computed_fields_with_extra_search_path() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");

    // Create a function in a different schema
    sqlx::query("CREATE SCHEMA IF NOT EXISTS extensions")
        .execute(db.pool())
        .await
        .unwrap();

    sqlx::query(
        r#"
        CREATE FUNCTION extensions.display_name(u test_api.users)
        RETURNS TEXT
        LANGUAGE SQL
        IMMUTABLE
        AS $$
            SELECT u.name || ' (' || u.email || ')';
        $$;
        "#,
    )
    .execute(db.pool())
    .await
    .unwrap();

    let introspector = SqlxIntrospector::new(db.pool());

    let mut config = AppConfig::default();
    config.db_schemas = vec!["test_api".to_string()];
    config.db_extra_search_path = vec!["extensions".to_string()];

    let cache = SchemaCache::load(&introspector, &config).await.unwrap();

    // Find users table
    let users_qi = dbrest::types::QualifiedIdentifier::new("test_api", "users");
    let users_table = cache
        .tables
        .get(&users_qi)
        .expect("users table should exist");

    // Check that computed field from extra search path is loaded
    assert!(users_table.computed_fields.contains_key("display_name"));

    let display_name_cf = users_table.get_computed_field("display_name").unwrap();
    assert_eq!(display_name_cf.function.schema.as_str(), "extensions");
    assert_eq!(display_name_cf.function.name.as_str(), "display_name");
}
