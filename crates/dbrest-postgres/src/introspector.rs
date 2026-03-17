//! PostgreSQL introspector — production implementation backed by sqlx::PgPool.

use async_trait::async_trait;

use dbrest_core::error::Error;
use dbrest_core::schema_cache::db::{
    ColumnJson, ComputedFieldRow, DbIntrospector, RelationshipRow, RoutineRow, TableRow,
};
use dbrest_core::schema_cache::queries::computed_fields::COMPUTED_FIELDS_QUERY;

/// Production database introspector backed by a real `sqlx::PgPool`.
///
/// Queries `pg_catalog` system tables to discover tables, columns,
/// relationships, routines, and timezones.
pub struct SqlxIntrospector<'a> {
    pool: &'a sqlx::PgPool,
}

impl<'a> SqlxIntrospector<'a> {
    /// Create a new introspector from a connection pool.
    pub fn new(pool: &'a sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Fetch column metadata for a table.
    async fn get_columns(&self, schema: &str, table: &str) -> Result<Vec<ColumnJson>, Error> {
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
            WHERE n.nspname = $1 AND c.relname = $2
                AND a.attnum > 0 AND NOT a.attisdropped
            GROUP BY a.attnum, a.attname, d.description, a.attnotnull,
                     a.atttypid, ad.adbin, ad.adrelid, a.atttypmod, e.enumlabel
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
                |(name, desc, nullable, data_type, default, max_len, enum_info)| ColumnJson {
                    is_composite: false,
                    composite_type_schema: None,
                    composite_type_name: None,
                    name: name.clone(),
                    description: desc,
                    nullable,
                    data_type: data_type.clone(),
                    nominal_type: data_type,
                    max_length: max_len,
                    default: if default.is_empty() {
                        None
                    } else {
                        Some(default)
                    },
                    enum_values: if enum_info.is_some() {
                        vec!["enum".to_string()]
                    } else {
                        vec![]
                    },
                },
            )
            .collect())
    }

    /// Fetch FK column pairs for a constraint.
    async fn get_fk_columns(&self, constraint_name: &str) -> Result<Vec<(String, String)>, Error> {
        let rows = sqlx::query_as::<_, (String, String)>(
            r#"
            SELECT
                a1.attname AS local_col,
                a2.attname AS foreign_col
            FROM pg_constraint c
            JOIN pg_attribute a1 ON a1.attrelid = c.conrelid AND a1.attnum = ANY(c.conkey)
            JOIN pg_attribute a2 ON a2.attrelid = c.confrelid AND a2.attnum = ANY(c.confkey)
            WHERE c.conname = $1 AND c.contype = 'f'
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

#[async_trait]
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
                has_table_privilege(c.oid, 'SELECT') AS readable,
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
        for (schema, name, desc, is_view, insertable, updatable, deletable, readable, pk_cols) in
            rows
        {
            let columns = self.get_columns(&schema, &name).await?;
            result.push(TableRow {
                table_schema: schema,
                table_name: name,
                table_description: desc,
                is_view,
                insertable,
                updatable,
                deletable,
                readable,
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
        let rows = sqlx::query_as::<_, (String, String, Option<String>, String, bool, bool)>(
            r#"
            SELECT
                pn.nspname AS routine_schema,
                p.proname AS routine_name,
                d.description,
                p.provolatile::text AS volatility,
                p.provariadic != 0 AS is_variadic,
                has_function_privilege(p.oid, 'EXECUTE') AS executable
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

        Ok(rows
            .into_iter()
            .map(|(schema, name, desc, vol, variadic, executable)| RoutineRow {
                routine_schema: schema,
                routine_name: name,
                description: desc,
                params_json: "[]".to_string(),
                return_type_json: r#"{"kind":"single","type_kind":"scalar","type_schema":"pg_catalog","type_name":"void","is_alias":false}"#.to_string(),
                volatility: vol,
                is_variadic: variadic,
                executable,
            })
            .collect())
    }

    async fn query_computed_fields(
        &self,
        schemas: &[String],
    ) -> Result<Vec<ComputedFieldRow>, Error> {
        tracing::debug!("Querying computed fields for schemas: {:?}", schemas);

        let rows = sqlx::query_as::<_, (String, String, String, String, String, bool)>(
            COMPUTED_FIELDS_QUERY,
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

        let computed_fields: Vec<ComputedFieldRow> = rows
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
                    tracing::trace!(
                        "Found computed field: {}.{} for table {}.{} returns {}",
                        function_schema,
                        function_name,
                        table_schema,
                        table_name,
                        return_type
                    );
                    ComputedFieldRow {
                        table_schema,
                        table_name,
                        function_schema,
                        function_name,
                        return_type,
                        returns_set,
                    }
                },
            )
            .collect();

        tracing::debug!("Found {} computed fields", computed_fields.len());
        Ok(computed_fields)
    }

    async fn query_timezones(&self) -> Result<Vec<String>, Error> {
        // Query timezones ordered by name, limit to 100 for performance
        let rows: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM pg_timezone_names ORDER BY name LIMIT 100")
                .fetch_all(self.pool)
                .await
                .map_err(|e| Error::Database {
                    code: None,
                    message: e.to_string(),
                    detail: None,
                    hint: None,
                })?;

        // Convert to Vec<String>
        let mut timezones: Vec<String> = rows.into_iter().map(|(name,)| name).collect();

        // Ensure UTC is always included (it's commonly used)
        // Check using string slice comparison
        let has_utc = timezones.iter().any(|tz| tz == "UTC");
        if !has_utc {
            // If we're at the limit, remove the last one to make room for UTC
            if timezones.len() >= 100 {
                timezones.pop();
            }
            timezones.push("UTC".to_string());
            // Re-sort to maintain alphabetical order
            timezones.sort();
        }

        Ok(timezones)
    }
}
