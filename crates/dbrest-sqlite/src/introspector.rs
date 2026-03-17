//! SQLite schema introspector — implements [`DbIntrospector`] for `sqlx::SqlitePool`.
//!
//! Uses `sqlite_master`, `PRAGMA table_info()`, and `PRAGMA foreign_key_list()`
//! to discover tables, columns, and relationships.

use async_trait::async_trait;
use sqlx::Row;

use dbrest_core::error::Error;
use dbrest_core::schema_cache::db::{
    ComputedFieldRow, DbIntrospector, RelationshipRow, RoutineRow, TableRow,
};

use crate::executor::map_sqlx_error;

/// SQLite introspector backed by `sqlx::SqlitePool`.
pub struct SqliteIntrospector<'a> {
    pool: &'a sqlx::SqlitePool,
}

impl<'a> SqliteIntrospector<'a> {
    pub fn new(pool: &'a sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl DbIntrospector for SqliteIntrospector<'_> {
    async fn query_tables(&self, _schemas: &[String]) -> Result<Vec<TableRow>, Error> {
        // SQLite has no schemas (we treat everything as "main").
        // Query sqlite_master for tables and views.
        let rows = sqlx::query(
            r#"
            SELECT
                type,
                name
            FROM sqlite_master
            WHERE type IN ('table', 'view')
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '_dbrest_%'
            ORDER BY name
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(map_sqlx_error)?;

        let mut tables = Vec::with_capacity(rows.len());

        for row in &rows {
            let obj_type: String = row.try_get("type").unwrap_or_default();
            let name: String = row.try_get("name").unwrap_or_default();
            let is_view = obj_type == "view";

            // Get column info via PRAGMA
            let pragma_sql = format!("PRAGMA table_info(\"{}\")", name.replace('"', "\"\""));
            let col_rows = sqlx::query(&pragma_sql)
                .fetch_all(self.pool)
                .await
                .map_err(map_sqlx_error)?;

            let mut pk_cols = Vec::new();
            let mut columns_json_parts = Vec::new();

            for col in &col_rows {
                let col_name: String = col.try_get("name").unwrap_or_default();
                let col_type: String = col.try_get("type").unwrap_or_default();
                let not_null: bool = col.try_get::<i32, _>("notnull").unwrap_or(0) != 0;
                let pk: i32 = col.try_get("pk").unwrap_or(0);
                let dflt: Option<String> = col.try_get("dflt_value").ok();

                if pk > 0 {
                    pk_cols.push(col_name.clone());
                }

                // Build a JSON object for each column matching the expected format
                let col_json = serde_json::json!({
                    "name": col_name,
                    "data_type": normalize_sqlite_type(&col_type),
                    "nominal_type": col_type,
                    "nullable": !not_null,
                    "default": dflt,
                    "max_length": null,
                    "description": null,
                    "enum_values": [],
                    "is_composite": false,
                });
                columns_json_parts.push(col_json);
            }

            let columns_json =
                serde_json::to_string(&columns_json_parts).unwrap_or_else(|_| "[]".to_string());

            tables.push(TableRow {
                table_schema: "main".to_string(),
                table_name: name,
                table_description: None,
                is_view,
                insertable: !is_view,
                updatable: !is_view,
                deletable: !is_view,
                readable: true,
                pk_cols,
                columns_json,
            });
        }

        Ok(tables)
    }

    async fn query_relationships(&self) -> Result<Vec<RelationshipRow>, Error> {
        // Discover foreign keys from all tables using PRAGMA foreign_key_list().
        let table_rows = sqlx::query(
            r#"
            SELECT name FROM sqlite_master
            WHERE type = 'table'
              AND name NOT LIKE 'sqlite_%'
              AND name NOT LIKE '_dbrest_%'
            "#,
        )
        .fetch_all(self.pool)
        .await
        .map_err(map_sqlx_error)?;

        let mut relationships = Vec::new();

        for table_row in &table_rows {
            let table_name: String = table_row.try_get("name").unwrap_or_default();
            let pragma_sql = format!(
                "PRAGMA foreign_key_list(\"{}\")",
                table_name.replace('"', "\"\"")
            );
            let fk_rows = sqlx::query(&pragma_sql)
                .fetch_all(self.pool)
                .await
                .map_err(map_sqlx_error)?;

            // Group by constraint id
            let mut fk_groups: std::collections::HashMap<i32, Vec<(String, String, String)>> =
                std::collections::HashMap::new();
            for fk in &fk_rows {
                let id: i32 = fk.try_get("id").unwrap_or(0);
                let foreign_table: String = fk.try_get("table").unwrap_or_default();
                let from_col: String = fk.try_get("from").unwrap_or_default();
                let to_col: String = fk.try_get("to").unwrap_or_default();
                fk_groups
                    .entry(id)
                    .or_default()
                    .push((foreign_table, from_col, to_col));
            }

            for (id, cols) in &fk_groups {
                if cols.is_empty() {
                    continue;
                }
                let foreign_table_name = &cols[0].0;
                let cols_and_fcols: Vec<(String, String)> = cols
                    .iter()
                    .map(|(_, f, t)| (f.clone(), t.clone()))
                    .collect();

                let is_self = table_name == *foreign_table_name;
                let constraint_name = format!("fk_{}_{}", table_name, id);

                relationships.push(RelationshipRow {
                    table_schema: "main".to_string(),
                    table_name: table_name.clone(),
                    foreign_table_schema: "main".to_string(),
                    foreign_table_name: foreign_table_name.clone(),
                    is_self,
                    constraint_name,
                    cols_and_fcols,
                    one_to_one: false, // Conservative default
                });
            }
        }

        Ok(relationships)
    }

    async fn query_routines(&self, _schemas: &[String]) -> Result<Vec<RoutineRow>, Error> {
        // SQLite has no user-defined stored routines.
        Ok(vec![])
    }

    async fn query_computed_fields(
        &self,
        _schemas: &[String],
    ) -> Result<Vec<ComputedFieldRow>, Error> {
        // SQLite has no computed fields via functions.
        Ok(vec![])
    }

    async fn query_timezones(&self) -> Result<Vec<String>, Error> {
        // SQLite has no timezone catalog. Return an empty list.
        Ok(vec![])
    }
}

/// Normalize SQLite type strings to standard affinity names.
fn normalize_sqlite_type(raw: &str) -> String {
    let upper = raw.to_uppercase();
    // SQLite type affinity rules (https://www.sqlite.org/datatype3.html)
    if upper.contains("INT") {
        "integer".to_string()
    } else if upper.contains("CHAR") || upper.contains("CLOB") || upper.contains("TEXT") {
        "text".to_string()
    } else if upper.contains("BLOB") || upper.is_empty() {
        "blob".to_string()
    } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
        "real".to_string()
    } else if upper.contains("BOOL") {
        "boolean".to_string()
    } else if upper.contains("DATE") || upper.contains("TIME") {
        "text".to_string() // SQLite stores dates as text
    } else if upper.contains("JSON") {
        "json".to_string()
    } else {
        "text".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_sqlite_type() {
        assert_eq!(normalize_sqlite_type("INTEGER"), "integer");
        assert_eq!(normalize_sqlite_type("INT"), "integer");
        assert_eq!(normalize_sqlite_type("BIGINT"), "integer");
        assert_eq!(normalize_sqlite_type("TEXT"), "text");
        assert_eq!(normalize_sqlite_type("VARCHAR(255)"), "text");
        assert_eq!(normalize_sqlite_type("REAL"), "real");
        assert_eq!(normalize_sqlite_type("DOUBLE PRECISION"), "real");
        assert_eq!(normalize_sqlite_type("BLOB"), "blob");
        assert_eq!(normalize_sqlite_type("BOOLEAN"), "boolean");
        assert_eq!(normalize_sqlite_type("DATETIME"), "text");
        assert_eq!(normalize_sqlite_type("JSON"), "json");
        assert_eq!(normalize_sqlite_type(""), "blob");
    }
}
