//! Database introspection trait
//!
//! This module defines a trait for database introspection that can be mocked
//! in tests. This allows unit testing schema cache logic without a real database.

use async_trait::async_trait;

use crate::error::Error;

use super::relationship::Relationship;
use super::routine::Routine;
use super::table::Table;

/// Row type returned from tables query
#[derive(Debug, Clone)]
pub struct TableRow {
    pub table_schema: String,
    pub table_name: String,
    pub table_description: Option<String>,
    pub is_view: bool,
    pub insertable: bool,
    pub updatable: bool,
    pub deletable: bool,
    pub readable: bool,
    pub pk_cols: Vec<String>,
    pub columns_json: String, // JSON array of columns
}

/// Row type returned from relationships query
#[derive(Debug, Clone)]
pub struct RelationshipRow {
    pub table_schema: String,
    pub table_name: String,
    pub foreign_table_schema: String,
    pub foreign_table_name: String,
    pub is_self: bool,
    pub constraint_name: String,
    pub cols_and_fcols: Vec<(String, String)>,
    pub one_to_one: bool,
}

/// Row type returned from routines query
#[derive(Debug, Clone)]
pub struct RoutineRow {
    pub routine_schema: String,
    pub routine_name: String,
    pub description: Option<String>,
    pub params_json: String,      // JSON array of params
    pub return_type_json: String, // JSON object
    pub volatility: String,
    pub is_variadic: bool,
    pub executable: bool,
}

/// Row type returned from computed fields query
#[derive(Debug, Clone)]
pub struct ComputedFieldRow {
    pub table_schema: String,
    pub table_name: String,
    pub function_schema: String,
    pub function_name: String,
    pub return_type: String,
    pub returns_set: bool,
}

/// Trait for database introspection
///
/// This trait abstracts database queries for schema introspection, allowing
/// the schema cache to be tested without a real database connection.
#[cfg_attr(test, mockall::automock)]
#[async_trait]
pub trait DbIntrospector: Send + Sync {
    /// Query all tables and views in the specified schemas
    async fn query_tables(&self, schemas: &[String]) -> Result<Vec<TableRow>, Error>;

    /// Query all foreign key relationships
    async fn query_relationships(&self) -> Result<Vec<RelationshipRow>, Error>;

    /// Query all routines (functions/procedures) in the specified schemas
    async fn query_routines(&self, schemas: &[String]) -> Result<Vec<RoutineRow>, Error>;

    /// Query computed field functions in the specified schemas
    async fn query_computed_fields(
        &self,
        schemas: &[String],
    ) -> Result<Vec<ComputedFieldRow>, Error>;

    /// Query available timezones
    async fn query_timezones(&self) -> Result<Vec<String>, Error>;
}

/// Parse a JSON string into a vector of columns for a table
pub fn parse_columns_json(json: &str) -> Result<Vec<ColumnJson>, serde_json::Error> {
    serde_json::from_str(json)
}

/// Parse a JSON string into a vector of routine parameters
pub fn parse_params_json(json: &str) -> Result<Vec<ParamJson>, serde_json::Error> {
    serde_json::from_str(json)
}

/// Parse a JSON string into a return type
pub fn parse_return_type_json(json: &str) -> Result<ReturnTypeJson, serde_json::Error> {
    serde_json::from_str(json)
}

/// JSON structure for column data
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ColumnJson {
    pub name: String,
    pub description: Option<String>,
    pub nullable: bool,
    pub data_type: String,
    pub nominal_type: String,
    pub max_length: Option<i32>,
    pub default: Option<String>,
    #[serde(default)]
    pub enum_values: Vec<String>,
    #[serde(default)]
    pub is_composite: bool,
    pub composite_type_schema: Option<String>,
    pub composite_type_name: Option<String>,
}

/// JSON structure for routine parameter data
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ParamJson {
    pub name: String,
    pub pg_type: String,
    pub type_max_length: String,
    pub required: bool,
    #[serde(default)]
    pub is_variadic: bool,
}

/// JSON structure for return type data
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ReturnTypeJson {
    pub kind: String,      // "single" or "setof"
    pub type_kind: String, // "scalar" or "composite"
    pub type_schema: String,
    pub type_name: String,
    #[serde(default)]
    pub is_alias: bool,
}

/// Convert TableRow into Table
impl TableRow {
    pub fn into_table(self) -> Result<Table, Error> {
        use compact_str::CompactString;
        use indexmap::IndexMap;
        use smallvec::SmallVec;
        use std::collections::HashMap;
        use std::sync::Arc;

        use super::table::Column;

        let columns_data: Vec<ColumnJson> = parse_columns_json(&self.columns_json)
            .map_err(|e| Error::Internal(format!("Failed to parse columns JSON: {}", e)))?;

        let mut columns = IndexMap::with_capacity(columns_data.len());
        for col in columns_data {
            // Trace: check if location column is being detected as composite
            if col.name == "location" {
                tracing::trace!(
                    "Loading 'location' column - is_composite: {}, data_type: {}, composite_type_schema: {:?}, composite_type_name: {:?}",
                    col.is_composite,
                    col.data_type,
                    col.composite_type_schema,
                    col.composite_type_name
                );
            }
            let column = Column {
                name: col.name.clone().into(),
                description: col.description,
                nullable: col.nullable,
                data_type: col.data_type.into(),
                nominal_type: col.nominal_type.into(),
                max_length: col.max_length,
                default: col.default,
                enum_values: col.enum_values.into_iter().collect(),
                is_composite: col.is_composite,
                composite_type_schema: col.composite_type_schema.map(|s| s.into()),
                composite_type_name: col.composite_type_name.map(|s| s.into()),
            };
            columns.insert(CompactString::from(col.name), column);
        }

        Ok(Table {
            schema: self.table_schema.into(),
            name: self.table_name.into(),
            description: self.table_description,
            is_view: self.is_view,
            insertable: self.insertable,
            updatable: self.updatable,
            deletable: self.deletable,
            readable: self.readable,
            pk_cols: self
                .pk_cols
                .into_iter()
                .map(|s| s.into())
                .collect::<SmallVec<_>>(),
            columns: Arc::new(columns),
            computed_fields: HashMap::new(), // Will be populated during schema cache load
        })
    }
}

/// Convert RelationshipRow into Relationship
impl RelationshipRow {
    pub fn into_relationship(self) -> Relationship {
        use super::relationship::Cardinality;
        use crate::types::QualifiedIdentifier;

        let cardinality = if self.one_to_one {
            Cardinality::O2O {
                constraint: self.constraint_name.into(),
                columns: self
                    .cols_and_fcols
                    .into_iter()
                    .map(|(a, b)| (a.into(), b.into()))
                    .collect(),
                is_parent: false,
            }
        } else {
            Cardinality::M2O {
                constraint: self.constraint_name.into(),
                columns: self
                    .cols_and_fcols
                    .into_iter()
                    .map(|(a, b)| (a.into(), b.into()))
                    .collect(),
            }
        };

        Relationship {
            table: QualifiedIdentifier::new(&self.table_schema, &self.table_name),
            foreign_table: QualifiedIdentifier::new(
                &self.foreign_table_schema,
                &self.foreign_table_name,
            ),
            is_self: self.is_self,
            cardinality,
            table_is_view: false, // Will be set later
            foreign_table_is_view: false,
        }
    }
}

/// Convert RoutineRow into Routine
impl RoutineRow {
    pub fn into_routine(self) -> Result<Routine, Error> {
        use super::routine::{PgType, ReturnType, RoutineParam, Volatility};
        use crate::types::QualifiedIdentifier;

        let params_data: Vec<ParamJson> = parse_params_json(&self.params_json)
            .map_err(|e| Error::Internal(format!("Failed to parse params JSON: {}", e)))?;

        let return_type_data: ReturnTypeJson = parse_return_type_json(&self.return_type_json)
            .map_err(|e| Error::Internal(format!("Failed to parse return type JSON: {}", e)))?;

        let params = params_data
            .into_iter()
            .map(|p| RoutineParam {
                name: p.name.into(),
                pg_type: p.pg_type.into(),
                type_max_length: p.type_max_length.into(),
                required: p.required,
                is_variadic: p.is_variadic,
            })
            .collect();

        let pg_type = match return_type_data.type_kind.as_str() {
            "composite" => PgType::Composite(
                QualifiedIdentifier::new(
                    &return_type_data.type_schema,
                    &return_type_data.type_name,
                ),
                return_type_data.is_alias,
            ),
            _ => PgType::Scalar(QualifiedIdentifier::new(
                &return_type_data.type_schema,
                &return_type_data.type_name,
            )),
        };

        let return_type = match return_type_data.kind.as_str() {
            "setof" => ReturnType::SetOf(pg_type),
            _ => ReturnType::Single(pg_type),
        };

        let volatility = Volatility::parse(&self.volatility).unwrap_or(Volatility::Volatile);

        Ok(Routine {
            schema: self.routine_schema.into(),
            name: self.routine_name.into(),
            description: self.description,
            params,
            return_type,
            volatility,
            is_variadic: self.is_variadic,
            executable: self.executable,
        })
    }
}

// ==========================================================================
// SqlxIntrospector — production implementation backed by sqlx::PgPool
// ==========================================================================

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
        use super::queries::computed_fields::COMPUTED_FIELDS_QUERY;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_columns_json() {
        let json = r#"[
            {"name": "id", "description": null, "nullable": false, "data_type": "integer", "nominal_type": "integer", "max_length": null, "default": "nextval('seq')", "enum_values": []},
            {"name": "name", "description": "User name", "nullable": true, "data_type": "text", "nominal_type": "text", "max_length": null, "default": null, "enum_values": []}
        ]"#;

        let cols = parse_columns_json(json).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert!(!cols[0].nullable);
        assert_eq!(cols[1].name, "name");
        assert!(cols[1].nullable);
    }

    #[test]
    fn test_parse_params_json() {
        let json = r#"[
            {"name": "user_id", "pg_type": "integer", "type_max_length": "integer", "required": true, "is_variadic": false},
            {"name": "limit", "pg_type": "integer", "type_max_length": "integer", "required": false, "is_variadic": false}
        ]"#;

        let params = parse_params_json(json).unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, "user_id");
        assert!(params[0].required);
        assert_eq!(params[1].name, "limit");
        assert!(!params[1].required);
    }

    #[test]
    fn test_parse_return_type_json_scalar() {
        let json = r#"{"kind": "single", "type_kind": "scalar", "type_schema": "pg_catalog", "type_name": "integer", "is_alias": false}"#;

        let rt = parse_return_type_json(json).unwrap();
        assert_eq!(rt.kind, "single");
        assert_eq!(rt.type_kind, "scalar");
        assert_eq!(rt.type_name, "integer");
    }

    #[test]
    fn test_parse_return_type_json_setof_composite() {
        let json = r#"{"kind": "setof", "type_kind": "composite", "type_schema": "public", "type_name": "users", "is_alias": false}"#;

        let rt = parse_return_type_json(json).unwrap();
        assert_eq!(rt.kind, "setof");
        assert_eq!(rt.type_kind, "composite");
        assert_eq!(rt.type_name, "users");
    }

    #[test]
    fn test_table_row_into_table() {
        let row = TableRow {
            table_schema: "public".to_string(),
            table_name: "users".to_string(),
            table_description: Some("User table".to_string()),
            is_view: false,
            insertable: true,
            updatable: true,
            deletable: true,
            readable: true,
            pk_cols: vec!["id".to_string()],
            columns_json: r#"[{"name": "id", "description": null, "nullable": false, "data_type": "integer", "nominal_type": "integer", "max_length": null, "default": null, "enum_values": []}]"#.to_string(),
        };

        let table = row.into_table().unwrap();
        assert_eq!(table.schema.as_str(), "public");
        assert_eq!(table.name.as_str(), "users");
        assert!(table.has_pk());
        assert_eq!(table.column_count(), 1);
    }

    #[test]
    fn test_relationship_row_into_relationship() {
        let row = RelationshipRow {
            table_schema: "public".to_string(),
            table_name: "posts".to_string(),
            foreign_table_schema: "public".to_string(),
            foreign_table_name: "users".to_string(),
            is_self: false,
            constraint_name: "fk_posts_user".to_string(),
            cols_and_fcols: vec![("user_id".to_string(), "id".to_string())],
            one_to_one: false,
        };

        let rel = row.into_relationship();
        assert_eq!(rel.table.name.as_str(), "posts");
        assert_eq!(rel.foreign_table.name.as_str(), "users");
        assert!(rel.is_to_one()); // M2O is to-one
        assert_eq!(rel.constraint_name(), "fk_posts_user");
    }

    #[test]
    fn test_routine_row_into_routine() {
        let row = RoutineRow {
            routine_schema: "api".to_string(),
            routine_name: "get_user".to_string(),
            description: Some("Get user by ID".to_string()),
            params_json: r#"[{"name": "user_id", "pg_type": "integer", "type_max_length": "integer", "required": true, "is_variadic": false}]"#.to_string(),
            return_type_json: r#"{"kind": "setof", "type_kind": "composite", "type_schema": "public", "type_name": "users", "is_alias": false}"#.to_string(),
            volatility: "s".to_string(),
            is_variadic: false,
            executable: true,
        };

        let routine = row.into_routine().unwrap();
        assert_eq!(routine.schema.as_str(), "api");
        assert_eq!(routine.name.as_str(), "get_user");
        assert!(routine.returns_set());
        assert!(routine.returns_composite());
        assert!(routine.is_stable());
        assert_eq!(routine.param_count(), 1);
    }
}
