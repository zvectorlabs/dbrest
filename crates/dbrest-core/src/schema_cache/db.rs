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
