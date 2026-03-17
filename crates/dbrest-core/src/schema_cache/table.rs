//! Table and Column types for schema cache
//!
//! This module defines the types for representing PostgreSQL tables, views,
//! and their columns in the schema cache.

use compact_str::CompactString;
use indexmap::IndexMap;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::sync::Arc;

use crate::types::QualifiedIdentifier;

/// Table/View metadata
///
/// Represents a PostgreSQL table, view, materialized view, or foreign table
/// with its metadata and columns.
#[derive(Debug, Clone)]
pub struct Table {
    /// Schema name
    pub schema: CompactString,
    /// Table/view name
    pub name: CompactString,
    /// Description from pg_description
    pub description: Option<String>,
    /// Whether this is a view (or materialized view)
    pub is_view: bool,
    /// Whether INSERT is allowed
    pub insertable: bool,
    /// Whether UPDATE is allowed
    pub updatable: bool,
    /// Whether DELETE is allowed
    pub deletable: bool,
    /// Whether SELECT is allowed (for current role)
    pub readable: bool,
    /// Primary key column names (sorted)
    pub pk_cols: SmallVec<[CompactString; 2]>,
    /// Columns indexed by name (preserves insertion order)
    pub columns: Arc<IndexMap<CompactString, Column>>,
    /// Computed fields available on this table
    /// Maps function name -> ComputedField
    pub computed_fields: HashMap<CompactString, ComputedField>,
}

impl Table {
    /// Get the qualified identifier for this table
    pub fn qi(&self) -> QualifiedIdentifier {
        QualifiedIdentifier::new(self.schema.clone(), self.name.clone())
    }

    /// Iterate over all columns
    pub fn columns_list(&self) -> impl Iterator<Item = &Column> {
        self.columns.values()
    }

    /// Get a column by name
    pub fn get_column(&self, name: &str) -> Option<&Column> {
        self.columns.get(name)
    }

    /// Check if this table has a primary key
    pub fn has_pk(&self) -> bool {
        !self.pk_cols.is_empty()
    }

    /// Check if a column is part of the primary key
    pub fn is_pk_column(&self, col_name: &str) -> bool {
        self.pk_cols.iter().any(|pk| pk.as_str() == col_name)
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Check if the table is read-only (no insert, update, or delete)
    pub fn is_read_only(&self) -> bool {
        !self.insertable && !self.updatable && !self.deletable
    }

    /// Get all insertable columns (non-generated, or with defaults)
    pub fn insertable_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.values().filter(|c| !c.is_generated())
    }

    /// Get all updatable columns (non-generated)
    pub fn updatable_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.values().filter(|c| !c.is_generated())
    }

    /// Get required columns for INSERT (non-nullable, no default, not generated)
    pub fn required_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns
            .values()
            .filter(|c| !c.nullable && !c.has_default() && !c.is_generated())
    }

    /// Get a computed field by function name
    pub fn get_computed_field(&self, name: &str) -> Option<&ComputedField> {
        self.computed_fields.get(name)
    }
}

/// Column metadata
///
/// Represents a PostgreSQL column with its type and constraints.
#[derive(Debug, Clone)]
pub struct Column {
    /// Column name
    pub name: CompactString,
    /// Description from pg_description
    pub description: Option<String>,
    /// Whether NULL values are allowed
    pub nullable: bool,
    /// The "coerced" data type (e.g., "character varying" instead of "varchar")
    pub data_type: CompactString,
    /// The actual declared type as written
    pub nominal_type: CompactString,
    /// Maximum length for character types
    pub max_length: Option<i32>,
    /// Default value expression
    pub default: Option<String>,
    /// Enum values if this is an enum type
    pub enum_values: SmallVec<[String; 8]>,
    /// Whether this is a composite type
    pub is_composite: bool,
    /// Composite type schema (if composite)
    pub composite_type_schema: Option<CompactString>,
    /// Composite type name (if composite)
    pub composite_type_name: Option<CompactString>,
}

impl Column {
    /// Check if the column has a default value
    pub fn has_default(&self) -> bool {
        self.default.is_some()
    }

    /// Check if this is an auto-generated column (serial, identity, generated)
    pub fn is_generated(&self) -> bool {
        if let Some(ref def) = self.default {
            def.starts_with("nextval(") || def.contains("generated")
        } else {
            false
        }
    }

    /// Check if this is an enum column
    pub fn is_enum(&self) -> bool {
        !self.enum_values.is_empty()
    }

    /// Check if this is a text/character type
    pub fn is_text_type(&self) -> bool {
        matches!(
            self.data_type.as_str(),
            "text" | "character varying" | "character" | "varchar" | "char" | "name"
        )
    }

    /// Check if this is a numeric type
    pub fn is_numeric_type(&self) -> bool {
        matches!(
            self.data_type.as_str(),
            "integer"
                | "bigint"
                | "smallint"
                | "numeric"
                | "decimal"
                | "real"
                | "double precision"
                | "int"
                | "int4"
                | "int8"
                | "int2"
                | "float4"
                | "float8"
        )
    }

    /// Check if this is a boolean type
    pub fn is_boolean_type(&self) -> bool {
        self.data_type.as_str() == "boolean" || self.data_type.as_str() == "bool"
    }

    /// Check if this is a JSON type
    pub fn is_json_type(&self) -> bool {
        self.data_type.as_str() == "json" || self.data_type.as_str() == "jsonb"
    }

    /// Check if this is an array type
    pub fn is_array_type(&self) -> bool {
        self.data_type.ends_with("[]") || self.data_type.starts_with("ARRAY")
    }

    /// Check if this is a timestamp/date type
    pub fn is_temporal_type(&self) -> bool {
        matches!(
            self.data_type.as_str(),
            "timestamp without time zone"
                | "timestamp with time zone"
                | "timestamptz"
                | "timestamp"
                | "date"
                | "time without time zone"
                | "time with time zone"
                | "timetz"
                | "time"
                | "interval"
        )
    }

    /// Check if this is a UUID type
    pub fn is_uuid_type(&self) -> bool {
        self.data_type.as_str() == "uuid"
    }

    /// Check if this is a composite type
    pub fn is_composite_type(&self) -> bool {
        self.is_composite
    }
}

/// Computed field metadata
///
/// Represents a function that can be used as a computed field on a table.
/// The function takes a table row type as the first parameter and returns a scalar value.
#[derive(Debug, Clone)]
pub struct ComputedField {
    /// Function qualified identifier
    pub function: QualifiedIdentifier,
    /// Return type (scalar)
    pub return_type: CompactString,
    /// Whether function returns a set
    pub returns_set: bool,
}

#[cfg(test)]
mod tests {
    use crate::test_helpers::*;

    // ========================================================================
    // Table Tests
    // ========================================================================

    #[test]
    fn test_table_qi() {
        let table = test_table().schema("api").name("users").build();

        let qi = table.qi();
        assert_eq!(qi.schema.as_str(), "api");
        assert_eq!(qi.name.as_str(), "users");
    }

    #[test]
    fn test_table_get_column() {
        let col1 = test_column().name("id").data_type("integer").build();
        let col2 = test_column().name("name").data_type("text").build();

        let table = test_table().column(col1).column(col2).build();

        assert!(table.get_column("id").is_some());
        assert!(table.get_column("name").is_some());
        assert!(table.get_column("nonexistent").is_none());
    }

    #[test]
    fn test_table_has_pk() {
        let table_with_pk = test_table().pk_col("id").build();
        assert!(table_with_pk.has_pk());

        let table_without_pk = test_table().build();
        assert!(!table_without_pk.has_pk());
    }

    #[test]
    fn test_table_is_pk_column() {
        let table = test_table().pk_cols(["id", "tenant_id"]).build();

        assert!(table.is_pk_column("id"));
        assert!(table.is_pk_column("tenant_id"));
        assert!(!table.is_pk_column("name"));
    }

    #[test]
    fn test_table_column_count() {
        let col1 = test_column().name("id").build();
        let col2 = test_column().name("name").build();
        let col3 = test_column().name("email").build();

        let table = test_table().column(col1).column(col2).column(col3).build();

        assert_eq!(table.column_count(), 3);
    }

    #[test]
    fn test_table_is_read_only() {
        let rw_table = test_table()
            .insertable(true)
            .updatable(true)
            .deletable(true)
            .build();
        assert!(!rw_table.is_read_only());

        let ro_table = test_table()
            .insertable(false)
            .updatable(false)
            .deletable(false)
            .build();
        assert!(ro_table.is_read_only());

        let partial_table = test_table()
            .insertable(false)
            .updatable(true)
            .deletable(false)
            .build();
        assert!(!partial_table.is_read_only());
    }

    #[test]
    fn test_table_columns_list() {
        let col1 = test_column().name("a").build();
        let col2 = test_column().name("b").build();

        let table = test_table().column(col1).column(col2).build();

        let names: Vec<_> = table.columns_list().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec!["a", "b"]);
    }

    #[test]
    fn test_table_insertable_columns() {
        let regular_col = test_column().name("name").build();
        let generated_col = test_column()
            .name("id")
            .default_value("nextval('users_id_seq')")
            .build();

        let table = test_table()
            .column(regular_col)
            .column(generated_col)
            .build();

        let insertable: Vec<_> = table
            .insertable_columns()
            .map(|c| c.name.as_str())
            .collect();
        assert_eq!(insertable, vec!["name"]);
    }

    #[test]
    fn test_table_required_columns() {
        let required_col = test_column().name("name").nullable(false).build();
        let optional_col = test_column().name("bio").nullable(true).build();
        let defaulted_col = test_column()
            .name("status")
            .nullable(false)
            .default_value("'active'")
            .build();
        let generated_col = test_column()
            .name("id")
            .nullable(false)
            .default_value("nextval('seq')")
            .build();

        let table = test_table()
            .column(required_col)
            .column(optional_col)
            .column(defaulted_col)
            .column(generated_col)
            .build();

        let required: Vec<_> = table.required_columns().map(|c| c.name.as_str()).collect();
        assert_eq!(required, vec!["name"]);
    }

    #[test]
    fn test_table_is_view() {
        let table = test_table().is_view(false).build();
        assert!(!table.is_view);

        let view = test_table().is_view(true).build();
        assert!(view.is_view);
    }

    // ========================================================================
    // Column Tests
    // ========================================================================

    #[test]
    fn test_column_has_default() {
        let col_with_default = test_column().default_value("now()").build();
        assert!(col_with_default.has_default());

        let col_without_default = test_column().build();
        assert!(!col_without_default.has_default());
    }

    #[test]
    fn test_column_is_generated_nextval() {
        let serial_col = test_column()
            .name("id")
            .default_value("nextval('users_id_seq'::regclass)")
            .build();
        assert!(serial_col.is_generated());
    }

    #[test]
    fn test_column_is_generated_identity() {
        let identity_col = test_column()
            .name("id")
            .default_value("generated always as identity")
            .build();
        assert!(identity_col.is_generated());
    }

    #[test]
    fn test_column_is_generated_regular_default() {
        let col = test_column()
            .name("created_at")
            .default_value("now()")
            .build();
        assert!(!col.is_generated());
    }

    #[test]
    fn test_column_is_enum() {
        let enum_col = test_column()
            .name("status")
            .enum_values(["active", "inactive", "pending"])
            .build();
        assert!(enum_col.is_enum());
        assert_eq!(enum_col.enum_values.len(), 3);

        let regular_col = test_column().name("name").build();
        assert!(!regular_col.is_enum());
    }

    #[test]
    fn test_column_is_text_type() {
        assert!(test_column().data_type("text").build().is_text_type());
        assert!(
            test_column()
                .data_type("character varying")
                .build()
                .is_text_type()
        );
        assert!(test_column().data_type("varchar").build().is_text_type());
        assert!(test_column().data_type("char").build().is_text_type());
        assert!(!test_column().data_type("integer").build().is_text_type());
    }

    #[test]
    fn test_column_is_numeric_type() {
        assert!(test_column().data_type("integer").build().is_numeric_type());
        assert!(test_column().data_type("bigint").build().is_numeric_type());
        assert!(test_column().data_type("numeric").build().is_numeric_type());
        assert!(
            test_column()
                .data_type("double precision")
                .build()
                .is_numeric_type()
        );
        assert!(!test_column().data_type("text").build().is_numeric_type());
    }

    #[test]
    fn test_column_is_boolean_type() {
        assert!(test_column().data_type("boolean").build().is_boolean_type());
        assert!(test_column().data_type("bool").build().is_boolean_type());
        assert!(!test_column().data_type("text").build().is_boolean_type());
    }

    #[test]
    fn test_column_is_json_type() {
        assert!(test_column().data_type("json").build().is_json_type());
        assert!(test_column().data_type("jsonb").build().is_json_type());
        assert!(!test_column().data_type("text").build().is_json_type());
    }

    #[test]
    fn test_column_is_array_type() {
        assert!(test_column().data_type("integer[]").build().is_array_type());
        assert!(test_column().data_type("text[]").build().is_array_type());
        assert!(!test_column().data_type("integer").build().is_array_type());
    }

    #[test]
    fn test_column_is_temporal_type() {
        assert!(
            test_column()
                .data_type("timestamp with time zone")
                .build()
                .is_temporal_type()
        );
        assert!(
            test_column()
                .data_type("timestamp without time zone")
                .build()
                .is_temporal_type()
        );
        assert!(test_column().data_type("date").build().is_temporal_type());
        assert!(
            test_column()
                .data_type("interval")
                .build()
                .is_temporal_type()
        );
        assert!(!test_column().data_type("text").build().is_temporal_type());
    }

    #[test]
    fn test_column_is_uuid_type() {
        assert!(test_column().data_type("uuid").build().is_uuid_type());
        assert!(!test_column().data_type("text").build().is_uuid_type());
    }

    #[test]
    fn test_column_max_length() {
        let col = test_column()
            .data_type("character varying")
            .max_length(255)
            .build();
        assert_eq!(col.max_length, Some(255));

        let col_no_limit = test_column().data_type("text").build();
        assert_eq!(col_no_limit.max_length, None);
    }

    #[test]
    fn test_column_nullable() {
        let nullable_col = test_column().nullable(true).build();
        assert!(nullable_col.nullable);

        let non_nullable_col = test_column().nullable(false).build();
        assert!(!non_nullable_col.nullable);
    }

    // ========================================================================
    // ComputedField Tests
    // ========================================================================

    #[test]
    fn test_computed_field_structure() {
        use super::ComputedField;
        use crate::types::QualifiedIdentifier;

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi.clone(),
            return_type: "text".into(),
            returns_set: false,
        };

        assert_eq!(computed.function.schema.as_str(), "test_api");
        assert_eq!(computed.function.name.as_str(), "full_name");
        assert_eq!(computed.return_type.as_str(), "text");
        assert!(!computed.returns_set);
    }

    #[test]
    fn test_table_get_computed_field() {
        use super::ComputedField;
        use crate::types::QualifiedIdentifier;

        let mut table = test_table().schema("test_api").name("users").build();

        // Add a computed field manually for testing
        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi,
            return_type: "text".into(),
            returns_set: false,
        };
        table.computed_fields.insert("full_name".into(), computed);

        assert!(table.get_computed_field("full_name").is_some());
        assert!(table.get_computed_field("nonexistent").is_none());

        let cf = table.get_computed_field("full_name").unwrap();
        assert_eq!(cf.return_type.as_str(), "text");
    }
}
