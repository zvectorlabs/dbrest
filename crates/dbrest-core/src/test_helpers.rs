//! Test helper utilities
//!
//! This module provides builder patterns and utilities for creating test data.
//! Only included in test builds.

use compact_str::CompactString;
use indexmap::IndexMap;
use smallvec::SmallVec;
use std::sync::Arc;

use crate::schema_cache::{
    Cardinality, Column, ComputedRelationship, Junction, PgType, Relationship, ReturnType, Routine,
    RoutineParam, Table, Volatility,
};
use crate::types::QualifiedIdentifier;

// ============================================================================
// Table Builder
// ============================================================================

/// Builder for creating test `Table` instances
#[derive(Default)]
pub struct TableBuilder {
    schema: Option<CompactString>,
    name: Option<CompactString>,
    description: Option<String>,
    is_view: bool,
    insertable: bool,
    updatable: bool,
    deletable: bool,
    pk_cols: SmallVec<[CompactString; 2]>,
    columns: IndexMap<CompactString, Column>,
}

impl TableBuilder {
    pub fn new() -> Self {
        Self {
            insertable: true,
            updatable: true,
            deletable: true,
            ..Default::default()
        }
    }

    pub fn schema(mut self, schema: impl Into<CompactString>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn name(mut self, name: impl Into<CompactString>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn is_view(mut self, is_view: bool) -> Self {
        self.is_view = is_view;
        self
    }

    pub fn insertable(mut self, insertable: bool) -> Self {
        self.insertable = insertable;
        self
    }

    pub fn updatable(mut self, updatable: bool) -> Self {
        self.updatable = updatable;
        self
    }

    pub fn deletable(mut self, deletable: bool) -> Self {
        self.deletable = deletable;
        self
    }

    pub fn pk_col(mut self, col: impl Into<CompactString>) -> Self {
        self.pk_cols.push(col.into());
        self
    }

    pub fn pk_cols(mut self, cols: impl IntoIterator<Item = impl Into<CompactString>>) -> Self {
        self.pk_cols = cols.into_iter().map(|c| c.into()).collect();
        self
    }

    pub fn column(mut self, col: Column) -> Self {
        self.columns.insert(col.name.clone(), col);
        self
    }

    pub fn columns(mut self, cols: impl IntoIterator<Item = Column>) -> Self {
        for col in cols {
            self.columns.insert(col.name.clone(), col);
        }
        self
    }

    pub fn build(self) -> Table {
        use std::collections::HashMap;
        Table {
            schema: self.schema.unwrap_or_else(|| "public".into()),
            name: self.name.unwrap_or_else(|| "test_table".into()),
            description: self.description,
            is_view: self.is_view,
            insertable: self.insertable,
            updatable: self.updatable,
            deletable: self.deletable,
            readable: true,
            pk_cols: self.pk_cols,
            columns: Arc::new(self.columns),
            computed_fields: HashMap::new(),
        }
    }
}

/// Convenience function to start building a test table
pub fn test_table() -> TableBuilder {
    TableBuilder::new()
}

// ============================================================================
// Column Builder
// ============================================================================

/// Builder for creating test `Column` instances
#[derive(Default)]
pub struct ColumnBuilder {
    name: Option<CompactString>,
    description: Option<String>,
    nullable: bool,
    data_type: Option<CompactString>,
    nominal_type: Option<CompactString>,
    max_length: Option<i32>,
    default: Option<String>,
    enum_values: SmallVec<[String; 8]>,
    is_composite: bool,
    composite_type_schema: Option<CompactString>,
    composite_type_name: Option<CompactString>,
}

impl ColumnBuilder {
    pub fn new() -> Self {
        Self {
            nullable: true,
            ..Default::default()
        }
    }

    pub fn name(mut self, name: impl Into<CompactString>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn data_type(mut self, dt: impl Into<CompactString>) -> Self {
        self.data_type = Some(dt.into());
        self
    }

    pub fn nominal_type(mut self, nt: impl Into<CompactString>) -> Self {
        self.nominal_type = Some(nt.into());
        self
    }

    pub fn max_length(mut self, len: i32) -> Self {
        self.max_length = Some(len);
        self
    }

    pub fn default_value(mut self, def: impl Into<String>) -> Self {
        self.default = Some(def.into());
        self
    }

    pub fn enum_values(mut self, values: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.enum_values = values.into_iter().map(|v| v.into()).collect();
        self
    }

    pub fn build(self) -> Column {
        let data_type = self.data_type.unwrap_or_else(|| "text".into());
        Column {
            name: self.name.unwrap_or_else(|| "test_col".into()),
            description: self.description,
            nullable: self.nullable,
            data_type: data_type.clone(),
            nominal_type: self.nominal_type.unwrap_or(data_type),
            max_length: self.max_length,
            default: self.default,
            enum_values: self.enum_values,
            is_composite: self.is_composite,
            composite_type_schema: self.composite_type_schema,
            composite_type_name: self.composite_type_name,
        }
    }
}

/// Convenience function to start building a test column
pub fn test_column() -> ColumnBuilder {
    ColumnBuilder::new()
}

// ============================================================================
// Relationship Builder
// ============================================================================

/// Builder for creating test `Relationship` instances
pub struct RelationshipBuilder {
    table: Option<QualifiedIdentifier>,
    foreign_table: Option<QualifiedIdentifier>,
    is_self: bool,
    cardinality: Option<Cardinality>,
    table_is_view: bool,
    foreign_table_is_view: bool,
}

impl Default for RelationshipBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RelationshipBuilder {
    pub fn new() -> Self {
        Self {
            table: None,
            foreign_table: None,
            is_self: false,
            cardinality: None,
            table_is_view: false,
            foreign_table_is_view: false,
        }
    }

    pub fn table(mut self, schema: &str, name: &str) -> Self {
        self.table = Some(QualifiedIdentifier::new(schema, name));
        self
    }

    pub fn foreign_table(mut self, schema: &str, name: &str) -> Self {
        self.foreign_table = Some(QualifiedIdentifier::new(schema, name));
        self
    }

    pub fn is_self(mut self, is_self: bool) -> Self {
        self.is_self = is_self;
        self
    }

    pub fn cardinality(mut self, card: Cardinality) -> Self {
        self.cardinality = Some(card);
        self
    }

    pub fn m2o(mut self, constraint: &str, cols: &[(&str, &str)]) -> Self {
        self.cardinality = Some(Cardinality::M2O {
            constraint: constraint.into(),
            columns: cols
                .iter()
                .map(|(a, b)| ((*a).into(), (*b).into()))
                .collect(),
        });
        self
    }

    pub fn o2m(mut self, constraint: &str, cols: &[(&str, &str)]) -> Self {
        self.cardinality = Some(Cardinality::O2M {
            constraint: constraint.into(),
            columns: cols
                .iter()
                .map(|(a, b)| ((*a).into(), (*b).into()))
                .collect(),
        });
        self
    }

    pub fn o2o(mut self, constraint: &str, cols: &[(&str, &str)], is_parent: bool) -> Self {
        self.cardinality = Some(Cardinality::O2O {
            constraint: constraint.into(),
            columns: cols
                .iter()
                .map(|(a, b)| ((*a).into(), (*b).into()))
                .collect(),
            is_parent,
        });
        self
    }

    pub fn m2m(mut self, junction: Junction) -> Self {
        self.cardinality = Some(Cardinality::M2M(junction));
        self
    }

    pub fn table_is_view(mut self, is_view: bool) -> Self {
        self.table_is_view = is_view;
        self
    }

    pub fn foreign_table_is_view(mut self, is_view: bool) -> Self {
        self.foreign_table_is_view = is_view;
        self
    }

    pub fn build(self) -> Relationship {
        Relationship {
            table: self
                .table
                .unwrap_or_else(|| QualifiedIdentifier::new("public", "source")),
            foreign_table: self
                .foreign_table
                .unwrap_or_else(|| QualifiedIdentifier::new("public", "target")),
            is_self: self.is_self,
            cardinality: self.cardinality.unwrap_or_else(|| Cardinality::M2O {
                constraint: "fk_test".into(),
                columns: smallvec::smallvec![("source_id".into(), "id".into())],
            }),
            table_is_view: self.table_is_view,
            foreign_table_is_view: self.foreign_table_is_view,
        }
    }
}

/// Convenience function to start building a test relationship
pub fn test_relationship() -> RelationshipBuilder {
    RelationshipBuilder::new()
}

// ============================================================================
// Junction Builder
// ============================================================================

/// Builder for creating test `Junction` instances
pub struct JunctionBuilder {
    table: Option<QualifiedIdentifier>,
    constraint1: CompactString,
    constraint2: CompactString,
    cols_source: SmallVec<[(CompactString, CompactString); 2]>,
    cols_target: SmallVec<[(CompactString, CompactString); 2]>,
}

impl Default for JunctionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl JunctionBuilder {
    pub fn new() -> Self {
        Self {
            table: None,
            constraint1: "fk1".into(),
            constraint2: "fk2".into(),
            cols_source: SmallVec::new(),
            cols_target: SmallVec::new(),
        }
    }

    pub fn table(mut self, schema: &str, name: &str) -> Self {
        self.table = Some(QualifiedIdentifier::new(schema, name));
        self
    }

    pub fn constraints(mut self, c1: &str, c2: &str) -> Self {
        self.constraint1 = c1.into();
        self.constraint2 = c2.into();
        self
    }

    pub fn cols_source(mut self, cols: &[(&str, &str)]) -> Self {
        self.cols_source = cols
            .iter()
            .map(|(a, b)| ((*a).into(), (*b).into()))
            .collect();
        self
    }

    pub fn cols_target(mut self, cols: &[(&str, &str)]) -> Self {
        self.cols_target = cols
            .iter()
            .map(|(a, b)| ((*a).into(), (*b).into()))
            .collect();
        self
    }

    pub fn build(self) -> Junction {
        Junction {
            table: self
                .table
                .unwrap_or_else(|| QualifiedIdentifier::new("public", "junction")),
            constraint1: self.constraint1,
            constraint2: self.constraint2,
            cols_source: self.cols_source,
            cols_target: self.cols_target,
        }
    }
}

/// Convenience function to start building a test junction
pub fn test_junction() -> JunctionBuilder {
    JunctionBuilder::new()
}

// ============================================================================
// Routine Builder
// ============================================================================

/// Builder for creating test `Routine` instances
pub struct RoutineBuilder {
    schema: Option<CompactString>,
    name: Option<CompactString>,
    description: Option<String>,
    params: SmallVec<[RoutineParam; 4]>,
    return_type: Option<ReturnType>,
    volatility: Volatility,
    is_variadic: bool,
}

impl Default for RoutineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl RoutineBuilder {
    pub fn new() -> Self {
        Self {
            schema: None,
            name: None,
            description: None,
            params: SmallVec::new(),
            return_type: None,
            volatility: Volatility::Volatile,
            is_variadic: false,
        }
    }

    pub fn schema(mut self, schema: impl Into<CompactString>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn name(mut self, name: impl Into<CompactString>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn param(mut self, param: RoutineParam) -> Self {
        self.params.push(param);
        self
    }

    pub fn params(mut self, params: impl IntoIterator<Item = RoutineParam>) -> Self {
        self.params = params.into_iter().collect();
        self
    }

    pub fn return_type(mut self, rt: ReturnType) -> Self {
        self.return_type = Some(rt);
        self
    }

    pub fn returns_scalar(mut self, type_name: &str) -> Self {
        self.return_type = Some(ReturnType::Single(PgType::Scalar(
            QualifiedIdentifier::new("pg_catalog", type_name),
        )));
        self
    }

    pub fn returns_setof_scalar(mut self, type_name: &str) -> Self {
        self.return_type = Some(ReturnType::SetOf(PgType::Scalar(QualifiedIdentifier::new(
            "pg_catalog",
            type_name,
        ))));
        self
    }

    pub fn returns_composite(mut self, schema: &str, name: &str) -> Self {
        self.return_type = Some(ReturnType::Single(PgType::Composite(
            QualifiedIdentifier::new(schema, name),
            false,
        )));
        self
    }

    pub fn returns_setof_composite(mut self, schema: &str, name: &str) -> Self {
        self.return_type = Some(ReturnType::SetOf(PgType::Composite(
            QualifiedIdentifier::new(schema, name),
            false,
        )));
        self
    }

    pub fn volatility(mut self, vol: Volatility) -> Self {
        self.volatility = vol;
        self
    }

    pub fn is_variadic(mut self, variadic: bool) -> Self {
        self.is_variadic = variadic;
        self
    }

    pub fn build(self) -> Routine {
        Routine {
            schema: self.schema.unwrap_or_else(|| "public".into()),
            name: self.name.unwrap_or_else(|| "test_func".into()),
            description: self.description,
            params: self.params,
            return_type: self.return_type.unwrap_or_else(|| {
                ReturnType::Single(PgType::Scalar(QualifiedIdentifier::new(
                    "pg_catalog",
                    "void",
                )))
            }),
            volatility: self.volatility,
            is_variadic: self.is_variadic,
            executable: true,
        }
    }
}

/// Convenience function to start building a test routine
pub fn test_routine() -> RoutineBuilder {
    RoutineBuilder::new()
}

// ============================================================================
// RoutineParam Builder
// ============================================================================

/// Builder for creating test `RoutineParam` instances
#[derive(Default)]
pub struct RoutineParamBuilder {
    name: Option<CompactString>,
    pg_type: Option<CompactString>,
    type_max_length: Option<CompactString>,
    required: bool,
    is_variadic: bool,
}

impl RoutineParamBuilder {
    pub fn new() -> Self {
        Self {
            required: true,
            ..Default::default()
        }
    }

    pub fn name(mut self, name: impl Into<CompactString>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn pg_type(mut self, t: impl Into<CompactString>) -> Self {
        self.pg_type = Some(t.into());
        self
    }

    pub fn type_max_length(mut self, t: impl Into<CompactString>) -> Self {
        self.type_max_length = Some(t.into());
        self
    }

    pub fn required(mut self, req: bool) -> Self {
        self.required = req;
        self
    }

    pub fn is_variadic(mut self, variadic: bool) -> Self {
        self.is_variadic = variadic;
        self
    }

    pub fn build(self) -> RoutineParam {
        let pg_type = self.pg_type.unwrap_or_else(|| "text".into());
        RoutineParam {
            name: self.name.unwrap_or_else(|| "param".into()),
            pg_type: pg_type.clone(),
            type_max_length: self.type_max_length.unwrap_or(pg_type),
            required: self.required,
            is_variadic: self.is_variadic,
        }
    }
}

/// Convenience function to start building a test routine param
pub fn test_param() -> RoutineParamBuilder {
    RoutineParamBuilder::new()
}

// ============================================================================
// ComputedRelationship Builder
// ============================================================================

/// Builder for creating test `ComputedRelationship` instances
pub struct ComputedRelBuilder {
    table: Option<QualifiedIdentifier>,
    function: Option<QualifiedIdentifier>,
    foreign_table: Option<QualifiedIdentifier>,
    table_alias: Option<QualifiedIdentifier>,
    is_self: bool,
    single_row: bool,
}

impl Default for ComputedRelBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ComputedRelBuilder {
    pub fn new() -> Self {
        Self {
            table: None,
            function: None,
            foreign_table: None,
            table_alias: None,
            is_self: false,
            single_row: false,
        }
    }

    pub fn table(mut self, schema: &str, name: &str) -> Self {
        self.table = Some(QualifiedIdentifier::new(schema, name));
        self
    }

    pub fn function(mut self, schema: &str, name: &str) -> Self {
        self.function = Some(QualifiedIdentifier::new(schema, name));
        self
    }

    pub fn foreign_table(mut self, schema: &str, name: &str) -> Self {
        self.foreign_table = Some(QualifiedIdentifier::new(schema, name));
        self
    }

    pub fn table_alias(mut self, schema: &str, name: &str) -> Self {
        self.table_alias = Some(QualifiedIdentifier::new(schema, name));
        self
    }

    pub fn is_self(mut self, is_self: bool) -> Self {
        self.is_self = is_self;
        self
    }

    pub fn single_row(mut self, single: bool) -> Self {
        self.single_row = single;
        self
    }

    pub fn build(self) -> ComputedRelationship {
        let table = self
            .table
            .clone()
            .unwrap_or_else(|| QualifiedIdentifier::new("public", "source"));
        ComputedRelationship {
            table: table.clone(),
            function: self
                .function
                .unwrap_or_else(|| QualifiedIdentifier::new("public", "compute_rel")),
            foreign_table: self
                .foreign_table
                .unwrap_or_else(|| QualifiedIdentifier::new("public", "target")),
            table_alias: self.table_alias.unwrap_or(table),
            is_self: self.is_self,
            single_row: self.single_row,
        }
    }
}

/// Convenience function to start building a test computed relationship
pub fn test_computed_rel() -> ComputedRelBuilder {
    ComputedRelBuilder::new()
}

// ============================================================================
// Test PostgreSQL Dialect (mirrors PgDialect for core tests)
// ============================================================================

use crate::backend::SqlDialect;
use crate::plan::types::CoercibleField;
use crate::query::sql_builder::{SqlBuilder, SqlParam};

/// PostgreSQL dialect for tests — mirrors the real PgDialect from dbrest-postgres.
///
/// This exists so that core tests can run without depending on the postgres crate.
#[derive(Debug, Clone, Copy)]
pub struct TestPgDialect;

impl SqlDialect for TestPgDialect {
    fn json_agg(&self, b: &mut SqlBuilder, alias: &str) {
        b.push("coalesce(json_agg(");
        b.push_ident(alias);
        b.push("), '[]')::text");
    }

    fn row_to_json(&self, b: &mut SqlBuilder, alias: &str) {
        b.push("row_to_json(");
        b.push_ident(alias);
        b.push(")::text");
    }

    fn count_expr(&self, b: &mut SqlBuilder, expr: &str) {
        b.push("pg_catalog.count(");
        b.push_ident(expr);
        b.push(")");
    }

    fn count_star(&self, b: &mut SqlBuilder) {
        b.push("SELECT COUNT(*) AS ");
        b.push_ident("pgrst_filtered_count");
    }

    fn set_session_var(&self, b: &mut SqlBuilder, key: &str, value: &str) {
        b.push("set_config(");
        b.push_literal(key);
        b.push(", ");
        b.push_literal(value);
        b.push(", true)");
    }

    fn get_session_var(&self, b: &mut SqlBuilder, key: &str, column_alias: &str) {
        b.push("nullif(current_setting('");
        b.push(key);
        b.push("', true), '') AS ");
        b.push(column_alias);
    }

    fn type_cast(&self, b: &mut SqlBuilder, expr: &str, ty: &str) {
        b.push(expr);
        b.push("::");
        b.push(ty);
    }

    fn from_json_body(
        &self,
        b: &mut SqlBuilder,
        columns: &[CoercibleField],
        json_bytes: &[u8],
    ) {
        let is_array = json_bytes.first().map(|&c| c == b'[').unwrap_or(false);
        let func = if is_array {
            "json_to_recordset"
        } else {
            "json_to_record"
        };
        b.push(func);
        b.push("(");
        b.push_param(SqlParam::Text(
            String::from_utf8_lossy(json_bytes).into_owned(),
        ));
        b.push("::json) AS _(");
        b.push_separated(", ", columns, |b, col| {
            b.push_ident(&col.name);
            b.push(" ");
            b.push(col.base_type.as_deref().unwrap_or("text"));
        });
        b.push(")");
    }

    fn push_type_cast_suffix(&self, b: &mut SqlBuilder, ty: &str) {
        b.push("::");
        b.push(ty);
    }

    fn push_array_type_cast_suffix(&self, b: &mut SqlBuilder, ty: &str) {
        b.push("::");
        b.push(ty);
        b.push("[]");
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn quote_literal(&self, lit: &str) -> String {
        format!("'{}'", lit.replace('\'', "''"))
    }

    fn supports_fts(&self) -> bool {
        true
    }

    fn fts_predicate(
        &self,
        b: &mut SqlBuilder,
        config: Option<&str>,
        column: &str,
        operator: &str,
    ) {
        b.push("to_tsvector(");
        if let Some(cfg) = config {
            b.push_literal(cfg);
            b.push(", ");
        }
        b.push_ident(column);
        b.push(") @@ ");
        b.push(operator);
        b.push("(");
        if let Some(cfg) = config {
            b.push_literal(cfg);
            b.push(", ");
        }
    }

    fn supports_lateral_join(&self) -> bool {
        true
    }

    fn named_param_assign(&self) -> &str {
        " := "
    }
}
