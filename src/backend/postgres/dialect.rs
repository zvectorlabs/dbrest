//! PostgreSQL SQL dialect implementation.

use crate::backend::SqlDialect;
use crate::plan::types::CoercibleField;
use crate::query::sql_builder::{SqlBuilder, SqlParam};

/// PostgreSQL dialect — generates PG-specific SQL syntax.
#[derive(Debug, Clone, Copy)]
pub struct PgDialect;

impl SqlDialect for PgDialect {
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
