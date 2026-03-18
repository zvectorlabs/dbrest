//! SQLite SQL dialect implementation.

use dbrest_core::backend::SqlDialect;
use dbrest_core::plan::types::CoercibleField;
use dbrest_core::query::sql_builder::{SqlBuilder, SqlParam};

/// SQLite dialect — generates SQLite-specific SQL syntax.
#[derive(Debug, Clone, Copy)]
pub struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn json_agg_with_columns(&self, b: &mut SqlBuilder, alias: &str, columns: &[&str]) {
        if columns.is_empty() {
            // Fallback: assume the alias refers to a single-column JSON text.
            b.push("COALESCE(json_group_array(json(");
            b.push_ident(alias);
            b.push(")), '[]')");
        } else {
            // Build: COALESCE(json_group_array(json_object('col1', "alias"."col1", ...)), '[]')
            b.push("COALESCE(json_group_array(json_object(");
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    b.push(", ");
                }
                b.push("'");
                b.push(col);
                b.push("', ");
                b.push_ident(alias);
                b.push(".");
                b.push_ident(col);
            }
            b.push(")), '[]')");
        }
    }

    fn row_to_json_with_columns(&self, b: &mut SqlBuilder, alias: &str, columns: &[&str]) {
        if columns.is_empty() {
            b.push("json(");
            b.push_ident(alias);
            b.push(")");
        } else {
            // Build: json_object('col1', "alias"."col1", ...)
            b.push("json_object(");
            for (i, col) in columns.iter().enumerate() {
                if i > 0 {
                    b.push(", ");
                }
                b.push("'");
                b.push(col);
                b.push("', ");
                b.push_ident(alias);
                b.push(".");
                b.push_ident(col);
            }
            b.push(")");
        }
    }

    fn count_expr(&self, b: &mut SqlBuilder, expr: &str) {
        b.push("COUNT(");
        b.push_ident(expr);
        b.push(")");
    }

    fn count_star(&self, b: &mut SqlBuilder) {
        b.push("SELECT COUNT(*) AS ");
        b.push_ident("dbrst_filtered_count");
    }

    fn set_session_var(&self, _b: &mut SqlBuilder, _key: &str, _value: &str) {
        tracing::warn!(
            "set_session_var called on SqliteDialect — this is a no-op; \
             use build_tx_vars_statement instead"
        );
    }

    fn session_vars_are_select_exprs(&self) -> bool {
        false
    }

    fn build_tx_vars_statement(&self, b: &mut SqlBuilder, vars: &[(&str, &str)]) {
        // Single INSERT OR REPLACE with multiple VALUES rows.
        b.push("INSERT OR REPLACE INTO _dbrest_vars(key, val) VALUES ");
        for (i, (key, value)) in vars.iter().enumerate() {
            if i > 0 {
                b.push(", ");
            }
            b.push("('");
            for ch in key.chars() {
                if ch == '\'' {
                    b.push("'");
                }
                b.push_char(ch);
            }
            b.push("', '");
            for ch in value.chars() {
                if ch == '\'' {
                    b.push("'");
                }
                b.push_char(ch);
            }
            b.push("')");
        }
    }

    fn get_session_var(&self, b: &mut SqlBuilder, key: &str, column_alias: &str) {
        // Read from the temp vars table, returning NULL if not set.
        b.push("(SELECT val FROM _dbrest_vars WHERE key = '");
        for ch in key.chars() {
            if ch == '\'' {
                b.push("'");
            }
            b.push_char(ch);
        }
        b.push("') AS ");
        b.push(column_alias);
    }

    fn type_cast(&self, b: &mut SqlBuilder, expr: &str, ty: &str) {
        b.push("CAST(");
        b.push(expr);
        b.push(" AS ");
        b.push(&sqlite_type(ty));
        b.push(")");
    }

    fn from_json_body(&self, b: &mut SqlBuilder, columns: &[CoercibleField], json_bytes: &[u8]) {
        // SQLite: Use json_each() to iterate over array elements,
        // then json_extract() to pull out each column.
        //
        // SELECT json_extract(value, '$.col1') AS "col1", ... FROM json_each($1)
        let is_array = json_bytes.first().map(|&c| c == b'[').unwrap_or(false);

        if is_array {
            b.push("(SELECT ");
            b.push_separated(", ", columns, |b, col| {
                b.push("json_extract(value, '$.");
                // Escape the column name for JSON path
                b.push(&col.name.replace('\'', "''"));
                b.push("') AS ");
                b.push_ident(&col.name);
            });
            b.push(" FROM json_each(");
            b.push_param(SqlParam::Text(
                String::from_utf8_lossy(json_bytes).into_owned(),
            ));
            b.push("))");
        } else {
            // Single object: wrap in array
            b.push("(SELECT ");
            b.push_separated(", ", columns, |b, col| {
                b.push("json_extract(");
                b.push_param(SqlParam::Text(
                    String::from_utf8_lossy(json_bytes).into_owned(),
                ));
                b.push(", '$.");
                b.push(&col.name.replace('\'', "''"));
                b.push("') AS ");
                b.push_ident(&col.name);
            });
            b.push(")");
        }
    }

    fn push_type_cast_suffix(&self, b: &mut SqlBuilder, ty: &str) {
        // SQLite doesn't support :: syntax. We can't easily wrap in CAST
        // after the fact, so for suffix-style casts we use a no-op for now.
        // The type affinity system in SQLite handles most cases automatically.
        let _ = (b, ty);
    }

    fn push_array_type_cast_suffix(&self, b: &mut SqlBuilder, _ty: &str) {
        // SQLite has no array types — this is a no-op.
        let _ = b;
    }

    fn quote_ident(&self, ident: &str) -> String {
        format!("\"{}\"", ident.replace('"', "\"\""))
    }

    fn quote_literal(&self, lit: &str) -> String {
        format!("'{}'", lit.replace('\'', "''"))
    }

    fn supports_fts(&self) -> bool {
        false // FTS5 support can be added later
    }

    fn fts_predicate(
        &self,
        _b: &mut SqlBuilder,
        _config: Option<&str>,
        _column: &str,
        _operator: &str,
    ) {
        // FTS5 support not yet implemented.
        // Future: column MATCH $1
    }

    fn row_to_json_star(&self, b: &mut SqlBuilder, source: &str) {
        // SQLite doesn't support source.* in function calls.
        // For scalar RPCs, we wrap with json_object from all columns.
        // Fallback: just select all columns as JSON.
        b.push("json_group_array(json_object(*)) FROM ");
        b.push(source);
    }

    fn count_star_from(&self, b: &mut SqlBuilder, source: &str) {
        b.push("(SELECT COUNT(*) FROM ");
        b.push(source);
        b.push(")");
    }

    fn push_literal(&self, b: &mut SqlBuilder, s: &str) {
        // SQLite uses standard SQL literal escaping (no E-string prefix).
        b.push("'");
        for ch in s.chars() {
            if ch == '\'' {
                b.push("'");
            }
            b.push_char(ch);
        }
        b.push("'");
    }

    fn supports_lateral_join(&self) -> bool {
        false
    }

    fn named_param_assign(&self) -> &str {
        // SQLite doesn't support named parameter assignment in function calls.
        // This won't typically be used since SQLite doesn't have stored procedures.
        " = "
    }

    fn supports_dml_cte(&self) -> bool {
        false
    }
}

/// Map PostgreSQL type names to SQLite type affinities.
fn sqlite_type(pg_type: &str) -> String {
    match pg_type.to_lowercase().as_str() {
        "integer" | "int" | "int4" | "int8" | "bigint" | "smallint" | "int2" | "serial"
        | "bigserial" => "INTEGER".to_string(),
        "real" | "float4" | "float8" | "double precision" | "numeric" | "decimal" => {
            "REAL".to_string()
        }
        "boolean" | "bool" => "INTEGER".to_string(), // SQLite uses 0/1
        "blob" | "bytea" => "BLOB".to_string(),
        "json" | "jsonb" => "TEXT".to_string(),
        _ => "TEXT".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dialect() -> SqliteDialect {
        SqliteDialect
    }

    #[test]
    fn test_json_agg() {
        let mut b = SqlBuilder::new();
        dialect().json_agg(&mut b, "_dbrst_t");
        assert_eq!(
            b.sql(),
            "COALESCE(json_group_array(json(\"_dbrst_t\")), '[]')"
        );
    }

    #[test]
    fn test_count_expr() {
        let mut b = SqlBuilder::new();
        dialect().count_expr(&mut b, "_dbrst_t");
        assert_eq!(b.sql(), "COUNT(\"_dbrst_t\")");
    }

    #[test]
    fn test_count_star_from() {
        let mut b = SqlBuilder::new();
        dialect().count_star_from(&mut b, "dbrst_source");
        assert_eq!(b.sql(), "(SELECT COUNT(*) FROM dbrst_source)");
    }

    #[test]
    fn test_type_cast() {
        let mut b = SqlBuilder::new();
        dialect().type_cast(&mut b, "col", "integer");
        assert_eq!(b.sql(), "CAST(col AS INTEGER)");
    }

    #[test]
    fn test_type_cast_text() {
        let mut b = SqlBuilder::new();
        dialect().type_cast(&mut b, "col", "varchar");
        assert_eq!(b.sql(), "CAST(col AS TEXT)");
    }

    #[test]
    fn test_push_literal_no_backslash() {
        let mut b = SqlBuilder::new();
        dialect().push_literal(&mut b, "hello");
        assert_eq!(b.sql(), "'hello'");
    }

    #[test]
    fn test_push_literal_with_quote() {
        let mut b = SqlBuilder::new();
        dialect().push_literal(&mut b, "it's");
        assert_eq!(b.sql(), "'it''s'");
    }

    #[test]
    fn test_push_literal_with_backslash() {
        let mut b = SqlBuilder::new();
        dialect().push_literal(&mut b, "back\\slash");
        // SQLite does NOT use E-string prefix
        assert_eq!(b.sql(), "'back\\slash'");
    }

    #[test]
    fn test_build_tx_vars_statement() {
        let mut b = SqlBuilder::new();
        dialect().build_tx_vars_statement(
            &mut b,
            &[("request.method", "GET"), ("request.path", "/users")],
        );
        let sql = b.sql();
        assert!(sql.contains("INSERT OR REPLACE INTO _dbrest_vars"));
        assert!(sql.contains("request.method"));
        assert!(sql.contains("GET"));
        assert!(sql.contains("request.path"));
        assert!(sql.contains("/users"));
    }

    #[test]
    fn test_session_vars_are_not_select_exprs() {
        assert!(!dialect().session_vars_are_select_exprs());
    }

    #[test]
    fn test_get_session_var() {
        let mut b = SqlBuilder::new();
        dialect().get_session_var(&mut b, "response.headers", "response_headers");
        assert!(b.sql().contains("_dbrest_vars"));
        assert!(b.sql().contains("response.headers"));
        assert!(b.sql().contains("AS response_headers"));
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(dialect().quote_ident("users"), "\"users\"");
        assert_eq!(dialect().quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn test_quote_literal() {
        assert_eq!(dialect().quote_literal("hello"), "'hello'");
        assert_eq!(dialect().quote_literal("it's"), "'it''s'");
    }

    #[test]
    fn test_supports_fts() {
        assert!(!dialect().supports_fts());
    }

    #[test]
    fn test_supports_lateral_join() {
        assert!(!dialect().supports_lateral_join());
    }

    #[test]
    fn test_set_session_var_does_not_panic() {
        let d = dialect();
        let mut b = SqlBuilder::new();
        // Should log a warning but not panic
        d.set_session_var(&mut b, "key", "value");
        assert_eq!(b.sql(), "");
    }

    #[test]
    fn test_sqlite_type_mapping() {
        assert_eq!(sqlite_type("integer"), "INTEGER");
        assert_eq!(sqlite_type("bigint"), "INTEGER");
        assert_eq!(sqlite_type("real"), "REAL");
        assert_eq!(sqlite_type("float8"), "REAL");
        assert_eq!(sqlite_type("boolean"), "INTEGER");
        assert_eq!(sqlite_type("bytea"), "BLOB");
        assert_eq!(sqlite_type("json"), "TEXT");
        assert_eq!(sqlite_type("text"), "TEXT");
        assert_eq!(sqlite_type("varchar"), "TEXT");
    }
}
