//! Core SQL builder for constructing parameterized queries.
//!
//! Sits at the foundation of the query module. Every other module in `query/`
//! uses `SqlBuilder` to accumulate SQL text and bind parameters.
//!
//! # Design
//!
//! `SqlBuilder` wraps a `String` buffer and a `Vec<SqlParam>`. As SQL fragments
//! are appended, user-supplied values are stored in the param list while the
//! buffer receives `$1`, `$2`, … placeholders. The final `build()` call returns
//! the completed SQL string paired with the ordered parameter vector, ready for
//! `sqlx::query_with`.
//!
//! # SQL Example
//!
//! ```sql
//! -- After pushing: push("SELECT "), push_ident("name"), push(" FROM "),
//! --   push_qi(qi), push(" WHERE "), push_ident("age"), push(" >= "),
//! --   push_param(Text("18"))
//! SELECT "name" FROM "public"."users" WHERE "age" >= $1
//! ```

use bytes::Bytes;

use crate::types::identifiers::QualifiedIdentifier;

// ==========================================================================
// SqlParam — typed bind parameter
// ==========================================================================

/// A bind parameter for a parameterized SQL query.
///
/// Created by `SqlBuilder::push_param` and consumed by the database executor.
/// Each variant maps to a different sqlx encode path.
#[derive(Debug, Clone)]
pub enum SqlParam {
    /// A text value (`TEXT` / `VARCHAR`).
    Text(String),
    /// A JSON value (`JSONB` / `JSON`). The bytes contain valid JSON.
    Json(Bytes),
    /// Raw binary data (`BYTEA`).
    Binary(Bytes),
    /// An explicit SQL NULL.
    Null,
}

// ==========================================================================
// SqlBuilder — accumulator for SQL text + bind parameters
// ==========================================================================

/// Accumulates SQL text and bind parameters into a parameterized query.
///
/// Constructed via `SqlBuilder::new()`, populated with `push*` methods, and
/// finalised with `build()` which returns `(String, Vec<SqlParam>)`.
///
/// # Invariants
///
/// - `push_param` always appends `$N` where N = params.len() after the push.
/// - Identifiers are always double-quote escaped via `push_ident` / `push_qi`.
/// - Literals are single-quote escaped via `push_literal`.
#[derive(Debug, Clone)]
pub struct SqlBuilder {
    /// The SQL text buffer.
    buffer: String,
    /// Ordered bind parameters referenced by `$N` placeholders.
    params: Vec<SqlParam>,
}

impl SqlBuilder {
    /// Create an empty builder.
    pub fn new() -> Self {
        Self {
            buffer: String::with_capacity(256),
            params: Vec::new(),
        }
    }

    /// Create a builder pre-loaded with the given SQL text.
    pub fn with_sql(sql: impl Into<String>) -> Self {
        Self {
            buffer: sql.into(),
            params: Vec::new(),
        }
    }

    // ------------------------------------------------------------------
    // Push methods
    // ------------------------------------------------------------------

    /// Append raw SQL text (no escaping).
    pub fn push(&mut self, s: &str) {
        self.buffer.push_str(s);
    }

    /// Append a single character.
    pub fn push_char(&mut self, c: char) {
        self.buffer.push(c);
    }

    /// Append a double-quoted SQL identifier.
    ///
    /// Internal double-quotes are doubled per the SQL standard.
    ///
    /// # SQL Example
    /// ```sql
    /// -- push_ident("user\"name") produces:
    /// "user""name"
    /// ```
    pub fn push_ident(&mut self, ident: &str) {
        self.buffer.push('"');
        for ch in ident.chars() {
            if ch == '"' {
                self.buffer.push('"');
            }
            self.buffer.push(ch);
        }
        self.buffer.push('"');
    }

    /// Append a schema-qualified identifier (`"schema"."name"`).
    ///
    /// If the schema is empty, only the name is emitted.
    ///
    /// # SQL Example
    /// ```sql
    /// -- push_qi(QI { schema: "public", name: "users" }) produces:
    /// "public"."users"
    /// ```
    pub fn push_qi(&mut self, qi: &QualifiedIdentifier) {
        if !qi.schema.is_empty() {
            self.push_ident(&qi.schema);
            self.buffer.push('.');
        }
        self.push_ident(&qi.name);
    }

    /// Append a single-quoted SQL literal.
    ///
    /// Single-quotes are doubled. If the value contains a backslash, the
    /// PostgreSQL E-string syntax (`E'...'`) is used so that `\\` is treated
    /// as a literal backslash regardless of `standard_conforming_strings`.
    ///
    /// # SQL Example
    /// ```sql
    /// -- push_literal("it's") produces:
    /// 'it''s'
    /// -- push_literal("back\\slash") produces:
    /// E'back\\slash'
    /// ```
    pub fn push_literal(&mut self, s: &str) {
        let has_backslash = s.contains('\\');
        if has_backslash {
            self.buffer.push('E');
        }
        self.buffer.push('\'');
        for ch in s.chars() {
            if ch == '\'' {
                self.buffer.push('\'');
            }
            self.buffer.push(ch);
        }
        self.buffer.push('\'');
    }

    /// Append a bind-parameter placeholder (`$N`) and store the value.
    ///
    /// The placeholder index is `self.params.len() + 1` (1-based).
    pub fn push_param(&mut self, param: SqlParam) {
        self.params.push(param);
        self.buffer.push_str(&format!("${}", self.params.len()));
    }

    /// Current number of bind parameters.
    pub fn param_count(&self) -> usize {
        self.params.len()
    }

    /// Whether the builder is empty (no SQL text).
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Current length of the SQL text buffer.
    pub fn sql_len(&self) -> usize {
        self.buffer.len()
    }

    // ------------------------------------------------------------------
    // Compound helpers
    // ------------------------------------------------------------------

    /// Append `items` separated by `sep`, where each item is written by
    /// the callback `f`.
    ///
    /// # Behaviour
    ///
    /// Does nothing if `items` is empty. Does not emit a trailing separator.
    pub fn push_separated<T, F>(&mut self, sep: &str, items: &[T], f: F)
    where
        F: Fn(&mut SqlBuilder, &T),
    {
        for (i, item) in items.iter().enumerate() {
            if i > 0 {
                self.buffer.push_str(sep);
            }
            f(self, item);
        }
    }

    /// Merge another builder's SQL and params into this one.
    ///
    /// The merged builder's `$N` placeholders are rewritten to continue
    /// from this builder's current param count.
    pub fn push_builder(&mut self, other: &SqlBuilder) {
        let offset = self.params.len();
        if offset == 0 {
            // No rewriting needed
            self.buffer.push_str(&other.buffer);
        } else {
            // Rewrite $N placeholders
            let mut rest = other.buffer.as_str();
            while let Some(idx) = rest.find('$') {
                self.buffer.push_str(&rest[..idx]);
                rest = &rest[idx + 1..];
                // Parse the number after $
                let num_end = rest
                    .find(|c: char| !c.is_ascii_digit())
                    .unwrap_or(rest.len());
                if num_end > 0 {
                    let n: usize = rest[..num_end].parse().unwrap_or(0);
                    self.buffer.push_str(&format!("${}", n + offset));
                    rest = &rest[num_end..];
                } else {
                    self.buffer.push('$');
                }
            }
            self.buffer.push_str(rest);
        }
        self.params.extend(other.params.iter().cloned());
    }

    // ------------------------------------------------------------------
    // Finalisation
    // ------------------------------------------------------------------

    /// Consume the builder and return the SQL string and parameters.
    pub fn build(self) -> (String, Vec<SqlParam>) {
        (self.buffer, self.params)
    }

    /// Borrow the current SQL text (for debugging / assertions).
    pub fn sql(&self) -> &str {
        &self.buffer
    }

    /// Borrow the current parameters (for debugging / assertions).
    pub fn params(&self) -> &[SqlParam] {
        &self.params
    }
}

impl Default for SqlBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ==========================================================================
// Standalone escape functions
// ==========================================================================

/// Escape a SQL identifier by doubling internal double-quotes.
///
/// Does **not** wrap in quotes — use `quote_ident` for that.
///
/// # Example
/// ```
/// assert_eq!(dbrest::query::sql_builder::escape_ident("col"), "col");
/// assert_eq!(dbrest::query::sql_builder::escape_ident("a\"b"), "a\"\"b");
/// ```
pub fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Double-quote a SQL identifier.
///
/// # Example
/// ```
/// assert_eq!(dbrest::query::sql_builder::quote_ident("col"), "\"col\"");
/// ```
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", escape_ident(s))
}

/// Escape a SQL literal by doubling single-quotes.
///
/// Returns the escaped content **without** surrounding quotes.
pub fn escape_literal(s: &str) -> String {
    s.replace('\'', "''")
}

/// Single-quote a SQL literal (with E-string for backslashes).
///
/// # Example
/// ```
/// assert_eq!(dbrest::query::sql_builder::quote_literal("it's"), "'it''s'");
/// assert_eq!(dbrest::query::sql_builder::quote_literal("a\\b"), "E'a\\b'");
/// ```
pub fn quote_literal(s: &str) -> String {
    let escaped = escape_literal(s);
    if s.contains('\\') {
        format!("E'{}'", escaped)
    } else {
        format!("'{}'", escaped)
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // SqlBuilder basic push
    // ------------------------------------------------------------------

    #[test]
    fn test_push_raw_sql() {
        let mut b = SqlBuilder::new();
        b.push("SELECT 1");
        assert_eq!(b.sql(), "SELECT 1");
    }

    #[test]
    fn test_push_char() {
        let mut b = SqlBuilder::new();
        b.push("a");
        b.push_char('b');
        assert_eq!(b.sql(), "ab");
    }

    #[test]
    fn test_push_ident_simple() {
        let mut b = SqlBuilder::new();
        b.push_ident("users");
        assert_eq!(b.sql(), "\"users\"");
    }

    #[test]
    fn test_push_ident_with_quote() {
        let mut b = SqlBuilder::new();
        b.push_ident("user\"name");
        assert_eq!(b.sql(), "\"user\"\"name\"");
    }

    #[test]
    fn test_push_qi_qualified() {
        let mut b = SqlBuilder::new();
        let qi = QualifiedIdentifier::new("public", "users");
        b.push_qi(&qi);
        assert_eq!(b.sql(), "\"public\".\"users\"");
    }

    #[test]
    fn test_push_qi_unqualified() {
        let mut b = SqlBuilder::new();
        let qi = QualifiedIdentifier::unqualified("users");
        b.push_qi(&qi);
        assert_eq!(b.sql(), "\"users\"");
    }

    #[test]
    fn test_push_literal_simple() {
        let mut b = SqlBuilder::new();
        b.push_literal("hello");
        assert_eq!(b.sql(), "'hello'");
    }

    #[test]
    fn test_push_literal_with_quote() {
        let mut b = SqlBuilder::new();
        b.push_literal("it's");
        assert_eq!(b.sql(), "'it''s'");
    }

    #[test]
    fn test_push_literal_with_backslash() {
        let mut b = SqlBuilder::new();
        b.push_literal("back\\slash");
        assert_eq!(b.sql(), "E'back\\slash'");
    }

    // ------------------------------------------------------------------
    // Bind parameters
    // ------------------------------------------------------------------

    #[test]
    fn test_push_param() {
        let mut b = SqlBuilder::new();
        b.push("SELECT * WHERE id = ");
        b.push_param(SqlParam::Text("42".to_string()));
        assert_eq!(b.sql(), "SELECT * WHERE id = $1");
        assert_eq!(b.param_count(), 1);
    }

    #[test]
    fn test_push_multiple_params() {
        let mut b = SqlBuilder::new();
        b.push_param(SqlParam::Text("a".into()));
        b.push(", ");
        b.push_param(SqlParam::Text("b".into()));
        b.push(", ");
        b.push_param(SqlParam::Null);
        assert_eq!(b.sql(), "$1, $2, $3");
        assert_eq!(b.param_count(), 3);
    }

    // ------------------------------------------------------------------
    // push_separated
    // ------------------------------------------------------------------

    #[test]
    fn test_push_separated_empty() {
        let mut b = SqlBuilder::new();
        let items: Vec<String> = vec![];
        b.push_separated(", ", &items, |b, item| b.push(item));
        assert_eq!(b.sql(), "");
    }

    #[test]
    fn test_push_separated_one() {
        let mut b = SqlBuilder::new();
        let items = vec!["a".to_string()];
        b.push_separated(", ", &items, |b, item| b.push(item));
        assert_eq!(b.sql(), "a");
    }

    #[test]
    fn test_push_separated_many() {
        let mut b = SqlBuilder::new();
        let items = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        b.push_separated(", ", &items, |b, item| b.push_ident(item));
        assert_eq!(b.sql(), "\"a\", \"b\", \"c\"");
    }

    // ------------------------------------------------------------------
    // push_builder (merge)
    // ------------------------------------------------------------------

    #[test]
    fn test_push_builder_no_offset() {
        let mut a = SqlBuilder::new();
        a.push("A ");

        let mut b = SqlBuilder::new();
        b.push("B ");
        b.push_param(SqlParam::Text("x".into()));

        a.push_builder(&b);
        assert_eq!(a.sql(), "A B $1");
        assert_eq!(a.param_count(), 1);
    }

    #[test]
    fn test_push_builder_with_offset() {
        let mut a = SqlBuilder::new();
        a.push_param(SqlParam::Text("p1".into()));
        a.push(" AND ");

        let mut b = SqlBuilder::new();
        b.push_param(SqlParam::Text("p2".into()));
        b.push(" OR ");
        b.push_param(SqlParam::Text("p3".into()));

        a.push_builder(&b);
        assert_eq!(a.sql(), "$1 AND $2 OR $3");
        assert_eq!(a.param_count(), 3);
    }

    // ------------------------------------------------------------------
    // build
    // ------------------------------------------------------------------

    #[test]
    fn test_build() {
        let mut b = SqlBuilder::new();
        b.push("SELECT ");
        b.push_ident("name");
        b.push(" FROM ");
        b.push_qi(&QualifiedIdentifier::new("public", "users"));
        b.push(" WHERE ");
        b.push_ident("age");
        b.push(" >= ");
        b.push_param(SqlParam::Text("18".into()));

        let (sql, params) = b.build();
        assert_eq!(
            sql,
            "SELECT \"name\" FROM \"public\".\"users\" WHERE \"age\" >= $1"
        );
        assert_eq!(params.len(), 1);
    }

    // ------------------------------------------------------------------
    // Standalone escape / quote functions
    // ------------------------------------------------------------------

    #[test]
    fn test_escape_ident() {
        assert_eq!(escape_ident("simple"), "simple");
        assert_eq!(escape_ident("with\"quote"), "with\"\"quote");
    }

    #[test]
    fn test_quote_ident_fn() {
        assert_eq!(quote_ident("col"), "\"col\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn test_escape_literal() {
        assert_eq!(escape_literal("hello"), "hello");
        assert_eq!(escape_literal("it's"), "it''s");
    }

    #[test]
    fn test_quote_literal_fn() {
        assert_eq!(quote_literal("hello"), "'hello'");
        assert_eq!(quote_literal("it's"), "'it''s'");
        assert_eq!(quote_literal("a\\b"), "E'a\\b'");
    }

    // ------------------------------------------------------------------
    // with_sql constructor
    // ------------------------------------------------------------------

    #[test]
    fn test_with_sql() {
        let b = SqlBuilder::with_sql("SELECT 1");
        assert_eq!(b.sql(), "SELECT 1");
        assert!(b.params().is_empty());
    }

    // ------------------------------------------------------------------
    // Edge cases
    // ------------------------------------------------------------------

    #[test]
    fn test_empty_builder() {
        let b = SqlBuilder::new();
        assert!(b.is_empty());
        assert_eq!(b.sql_len(), 0);
        let (sql, params) = b.build();
        assert_eq!(sql, "");
        assert!(params.is_empty());
    }

    #[test]
    fn test_push_ident_empty_string() {
        let mut b = SqlBuilder::new();
        b.push_ident("");
        assert_eq!(b.sql(), "\"\"");
    }

    #[test]
    fn test_push_literal_empty_string() {
        let mut b = SqlBuilder::new();
        b.push_literal("");
        assert_eq!(b.sql(), "''");
    }
}
