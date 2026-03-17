//! SQL fragment formatting functions.
//!
//! Provides reusable, pure functions that append SQL fragments to an
//! `SqlBuilder`. Each function handles one atomic piece of SQL syntax
//! (an identifier, a filter predicate, an ORDER BY clause, etc.).
//!
//! This module sits between the plan types and the higher-level
//! `builder` / `statements` modules: the builder calls these functions
//! to assemble full queries from plan trees.
//!
//! # SQL Example
//!
//! ```sql
//! -- fmt_filter produces predicates like:
//! "age" >= $1
//! NOT "status" = ANY($2)
//! "name" IS NULL
//! ```

use crate::api_request::types::{
    FtsOperator, IsValue, JsonOperand, JsonOperation, LogicOperator, OpExpr, Operation,
    OrderDirection, OrderNulls, QuantOperator, SimpleOperator,
};
use crate::backend::SqlDialect;
use crate::plan::read_plan::JoinCondition;
use crate::plan::types::{
    CoercibleField, CoercibleFilter, CoercibleLogicTree, CoercibleOrderTerm, CoercibleSelectField,
};
use crate::types::identifiers::QualifiedIdentifier;

use super::sql_builder::{SqlBuilder, SqlParam};

// ==========================================================================
// Identifier & field formatting
// ==========================================================================

/// Append a double-quoted identifier to the builder.
///
/// Delegates to `SqlBuilder::push_ident`.
pub fn fmt_ident(b: &mut SqlBuilder, ident: &str) {
    b.push_ident(ident);
}

/// Append a qualified column reference: `"table"."col"` or `"table".*`.
///
/// # SQL Example
/// ```sql
/// "test_api"."users"."name"
/// "test_api"."users".*
/// ```
pub fn fmt_column(b: &mut SqlBuilder, qi: &QualifiedIdentifier, col: &str) {
    b.push_qi(qi);
    b.push(".");
    if col == "*" {
        b.push("*");
    } else {
        b.push_ident(col);
    }
}

/// Format a computed field function call
///
/// # SQL Example
/// ```sql
/// full_name("people")
/// "schema"."full_name"("people")
/// ```
pub fn fmt_computed_field(b: &mut SqlBuilder, qi: &QualifiedIdentifier, field: &CoercibleField) {
    if let Some(ref func_qi) = field.computed_function {
        // Emit function_schema.function_name(table_name)
        // PostgreSQL computed field functions expect the table name (unqualified) or alias
        // Use just the table name part, not the full qualified identifier
        b.push_qi(func_qi);
        b.push("(");
        b.push_ident(&qi.name); // Use just the table name, not the full QualifiedIdentifier
        b.push(")");
    } else {
        // This should never happen - computed fields should always have computed_function set
        // If we reach here, it's a bug. Format as a column reference instead of a function call
        // to avoid generating invalid SQL like "unknown(table)"
        b.push_qi(qi);
        b.push(".");
        b.push_ident(&field.name);
    }
}

/// Append a field reference with optional JSON path and tsvector conversion.
///
/// # Behaviour
///
/// - If the field is `*` (full row), emits `"table".*`
/// - If the field is a computed field, emits `function_name(table_alias)`
/// - If there is a JSON path, appends `->` / `->>` operators
/// - If `to_tsvector` is set, wraps in `to_tsvector('lang', ...)`
///
/// # SQL Example
/// ```sql
/// "users"."data"->'address'->>'city'
/// to_tsvector('english', "posts"."body")
/// full_name("people")
/// ```
pub fn fmt_field(b: &mut SqlBuilder, qi: &QualifiedIdentifier, field: &CoercibleField) {
    let needs_tsvector = field.to_tsvector.is_some();
    let needs_json_wrapper = field.to_json && !field.json_path.is_empty() && !field.is_computed;

    if needs_tsvector {
        b.push("to_tsvector(");
        if let Some(ref lang) = field.to_tsvector {
            b.push_literal(lang);
            b.push(", ");
        }
    }

    // Wrap with to_jsonb() for composite/array types when JSON path is present
    if needs_json_wrapper {
        b.push("to_jsonb(");
    }

    if field.full_row {
        b.push_qi(qi);
        b.push(".*");
    } else if field.is_computed {
        // Computed field: function call
        fmt_computed_field(b, qi, field);
    } else {
        // Regular column
        fmt_column(b, qi, &field.name);
    }

    // Close to_jsonb() wrapper if opened
    if needs_json_wrapper {
        b.push(")");
    }

    // JSON path (not applicable to computed fields, but handle for consistency)
    if !field.json_path.is_empty() && !field.is_computed {
        fmt_json_path(b, &field.json_path);
    }

    if needs_tsvector {
        b.push(")");
    }
}

/// Append a field with optional transform procedure wrapping.
///
/// If the field has a `transform`, it wraps the field in the procedure call:
/// `my_transform("schema"."table"."col")`
///
/// # SQL Example
/// ```sql
/// my_parser("users"."bio")
/// ```
pub fn fmt_table_coerce(b: &mut SqlBuilder, qi: &QualifiedIdentifier, field: &CoercibleField) {
    if let Some(ref transform) = field.transform {
        // Wrap in transform function
        b.push(&transform.function);
        b.push("(");
        fmt_field(b, qi, field);
        b.push(")");
    } else {
        fmt_field(b, qi, field);
    }
}

/// Append a full select expression: coerce + cast + aggregate + alias.
///
/// # Behaviour
///
/// - Applies aggregate function (COUNT, SUM, …) if present
/// - Applies aggregate cast (`::bigint`) if present
/// - Applies field cast if present
/// - Emits `AS "alias"` if an alias is given
///
/// # SQL Example
/// ```sql
/// COUNT("id")::bigint AS "total"
/// "name"::text AS "user_name"
/// ```
pub fn fmt_select_item(b: &mut SqlBuilder, qi: &QualifiedIdentifier, sel: &CoercibleSelectField, dialect: &dyn SqlDialect) {
    // Aggregate wrapper
    if let Some(ref agg) = sel.agg_function {
        b.push(&agg.to_string().to_uppercase());
        b.push("(");
    }

    // The field itself (with coercion / transform)
    fmt_table_coerce(b, qi, &sel.field);

    // Close aggregate
    if sel.agg_function.is_some() {
        b.push(")");

        // Aggregate cast
        if let Some(ref agg_cast) = sel.agg_cast {
            push_type_cast(b, dialect, Some(agg_cast.as_str()));
        }
    } else {
        // Field-level cast
        if let Some(ref cast) = sel.cast {
            push_type_cast(b, dialect, Some(cast.as_str()));
        }
    }

    // Alias
    let alias = sel.alias.as_ref().unwrap_or(&sel.field.name);
    b.push(" AS ");
    b.push_ident(alias);
}

// ==========================================================================
// JSON path
// ==========================================================================

/// Append JSON arrow operators for a JSON path.
///
/// # SQL Example
/// ```sql
/// ->'address'->>'city'
/// ->0->>1
/// ```
pub fn fmt_json_path(b: &mut SqlBuilder, path: &[JsonOperation]) {
    for op in path {
        match op {
            JsonOperation::Arrow(operand) => {
                b.push("->");
                fmt_json_operand(b, operand);
            }
            JsonOperation::Arrow2(operand) => {
                b.push("->>");
                fmt_json_operand(b, operand);
            }
        }
    }
}

/// Append a single JSON operand — either a quoted key or a numeric index.
///
/// # SQL Example
/// ```sql
/// -- Key:  'address'
/// -- Index: 0
/// ```
fn fmt_json_operand(b: &mut SqlBuilder, operand: &JsonOperand) {
    match operand {
        JsonOperand::Key(key) => b.push_literal(key),
        JsonOperand::Idx(idx) => b.push(idx),
    }
}

// ==========================================================================
// Operators
// ==========================================================================

/// Return the SQL operator string for a simple (non-quantifiable) operator.
///
/// # SQL Example
/// ```sql
/// -- SimpleOperator::NotEqual  ->  "<>"
/// -- SimpleOperator::Contains  ->  "@>"
/// -- SimpleOperator::Overlap   ->  "&&"
/// ```
pub fn simple_operator(op: SimpleOperator) -> &'static str {
    match op {
        SimpleOperator::NotEqual => "<>",
        SimpleOperator::Contains => "@>",
        SimpleOperator::Contained => "<@",
        SimpleOperator::Overlap => "&&",
        SimpleOperator::StrictlyLeft => "<<",
        SimpleOperator::StrictlyRight => ">>",
        SimpleOperator::NotExtendsRight => "&<",
        SimpleOperator::NotExtendsLeft => "&>",
        SimpleOperator::Adjacent => "-|-",
    }
}

/// Return the SQL operator string for a quantifiable operator.
///
/// These operators can be used with `ANY` / `ALL` modifiers.
///
/// # SQL Example
/// ```sql
/// -- QuantOperator::Equal          ->  "="
/// -- QuantOperator::GreaterThan    ->  ">"
/// -- QuantOperator::Like           ->  "LIKE"
/// ```
pub fn quant_operator(op: QuantOperator) -> &'static str {
    match op {
        QuantOperator::Equal => "=",
        QuantOperator::GreaterThanEqual => ">=",
        QuantOperator::GreaterThan => ">",
        QuantOperator::LessThanEqual => "<=",
        QuantOperator::LessThan => "<",
        QuantOperator::Like => " LIKE ",
        QuantOperator::ILike => " ILIKE ",
        QuantOperator::Match => "~",
        QuantOperator::IMatch => "~*",
    }
}

/// Append a full-text search operator expression.
///
/// # SQL Example
/// ```sql
/// @@ to_tsquery('english', $1)
/// @@ plainto_tsquery($1)
/// @@ phraseto_tsquery('english', $1)
/// @@ websearch_to_tsquery($1)
/// ```
pub fn fts_operator(b: &mut SqlBuilder, dialect: &dyn SqlDialect, op: FtsOperator, lang: Option<&str>, val: &str) {
    let operator = match op {
        FtsOperator::Fts => "to_tsquery",
        FtsOperator::FtsPlain => "plainto_tsquery",
        FtsOperator::FtsPhrase => "phraseto_tsquery",
        FtsOperator::FtsWebsearch => "websearch_to_tsquery",
    };
    // The fts_predicate on the dialect expects the column to already be in the builder.
    // Here we're appending the RHS of a filter (column @@ tsquery), so we directly
    // emit the @@ operator and the tsquery function.
    b.push(" @@ ");
    b.push(operator);
    b.push("(");
    if let Some(lang) = lang {
        b.push_literal(lang);
        b.push(", ");
    }
    b.push_param(SqlParam::Text(val.to_string()));
    b.push(")");
    let _ = dialect; // dialect available for future backends that use different FTS syntax
}

// ==========================================================================
// Filter formatting
// ==========================================================================

/// Append a filter predicate (one WHERE condition).
///
/// # Behaviour
///
/// - Negated filters are prefixed with `NOT `
/// - `IN` uses `= ANY($N)` to keep the prepared statement cacheable
/// - `IS` values (`NULL`, `TRUE`, `FALSE`, `UNKNOWN`) are not parameterised
/// - `NoOp` expressions (RPC GET params) emit `= $N`
///
/// # SQL Example
/// ```sql
/// -- field=age, op=gte, value=18
/// "age" >= $1
/// -- negated, field=status, op=eq, value=active
/// NOT "status" = $1
/// ```
pub fn fmt_filter(b: &mut SqlBuilder, qi: &QualifiedIdentifier, filter: &CoercibleFilter, dialect: &dyn SqlDialect) {
    match filter {
        CoercibleFilter::Filter { field, op_expr } => {
            fmt_op_expr(b, qi, field, op_expr, dialect);
        }
        CoercibleFilter::NullEmbed(negated, embed_name) => {
            // Null embed check: the embedded relation subquery IS (NOT) NULL
            if *negated {
                b.push_ident(embed_name);
                b.push(" IS NOT NULL");
            } else {
                b.push_ident(embed_name);
                b.push(" IS NULL");
            }
        }
    }
}

/// Append an operator expression (field reference followed by the operation).
///
/// # Behaviour
///
/// - `Expr` — emits the field, optionally prefixed with `NOT`, then the
///   operation (comparison, FTS, IS, etc.)
/// - `NoOp` — emits `field = $N` (used for RPC GET query-string parameters)
///
/// # SQL Example
/// ```sql
/// -- Expr:  "public"."users"."age" >= $1
/// -- Expr negated: NOT "public"."users"."status"=$1
/// -- NoOp:  "public"."users"."limit" = $1
/// ```
///
/// # JSON Path Handling
///
/// When JSON path operators (`->`, `->>`) are present, we don't cast parameters
/// to match PostgREST behavior. PostgreSQL handles implicit type coercion:
/// - `->>` returns TEXT, PostgreSQL will coerce TEXT to the comparison type
/// - `->` returns JSONB, PostgreSQL will coerce JSONB appropriately
fn fmt_op_expr(
    b: &mut SqlBuilder,
    qi: &QualifiedIdentifier,
    field: &CoercibleField,
    op_expr: &OpExpr,
    dialect: &dyn SqlDialect,
) {
    // When JSON path operators are present, don't cast parameters for simple operations.
    // This matches PostgREST behavior: rely on PostgreSQL's implicit type coercion.
    // The JSON path operators change the result type (->> returns TEXT, -> returns JSONB),
    // so casting to the original column type would be incorrect.
    // Store whether JSON paths are present for special handling in IN operations.
    let has_json_path = !field.json_path.is_empty();
    let col_type = if has_json_path {
        None // Don't cast when JSON paths are used (for simple operations)
    } else {
        field.base_type.as_deref() // Normal case: use base type
    };

    match op_expr {
        OpExpr::Expr { negated, operation } => {
            if *negated {
                b.push("NOT ");
            }
            fmt_field(b, qi, field);
            fmt_operation(b, operation, col_type, has_json_path, dialect);
        }
        OpExpr::NoOp(val) => {
            // RPC GET parameter — treat as equality
            fmt_field(b, qi, field);
            b.push(" = ");
            b.push_param(SqlParam::Text(val.to_string()));
            push_type_cast(b, dialect, col_type);
        }
    }
}

/// Append a type cast suffix via the dialect if a column type is known.
fn push_type_cast(b: &mut SqlBuilder, dialect: &dyn SqlDialect, col_type: Option<&str>) {
    if let Some(ty) = col_type {
        dialect.push_type_cast_suffix(b, ty);
    }
}

/// Append the operation part of a filter (operator + value).
///
/// `col_type` is the column's PostgreSQL type (e.g. `"integer"`, `"boolean"`).
/// When present, an explicit `::type` cast is appended to parameter placeholders
/// so PostgreSQL does not reject `text = integer` comparisons.
///
/// `has_json_path` indicates if JSON path operators are present. When true and
/// `col_type` is None, special handling may be needed (e.g., casting IN arrays to text[]).
///
/// # Behaviour
///
/// | Variant            | SQL output                              |
/// |--------------------|-----------------------------------------|
/// | `Simple(op, val)`  | ` <op> $N::type`                        |
/// | `Quant(op, q, v)`  | `<op>ANY($N::type[])` or `<op>$N::type` |
/// | `In(vals)`         | ` = ANY($N::type[])` (array formatted)  |
/// | `Is(is_val)`       | ` IS NULL` / ` IS NOT NULL` / etc.      |
/// | `IsDistinctFrom`   | ` IS DISTINCT FROM $N::type`            |
/// | `Fts(op, lang, v)` | ` @@ to_tsquery('lang', $N)`            |
fn fmt_operation(b: &mut SqlBuilder, op: &Operation, col_type: Option<&str>, has_json_path: bool, dialect: &dyn SqlDialect) {
    match op {
        Operation::Simple(sop, val) => {
            b.push(" ");
            b.push(simple_operator(*sop));
            b.push(" ");
            b.push_param(SqlParam::Text(val.to_string()));
            push_type_cast(b, dialect, col_type);
        }
        Operation::Quant(qop, quantifier, val) => {
            // Convert * wildcards to % for LIKE/ILIKE operators
            let effective_val = if matches!(qop, QuantOperator::Like | QuantOperator::ILike) {
                val.replace('*', "%")
            } else {
                val.to_string()
            };
            b.push(quant_operator(*qop));
            if let Some(q) = quantifier {
                let q_str = match q {
                    crate::api_request::types::OpQuantifier::Any => "ANY",
                    crate::api_request::types::OpQuantifier::All => "ALL",
                };
                b.push(q_str);
                b.push("(");
                b.push_param(SqlParam::Text(effective_val));
                // Array cast
                if let Some(ty) = col_type {
                    dialect.push_array_type_cast_suffix(b, ty);
                }
                b.push(")");
            } else {
                b.push_param(SqlParam::Text(effective_val));
                push_type_cast(b, dialect, col_type);
            }
        }
        Operation::In(vals) => {
            // Use = ANY('{...}') for prepared statement caching
            b.push(" = ANY(");
            let arr = format!(
                "{{{}}}",
                vals.iter()
                    .map(|v| v.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            b.push_param(SqlParam::Text(arr));
            // Array cast
            if let Some(ty) = col_type {
                dialect.push_array_type_cast_suffix(b, ty);
            } else if has_json_path {
                dialect.push_array_type_cast_suffix(b, "text");
            }
            b.push(")");
        }
        Operation::Is(is_val) => {
            b.push(" IS ");
            match is_val {
                IsValue::Null => b.push("NULL"),
                IsValue::NotNull => {
                    b.push("NOT NULL");
                }
                IsValue::True => b.push("TRUE"),
                IsValue::False => b.push("FALSE"),
                IsValue::Unknown => b.push("UNKNOWN"),
            }
        }
        Operation::IsDistinctFrom(val) => {
            b.push(" IS DISTINCT FROM ");
            b.push_param(SqlParam::Text(val.to_string()));
            push_type_cast(b, dialect, col_type);
        }
        Operation::Fts(fts_op, lang, val) => {
            fts_operator(b, dialect, *fts_op, lang.as_deref(), val);
        }
    }
}

// ==========================================================================
// Logic tree formatting
// ==========================================================================

/// Append a logic tree (recursive AND/OR) as a parenthesised predicate.
///
/// # Behaviour
///
/// - Negated expressions are prefixed with `NOT`
/// - Children are combined with `AND` or `OR`
/// - Leaf nodes (`Stmnt`) delegate to `fmt_filter`
///
/// # SQL Example
/// ```sql
/// ("a" = $1 AND "b" > $2)
/// NOT ("status" = $1 OR "status" = $2)
/// ```
pub fn fmt_logic_tree(b: &mut SqlBuilder, qi: &QualifiedIdentifier, tree: &CoercibleLogicTree, dialect: &dyn SqlDialect) {
    match tree {
        CoercibleLogicTree::Expr(negated, op, children) => {
            if *negated {
                b.push("NOT ");
            }
            b.push("(");
            let sep = match op {
                LogicOperator::And => " AND ",
                LogicOperator::Or => " OR ",
            };
            for (i, child) in children.iter().enumerate() {
                if i > 0 {
                    b.push(sep);
                }
                fmt_logic_tree(b, qi, child, dialect);
            }
            b.push(")");
        }
        CoercibleLogicTree::Stmnt(filter) => {
            fmt_filter(b, qi, filter, dialect);
        }
    }
}

// ==========================================================================
// ORDER BY
// ==========================================================================

/// Append a single ORDER BY term.
///
/// # SQL Example
/// ```sql
/// "name" ASC NULLS LAST
/// "created_at" DESC NULLS FIRST
/// ```
pub fn fmt_order_term(b: &mut SqlBuilder, qi: &QualifiedIdentifier, term: &CoercibleOrderTerm) {
    match term {
        CoercibleOrderTerm::Term {
            field,
            direction,
            nulls,
        } => {
            fmt_field(b, qi, field);
            if let Some(dir) = direction {
                match dir {
                    OrderDirection::Asc => b.push(" ASC"),
                    OrderDirection::Desc => b.push(" DESC"),
                }
            }
            if let Some(nulls) = nulls {
                match nulls {
                    OrderNulls::First => b.push(" NULLS FIRST"),
                    OrderNulls::Last => b.push(" NULLS LAST"),
                }
            }
        }
        CoercibleOrderTerm::RelationTerm {
            relation,
            rel_term,
            direction,
            nulls,
        } => {
            // Order by a field in an embedded relation
            b.push_ident(relation);
            b.push(".");
            b.push_ident(&rel_term.name);
            if let Some(dir) = direction {
                match dir {
                    OrderDirection::Asc => b.push(" ASC"),
                    OrderDirection::Desc => b.push(" DESC"),
                }
            }
            if let Some(nulls) = nulls {
                match nulls {
                    OrderNulls::First => b.push(" NULLS FIRST"),
                    OrderNulls::Last => b.push(" NULLS LAST"),
                }
            }
        }
    }
}

/// Append a full ORDER BY clause.
///
/// Does nothing if `terms` is empty.
///
/// # SQL Example
/// ```sql
/// ORDER BY "name" ASC, "id" DESC NULLS LAST
/// ```
pub fn order_clause(b: &mut SqlBuilder, qi: &QualifiedIdentifier, terms: &[CoercibleOrderTerm]) {
    if terms.is_empty() {
        return;
    }
    b.push(" ORDER BY ");
    b.push_separated(", ", terms, |b, t| fmt_order_term(b, qi, t));
}

// ==========================================================================
// LIMIT / OFFSET
// ==========================================================================

/// Append `LIMIT n OFFSET m` clause based on range values.
///
/// # Behaviour
///
/// - `limit_to` is an inclusive upper bound index, so LIMIT = `limit_to - offset + 1`
/// - If `limit_to` is `None`, no LIMIT is emitted
/// - If `offset` is 0, no OFFSET is emitted
///
/// # SQL Example
/// ```sql
/// LIMIT 10 OFFSET 20
/// LIMIT 25
/// ```
pub fn limit_offset(b: &mut SqlBuilder, offset: i64, limit_to: Option<i64>) {
    if let Some(lim) = limit_to {
        let limit = lim - offset + 1;
        b.push(" LIMIT ");
        b.push(&limit.to_string());
    }
    if offset > 0 {
        b.push(" OFFSET ");
        b.push(&offset.to_string());
    }
}

// ==========================================================================
// JOIN condition
// ==========================================================================

/// Append a join condition: `"parent"."col" = "child"."col"`.
///
/// # SQL Example
/// ```sql
/// "users"."id" = "posts"."user_id"
/// ```
pub fn fmt_join_condition(b: &mut SqlBuilder, jc: &JoinCondition) {
    fmt_column(b, &jc.parent.0, &jc.parent.1);
    b.push(" = ");
    fmt_column(b, &jc.child.0, &jc.child.1);
}

// ==========================================================================
// WHERE clause
// ==========================================================================

/// Append a WHERE clause from a list of logic trees.
///
/// Each tree becomes one AND-ed condition. Does nothing if `trees` is empty.
///
/// # SQL Example
/// ```sql
/// WHERE "id" = $1 AND "status" = $2
/// ```
pub fn where_clause(b: &mut SqlBuilder, qi: &QualifiedIdentifier, trees: &[CoercibleLogicTree], dialect: &dyn SqlDialect) {
    if trees.is_empty() {
        return;
    }
    b.push(" WHERE ");
    for (i, tree) in trees.iter().enumerate() {
        if i > 0 {
            b.push(" AND ");
        }
        fmt_logic_tree(b, qi, tree, dialect);
    }
}

// ==========================================================================
// RETURNING clause
// ==========================================================================

/// Append a RETURNING clause from select fields.
///
/// Does nothing if `fields` is empty.
///
/// # SQL Example
/// ```sql
/// RETURNING "id", "name", "email"
/// ```
pub fn returning_clause(
    b: &mut SqlBuilder,
    qi: &QualifiedIdentifier,
    fields: &[CoercibleSelectField],
    dialect: &dyn SqlDialect,
) {
    if fields.is_empty() {
        return;
    }
    b.push(" RETURNING ");
    if dialect.supports_dml_cte() {
        // PostgreSQL: use table-qualified column names (works in CTE context)
        b.push_separated(", ", fields, |b, f| {
            fmt_select_item(b, qi, f, dialect);
        });
    } else {
        // SQLite etc: use unqualified column names in RETURNING
        b.push_separated(", ", fields, |b, f| {
            fmt_returning_item_unqualified(b, f);
        });
    }
}

/// Format a RETURNING item with unqualified column names.
fn fmt_returning_item_unqualified(b: &mut SqlBuilder, sel: &CoercibleSelectField) {
    b.push_ident(&sel.field.name);
    let alias = sel.alias.as_ref().unwrap_or(&sel.field.name);
    b.push(" AS ");
    b.push_ident(alias);
}

// ==========================================================================
// FROM json body
// ==========================================================================

/// Append a JSON body unpacking expression for mutations.
///
/// Generates `json_to_recordset($N) AS _("col1" type, "col2" type, ...)`.
///
/// # Behaviour
///
/// - Uses `json_to_recordset` for arrays and `json_to_record` for objects
///   (caller decides by payload shape; we default to `json_to_recordset`)
///
/// # SQL Example
/// ```sql
/// json_to_recordset($1) AS _("id" integer, "name" text)
/// ```
pub fn from_json_body(b: &mut SqlBuilder, columns: &[CoercibleField], json_body: &[u8], dialect: &dyn SqlDialect) {
    dialect.from_json_body(b, columns, json_body);
}

// ==========================================================================
// COUNT CTE
// ==========================================================================

/// Append a count query snippet.
///
/// # SQL Example
/// ```sql
/// SELECT COUNT(*) AS "pgrst_filtered_count" FROM (source_query) AS _pgrst_count_t
/// ```
pub fn count_f(b: &mut SqlBuilder, dialect: &dyn SqlDialect) {
    dialect.count_star(b);
}

// ==========================================================================
// GROUP BY
// ==========================================================================

/// Append a GROUP BY clause from a list of non-aggregate select fields.
///
/// # Behaviour
///
/// Only emits GROUP BY when at least one select field uses an aggregate function.
/// Groups by all non-aggregate fields.
///
/// # SQL Example
/// ```sql
/// GROUP BY "name", "status"
/// ```
pub fn group_clause(b: &mut SqlBuilder, qi: &QualifiedIdentifier, select: &[CoercibleSelectField]) {
    let has_agg = select.iter().any(|s| s.agg_function.is_some());
    if !has_agg {
        return;
    }

    let non_agg: Vec<_> = select.iter().filter(|s| s.agg_function.is_none()).collect();
    if non_agg.is_empty() {
        return;
    }

    b.push(" GROUP BY ");
    b.push_separated(", ", &non_agg, |b, s| {
        fmt_field(b, qi, &s.field);
    });
}

// ==========================================================================
// Handler aggregation
// ==========================================================================

/// Append the response body aggregation expression.
///
/// # Behaviour
///
/// By default, wraps results in `coalesce(json_agg(_pgrest_t), '[]')::text`
/// for JSON output.
///
/// # SQL Example
/// ```sql
/// coalesce(json_agg(_pgrest_t), '[]')::text
/// ```
/// Append a handler aggregation expression based on the media type.
///
/// Different media types use different aggregation strategies:
/// - JSON: `coalesce(json_agg(_pgrest_t), '[]')::text`
/// - CSV: Custom CSV formatting with headers
/// - Binary: Raw output (no aggregation)
///
/// # Arguments
/// * `b` - The SQL builder to append to
/// * `handler` - The media handler determining output format
/// * `is_scalar` - Whether the result is a scalar value
pub fn handler_agg_with_media(
    b: &mut SqlBuilder,
    handler: &crate::schema_cache::media_handler::MediaHandler,
    _is_scalar: bool,
    dialect: &dyn SqlDialect,
) {
    handler_agg_with_media_cols(b, handler, _is_scalar, dialect, &[])
}

/// Append handler aggregation with explicit column names.
///
/// Column names are needed for backends like SQLite that cannot aggregate
/// a whole row alias.
pub fn handler_agg_with_media_cols(
    b: &mut SqlBuilder,
    handler: &crate::schema_cache::media_handler::MediaHandler,
    _is_scalar: bool,
    dialect: &dyn SqlDialect,
    columns: &[&str],
) {
    use crate::schema_cache::media_handler::MediaHandler;

    match handler {
        MediaHandler::BuiltinOvAggJson
        | MediaHandler::BuiltinAggSingleJson(_)
        | MediaHandler::BuiltinAggArrayJsonStrip => {
            // JSON aggregation (default)
            dialect.json_agg_with_columns(b, "_pgrest_t", columns);
        }
        MediaHandler::BuiltinOvAggCsv => {
            // CSV formatting with headers — PG-specific string_agg / json_each_text
            // TODO: delegate to dialect.csv_agg() when adding non-PG backends
            b.push("(SELECT coalesce(");
            b.push("(SELECT ");
            b.push("string_agg(key, ',') FROM json_object_keys(row_to_json(");
            b.push_ident("_pgrest_t");
            b.push(")) || E'\\n' || ");
            b.push("string_agg(");
            b.push("(SELECT string_agg(");
            b.push("CASE WHEN value::text LIKE '%\"%' OR value::text LIKE '%,%' OR value::text LIKE '%\\n%' ");
            b.push("THEN '\"' || replace(value::text, '\"', '\"\"') || '\"' ");
            b.push("ELSE value::text END, ',')");
            b.push(" FROM json_each_text(row_to_json(");
            b.push_ident("_pgrest_t");
            b.push("))), E'\\n')");
            b.push(" FROM ");
            b.push_ident("_pgrest_t");
            b.push("), ''))");
        }
        MediaHandler::NoAgg => {
            // No aggregation - first column of first row as text
            b.push("(SELECT (row_to_json(");
            b.push_ident("_pgrest_t");
            b.push(")->>0)::text FROM ");
            b.push_ident("_pgrest_t");
            b.push(" LIMIT 1)");
        }
        MediaHandler::CustomFunc(func_qi, _) => {
            // Custom function - call it with the aggregated JSON
            b.push_qi(func_qi);
            b.push("(");
            dialect.json_agg(b, "_pgrest_t");
            b.push(")");
        }
        MediaHandler::BuiltinOvAggGeoJson => {
            dialect.json_agg(b, "_pgrest_t");
        }
    }
}

/// Append a handler aggregation expression (legacy version, uses default JSON).
///
/// # Deprecated
/// Use `handler_agg_with_media` instead to support multiple output formats.
pub fn handler_agg(b: &mut SqlBuilder, _is_scalar: bool, dialect: &dyn SqlDialect) {
    dialect.json_agg(b, "_pgrest_t");
}

/// Append handler aggregation with explicit columns (for non-PG backends).
pub fn handler_agg_cols(b: &mut SqlBuilder, _is_scalar: bool, dialect: &dyn SqlDialect, columns: &[&str]) {
    dialect.json_agg_with_columns(b, "_pgrest_t", columns);
}

/// Append a single-object handler aggregation (for to-one relations).
///
/// # SQL Example (PostgreSQL)
/// ```sql
/// row_to_json(_pgrest_t)::text
/// ```
pub fn handler_agg_single(b: &mut SqlBuilder, dialect: &dyn SqlDialect) {
    dialect.row_to_json(b, "_pgrest_t");
}

// ==========================================================================
// Location header (POST)
// ==========================================================================

/// Append the location header expression for POST responses.
///
/// Generates an expression that concatenates PK values for the Location header.
///
/// # SQL Example
/// ```sql
/// '/' || "id"::text
/// '/' || "id"::text || ',' || "name"::text
/// ```
pub fn location_f(b: &mut SqlBuilder, pk_cols: &[compact_str::CompactString]) {
    if pk_cols.is_empty() {
        b.push("''");
        return;
    }
    for (i, col) in pk_cols.iter().enumerate() {
        if i > 0 {
            b.push(" || ',' || ");
        }
        b.push("'/' || ");
        b.push_ident(col);
        b.push("::text");
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_request::types::*;
    use crate::test_helpers::TestPgDialect;
    use crate::plan::types::*;
    use smallvec::SmallVec;

    fn test_qi() -> QualifiedIdentifier {
        QualifiedIdentifier::new("public", "users")
    }

    fn dialect() -> &'static dyn SqlDialect {
        &TestPgDialect
    }

    fn field(name: &str) -> CoercibleField {
        CoercibleField::unknown(name.into(), SmallVec::new())
    }

    fn typed_field(name: &str, base_type: &str) -> CoercibleField {
        CoercibleField::from_column(name.into(), SmallVec::new(), base_type.into())
    }

    fn select_field(name: &str) -> CoercibleSelectField {
        CoercibleSelectField {
            field: field(name),
            agg_function: None,
            agg_cast: None,
            cast: None,
            alias: None,
        }
    }

    // ------------------------------------------------------------------
    // Identifier / field tests
    // ------------------------------------------------------------------

    #[test]
    fn test_fmt_column_regular() {
        let mut b = SqlBuilder::new();
        fmt_column(&mut b, &test_qi(), "name");
        assert_eq!(b.sql(), "\"public\".\"users\".\"name\"");
    }

    #[test]
    fn test_fmt_column_star() {
        let mut b = SqlBuilder::new();
        fmt_column(&mut b, &test_qi(), "*");
        assert_eq!(b.sql(), "\"public\".\"users\".*");
    }

    #[test]
    fn test_fmt_field_simple() {
        let mut b = SqlBuilder::new();
        let f = field("email");
        fmt_field(&mut b, &test_qi(), &f);
        assert_eq!(b.sql(), "\"public\".\"users\".\"email\"");
    }

    #[test]
    fn test_fmt_field_full_row() {
        let mut b = SqlBuilder::new();
        let f = CoercibleField::full_row();
        fmt_field(&mut b, &test_qi(), &f);
        assert_eq!(b.sql(), "\"public\".\"users\".*");
    }

    #[test]
    fn test_fmt_field_with_json_path() {
        let mut b = SqlBuilder::new();
        let mut f = field("data");
        f.json_path = SmallVec::from_vec(vec![
            JsonOperation::Arrow(JsonOperand::Key("address".into())),
            JsonOperation::Arrow2(JsonOperand::Key("city".into())),
        ]);
        fmt_field(&mut b, &test_qi(), &f);
        assert_eq!(b.sql(), "\"public\".\"users\".\"data\"->'address'->>'city'");
    }

    #[test]
    fn test_fmt_select_item_simple() {
        let mut b = SqlBuilder::new();
        let sel = select_field("name");
        fmt_select_item(&mut b, &test_qi(), &sel, dialect());
        assert_eq!(b.sql(), "\"public\".\"users\".\"name\" AS \"name\"");
    }

    #[test]
    fn test_fmt_select_item_with_alias() {
        let mut b = SqlBuilder::new();
        let sel = CoercibleSelectField {
            field: field("name"),
            agg_function: None,
            agg_cast: None,
            cast: Some("text".into()),
            alias: Some("user_name".into()),
        };
        fmt_select_item(&mut b, &test_qi(), &sel, dialect());
        assert_eq!(
            b.sql(),
            "\"public\".\"users\".\"name\"::text AS \"user_name\""
        );
    }

    #[test]
    fn test_fmt_select_item_with_aggregate() {
        let mut b = SqlBuilder::new();
        let sel = CoercibleSelectField {
            field: field("id"),
            agg_function: Some(AggregateFunction::Count),
            agg_cast: Some("bigint".into()),
            cast: None,
            alias: Some("total".into()),
        };
        fmt_select_item(&mut b, &test_qi(), &sel, dialect());
        assert_eq!(
            b.sql(),
            "COUNT(\"public\".\"users\".\"id\")::bigint AS \"total\""
        );
    }

    // ------------------------------------------------------------------
    // Operator tests
    // ------------------------------------------------------------------

    #[test]
    fn test_simple_operators() {
        assert_eq!(simple_operator(SimpleOperator::NotEqual), "<>");
        assert_eq!(simple_operator(SimpleOperator::Contains), "@>");
        assert_eq!(simple_operator(SimpleOperator::Contained), "<@");
        assert_eq!(simple_operator(SimpleOperator::Overlap), "&&");
        assert_eq!(simple_operator(SimpleOperator::StrictlyLeft), "<<");
        assert_eq!(simple_operator(SimpleOperator::StrictlyRight), ">>");
        assert_eq!(simple_operator(SimpleOperator::NotExtendsRight), "&<");
        assert_eq!(simple_operator(SimpleOperator::NotExtendsLeft), "&>");
        assert_eq!(simple_operator(SimpleOperator::Adjacent), "-|-");
    }

    #[test]
    fn test_quant_operators() {
        assert_eq!(quant_operator(QuantOperator::Equal), "=");
        assert_eq!(quant_operator(QuantOperator::GreaterThan), ">");
        assert_eq!(quant_operator(QuantOperator::LessThan), "<");
        assert_eq!(quant_operator(QuantOperator::Like), " LIKE ");
        assert_eq!(quant_operator(QuantOperator::ILike), " ILIKE ");
        assert_eq!(quant_operator(QuantOperator::Match), "~");
        assert_eq!(quant_operator(QuantOperator::IMatch), "~*");
    }

    // ------------------------------------------------------------------
    // Filter tests
    // ------------------------------------------------------------------

    #[test]
    fn test_fmt_filter_eq() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::Filter {
            field: field("id"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "5".into()),
            },
        };
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(b.sql(), "\"public\".\"users\".\"id\"=$1");
    }

    #[test]
    fn test_fmt_filter_negated() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::Filter {
            field: field("status"),
            op_expr: OpExpr::Expr {
                negated: true,
                operation: Operation::Quant(QuantOperator::Equal, None, "active".into()),
            },
        };
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(b.sql(), "NOT \"public\".\"users\".\"status\"=$1");
    }

    #[test]
    fn test_fmt_filter_is_null() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::Filter {
            field: field("deleted_at"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Is(IsValue::Null),
            },
        };
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(b.sql(), "\"public\".\"users\".\"deleted_at\" IS NULL");
    }

    #[test]
    fn test_fmt_filter_in() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::Filter {
            field: field("status"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::In(vec!["active".into(), "pending".into()]),
            },
        };
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(b.sql(), "\"public\".\"users\".\"status\" = ANY($1)");
    }

    #[test]
    fn test_fmt_filter_simple_op() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::Filter {
            field: field("tags"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Simple(SimpleOperator::Contains, "{a,b}".into()),
            },
        };
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(b.sql(), "\"public\".\"users\".\"tags\" @> $1");
    }

    #[test]
    fn test_fmt_filter_fts() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::Filter {
            field: field("body"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Fts(
                    FtsOperator::Fts,
                    Some("english".into()),
                    "search".into(),
                ),
            },
        };
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(
            b.sql(),
            "\"public\".\"users\".\"body\" @@ to_tsquery('english', $1)"
        );
    }

    #[test]
    fn test_fmt_filter_null_embed() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::NullEmbed(false, "posts".into());
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(b.sql(), "\"posts\" IS NULL");
    }

    #[test]
    fn test_fmt_filter_null_embed_negated() {
        let mut b = SqlBuilder::new();
        let filter = CoercibleFilter::NullEmbed(true, "posts".into());
        fmt_filter(&mut b, &test_qi(), &filter, dialect());
        assert_eq!(b.sql(), "\"posts\" IS NOT NULL");
    }

    // ------------------------------------------------------------------
    // Logic tree tests
    // ------------------------------------------------------------------

    #[test]
    fn test_fmt_logic_tree_single() {
        let mut b = SqlBuilder::new();
        let tree = CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
            field: field("id"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
            },
        });
        fmt_logic_tree(&mut b, &test_qi(), &tree, dialect());
        assert_eq!(b.sql(), "\"public\".\"users\".\"id\"=$1");
    }

    #[test]
    fn test_fmt_logic_tree_and() {
        let mut b = SqlBuilder::new();
        let tree = CoercibleLogicTree::Expr(
            false,
            LogicOperator::And,
            vec![
                CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
                    field: field("a"),
                    op_expr: OpExpr::Expr {
                        negated: false,
                        operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
                    },
                }),
                CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
                    field: field("b"),
                    op_expr: OpExpr::Expr {
                        negated: false,
                        operation: Operation::Quant(QuantOperator::GreaterThan, None, "5".into()),
                    },
                }),
            ],
        );
        fmt_logic_tree(&mut b, &test_qi(), &tree, dialect());
        assert_eq!(
            b.sql(),
            "(\"public\".\"users\".\"a\"=$1 AND \"public\".\"users\".\"b\">$2)"
        );
    }

    #[test]
    fn test_fmt_logic_tree_negated_or() {
        let mut b = SqlBuilder::new();
        let tree = CoercibleLogicTree::Expr(
            true,
            LogicOperator::Or,
            vec![CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
                field: field("x"),
                op_expr: OpExpr::Expr {
                    negated: false,
                    operation: Operation::Quant(QuantOperator::Equal, None, "a".into()),
                },
            })],
        );
        fmt_logic_tree(&mut b, &test_qi(), &tree, dialect());
        assert_eq!(b.sql(), "NOT (\"public\".\"users\".\"x\"=$1)");
    }

    // ------------------------------------------------------------------
    // ORDER BY tests
    // ------------------------------------------------------------------

    #[test]
    fn test_fmt_order_term_asc() {
        let mut b = SqlBuilder::new();
        let term = CoercibleOrderTerm::Term {
            field: field("name"),
            direction: Some(OrderDirection::Asc),
            nulls: Some(OrderNulls::Last),
        };
        fmt_order_term(&mut b, &test_qi(), &term);
        assert_eq!(b.sql(), "\"public\".\"users\".\"name\" ASC NULLS LAST");
    }

    #[test]
    fn test_order_clause_empty() {
        let mut b = SqlBuilder::new();
        order_clause(&mut b, &test_qi(), &[]);
        assert_eq!(b.sql(), "");
    }

    #[test]
    fn test_order_clause_multiple() {
        let mut b = SqlBuilder::new();
        let terms = vec![
            CoercibleOrderTerm::Term {
                field: field("name"),
                direction: Some(OrderDirection::Asc),
                nulls: None,
            },
            CoercibleOrderTerm::Term {
                field: field("id"),
                direction: Some(OrderDirection::Desc),
                nulls: None,
            },
        ];
        order_clause(&mut b, &test_qi(), &terms);
        assert_eq!(
            b.sql(),
            " ORDER BY \"public\".\"users\".\"name\" ASC, \"public\".\"users\".\"id\" DESC"
        );
    }

    // ------------------------------------------------------------------
    // LIMIT / OFFSET tests
    // ------------------------------------------------------------------

    #[test]
    fn test_limit_offset_both() {
        let mut b = SqlBuilder::new();
        limit_offset(&mut b, 20, Some(29));
        assert_eq!(b.sql(), " LIMIT 10 OFFSET 20");
    }

    #[test]
    fn test_limit_offset_only_limit() {
        let mut b = SqlBuilder::new();
        limit_offset(&mut b, 0, Some(24));
        assert_eq!(b.sql(), " LIMIT 25");
    }

    #[test]
    fn test_limit_offset_none() {
        let mut b = SqlBuilder::new();
        limit_offset(&mut b, 0, None);
        assert_eq!(b.sql(), "");
    }

    #[test]
    fn test_limit_offset_only_offset() {
        let mut b = SqlBuilder::new();
        limit_offset(&mut b, 10, None);
        assert_eq!(b.sql(), " OFFSET 10");
    }

    // ------------------------------------------------------------------
    // JOIN condition tests
    // ------------------------------------------------------------------

    #[test]
    fn test_fmt_join_condition() {
        let mut b = SqlBuilder::new();
        let jc = JoinCondition {
            parent: (QualifiedIdentifier::new("public", "users"), "id".into()),
            child: (
                QualifiedIdentifier::new("public", "posts"),
                "user_id".into(),
            ),
        };
        fmt_join_condition(&mut b, &jc);
        assert_eq!(
            b.sql(),
            "\"public\".\"users\".\"id\" = \"public\".\"posts\".\"user_id\""
        );
    }

    // ------------------------------------------------------------------
    // WHERE clause tests
    // ------------------------------------------------------------------

    #[test]
    fn test_where_clause_empty() {
        let mut b = SqlBuilder::new();
        where_clause(&mut b, &test_qi(), &[], dialect());
        assert_eq!(b.sql(), "");
    }

    #[test]
    fn test_where_clause_single() {
        let mut b = SqlBuilder::new();
        let tree = CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
            field: field("id"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
            },
        });
        where_clause(&mut b, &test_qi(), &[tree], dialect());
        assert_eq!(b.sql(), " WHERE \"public\".\"users\".\"id\"=$1");
    }

    // ------------------------------------------------------------------
    // RETURNING clause tests
    // ------------------------------------------------------------------

    #[test]
    fn test_returning_clause_empty() {
        let mut b = SqlBuilder::new();
        returning_clause(&mut b, &test_qi(), &[], dialect());
        assert_eq!(b.sql(), "");
    }

    #[test]
    fn test_returning_clause_fields() {
        let mut b = SqlBuilder::new();
        let fields = vec![select_field("id"), select_field("name")];
        returning_clause(&mut b, &test_qi(), &fields, dialect());
        assert!(b.sql().starts_with(" RETURNING "));
        assert!(b.sql().contains("\"id\""));
        assert!(b.sql().contains("\"name\""));
    }

    // ------------------------------------------------------------------
    // FROM json body tests
    // ------------------------------------------------------------------

    #[test]
    fn test_from_json_body() {
        let mut b = SqlBuilder::new();
        let cols = vec![typed_field("id", "integer"), typed_field("name", "text")];
        let json = b"[{\"id\":1,\"name\":\"test\"}]";
        from_json_body(&mut b, &cols, json, dialect());
        assert!(b.sql().starts_with("json_to_recordset($1::json) AS _("));
        assert!(b.sql().contains("\"id\" integer"));
        assert!(b.sql().contains("\"name\" text"));
    }

    // ------------------------------------------------------------------
    // GROUP BY tests
    // ------------------------------------------------------------------

    #[test]
    fn test_group_clause_no_agg() {
        let mut b = SqlBuilder::new();
        let select = vec![select_field("name"), select_field("status")];
        group_clause(&mut b, &test_qi(), &select);
        assert_eq!(b.sql(), "");
    }

    #[test]
    fn test_group_clause_with_agg() {
        let mut b = SqlBuilder::new();
        let select = vec![
            select_field("status"),
            CoercibleSelectField {
                field: field("id"),
                agg_function: Some(AggregateFunction::Count),
                agg_cast: None,
                cast: None,
                alias: Some("total".into()),
            },
        ];
        group_clause(&mut b, &test_qi(), &select);
        assert_eq!(b.sql(), " GROUP BY \"public\".\"users\".\"status\"");
    }

    // ------------------------------------------------------------------
    // Location header tests
    // ------------------------------------------------------------------

    #[test]
    fn test_location_f_single_pk() {
        let mut b = SqlBuilder::new();
        location_f(&mut b, &["id".into()]);
        assert_eq!(b.sql(), "'/' || \"id\"::text");
    }

    #[test]
    fn test_location_f_composite_pk() {
        let mut b = SqlBuilder::new();
        location_f(&mut b, &["id".into(), "name".into()]);
        assert_eq!(
            b.sql(),
            "'/' || \"id\"::text || ',' || '/' || \"name\"::text"
        );
    }

    #[test]
    fn test_location_f_empty() {
        let mut b = SqlBuilder::new();
        location_f(&mut b, &[]);
        assert_eq!(b.sql(), "''");
    }

    // ------------------------------------------------------------------
    // Handler aggregation tests
    // ------------------------------------------------------------------

    #[test]
    fn test_handler_agg() {
        let mut b = SqlBuilder::new();
        handler_agg(&mut b, false, dialect());
        assert_eq!(b.sql(), "coalesce(json_agg(\"_pgrest_t\"), '[]')::text");
    }

    #[test]
    fn test_handler_agg_single() {
        let mut b = SqlBuilder::new();
        handler_agg_single(&mut b, dialect());
        assert_eq!(b.sql(), "row_to_json(\"_pgrest_t\")::text");
    }

    // ------------------------------------------------------------------
    // Computed field tests
    // ------------------------------------------------------------------

    #[test]
    fn test_fmt_computed_field() {
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "users");
        let func_qi = QualifiedIdentifier::new("test_api", "full_name");

        let field = CoercibleField::from_computed_field(
            "full_name".into(),
            Default::default(),
            func_qi,
            "text".into(),
        );

        fmt_computed_field(&mut b, &table_qi, &field);
        assert_eq!(b.sql(), "\"test_api\".\"full_name\"(\"users\")");
    }

    #[test]
    fn test_fmt_field_computed() {
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "users");
        let func_qi = QualifiedIdentifier::new("test_api", "full_name");

        let field = CoercibleField::from_computed_field(
            "full_name".into(),
            Default::default(),
            func_qi,
            "text".into(),
        );

        fmt_field(&mut b, &table_qi, &field);
        assert_eq!(b.sql(), "\"test_api\".\"full_name\"(\"users\")");
    }

    #[test]
    fn test_fmt_field_computed_vs_column() {
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;

        let mut b1 = SqlBuilder::new();
        let mut b2 = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "users");

        // Regular column
        let col_field =
            CoercibleField::from_column("name".into(), Default::default(), "text".into());
        fmt_field(&mut b1, &table_qi, &col_field);
        assert_eq!(b1.sql(), "\"test_api\".\"users\".\"name\"");

        // Computed field
        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed_field = CoercibleField::from_computed_field(
            "full_name".into(),
            Default::default(),
            func_qi,
            "text".into(),
        );
        fmt_field(&mut b2, &table_qi, &computed_field);
        assert_eq!(b2.sql(), "\"test_api\".\"full_name\"(\"users\")");
    }

    #[test]
    fn test_fmt_computed_field_different_schemas() {
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("public", "users");
        let func_qi = QualifiedIdentifier::new("extensions", "full_name");

        let field = CoercibleField::from_computed_field(
            "full_name".into(),
            Default::default(),
            func_qi,
            "text".into(),
        );

        fmt_computed_field(&mut b, &table_qi, &field);
        assert_eq!(b.sql(), "\"extensions\".\"full_name\"(\"users\")");
    }

    #[test]
    fn test_fmt_computed_field_with_cast() {
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "users");
        let func_qi = QualifiedIdentifier::new("test_api", "full_name");

        let mut field = CoercibleField::from_computed_field(
            "full_name".into(),
            Default::default(),
            func_qi,
            "text".into(),
        );
        field.base_type = Some("varchar".into());

        fmt_computed_field(&mut b, &table_qi, &field);
        // Cast should be applied in fmt_field, not fmt_computed_field
        // But test that computed field formatting works with different return types
        assert_eq!(b.sql(), "\"test_api\".\"full_name\"(\"users\")");
    }

    #[test]
    fn test_fmt_field_computed_with_json_path() {
        use crate::api_request::types::{JsonOperand, JsonOperation, JsonPath};
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;
        use smallvec::SmallVec;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "users");
        let func_qi = QualifiedIdentifier::new("test_api", "full_name");

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow2(JsonOperand::Key("metadata".into())));
        json_path.push(JsonOperation::Arrow2(JsonOperand::Key("display".into())));

        let field = CoercibleField::from_computed_field(
            "full_name".into(),
            json_path,
            func_qi,
            "text".into(),
        );

        fmt_field(&mut b, &table_qi, &field);
        // JSON paths are NOT applied to computed fields (they're function calls)
        // The function call should be present, but JSON path operators should not
        assert_eq!(b.sql(), "\"test_api\".\"full_name\"(\"users\")");
    }

    #[test]
    fn test_fmt_field_composite_with_json_path() {
        use crate::api_request::types::{JsonOperand, JsonOperation, JsonPath};
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;
        use smallvec::SmallVec;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "countries");

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow2(JsonOperand::Key("lat".into())));

        let mut field = CoercibleField::from_column(
            "location".into(),
            json_path,
            "test_api.coordinates".into(),
        );
        field.to_json = true; // Composite type needs wrapper

        fmt_field(&mut b, &table_qi, &field);
        assert_eq!(
            b.sql(),
            "to_jsonb(\"test_api\".\"countries\".\"location\")->>'lat'"
        );
    }

    #[test]
    fn test_fmt_field_array_with_json_path() {
        use crate::api_request::types::{JsonOperand, JsonOperation, JsonPath};
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;
        use smallvec::SmallVec;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "countries");

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow(JsonOperand::Idx("0".into())));

        let mut field = CoercibleField::from_column("languages".into(), json_path, "text[]".into());
        field.to_json = true; // Array type needs wrapper

        fmt_field(&mut b, &table_qi, &field);
        assert_eq!(
            b.sql(),
            "to_jsonb(\"test_api\".\"countries\".\"languages\")->0"
        );
    }

    #[test]
    fn test_fmt_field_json_no_wrapper() {
        use crate::api_request::types::{JsonOperand, JsonOperation, JsonPath};
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;
        use smallvec::SmallVec;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "posts");

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow2(JsonOperand::Key("title".into())));

        let mut field = CoercibleField::from_column("metadata".into(), json_path, "jsonb".into());
        field.to_json = false; // JSON/JSONB don't need wrapper

        fmt_field(&mut b, &table_qi, &field);
        assert_eq!(b.sql(), "\"test_api\".\"posts\".\"metadata\"->>'title'");
    }

    #[test]
    fn test_fmt_computed_field_multiple() {
        use crate::plan::types::CoercibleField;
        use crate::types::QualifiedIdentifier;

        let mut b = SqlBuilder::new();
        let table_qi = QualifiedIdentifier::new("test_api", "users");

        // First computed field
        let func_qi1 = QualifiedIdentifier::new("test_api", "full_name");
        let field1 = CoercibleField::from_computed_field(
            "full_name".into(),
            Default::default(),
            func_qi1,
            "text".into(),
        );
        fmt_computed_field(&mut b, &table_qi, &field1);
        b.push(", ");

        // Second computed field
        let func_qi2 = QualifiedIdentifier::new("test_api", "initials");
        let field2 = CoercibleField::from_computed_field(
            "initials".into(),
            Default::default(),
            func_qi2,
            "text".into(),
        );
        fmt_computed_field(&mut b, &table_qi, &field2);

        assert_eq!(
            b.sql(),
            "\"test_api\".\"full_name\"(\"users\"), \"test_api\".\"initials\"(\"users\")"
        );
    }
}
