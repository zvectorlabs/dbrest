//! Final SQL statement assembly.
//!
//! Wraps the SQL produced by [`super::builder`] into CTE-based final statements
//! that return a uniform response shape: total count, page count, body,
//! response headers, and response status. This module sits at the top of the
//! SQL generation pipeline, right before the query is handed to the executor.
//!
//! # Pipeline
//!
//! ```text
//! builder.rs (SELECT / INSERT / CALL …) ──▶ statements.rs (CTE wrapper) ──▶ executor
//! ```
//!
//! # SQL Example
//!
//! ```sql
//! WITH dbrst_source AS (
//!   SELECT "public"."users"."id" AS "id", "public"."users"."name" AS "name"
//!   FROM "public"."users"
//! )
//! SELECT
//!   NULL AS total_result_set,
//!   pg_catalog.count(_dbrst_t) AS page_total,
//!   coalesce(json_agg(_dbrst_t), '[]')::text AS body,
//!   nullif(current_setting('response.headers', true), '') AS response_headers,
//!   nullif(current_setting('response.status', true), '') AS response_status
//! FROM (SELECT * FROM dbrst_source) AS _dbrst_t
//! ```

use crate::api_request::preferences::PreferCount;
use crate::backend::SqlDialect;
use crate::plan::call_plan::CallPlan;
use crate::plan::mutate_plan::MutatePlan;
use crate::plan::read_plan::ReadPlanTree;

use super::builder;
use super::fragment;
use super::sql_builder::SqlBuilder;

// ==========================================================================
// main_read — CTE wrapper for SELECT queries
// ==========================================================================

/// Build the final read statement with CTE wrapper.
///
/// Wraps a `ReadPlanTree` query in a CTE that returns the standard response
/// shape: total count, page count, body, response headers, and response status.
///
/// # Behaviour
///
/// - If `prefer_count` is `Exact`, adds a count CTE for the total result set
/// - If `prefer_count` is `Planned`, uses `EXPLAIN` row estimate
/// - The `max_rows` config limit is applied as an additional cap
/// - `headers_only` omits the body column value (for HEAD requests)
/// - `handler` determines the output format (JSON, CSV, binary, etc.)
///
/// # SQL Example
///
/// ```sql
/// WITH dbrst_source AS (
///   SELECT … FROM "public"."users" WHERE …
/// )
/// SELECT
///   NULL AS total_result_set,
///   pg_catalog.count(_dbrst_t) AS page_total,
///   coalesce(json_agg(_dbrst_t), '[]')::text AS body,
///   nullif(current_setting('response.headers', true), '') AS response_headers,
///   nullif(current_setting('response.status', true), '') AS response_status
/// FROM (SELECT * FROM dbrst_source) AS _dbrst_t
/// ```
pub fn main_read(
    read_plan: &ReadPlanTree,
    prefer_count: Option<PreferCount>,
    max_rows: Option<i64>,
    headers_only: bool,
    handler: Option<&crate::schema_cache::media_handler::MediaHandler>,
    dialect: &dyn SqlDialect,
) -> SqlBuilder {
    let inner = builder::read_plan_to_query(read_plan, dialect);
    let mut b = SqlBuilder::new();

    // CTE: dbrst_source
    b.push("WITH dbrst_source AS (");
    b.push_builder(&inner);
    b.push(")");

    // Optional count CTE
    let has_exact_count = matches!(prefer_count, Some(PreferCount::Exact));
    if has_exact_count {
        let count_q = builder::read_plan_to_count_query(read_plan, dialect);
        b.push(", dbrst_count AS (");
        b.push_builder(&count_q);
        b.push(")");
    }

    // Main SELECT
    b.push(" SELECT ");

    // total_result_set
    if has_exact_count {
        b.push("(SELECT ");
        b.push_ident("dbrst_filtered_count");
        b.push(" FROM dbrst_count)");
    } else {
        b.push("NULL");
    }
    b.push(" AS total_result_set");

    // page_total
    b.push(", ");
    dialect.count_expr(&mut b, "_dbrst_t");
    b.push(" AS page_total");

    // body
    if headers_only {
        b.push(", NULL AS body");
    } else {
        b.push(", ");
        if let Some(h) = handler {
            fragment::handler_agg_with_media(&mut b, h, false, dialect);
        } else {
            fragment::handler_agg(&mut b, false, dialect);
        }
        b.push(" AS body");
    }

    // response_headers & response_status
    b.push(", ");
    dialect.get_session_var(&mut b, "response.headers", "response_headers");
    b.push(", ");
    dialect.get_session_var(&mut b, "response.status", "response_status");

    // FROM dbrst_source
    b.push(" FROM (SELECT * FROM dbrst_source");

    // Apply max_rows if configured
    if let Some(max) = max_rows {
        b.push(" LIMIT ");
        b.push(&max.to_string());
    }

    b.push(") AS ");
    b.push_ident("_dbrst_t");

    b
}

// ==========================================================================
// main_write — CTE wrapper for mutation queries
// ==========================================================================

/// Build the final mutation statement with CTE wrapper.
///
/// Wraps a `MutatePlan` query in a CTE, optionally adding a read sub-select
/// for `Prefer: return=representation`.
///
/// # Behaviour
///
/// - The mutation CTE (`dbrst_source`) contains the INSERT/UPDATE/DELETE
/// - If `return_representation` is true, the response body includes the
///   returned rows as JSON
/// - The location header expression is included for INSERT operations
///
/// # SQL Example
///
/// ```sql
/// WITH dbrst_source AS (
///   INSERT INTO "public"."users"("name") VALUES ($1) RETURNING "id", "name"
/// )
/// SELECT
///   '' AS total_result_set,
///   pg_catalog.count(_dbrst_t) AS page_total,
///   coalesce(json_agg(_dbrst_t), '[]')::text AS body,
///   nullif(current_setting('response.headers', true), '') AS response_headers,
///   nullif(current_setting('response.status', true), '') AS response_status
/// FROM (SELECT * FROM dbrst_source) AS _dbrst_t
/// ```
pub fn main_write(
    mutate_plan: &MutatePlan,
    _read_plan: &ReadPlanTree,
    return_representation: bool,
    handler: Option<&crate::schema_cache::media_handler::MediaHandler>,
    dialect: &dyn SqlDialect,
) -> SqlBuilder {
    let inner = builder::mutate_plan_to_query(mutate_plan, dialect);
    let has_returning = !mutate_plan.returning().is_empty();
    let mut b = SqlBuilder::new();

    b.push("WITH dbrst_source AS (");
    b.push_builder(&inner);
    if !has_returning {
        b.push(" RETURNING 1");
    }
    b.push(")");

    // Main SELECT
    b.push(" SELECT ");

    // total_result_set (mutations don't support count)
    b.push("'' AS total_result_set");

    // page_total
    b.push(", ");
    dialect.count_expr(&mut b, "_dbrst_t");
    b.push(" AS page_total");

    // body
    if return_representation && has_returning {
        b.push(", ");
        if let Some(h) = handler {
            fragment::handler_agg_with_media(&mut b, h, false, dialect);
        } else {
            fragment::handler_agg(&mut b, false, dialect);
        }
        b.push(" AS body");
    } else {
        b.push(", NULL AS body");
    }

    // response_headers & response_status
    b.push(", ");
    dialect.get_session_var(&mut b, "response.headers", "response_headers");
    b.push(", ");
    dialect.get_session_var(&mut b, "response.status", "response_status");

    // FROM dbrst_source
    b.push(" FROM (SELECT * FROM dbrst_source) AS ");
    b.push_ident("_dbrst_t");

    b
}

// ==========================================================================
// main_call — CTE wrapper for function call queries
// ==========================================================================

/// Build the final function call statement with CTE wrapper.
///
/// Wraps a `CallPlan` query in a CTE. Handles both scalar and set-returning
/// functions.
///
/// # Behaviour
///
/// - Scalar functions: body is a single JSON value
/// - Set-returning functions: body is a JSON array
/// - The count CTE is included when `prefer_count` is `Exact`
///
/// # SQL Example
///
/// ```sql
/// WITH dbrst_source AS (
///   SELECT * FROM "public"."get_users"()
/// )
/// SELECT
///   NULL AS total_result_set,
///   pg_catalog.count(_dbrst_t) AS page_total,
///   coalesce(json_agg(_dbrst_t), '[]')::text AS body,
///   nullif(current_setting('response.headers', true), '') AS response_headers,
///   nullif(current_setting('response.status', true), '') AS response_status
/// FROM (SELECT * FROM dbrst_source) AS _dbrst_t
/// ```
pub fn main_call(
    call_plan: &CallPlan,
    prefer_count: Option<PreferCount>,
    max_rows: Option<i64>,
    handler: Option<&crate::schema_cache::media_handler::MediaHandler>,
    dialect: &dyn SqlDialect,
) -> SqlBuilder {
    let inner = builder::call_plan_to_query(call_plan, dialect);
    let mut b = SqlBuilder::new();

    // CTE: dbrst_source
    b.push("WITH dbrst_source AS (");
    b.push_builder(&inner);
    b.push(")");

    let has_exact_count = matches!(prefer_count, Some(PreferCount::Exact));

    // Main SELECT
    b.push(" SELECT ");

    // total_result_set
    if has_exact_count {
        b.push("(SELECT pg_catalog.count(*) FROM dbrst_source)");
    } else {
        b.push("NULL");
    }
    b.push(" AS total_result_set");

    // page_total
    if call_plan.scalar {
        b.push(", 1 AS page_total");
    } else {
        b.push(", ");
        dialect.count_expr(&mut b, "_dbrst_t");
        b.push(" AS page_total");
    }

    // body
    b.push(", ");
    if call_plan.scalar {
        // Scalar function: row_to_json(dbrst_source.*)::text
        // We use a specialized form because the source is "table.*" not just an alias
        b.push("row_to_json(dbrst_source.*)::text");
    } else if let Some(h) = handler {
        fragment::handler_agg_with_media(&mut b, h, false, dialect);
    } else {
        fragment::handler_agg(&mut b, false, dialect);
    }
    b.push(" AS body");

    // response_headers & response_status
    b.push(", ");
    dialect.get_session_var(&mut b, "response.headers", "response_headers");
    b.push(", ");
    dialect.get_session_var(&mut b, "response.status", "response_status");

    // FROM dbrst_source
    if call_plan.scalar {
        b.push(" FROM dbrst_source");
    } else {
        b.push(" FROM (SELECT * FROM dbrst_source");

        if let Some(max) = max_rows {
            b.push(" LIMIT ");
            b.push(&max.to_string());
        }

        b.push(") AS ");
        b.push_ident("_dbrst_t");
    }

    b
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_request::types::Payload;
    use crate::backend::postgres::PgDialect;
    use crate::plan::call_plan::{CallArgs, CallParams, CallPlan};
    use crate::plan::mutate_plan::{InsertPlan, MutatePlan};
    use crate::plan::read_plan::{ReadPlan, ReadPlanTree};
    use crate::plan::types::*;
    use crate::types::identifiers::QualifiedIdentifier;
    use bytes::Bytes;
    use smallvec::SmallVec;

    fn dialect() -> &'static dyn SqlDialect {
        &PgDialect
    }

    fn test_qi() -> QualifiedIdentifier {
        QualifiedIdentifier::new("public", "users")
    }

    fn select_field(name: &str) -> CoercibleSelectField {
        CoercibleSelectField {
            field: CoercibleField::unknown(name.into(), SmallVec::new()),
            agg_function: None,
            agg_cast: None,
            cast: None,
            alias: None,
        }
    }

    fn typed_field(name: &str, base_type: &str) -> CoercibleField {
        CoercibleField::from_column(name.into(), SmallVec::new(), base_type.into())
    }

    // ------------------------------------------------------------------
    // main_read tests
    // ------------------------------------------------------------------

    #[test]
    fn test_main_read_basic() {
        let mut plan = ReadPlan::root(test_qi());
        plan.select = vec![select_field("id"), select_field("name")];
        let tree = ReadPlanTree::leaf(plan);

        let b = main_read(&tree, None, None, false, None, dialect());
        let sql = b.sql();

        assert!(sql.starts_with("WITH dbrst_source AS ("));
        assert!(sql.contains("AS total_result_set"));
        assert!(sql.contains("AS page_total"));
        assert!(sql.contains("AS body"));
        assert!(sql.contains("AS response_headers"));
        assert!(sql.contains("AS response_status"));
    }

    #[test]
    fn test_main_read_with_exact_count() {
        let plan = ReadPlan::root(test_qi());
        let tree = ReadPlanTree::leaf(plan);

        let b = main_read(&tree, Some(PreferCount::Exact), None, false, None, dialect());
        let sql = b.sql();

        assert!(sql.contains("dbrst_count"));
        assert!(sql.contains("dbrst_filtered_count"));
    }

    #[test]
    fn test_main_read_headers_only() {
        let plan = ReadPlan::root(test_qi());
        let tree = ReadPlanTree::leaf(plan);

        let b = main_read(&tree, None, None, true, None, dialect());
        let sql = b.sql();

        assert!(sql.contains("NULL AS body"));
    }

    #[test]
    fn test_main_read_with_max_rows() {
        let plan = ReadPlan::root(test_qi());
        let tree = ReadPlanTree::leaf(plan);

        let b = main_read(&tree, None, Some(100), false, None, dialect());
        let sql = b.sql();

        assert!(sql.contains("LIMIT 100"));
    }

    // ------------------------------------------------------------------
    // main_write tests
    // ------------------------------------------------------------------

    #[test]
    fn test_main_write_basic() {
        let mutate = MutatePlan::Insert(InsertPlan {
            into: test_qi(),
            columns: vec![typed_field("name", "text")],
            body: Payload::RawJSON(Bytes::from(r#"[{"name":"test"}]"#)),
            on_conflict: None,
            where_: vec![],
            returning: vec![select_field("id")],
            pk_cols: vec!["id".into()],
            apply_defaults: false,
        });
        let read = ReadPlanTree::leaf(ReadPlan::root(test_qi()));

        let b = main_write(&mutate, &read, true, None, dialect());
        let sql = b.sql();

        assert!(sql.starts_with("WITH dbrst_source AS ("));
        assert!(sql.contains("INSERT INTO"));
        assert!(sql.contains("AS body"));
    }

    #[test]
    fn test_main_write_no_representation() {
        let mutate = MutatePlan::Insert(InsertPlan {
            into: test_qi(),
            columns: vec![],
            body: Payload::RawJSON(Bytes::from("{}")),
            on_conflict: None,
            where_: vec![],
            returning: vec![],
            pk_cols: vec![],
            apply_defaults: false,
        });
        let read = ReadPlanTree::leaf(ReadPlan::root(test_qi()));

        let b = main_write(&mutate, &read, false, None, dialect());
        let sql = b.sql();

        assert!(sql.contains("NULL AS body"));
    }

    // ------------------------------------------------------------------
    // main_call tests
    // ------------------------------------------------------------------

    #[test]
    fn test_main_call_basic() {
        let call = CallPlan {
            qi: QualifiedIdentifier::new("public", "get_time"),
            params: CallParams::KeyParams(vec![]),
            args: CallArgs::JsonArgs(None),
            scalar: false,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let b = main_call(&call, None, None, None, dialect());
        let sql = b.sql();

        assert!(sql.starts_with("WITH dbrst_source AS ("));
        assert!(sql.contains("get_time"));
        assert!(sql.contains("AS body"));
    }

    #[test]
    fn test_main_call_scalar() {
        let call = CallPlan {
            qi: QualifiedIdentifier::new("public", "add_numbers"),
            params: CallParams::KeyParams(vec![]),
            args: CallArgs::JsonArgs(None),
            scalar: true,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let b = main_call(&call, None, None, None, dialect());
        let sql = b.sql();

        // Scalar uses row_to_json instead of json_agg
        assert!(sql.contains("row_to_json"));
    }

    #[test]
    fn test_main_call_with_count() {
        let call = CallPlan {
            qi: QualifiedIdentifier::new("public", "get_data"),
            params: CallParams::KeyParams(vec![]),
            args: CallArgs::JsonArgs(None),
            scalar: false,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let b = main_call(&call, Some(PreferCount::Exact), None, None, dialect());
        let sql = b.sql();

        assert!(sql.contains("pg_catalog.count(*)"));
    }

    #[test]
    fn test_main_call_with_max_rows() {
        let call = CallPlan {
            qi: QualifiedIdentifier::new("public", "get_data"),
            params: CallParams::KeyParams(vec![]),
            args: CallArgs::JsonArgs(None),
            scalar: false,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let b = main_call(&call, None, Some(50), None, dialect());
        let sql = b.sql();

        assert!(sql.contains("LIMIT 50"));
    }
}
