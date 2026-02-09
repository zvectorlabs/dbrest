//! Plan-to-SQL conversion functions.
//!
//! Converts typed plan trees (`ReadPlanTree`, `MutatePlan`, `CallPlan`) into
//! parameterised SQL queries. This module sits between the plan layer and the
//! statement assemblers: the plan describes *what* to query, and this module
//! decides *how* to express it in SQL.
//!
//! # Pipeline
//!
//! ```text
//! ReadPlanTree  ──▶ read_plan_to_query()       ──▶ SqlBuilder (SELECT …)
//! ReadPlanTree  ──▶ read_plan_to_count_query()  ──▶ SqlBuilder (SELECT COUNT(*) …)
//! MutatePlan    ──▶ mutate_plan_to_query()      ──▶ SqlBuilder (INSERT / UPDATE / DELETE …)
//! CallPlan      ──▶ call_plan_to_query()         ──▶ SqlBuilder (SELECT func(…) …)
//! ```

use crate::api_request::types::Payload;
use crate::plan::call_plan::{CallArgs, CallParams, CallPlan, RpcParamValue};
use crate::plan::mutate_plan::{DeletePlan, InsertPlan, MutatePlan, UpdatePlan};
use crate::plan::read_plan::ReadPlanTree;

use super::fragment;
use super::sql_builder::{SqlBuilder, SqlParam};

// ==========================================================================
// Read plan → SQL
// ==========================================================================

/// Convert a `ReadPlanTree` into a SELECT query.
///
/// Generates a recursive SELECT with lateral joins for embedded resources.
/// Each child in the tree becomes a lateral subquery that is joined to the
/// parent.
///
/// # Behaviour
///
/// - Root node produces the main `SELECT … FROM …`
/// - Each child becomes a `LATERAL (SELECT …) AS alias` joined via ON conditions
/// - Filters, order, limit/offset, and group-by are applied per node
/// - JSON aggregation is used for to-many relations
///
/// # SQL Example
///
/// ```sql
/// SELECT "public"."users"."id" AS "id",
///        "public"."users"."name" AS "name",
///        _pgrest_agg_1.body AS "posts"
/// FROM "public"."users"
/// LEFT JOIN LATERAL (
///   SELECT coalesce(json_agg(_pgrest_t), '[]')::text AS body
///   FROM (
///     SELECT "public"."posts"."id" AS "id",
///            "public"."posts"."title" AS "title"
///     FROM "public"."posts"
///     WHERE "public"."posts"."user_id" = "public"."users"."id"
///   ) AS _pgrest_t
/// ) AS _pgrest_agg_1 ON TRUE
/// WHERE "public"."users"."id" = $1
/// ORDER BY "public"."users"."name" ASC
/// LIMIT 10
/// ```
pub fn read_plan_to_query(tree: &ReadPlanTree) -> SqlBuilder {
    let plan = &tree.node;
    let qi = &plan.from;

    let mut b = SqlBuilder::new();

    // SELECT clause
    b.push("SELECT ");
    if plan.select.is_empty() {
        // Default: select all columns
        b.push_qi(qi);
        b.push(".*");
    } else {
        b.push_separated(", ", &plan.select, |b, sel| {
            fragment::fmt_select_item(b, qi, sel);
        });
    }

    // Add join select expressions for children
    for (i, child) in tree.forest.iter().enumerate() {
        b.push(", ");
        let agg_alias = &child.node.rel_agg_alias;
        b.push_ident(agg_alias);
        b.push(".body");
        // Alias as the relation name
        let sel_name = child
            .node
            .rel_alias
            .as_ref()
            .unwrap_or(&child.node.rel_name);
        b.push(" AS ");
        b.push_ident(sel_name);
        let _ = i; // suppress unused warning
    }

    // FROM clause
    b.push(" FROM ");
    b.push_qi(qi);
    if let Some(ref alias) = plan.from_alias {
        b.push(" AS ");
        b.push_ident(alias);
    }

    // LATERAL JOINs for children
    for child in &tree.forest {
        let is_inner = child
            .node
            .rel_join_type
            .map(|jt| matches!(jt, crate::api_request::types::JoinType::Inner))
            .unwrap_or(false);

        let join_type = if is_inner {
            "INNER JOIN LATERAL"
        } else {
            "LEFT JOIN LATERAL"
        };

        b.push(" ");
        b.push(join_type);
        b.push(" (");

        // Inner aggregation subquery
        let is_to_one = child
            .node
            .rel_to_parent
            .as_ref()
            .map(|r| r.is_to_one())
            .unwrap_or(false);

        if is_to_one {
            b.push("SELECT row_to_json(");
            b.push_ident("_pgrest_t");
            b.push(")::text AS body FROM (");
        } else {
            b.push("SELECT coalesce(json_agg(");
            b.push_ident("_pgrest_t");
            b.push("), '[]')::text AS body FROM (");
        }

        // Recursive child query
        let child_query = read_plan_to_query(child);
        b.push_builder(&child_query);

        b.push(") AS ");
        b.push_ident("_pgrest_t");
        b.push(") AS ");
        b.push_ident(&child.node.rel_agg_alias);
        b.push(" ON TRUE");
    }

    // WHERE clause (includes join conditions for child nodes)
    let mut has_where = false;
    if !plan.where_.is_empty() {
        fragment::where_clause(&mut b, qi, &plan.where_);
        has_where = true;
    }

    // Join conditions to parent (for child nodes)
    if !plan.rel_join_conds.is_empty() {
        if has_where {
            b.push(" AND ");
        } else {
            b.push(" WHERE ");
        }
        b.push_separated(" AND ", &plan.rel_join_conds, |b, jc| {
            fragment::fmt_join_condition(b, jc);
        });
    }

    // GROUP BY
    fragment::group_clause(&mut b, qi, &plan.select);

    // ORDER BY
    fragment::order_clause(&mut b, qi, &plan.order);

    // LIMIT / OFFSET
    fragment::limit_offset(&mut b, plan.range.offset, plan.range.limit_to);

    b
}

/// Convert a `ReadPlanTree` into a COUNT query.
///
/// Produces `SELECT COUNT(*) FROM (source_query) AS _pgrst_count_t`.
///
/// # SQL Example
/// ```sql
/// SELECT COUNT(*) AS "pgrst_filtered_count"
/// FROM (SELECT … FROM "public"."users" WHERE …) AS _pgrst_count_t
/// ```
pub fn read_plan_to_count_query(tree: &ReadPlanTree) -> SqlBuilder {
    let mut b = SqlBuilder::new();
    fragment::count_f(&mut b);

    // Build the inner query without LIMIT/OFFSET for accurate counting
    let plan = &tree.node;
    let qi = &plan.from;

    b.push(" FROM (SELECT 1 FROM ");
    b.push_qi(qi);

    // WHERE clause
    if !plan.where_.is_empty() {
        fragment::where_clause(&mut b, qi, &plan.where_);
    }

    b.push(") AS _pgrst_count_t");

    b
}

// ==========================================================================
// Mutate plan → SQL
// ==========================================================================

/// Convert a `MutatePlan` into an INSERT, UPDATE, or DELETE query.
///
/// # Behaviour
///
/// - **INSERT**: `INSERT INTO … SELECT … FROM json_to_recordset($1) … RETURNING …`
/// - **UPDATE**: `UPDATE … SET (cols) = (SELECT … FROM json_to_recordset($1) …) WHERE … RETURNING …`
/// - **DELETE**: `DELETE FROM … WHERE … RETURNING …`
///
/// # SQL Example
/// ```sql
/// -- Insert
/// INSERT INTO "public"."users"("id", "name")
/// SELECT "id", "name" FROM json_to_recordset($1) AS _("id" integer, "name" text)
/// RETURNING "id" AS "id", "name" AS "name"
///
/// -- Update
/// UPDATE "public"."users" SET ("name") =
///   (SELECT "name" FROM json_to_recordset($1) AS _("name" text))
/// WHERE "id" = $2
/// RETURNING "id" AS "id", "name" AS "name"
///
/// -- Delete
/// DELETE FROM "public"."users" WHERE "id" = $1
/// RETURNING "id" AS "id"
/// ```
pub fn mutate_plan_to_query(plan: &MutatePlan) -> SqlBuilder {
    match plan {
        MutatePlan::Insert(insert) => insert_to_query(insert),
        MutatePlan::Update(update) => update_to_query(update),
        MutatePlan::Delete(delete) => delete_to_query(delete),
    }
}

/// Generate an INSERT query from an `InsertPlan`.
///
/// # Behaviour
///
/// - If `columns` is empty, emits `INSERT INTO … DEFAULT VALUES`
/// - Otherwise, emits `INSERT INTO …(cols) SELECT cols FROM json_to_recordset($1) AS _(…)`
/// - Appends ON CONFLICT clause if present (DO UPDATE SET or DO NOTHING)
/// - Appends WHERE and RETURNING clauses from the plan
///
/// # SQL Example
///
/// ```sql
/// INSERT INTO "public"."users"("id", "name")
/// SELECT "id", "name" FROM json_to_recordset($1) AS _("id" integer, "name" text)
/// ON CONFLICT("id") DO UPDATE SET "name" = EXCLUDED."name"
/// RETURNING "id" AS "id", "name" AS "name"
/// ```
fn insert_to_query(plan: &InsertPlan) -> SqlBuilder {
    let qi = &plan.into;
    let mut b = SqlBuilder::new();

    b.push("INSERT INTO ");
    b.push_qi(qi);

    if plan.columns.is_empty() {
        // Empty insert (DEFAULT VALUES)
        b.push(" DEFAULT VALUES");
    } else {
        // Column list
        b.push("(");
        b.push_separated(", ", &plan.columns, |b, col| {
            b.push_ident(&col.name);
        });
        b.push(")");

        // SELECT from JSON body
        b.push(" SELECT ");
        b.push_separated(", ", &plan.columns, |b, col| {
            b.push_ident(&col.name);
        });
        b.push(" FROM ");

        let json_bytes = payload_to_bytes(&plan.body);
        fragment::from_json_body(&mut b, &plan.columns, &json_bytes);
    }

    // ON CONFLICT
    if let Some(ref oc) = plan.on_conflict {
        b.push(" ON CONFLICT(");
        b.push_separated(", ", &oc.columns, |b, col| {
            b.push_ident(col);
        });
        b.push(")");

        if oc.merge_duplicates {
            b.push(" DO UPDATE SET ");
            b.push_separated(", ", &plan.columns, |b, col| {
                b.push_ident(&col.name);
                b.push(" = EXCLUDED.");
                b.push_ident(&col.name);
            });
        } else {
            b.push(" DO NOTHING");
        }
    }

    // WHERE
    fragment::where_clause(&mut b, qi, &plan.where_);

    // RETURNING
    fragment::returning_clause(&mut b, qi, &plan.returning);

    b
}

/// Generate an UPDATE query from an `UpdatePlan`.
///
/// # Behaviour
///
/// - Single column: `UPDATE … SET "col" = (SELECT "col" FROM json_to_recordset(…))`
/// - Multiple columns: `UPDATE … SET (cols) = (SELECT cols FROM json_to_recordset(…))`
/// - The JSON body is unpacked via `json_to_recordset` with typed column definitions
/// - Appends WHERE and RETURNING clauses from the plan
///
/// # SQL Example
///
/// ```sql
/// UPDATE "public"."users" SET "name" = (SELECT "name"
///   FROM json_to_recordset($1) AS _("name" text))
/// WHERE "public"."users"."id"=$2
/// RETURNING "id" AS "id", "name" AS "name"
/// ```
fn update_to_query(plan: &UpdatePlan) -> SqlBuilder {
    let qi = &plan.into;
    let mut b = SqlBuilder::new();

    b.push("UPDATE ");
    b.push_qi(qi);
    b.push(" SET ");

    if plan.columns.len() == 1 {
        b.push_ident(&plan.columns[0].name);
        b.push(" = (SELECT ");
        b.push_ident(&plan.columns[0].name);
    } else {
        b.push("(");
        b.push_separated(", ", &plan.columns, |b, col| {
            b.push_ident(&col.name);
        });
        b.push(") = (SELECT ");
        b.push_separated(", ", &plan.columns, |b, col| {
            b.push_ident(&col.name);
        });
    }

    b.push(" FROM ");
    let json_bytes = payload_to_bytes(&plan.body);
    fragment::from_json_body(&mut b, &plan.columns, &json_bytes);
    b.push(")");

    // WHERE
    fragment::where_clause(&mut b, qi, &plan.where_);

    // RETURNING
    fragment::returning_clause(&mut b, qi, &plan.returning);

    b
}

/// Generate a DELETE query from a `DeletePlan`.
///
/// # Behaviour
///
/// Emits `DELETE FROM "schema"."table"` with optional WHERE and RETURNING
/// clauses. The simplest of the mutation queries.
///
/// # SQL Example
///
/// ```sql
/// DELETE FROM "public"."users"
/// WHERE "public"."users"."id"=$1
/// RETURNING "id" AS "id"
/// ```
fn delete_to_query(plan: &DeletePlan) -> SqlBuilder {
    let qi = &plan.from;
    let mut b = SqlBuilder::new();

    b.push("DELETE FROM ");
    b.push_qi(qi);

    // WHERE
    fragment::where_clause(&mut b, qi, &plan.where_);

    // RETURNING
    fragment::returning_clause(&mut b, qi, &plan.returning);

    b
}

// ==========================================================================
// Call plan → SQL
// ==========================================================================

/// Convert a `CallPlan` into a function call query.
///
/// # Behaviour
///
/// - Named parameters: `SELECT * FROM "schema"."func"("p1" := $1, "p2" := $2)`
/// - JSON body: `SELECT * FROM "schema"."func"($1::jsonb)` (for single-param JSON functions)
/// - No args: `SELECT * FROM "schema"."func"()`
///
/// # SQL Example
/// ```sql
/// SELECT * FROM "public"."add_numbers"("a" := $1, "b" := $2)
/// SELECT * FROM "public"."get_data"($1::jsonb)
/// ```
pub fn call_plan_to_query(plan: &CallPlan) -> SqlBuilder {
    let mut b = SqlBuilder::new();

    b.push("SELECT * FROM ");
    b.push_qi(&plan.qi);
    b.push("(");

    match &plan.args {
        CallArgs::DirectArgs(args) => {
            match &plan.params {
                CallParams::KeyParams(params) => {
                    let mut first = true;
                    for param in params {
                        if let Some(val) = args.get(&param.name) {
                            if !first {
                                b.push(", ");
                            }
                            first = false;
                            b.push_ident(&param.name);
                            b.push(" := ");
                            match val {
                                RpcParamValue::Fixed(v) => {
                                    b.push_param(SqlParam::Text(v.to_string()));
                                }
                                RpcParamValue::Variadic(vals) => {
                                    b.push("VARIADIC ARRAY[");
                                    for (i, v) in vals.iter().enumerate() {
                                        if i > 0 {
                                            b.push(", ");
                                        }
                                        b.push_param(SqlParam::Text(v.to_string()));
                                    }
                                    b.push("]");
                                }
                            }
                        }
                    }
                }
                CallParams::OnePosParam(_) => {
                    // Single positional — take first value
                    if let Some((_, val)) = args.iter().next() {
                        match val {
                            RpcParamValue::Fixed(v) => {
                                b.push_param(SqlParam::Text(v.to_string()));
                            }
                            RpcParamValue::Variadic(vals) => {
                                b.push_param(SqlParam::Text(vals.join(",").to_string()));
                            }
                        }
                    }
                }
            }
        }
        CallArgs::JsonArgs(json) => {
            if let Some(body) = json {
                b.push_param(SqlParam::Json(body.clone()));
            }
        }
    }

    b.push(")");

    b
}

// ==========================================================================
// Helpers
// ==========================================================================

/// Extract raw bytes from a `Payload` for use in `json_to_recordset()`.
///
/// # Behaviour
///
/// - `ProcessedJSON` / `RawJSON` / `RawPayload` — returns the raw bytes directly
/// - `ProcessedUrlEncoded` — converts key-value pairs to a JSON object string
///   so they can be consumed by `json_to_recordset`
fn payload_to_bytes(payload: &Payload) -> Vec<u8> {
    match payload {
        Payload::ProcessedJSON { raw, .. } => raw.to_vec(),
        Payload::RawJSON(raw) => raw.to_vec(),
        Payload::RawPayload(raw) => raw.to_vec(),
        Payload::ProcessedUrlEncoded { params, .. } => {
            // Convert URL-encoded params to JSON for json_to_recordset
            let json = serde_json::json!(
                params.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect::<std::collections::HashMap<_, _>>()
            );
            json.to_string().into_bytes()
        }
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_request::range::Range;
    use crate::api_request::types::*;
    use crate::plan::call_plan::*;
    use crate::plan::mutate_plan::*;
    use crate::plan::read_plan::*;
    use crate::plan::types::*;
    use crate::types::identifiers::QualifiedIdentifier;
    use bytes::Bytes;
    use compact_str::CompactString;
    use smallvec::SmallVec;
    use std::collections::HashMap;

    fn test_qi() -> QualifiedIdentifier {
        QualifiedIdentifier::new("public", "users")
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
    // Read plan tests
    // ------------------------------------------------------------------

    #[test]
    fn test_read_plan_simple() {
        let mut plan = ReadPlan::root(test_qi());
        plan.select = vec![select_field("id"), select_field("name")];

        let tree = ReadPlanTree::leaf(plan);
        let b = read_plan_to_query(&tree);
        let sql = b.sql();

        assert!(sql.starts_with("SELECT "));
        assert!(sql.contains("\"id\""));
        assert!(sql.contains("\"name\""));
        assert!(sql.contains("FROM \"public\".\"users\""));
    }

    #[test]
    fn test_read_plan_with_where() {
        let mut plan = ReadPlan::root(test_qi());
        plan.select = vec![select_field("id")];
        plan.where_ = vec![CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
            field: field("id"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
            },
        })];

        let tree = ReadPlanTree::leaf(plan);
        let b = read_plan_to_query(&tree);

        assert!(b.sql().contains("WHERE"));
        assert!(b.sql().contains("$1"));
        assert_eq!(b.param_count(), 1);
    }

    #[test]
    fn test_read_plan_with_order() {
        let mut plan = ReadPlan::root(test_qi());
        plan.select = vec![select_field("name")];
        plan.order = vec![CoercibleOrderTerm::Term {
            field: field("name"),
            direction: Some(OrderDirection::Asc),
            nulls: None,
        }];

        let tree = ReadPlanTree::leaf(plan);
        let sql = read_plan_to_query(&tree).sql().to_string();
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("ASC"));
    }

    #[test]
    fn test_read_plan_with_limit_offset() {
        let mut plan = ReadPlan::root(test_qi());
        plan.select = vec![select_field("id")];
        plan.range = Range {
            offset: 5,
            limit_to: Some(14),
        };

        let tree = ReadPlanTree::leaf(plan);
        let sql = read_plan_to_query(&tree).sql().to_string();
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 5"));
    }

    #[test]
    fn test_read_plan_default_select() {
        let plan = ReadPlan::root(test_qi());
        let tree = ReadPlanTree::leaf(plan);
        let sql = read_plan_to_query(&tree).sql().to_string();
        assert!(sql.contains("\"public\".\"users\".*"));
    }

    #[test]
    fn test_read_plan_count_query() {
        let mut plan = ReadPlan::root(test_qi());
        plan.where_ = vec![CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
            field: field("status"),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "active".into()),
            },
        })];

        let tree = ReadPlanTree::leaf(plan);
        let b = read_plan_to_count_query(&tree);

        assert!(b.sql().contains("COUNT(*)"));
        assert!(b.sql().contains("_pgrst_count_t"));
    }

    #[test]
    fn test_read_plan_with_lateral_join() {
        use crate::schema_cache::relationship::{AnyRelationship, Cardinality, Relationship};

        let root = ReadPlan::root(test_qi());
        let mut child = ReadPlan::child(
            QualifiedIdentifier::new("public", "posts"),
            "posts".into(),
            1,
        );
        child.select = vec![select_field("id"), select_field("title")];
        child.rel_to_parent = Some(AnyRelationship::ForeignKey(Relationship {
            table: QualifiedIdentifier::new("public", "users"),
            foreign_table: QualifiedIdentifier::new("public", "posts"),
            is_self: false,
            cardinality: Cardinality::O2M {
                constraint: "fk_posts".into(),
                columns: smallvec::smallvec![("id".into(), "user_id".into())],
            },
            table_is_view: false,
            foreign_table_is_view: false,
        }));
        child.rel_join_conds = vec![JoinCondition {
            parent: (test_qi(), "id".into()),
            child: (
                QualifiedIdentifier::new("public", "posts"),
                "user_id".into(),
            ),
        }];

        let tree = ReadPlanTree::with_children(root, vec![ReadPlanTree::leaf(child)]);
        let sql = read_plan_to_query(&tree).sql().to_string();

        assert!(sql.contains("LEFT JOIN LATERAL"));
        assert!(sql.contains("json_agg"));
        assert!(sql.contains("ON TRUE"));
    }

    // ------------------------------------------------------------------
    // Mutate plan tests
    // ------------------------------------------------------------------

    #[test]
    fn test_insert_query() {
        let plan = MutatePlan::Insert(InsertPlan {
            into: test_qi(),
            columns: vec![typed_field("id", "integer"), typed_field("name", "text")],
            body: Payload::RawJSON(Bytes::from(r#"[{"id":1,"name":"test"}]"#)),
            on_conflict: None,
            where_: vec![],
            returning: vec![select_field("id")],
            pk_cols: vec!["id".into()],
            apply_defaults: false,
        });

        let b = mutate_plan_to_query(&plan);
        let sql = b.sql();
        assert!(sql.starts_with("INSERT INTO "));
        assert!(sql.contains("json_to_recordset"));
        assert!(sql.contains("RETURNING"));
    }

    #[test]
    fn test_insert_with_on_conflict() {
        let plan = MutatePlan::Insert(InsertPlan {
            into: test_qi(),
            columns: vec![typed_field("id", "integer"), typed_field("name", "text")],
            body: Payload::RawJSON(Bytes::from(r#"[{"id":1,"name":"test"}]"#)),
            on_conflict: Some(crate::plan::mutate_plan::OnConflict {
                columns: vec!["id".into()],
                merge_duplicates: true,
            }),
            where_: vec![],
            returning: vec![],
            pk_cols: vec!["id".into()],
            apply_defaults: false,
        });

        let sql = mutate_plan_to_query(&plan).sql().to_string();
        assert!(sql.contains("ON CONFLICT"));
        assert!(sql.contains("DO UPDATE SET"));
        assert!(sql.contains("EXCLUDED"));
    }

    #[test]
    fn test_insert_do_nothing() {
        let plan = MutatePlan::Insert(InsertPlan {
            into: test_qi(),
            columns: vec![typed_field("id", "integer")],
            body: Payload::RawJSON(Bytes::from(r#"[{"id":1}]"#)),
            on_conflict: Some(crate::plan::mutate_plan::OnConflict {
                columns: vec!["id".into()],
                merge_duplicates: false,
            }),
            where_: vec![],
            returning: vec![],
            pk_cols: vec!["id".into()],
            apply_defaults: false,
        });

        let sql = mutate_plan_to_query(&plan).sql().to_string();
        assert!(sql.contains("DO NOTHING"));
    }

    #[test]
    fn test_update_query() {
        let plan = MutatePlan::Update(UpdatePlan {
            into: test_qi(),
            columns: vec![typed_field("name", "text")],
            body: Payload::RawJSON(Bytes::from(r#"{"name":"updated"}"#)),
            where_: vec![CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
                field: field("id"),
                op_expr: OpExpr::Expr {
                    negated: false,
                    operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
                },
            })],
            returning: vec![select_field("id"), select_field("name")],
            apply_defaults: false,
        });

        let sql = mutate_plan_to_query(&plan).sql().to_string();
        assert!(sql.starts_with("UPDATE "));
        assert!(sql.contains("SET "));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("RETURNING"));
    }

    #[test]
    fn test_delete_query() {
        let plan = MutatePlan::Delete(DeletePlan {
            from: test_qi(),
            where_: vec![CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
                field: field("id"),
                op_expr: OpExpr::Expr {
                    negated: false,
                    operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
                },
            })],
            returning: vec![],
        });

        let sql = mutate_plan_to_query(&plan).sql().to_string();
        assert!(sql.starts_with("DELETE FROM "));
        assert!(sql.contains("WHERE"));
    }

    // ------------------------------------------------------------------
    // Call plan tests
    // ------------------------------------------------------------------

    #[test]
    fn test_call_plan_named_args() {
        let mut args = HashMap::new();
        args.insert(
            CompactString::from("a"),
            RpcParamValue::Fixed("1".into()),
        );
        args.insert(
            CompactString::from("b"),
            RpcParamValue::Fixed("2".into()),
        );

        let plan = CallPlan {
            qi: QualifiedIdentifier::new("public", "add_numbers"),
            params: CallParams::KeyParams(vec![
                crate::schema_cache::routine::RoutineParam {
                    name: "a".into(),
                    pg_type: "integer".into(),
                    type_max_length: "integer".into(),
                    required: true,
                    is_variadic: false,
                },
                crate::schema_cache::routine::RoutineParam {
                    name: "b".into(),
                    pg_type: "integer".into(),
                    type_max_length: "integer".into(),
                    required: true,
                    is_variadic: false,
                },
            ]),
            args: CallArgs::DirectArgs(args),
            scalar: true,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let sql = call_plan_to_query(&plan).sql().to_string();
        assert!(sql.starts_with("SELECT * FROM \"public\".\"add_numbers\"("));
        assert!(sql.contains(":="));
    }

    #[test]
    fn test_call_plan_json_body() {
        let plan = CallPlan {
            qi: QualifiedIdentifier::new("public", "process_data"),
            params: CallParams::KeyParams(vec![]),
            args: CallArgs::JsonArgs(Some(Bytes::from(r#"{"key":"value"}"#))),
            scalar: false,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let b = call_plan_to_query(&plan);
        assert!(b.sql().contains("$1"));
        assert_eq!(b.param_count(), 1);
    }

    #[test]
    fn test_call_plan_no_args() {
        let plan = CallPlan {
            qi: QualifiedIdentifier::new("public", "get_time"),
            params: CallParams::KeyParams(vec![]),
            args: CallArgs::JsonArgs(None),
            scalar: true,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let sql = call_plan_to_query(&plan).sql().to_string();
        assert_eq!(sql, "SELECT * FROM \"public\".\"get_time\"()");
    }

    #[test]
    fn test_call_plan_variadic() {
        let mut args = HashMap::new();
        args.insert(
            CompactString::from("vals"),
            RpcParamValue::Variadic(vec!["a".into(), "b".into(), "c".into()]),
        );

        let plan = CallPlan {
            qi: QualifiedIdentifier::new("public", "concat_vals"),
            params: CallParams::KeyParams(vec![
                crate::schema_cache::routine::RoutineParam {
                    name: "vals".into(),
                    pg_type: "text".into(),
                    type_max_length: "text".into(),
                    required: true,
                    is_variadic: true,
                },
            ]),
            args: CallArgs::DirectArgs(args),
            scalar: true,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        let sql = call_plan_to_query(&plan).sql().to_string();
        assert!(sql.contains("VARIADIC ARRAY["));
    }

    // ------------------------------------------------------------------
    // Payload helpers
    // ------------------------------------------------------------------

    #[test]
    fn test_payload_to_bytes_raw_json() {
        let payload = Payload::RawJSON(Bytes::from(r#"[{"id":1}]"#));
        let bytes = payload_to_bytes(&payload);
        assert_eq!(bytes, b"[{\"id\":1}]");
    }

    #[test]
    fn test_insert_default_values() {
        let plan = MutatePlan::Insert(InsertPlan {
            into: test_qi(),
            columns: vec![],
            body: Payload::RawJSON(Bytes::from("{}")),
            on_conflict: None,
            where_: vec![],
            returning: vec![],
            pk_cols: vec![],
            apply_defaults: true,
        });

        let sql = mutate_plan_to_query(&plan).sql().to_string();
        assert!(sql.contains("DEFAULT VALUES"));
    }
}
