//! Plan module for PgREST
//!
//! Transforms `ApiRequest` + `SchemaCache` into typed execution plans.
//! This is the core request planner that sits between API request parsing
//! and SQL generation.
//!
//! # Architecture
//!
//! ```text
//! ApiRequest ──┐
//!              ├──▶ action_plan() ──▶ ActionPlan
//! SchemaCache ─┘
//!                                      ├─ WrappedReadPlan  (GET / HEAD)
//!                                      ├─ MutateReadPlan   (POST / PATCH / PUT / DELETE)
//!                                      └─ CallReadPlan     (RPC)
//! ```

pub mod call_plan;
pub mod mutate_plan;
pub mod negotiate;
pub mod read_plan;
pub mod types;

// Re-export key types
pub use call_plan::{CallArgs, CallParams, CallPlan, RpcParamValue};
pub use mutate_plan::{DeletePlan, InsertPlan, MutatePlan, OnConflict, UpdatePlan};
pub use negotiate::negotiate_content;
pub use read_plan::{JoinCondition, ReadPlan, ReadPlanTree};
pub use types::*;

use compact_str::CompactString;

use crate::api_request::types::{
    Action, DbAction, InvokeMethod, Mutation, OrderTerm, SelectItem,
};
use crate::api_request::{ApiRequest, Preferences, QueryParams};
use crate::config::AppConfig;
use crate::error::Error;
use crate::schema_cache::media_handler::ResolvedHandler;
use crate::schema_cache::relationship::AnyRelationship;
use crate::schema_cache::routine::Routine;
use crate::schema_cache::table::Table;
use crate::schema_cache::SchemaCache;
use crate::types::identifiers::{QualifiedIdentifier, RelIdentifier};
use crate::types::media::MediaType;

use crate::api_request::preferences::{PreferRepresentation, PreferTransaction};
use crate::config::types::IsolationLevel;

// ==========================================================================
// ActionPlan -- top-level plan type
// ==========================================================================

/// Top-level action plan
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum ActionPlan {
    /// A database action plan
    Db(DbActionPlan),
    /// A non-database info plan
    NoDb(InfoPlan),
}

/// A database action plan
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum DbActionPlan {
    /// A CRUD operation (read, mutate, or call)
    DbCrud {
        /// Whether to include EXPLAIN output
        is_explain: bool,
        /// The CRUD plan
        plan: CrudPlan,
    },
    /// An inspect operation (schema introspection via GET /)
    MayUseDb(InspectPlan),
}

/// A CRUD plan
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum CrudPlan {
    /// Wrapped read plan (GET / HEAD)
    WrappedReadPlan {
        read_plan: ReadPlanTree,
        tx_mode: TxMode,
        handler: ResolvedHandler,
        media: MediaType,
        headers_only: bool,
        qi: QualifiedIdentifier,
    },
    /// Mutate + read plan (POST / PATCH / PUT / DELETE)
    MutateReadPlan {
        read_plan: ReadPlanTree,
        mutate_plan: MutatePlan,
        tx_mode: TxMode,
        handler: ResolvedHandler,
        media: MediaType,
        mutation: Mutation,
        qi: QualifiedIdentifier,
    },
    /// Call + read plan (RPC)
    CallReadPlan {
        read_plan: ReadPlanTree,
        call_plan: CallPlan,
        tx_mode: TxMode,
        proc: Routine,
        handler: ResolvedHandler,
        media: MediaType,
        inv_method: InvokeMethod,
        qi: QualifiedIdentifier,
    },
}

/// Inspect plan (for GET / on the root)
#[derive(Debug)]
pub struct InspectPlan {
    pub media: MediaType,
    pub tx_mode: TxMode,
    pub headers_only: bool,
    pub schema: CompactString,
}

/// Non-database info plan
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum InfoPlan {
    /// OPTIONS on a relation
    RelInfoPlan(QualifiedIdentifier),
    /// OPTIONS on a routine
    RoutineInfoPlan(Routine),
    /// OPTIONS on root schema
    SchemaInfoPlan,
}

/// Transaction mode settings
#[derive(Debug, Clone)]
pub struct TxMode {
    /// Isolation level for the transaction
    pub isolation_level: IsolationLevel,
    /// Whether to rollback the transaction
    pub rollback: bool,
}

impl TxMode {
    /// Default transaction mode
    pub fn default_mode() -> Self {
        Self {
            isolation_level: IsolationLevel::ReadCommitted,
            rollback: false,
        }
    }

    /// Read-only transaction mode
    pub fn read_only() -> Self {
        Self {
            isolation_level: IsolationLevel::ReadCommitted,
            rollback: false,
        }
    }
}

// ==========================================================================
// action_plan -- main entry point
// ==========================================================================

/// Build an action plan from an API request
///
/// This is the main entry point for planning. It resolves the action type,
/// finds the relevant table/routine, builds the appropriate plan, and
/// negotiates content types.
pub fn action_plan(
    config: &AppConfig,
    api_request: &ApiRequest,
    schema_cache: &SchemaCache,
) -> Result<ActionPlan, Error> {
    let action = &api_request.action;

    match action {
        // Info actions (OPTIONS) -> no DB needed
        Action::RelationInfo(qi) => {
            Ok(ActionPlan::NoDb(InfoPlan::RelInfoPlan(qi.clone())))
        }
        Action::RoutineInfo(qi, _) => {
            let proc = find_proc(schema_cache, qi)?;
            Ok(ActionPlan::NoDb(InfoPlan::RoutineInfoPlan(proc.clone())))
        }
        Action::SchemaInfo => {
            Ok(ActionPlan::NoDb(InfoPlan::SchemaInfoPlan))
        }

        // DB actions
        Action::Db(db_action) => match db_action {
            // Schema read (GET /)
            DbAction::SchemaRead {
                schema,
                headers_only,
            } => {
                let tx_mode = resolve_tx_mode(config, &api_request.preferences, true);
                let media = api_request
                    .accept_media_types
                    .first()
                    .cloned()
                    .unwrap_or(MediaType::ApplicationOpenApi);

                Ok(ActionPlan::Db(DbActionPlan::MayUseDb(InspectPlan {
                    media,
                    tx_mode,
                    headers_only: *headers_only,
                    schema: schema.clone(),
                })))
            }

            // Relation read (GET / HEAD on a table)
            DbAction::RelationRead { qi, headers_only } => {
                let table = find_table(schema_cache, qi)?;
                let rel_id = RelIdentifier::Table(qi.clone());

                let handler = negotiate_content(
                    &api_request.accept_media_types,
                    &schema_cache.media_handlers,
                    &rel_id,
                    action,
                    config.db_plan_enabled,
                )?;

                let read_tree = build_read_plan(
                    config,
                    schema_cache,
                    api_request,
                    qi,
                    table,
                )?;

                let tx_mode = resolve_tx_mode(config, &api_request.preferences, true);

                Ok(ActionPlan::Db(DbActionPlan::DbCrud {
                    is_explain: false,
                    plan: CrudPlan::WrappedReadPlan {
                        read_plan: read_tree,
                        tx_mode,
                        handler: handler.clone(),
                        media: handler.1,
                        headers_only: *headers_only,
                        qi: qi.clone(),
                    },
                }))
            }

            // Relation mutation (POST / PATCH / PUT / DELETE)
            DbAction::RelationMut { qi, mutation } => {
                let table = find_table(schema_cache, qi)?;
                let rel_id = RelIdentifier::Table(qi.clone());

                let handler = negotiate_content(
                    &api_request.accept_media_types,
                    &schema_cache.media_handlers,
                    &rel_id,
                    action,
                    config.db_plan_enabled,
                )?;

                let read_tree = build_read_plan(
                    config,
                    schema_cache,
                    api_request,
                    qi,
                    table,
                )?;

                let mutate = build_mutate_plan(
                    qi,
                    table,
                    *mutation,
                    api_request,
                )?;

                let tx_mode = resolve_tx_mode(config, &api_request.preferences, false);

                Ok(ActionPlan::Db(DbActionPlan::DbCrud {
                    is_explain: false,
                    plan: CrudPlan::MutateReadPlan {
                        read_plan: read_tree,
                        mutate_plan: mutate,
                        tx_mode,
                        handler: handler.clone(),
                        media: handler.1,
                        mutation: *mutation,
                        qi: qi.clone(),
                    },
                }))
            }

            // Routine invocation (RPC)
            DbAction::Routine { qi, inv_method } => {
                let proc = find_proc(schema_cache, qi)?;
                let rel_id = proc
                    .table_qi()
                    .map(|tqi| RelIdentifier::Table(tqi.clone()))
                    .unwrap_or(RelIdentifier::AnyElement);

                let handler = negotiate_content(
                    &api_request.accept_media_types,
                    &schema_cache.media_handlers,
                    &rel_id,
                    action,
                    config.db_plan_enabled,
                )?;

                let read_tree = build_call_read_plan(
                    config,
                    schema_cache,
                    api_request,
                    qi,
                    proc,
                )?;

                let call = build_call_plan(proc, api_request)?;

                let is_read = matches!(inv_method, InvokeMethod::InvRead(_));
                let tx_mode = resolve_tx_mode(config, &api_request.preferences, is_read);

                Ok(ActionPlan::Db(DbActionPlan::DbCrud {
                    is_explain: false,
                    plan: CrudPlan::CallReadPlan {
                        read_plan: read_tree,
                        call_plan: call,
                        tx_mode,
                        proc: proc.clone(),
                        handler: handler.clone(),
                        media: handler.1,
                        inv_method: inv_method.clone(),
                        qi: qi.clone(),
                    },
                }))
            }
        },
    }
}

// ==========================================================================
// Schema cache lookups
// ==========================================================================

/// Find a table in the schema cache
pub fn find_table<'a>(
    schema_cache: &'a SchemaCache,
    qi: &QualifiedIdentifier,
) -> Result<&'a Table, Error> {
    schema_cache.get_table(qi).ok_or_else(|| Error::TableNotFound {
        name: qi.to_string(),
        suggestion: None,
    })
}

/// Find a routine (function) in the schema cache
pub fn find_proc<'a>(
    schema_cache: &'a SchemaCache,
    qi: &QualifiedIdentifier,
) -> Result<&'a Routine, Error> {
    schema_cache
        .get_routines(qi)
        .and_then(|routines| routines.first())
        .ok_or_else(|| Error::FunctionNotFound {
            name: qi.to_string(),
        })
}

/// Find relationships between two tables
pub fn find_rels<'a>(
    schema_cache: &'a SchemaCache,
    source: &QualifiedIdentifier,
    target_name: &str,
) -> Vec<&'a AnyRelationship> {
    schema_cache.find_relationships_to(source, target_name)
}

// ==========================================================================
// Read plan builder
// ==========================================================================

/// Build a ReadPlanTree from an API request for a relation
fn build_read_plan(
    config: &AppConfig,
    schema_cache: &SchemaCache,
    api_request: &ApiRequest,
    qi: &QualifiedIdentifier,
    table: &Table,
) -> Result<ReadPlanTree, Error> {
    let qp = &api_request.query_params;

    // Initialize root read plan
    let mut root = ReadPlan::root(qi.clone());

    // Add select fields
    root.select = resolve_select(&qp.select, Some(table))?;

    // Add filters (resolve column types for proper casting)
    root.where_ = resolve_filters(&qp.filters_root, Some(table))?;

    // Add orders
    root.order = resolve_orders(&qp.order, Some(table));

    // Add range
    root.range = api_request.top_level_range;

    // Build children from embedded relations in select
    let children = build_children(config, schema_cache, qi, &qp.select, qp, 1)?;

    // Restrict range based on config max rows
    let mut tree = ReadPlanTree::with_children(root, children);
    if let Some(max_rows) = config.db_max_rows {
        restrict_range(&mut tree, max_rows);
    }

    Ok(tree)
}

/// Build a ReadPlanTree for an RPC call
fn build_call_read_plan(
    config: &AppConfig,
    schema_cache: &SchemaCache,
    api_request: &ApiRequest,
    qi: &QualifiedIdentifier,
    proc: &Routine,
) -> Result<ReadPlanTree, Error> {
    let qp = &api_request.query_params;

    // For RPC, the "table" is the function's return type
    let from_qi = proc.table_qi().cloned().unwrap_or_else(|| qi.clone());

    let mut root = ReadPlan::root(from_qi.clone());
    // RPC return type — look up table in cache for type resolution
    let rpc_table = schema_cache.get_table(&from_qi);
    root.select = resolve_select(&qp.select, rpc_table)?;
    root.where_ = resolve_filters(&qp.filters_root, rpc_table)?;
    root.order = resolve_orders(&qp.order, rpc_table);
    root.range = api_request.top_level_range;

    let children = build_children(config, schema_cache, &from_qi, &qp.select, qp, 1)?;

    let mut tree = ReadPlanTree::with_children(root, children);
    if let Some(max_rows) = config.db_max_rows {
        restrict_range(&mut tree, max_rows);
    }

    Ok(tree)
}

/// Build child read plans from embedded relations in the select.
///
/// Each child receives a unique `rel_agg_alias` based on depth and
/// sibling index to avoid alias collisions in the generated SQL.
fn build_children(
    config: &AppConfig,
    schema_cache: &SchemaCache,
    parent_qi: &QualifiedIdentifier,
    select_items: &[SelectItem],
    qp: &QueryParams,
    depth: usize,
) -> Result<Vec<ReadPlanTree>, Error> {
    let mut children = Vec::new();
    let mut sibling_idx: usize = 0;

    for item in select_items {
        match item {
            SelectItem::Relation {
                relation,
                alias,
                hint,
                join_type,
                children: sub_select,
            } => {
                let mut child_tree = build_child_plan(
                    config,
                    schema_cache,
                    parent_qi,
                    relation,
                    alias.as_ref(),
                    hint.as_ref(),
                    *join_type,
                    sub_select,
                    qp,
                    depth,
                    false, // not spread
                )?;
                // Unique alias: depth + sibling index
                child_tree.node.rel_agg_alias =
                    CompactString::from(format!("pgrst_agg_{}_{}", depth, sibling_idx));
                sibling_idx += 1;
                children.push(child_tree);
            }
            SelectItem::Spread {
                relation,
                hint,
                join_type,
                children: sub_select,
            } => {
                let mut child_tree = build_child_plan(
                    config,
                    schema_cache,
                    parent_qi,
                    relation,
                    None,
                    hint.as_ref(),
                    *join_type,
                    sub_select,
                    qp,
                    depth,
                    true, // spread
                )?;
                child_tree.node.rel_agg_alias =
                    CompactString::from(format!("pgrst_agg_{}_{}", depth, sibling_idx));
                sibling_idx += 1;
                children.push(child_tree);
            }
            SelectItem::Field { .. } => {
                // Fields are handled at the select resolution level, not as children
            }
        }
    }

    Ok(children)
}

/// Build a single child read plan for an embedded relation
#[allow(clippy::too_many_arguments)]
fn build_child_plan(
    config: &AppConfig,
    schema_cache: &SchemaCache,
    parent_qi: &QualifiedIdentifier,
    relation_name: &str,
    alias: Option<&CompactString>,
    hint: Option<&CompactString>,
    join_type: Option<crate::api_request::types::JoinType>,
    sub_select: &[SelectItem],
    qp: &QueryParams,
    depth: usize,
    is_spread: bool,
) -> Result<ReadPlanTree, Error> {
    // Find the relationship
    let rels = find_rels(schema_cache, parent_qi, relation_name);

    let rel = if rels.is_empty() {
        // Try as a table in the same schema
        let child_qi = QualifiedIdentifier::new(parent_qi.schema.clone(), relation_name);
        if schema_cache.get_table(&child_qi).is_some() {
            // No FK relationship, but table exists — use it as a subquery
            None
        } else {
            return Err(Error::RelationshipNotFound {
                from_table: parent_qi.to_string(),
                to_table: relation_name.to_string(),
            });
        }
    } else if rels.len() == 1 {
        Some(rels[0])
    } else {
        // Multiple relationships found — try to disambiguate with hint
        if let Some(hint) = hint {
            rels.iter()
                .find(|r| {
                    if let Some(fk) = r.as_fk() {
                        fk.constraint_name() == hint.as_str()
                    } else {
                        false
                    }
                })
                .copied()
        } else {
            return Err(Error::AmbiguousEmbedding(relation_name.to_string()));
        }
    };

    // Build the child QI
    let child_qi = if let Some(rel) = rel {
        rel.foreign_table().clone()
    } else {
        QualifiedIdentifier::new(parent_qi.schema.clone(), relation_name)
    };

    let mut child_plan = ReadPlan::child(child_qi.clone(), relation_name.into(), depth);

    // Set relationship metadata
    if let Some(rel) = rel {
        child_plan.rel_to_parent = Some(rel.clone());

        // Build join conditions
        if let Some(fk) = rel.as_fk() {
            for (src, tgt) in fk.columns() {
                child_plan.rel_join_conds.push(JoinCondition {
                    parent: (parent_qi.clone(), src.clone()),
                    child: (child_qi.clone(), tgt.clone()),
                });
            }
        }
    }

    child_plan.rel_alias = alias.cloned();
    child_plan.rel_hint = hint.cloned();
    child_plan.rel_join_type = join_type;

    if is_spread {
        child_plan.rel_spread = Some(SpreadType::ToOneSpread);
    }

    // Get filters for this embed path
    let embed_path = vec![CompactString::from(relation_name)];
    let child_filters: Vec<_> = qp
        .filters_not_root
        .iter()
        .filter(|(path, _)| *path == embed_path)
        .map(|(_, f)| f.clone())
        .collect();
    let child_table = schema_cache.get_table(&child_qi);
    
    // Resolve select, filters, and orders for the child
    child_plan.select = resolve_select(sub_select, child_table)?;
    child_plan.where_ = resolve_filters(&child_filters, child_table)?;

    // Get orders for this embed path
    let child_orders: Vec<_> = qp
        .order
        .iter()
        .filter(|(path, _)| *path == embed_path)
        .flat_map(|(_, orders)| orders.clone())
        .collect();
    child_plan.order = resolve_order_terms(&child_orders, child_table);

    // Recursively build grandchildren
    let grandchildren = build_children(config, schema_cache, &child_qi, sub_select, qp, depth + 1)?;

    Ok(ReadPlanTree::with_children(child_plan, grandchildren))
}

// ==========================================================================
// Mutate plan builder
// ==========================================================================

/// Build a MutatePlan from an API request
fn build_mutate_plan(
    qi: &QualifiedIdentifier,
    table: &Table,
    mutation: Mutation,
    api_request: &ApiRequest,
) -> Result<MutatePlan, Error> {
    let qp = &api_request.query_params;

    match mutation {
        Mutation::MutationCreate | Mutation::MutationSingleUpsert => {
            let payload = api_request
                .payload
                .clone()
                .ok_or(Error::MissingPayload)?;

            let columns = resolve_mutation_columns(table, &api_request.columns);

            let on_conflict = if mutation == Mutation::MutationSingleUpsert {
                Some(OnConflict {
                    columns: table.pk_cols.iter().cloned().collect(),
                    merge_duplicates: true,
                })
            } else {
                qp.on_conflict.as_ref().map(|cols| OnConflict {
                    columns: cols.clone(),
                    merge_duplicates: api_request
                        .preferences
                        .resolution
                        .map(|r| {
                            matches!(
                                r,
                                crate::api_request::preferences::PreferResolution::MergeDuplicates
                            )
                        })
                        .unwrap_or(false),
                })
            };

            let apply_defaults = api_request
                .preferences
                .missing
                .map(|m| matches!(m, crate::api_request::preferences::PreferMissing::ApplyDefaults))
                .unwrap_or(false);

            Ok(MutatePlan::Insert(InsertPlan {
                into: qi.clone(),
                columns,
                body: payload,
                on_conflict,
                where_: resolve_filters(&qp.filters_root, Some(table))?,
                returning: resolve_returning(table, &api_request.preferences),
                pk_cols: table.pk_cols.iter().cloned().collect(),
                apply_defaults,
            }))
        }
        Mutation::MutationUpdate => {
            let payload = api_request
                .payload
                .clone()
                .ok_or(Error::MissingPayload)?;

            let columns = resolve_mutation_columns(table, &api_request.columns);

            let apply_defaults = api_request
                .preferences
                .missing
                .map(|m| matches!(m, crate::api_request::preferences::PreferMissing::ApplyDefaults))
                .unwrap_or(false);

            Ok(MutatePlan::Update(UpdatePlan {
                into: qi.clone(),
                columns,
                body: payload,
                where_: resolve_filters(&qp.filters_root, Some(table))?,
                returning: resolve_returning(table, &api_request.preferences),
                apply_defaults,
            }))
        }
        Mutation::MutationDelete => {
            Ok(MutatePlan::Delete(DeletePlan {
                from: qi.clone(),
                where_: resolve_filters(&qp.filters_root, Some(table))?,
                returning: resolve_returning(table, &api_request.preferences),
            }))
        }
    }
}

// ==========================================================================
// Call plan builder
// ==========================================================================

/// Build a CallPlan from an API request
fn build_call_plan(
    proc: &Routine,
    api_request: &ApiRequest,
) -> Result<CallPlan, Error> {
    let qp = &api_request.query_params;

    // Determine call params
    let params = if proc.param_count() == 1 && proc.params[0].is_json_type() && !qp.params.is_empty() {
        // Single JSON param — can use positional
        CallParams::OnePosParam(proc.params[0].clone())
    } else {
        CallParams::KeyParams(proc.params.to_vec())
    };

    // Build call args
    let args = if !qp.params.is_empty() {
        // From query parameters
        let rpc_params = call_plan::to_rpc_params(proc, &qp.params);
        CallArgs::DirectArgs(rpc_params)
    } else if let Some(ref payload) = api_request.payload {
        // From body
        match payload {
            crate::api_request::types::Payload::ProcessedJSON { raw, .. }
            | crate::api_request::types::Payload::RawJSON(raw) => {
                CallArgs::JsonArgs(Some(raw.clone()))
            }
            crate::api_request::types::Payload::RawPayload(raw) => {
                CallArgs::JsonArgs(Some(raw.clone()))
            }
            crate::api_request::types::Payload::ProcessedUrlEncoded { params, .. } => {
                let rpc_params: std::collections::HashMap<CompactString, RpcParamValue> = params
                    .iter()
                    .map(|(k, v)| (k.clone(), RpcParamValue::Fixed(v.clone())))
                    .collect();
                CallArgs::DirectArgs(rpc_params)
            }
        }
    } else {
        CallArgs::JsonArgs(None)
    };

    Ok(CallPlan {
        qi: proc.qi(),
        params,
        args,
        scalar: proc.returns_scalar(),
        set_of_scalar: proc.returns_set_of_scalar(),
        filter_fields: qp.filter_fields.iter().cloned().collect(),
        returning: vec![],
    })
}

// ==========================================================================
// Cast validation
// ==========================================================================

/// Validate a cast type name
///
/// Checks that the cast type has valid syntax (alphanumeric, underscores, spaces).
/// The actual type existence will be validated by PostgreSQL.
fn validate_cast_type(cast_type: &str) -> Result<(), Error> {
    let cast_type = cast_type.trim();
    
    // Empty cast type is invalid
    if cast_type.is_empty() {
        return Err(Error::InvalidQueryParam {
            param: "select".to_string(),
            message: "empty cast type".to_string(),
        });
    }
    
    // Check for valid characters: alphanumeric, underscore, space, parentheses, brackets
    // PostgreSQL allows types like "character varying", "int4", "text[]", "numeric(10,2)"
    let is_valid = cast_type.chars().all(|c| {
        c.is_alphanumeric()
            || c == '_'
            || c == ' '
            || c == '('
            || c == ')'
            || c == '['
            || c == ']'
            || c == ','
    });
    
    if !is_valid {
        return Err(Error::InvalidQueryParam {
            param: "select".to_string(),
            message: format!("invalid cast type: {}", cast_type),
        });
    }
    
    Ok(())
}

// ==========================================================================
// Resolution helpers
// ==========================================================================

/// Resolve select items into coercible select fields
///
/// When a `table` is provided, fields are resolved against the table's columns
/// and computed fields so that `base_type` is set for proper type casting.
fn resolve_select(items: &[SelectItem], table: Option<&Table>) -> Result<Vec<CoercibleSelectField>, Error> {
    let mut result = Vec::new();
    
    for item in items {
        match item {
            SelectItem::Field {
                field,
                alias,
                cast,
                aggregate,
                aggregate_cast,
            } => {
                let resolved_field = if let Some(t) = table {
                    // Check regular columns first
                    if let Some(col) = t.get_column(&field.0) {
                        CoercibleField::from_column(
                            field.0.clone(),
                            field.1.clone(),
                            col.data_type.clone(),
                        )
                        .with_to_json(Some(col))
                    } else if let Some(computed) = t.get_computed_field(&field.0) {
                        // Check computed fields
                        CoercibleField::from_computed_field(
                            field.0.clone(),
                            field.1.clone(),
                            computed.function.clone(),
                            computed.return_type.clone(),
                        )
                        // Computed fields don't need to_jsonb wrapper
                    } else {
                        // Column/computed field not found - return error instead of creating unknown field
                        // Exception: allow "*" wildcard for select-all
                        if field.0.as_str() == "*" {
                            CoercibleField::unknown(field.0.clone(), field.1.clone())
                                .with_to_json(None)
                        } else {
                            return Err(Error::ColumnNotFound {
                                table: t.qi().to_string(),
                                column: field.0.to_string(),
                            });
                        }
                    }
                } else {
                    CoercibleField::unknown(field.0.clone(), field.1.clone())
                        .with_to_json(None)
                };
                
                // Validate cast types if present
                if let Some(cast_type) = cast {
                    validate_cast_type(cast_type)?;
                }
                if let Some(agg_cast_type) = aggregate_cast {
                    validate_cast_type(agg_cast_type)?;
                }
                
                result.push(CoercibleSelectField {
                    field: resolved_field,
                    agg_function: *aggregate,
                    agg_cast: aggregate_cast.clone(),
                    cast: cast.clone(),
                    alias: alias.clone(),
                });
            }
            _ => {
                // Relations/Spreads are handled as children - skip here
            }
        }
    }
    
    Ok(result)
}

/// Resolve filters into coercible logic trees.
///
/// When a `table` is provided, each filter field is resolved against the
/// table's columns so that `base_type` is set. This allows the query builder
/// to emit explicit `::type` casts on bind-parameter placeholders.
/// Also checks for computed fields if the column is not found.
fn resolve_filters(
    filters: &[crate::api_request::types::Filter],
    table: Option<&Table>,
) -> Result<Vec<CoercibleLogicTree>, Error> {
    filters
        .iter()
        .map(|f| {
            let field = if let Some(t) = table {
                // Check regular columns first
                if let Some(col) = t.get_column(&f.field.0) {
                    let mut field = CoercibleField::from_column(
                        f.field.0.clone(),
                        f.field.1.clone(),
                        col.data_type.clone(),
                    );
                    // Trace: check if column is composite/array and JSON path is present
                    if !f.field.1.is_empty() {
                        tracing::trace!(
                            "Filter field '{}' has JSON path: {:?}, is_composite: {}, is_array: {}",
                            f.field.0, f.field.1, col.is_composite_type(), col.is_array_type()
                        );
                    }
                    field = field.with_to_json(Some(col));
                    field
                } else if let Some(computed) = t.get_computed_field(&f.field.0) {
                    // Check computed fields
                    CoercibleField::from_computed_field(
                        f.field.0.clone(),
                        f.field.1.clone(),
                        computed.function.clone(),
                        computed.return_type.clone(),
                    )
                    // Computed fields don't need to_jsonb wrapper
                } else {
                    // Column not found - return error instead of creating unknown field
                    return Err(Error::ColumnNotFound {
                        table: t.qi().to_string(),
                        column: f.field.0.to_string(),
                    });
                }
            } else {
                // No table provided - allow unknown fields (for unit tests, etc.)
                CoercibleField::unknown(f.field.0.clone(), f.field.1.clone())
                    .with_to_json(None)
            };
            Ok(CoercibleLogicTree::Stmnt(CoercibleFilter::Filter {
                field,
                op_expr: f.op_expr.clone(),
            }))
        })
        .collect()
}

/// Resolve order parameters into coercible order terms
///
/// When a `table` is provided, order fields are resolved against the table's columns
/// and computed fields so that `base_type` is set for proper type casting.
fn resolve_orders(
    orders: &[(Vec<CompactString>, Vec<OrderTerm>)],
    table: Option<&Table>,
) -> Vec<CoercibleOrderTerm> {
    // Only include root-level orders (empty embed path)
    orders
        .iter()
        .filter(|(path, _)| path.is_empty())
        .flat_map(|(_, terms)| resolve_order_terms(terms, table))
        .collect()
}

/// Resolve order terms into coercible order terms
fn resolve_order_terms(terms: &[OrderTerm], table: Option<&Table>) -> Vec<CoercibleOrderTerm> {
    terms
        .iter()
        .map(|t| match t {
            OrderTerm::Term {
                field,
                direction,
                nulls,
            } => {
                let resolved_field = if let Some(t) = table {
                    // Check regular columns first
                    if let Some(col) = t.get_column(&field.0) {
                        CoercibleField::from_column(
                            field.0.clone(),
                            field.1.clone(),
                            col.data_type.clone(),
                        )
                        .with_to_json(Some(col))
                    } else if let Some(computed) = t.get_computed_field(&field.0) {
                        // Check computed fields
                        CoercibleField::from_computed_field(
                            field.0.clone(),
                            field.1.clone(),
                            computed.function.clone(),
                            computed.return_type.clone(),
                        )
                        // Computed fields don't need to_jsonb wrapper
                    } else {
                        CoercibleField::unknown(field.0.clone(), field.1.clone())
                            .with_to_json(None)
                    }
                } else {
                    CoercibleField::unknown(field.0.clone(), field.1.clone())
                        .with_to_json(None)
                };
                CoercibleOrderTerm::Term {
                    field: resolved_field,
                    direction: *direction,
                    nulls: *nulls,
                }
            }
            OrderTerm::RelationTerm {
                relation,
                field,
                direction,
                nulls,
            } => CoercibleOrderTerm::RelationTerm {
                relation: relation.clone(),
                rel_term: CoercibleField::unknown(field.0.clone(), field.1.clone())
                    .with_to_json(None),
                direction: *direction,
                nulls: *nulls,
            },
        })
        .collect()
}

/// Resolve mutation columns from the table and payload columns
fn resolve_mutation_columns(
    table: &Table,
    payload_cols: &std::collections::HashSet<CompactString>,
) -> Vec<CoercibleField> {
    if payload_cols.is_empty() {
        // No &columns specified — use all table columns
        table
            .columns_list()
            .map(|col| {
                CoercibleField::from_column(
                    col.name.clone(),
                    Default::default(),
                    col.data_type.clone(),
                )
            })
            .collect()
    } else {
        // Use only the specified columns
        payload_cols
            .iter()
            .filter_map(|col_name| {
                table.get_column(col_name).map(|col| {
                    CoercibleField::from_column(
                        col.name.clone(),
                        Default::default(),
                        col.data_type.clone(),
                    )
                })
            })
            .collect()
    }
}

/// Resolve RETURNING columns based on Prefer header
fn resolve_returning(
    table: &Table,
    prefs: &Preferences,
) -> Vec<CoercibleSelectField> {
    match prefs.representation {
        Some(PreferRepresentation::Full) | Some(PreferRepresentation::HeadersOnly) => {
            // Return all columns
            table
                .columns_list()
                .map(|col| CoercibleSelectField {
                    field: CoercibleField::from_column(
                        col.name.clone(),
                        Default::default(),
                        col.data_type.clone(),
                    ),
                    agg_function: None,
                    agg_cast: None,
                    cast: None,
                    alias: None,
                })
                .collect()
        }
        _ => vec![],
    }
}

/// Restrict range based on max rows config
fn restrict_range(tree: &mut ReadPlanTree, max_rows: i64) {
    let plan = &mut tree.node;
    if plan.range.is_all() && plan.depth == 0 {
        plan.range.limit_to = Some(max_rows);
    }
}

/// Resolve transaction mode from config and preferences
fn resolve_tx_mode(
    config: &AppConfig,
    prefs: &Preferences,
    is_read: bool,
) -> TxMode {
    let rollback = if config.db_tx_rollback_all {
        true
    } else if config.db_tx_allow_override {
        matches!(prefs.transaction, Some(PreferTransaction::Rollback))
    } else {
        false
    };

    // Select isolation level based on operation type
    // Read operations use db_tx_read_isolation, write operations use db_tx_write_isolation
    let isolation_level = if is_read {
        config.db_tx_read_isolation
    } else {
        config.db_tx_write_isolation
    };

    TxMode {
        isolation_level,
        rollback,
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn test_config() -> AppConfig {
        AppConfig {
            db_schemas: vec!["public".to_string()],
            ..Default::default()
        }
    }

    fn test_schema_cache() -> SchemaCache {
        let mut tables = HashMap::new();

        let users_table = test_table()
            .schema("public")
            .name("users")
            .pk_col("id")
            .column(test_column().name("id").data_type("integer").nullable(false).build())
            .column(test_column().name("name").data_type("text").build())
            .column(test_column().name("email").data_type("text").build())
            .build();

        let posts_table = test_table()
            .schema("public")
            .name("posts")
            .pk_col("id")
            .column(test_column().name("id").data_type("integer").nullable(false).build())
            .column(test_column().name("user_id").data_type("integer").build())
            .column(test_column().name("title").data_type("text").build())
            .build();

        tables.insert(users_table.qi(), users_table);
        tables.insert(posts_table.qi(), posts_table);

        let rel = test_relationship()
            .table("public", "posts")
            .foreign_table("public", "users")
            .m2o("fk_posts_user", &[("user_id", "id")])
            .build();

        let mut relationships = HashMap::new();
        let key = (
            QualifiedIdentifier::new("public", "posts"),
            "public".to_string(),
        );
        relationships.insert(key, vec![AnyRelationship::ForeignKey(rel)]);

        // Also add reverse relationship (users -> posts as O2M)
        let rev_rel = test_relationship()
            .table("public", "users")
            .foreign_table("public", "posts")
            .o2m("fk_posts_user", &[("id", "user_id")])
            .build();
        let rev_key = (
            QualifiedIdentifier::new("public", "users"),
            "public".to_string(),
        );
        relationships.insert(rev_key, vec![AnyRelationship::ForeignKey(rev_rel)]);

        let routine = test_routine()
            .schema("public")
            .name("get_user")
            .param(test_param().name("user_id").pg_type("integer").build())
            .returns_setof_composite("public", "users")
            .build();

        let mut routines = HashMap::new();
        routines.insert(routine.qi(), vec![routine]);

        SchemaCache {
            tables: Arc::new(tables),
            relationships: Arc::new(relationships),
            routines: Arc::new(routines),
            timezones: Arc::new(std::collections::HashSet::new()),
            representations: Arc::new(HashMap::new()),
            media_handlers: Arc::new(HashMap::new()),
        }
    }

    #[test]
    fn test_find_table_exists() {
        let cache = test_schema_cache();
        let qi = QualifiedIdentifier::new("public", "users");
        let table = find_table(&cache, &qi);
        assert!(table.is_ok());
        assert_eq!(table.unwrap().name.as_str(), "users");
    }

    #[test]
    fn test_find_table_not_found() {
        let cache = test_schema_cache();
        let qi = QualifiedIdentifier::new("public", "nonexistent");
        let result = find_table(&cache, &qi);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::TableNotFound { .. }));
    }

    #[test]
    fn test_find_proc_exists() {
        let cache = test_schema_cache();
        let qi = QualifiedIdentifier::new("public", "get_user");
        let proc = find_proc(&cache, &qi);
        assert!(proc.is_ok());
    }

    #[test]
    fn test_find_proc_not_found() {
        let cache = test_schema_cache();
        let qi = QualifiedIdentifier::new("public", "nonexistent_func");
        let result = find_proc(&cache, &qi);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::FunctionNotFound { .. }));
    }

    #[test]
    fn test_find_rels() {
        let cache = test_schema_cache();
        let source = QualifiedIdentifier::new("public", "users");
        let rels = find_rels(&cache, &source, "posts");
        assert_eq!(rels.len(), 1);
    }

    #[test]
    fn test_resolve_select_fields() {
        use smallvec::SmallVec;
        let items = vec![
            SelectItem::Field {
                field: ("id".into(), SmallVec::new()),
                alias: None,
                cast: None,
                aggregate: None,
                aggregate_cast: None,
            },
            SelectItem::Field {
                field: ("name".into(), SmallVec::new()),
                alias: Some("user_name".into()),
                cast: Some("text".into()),
                aggregate: None,
                aggregate_cast: None,
            },
        ];

        let resolved = resolve_select(&items, None).unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].field.name.as_str(), "id");
        assert!(resolved[0].alias.is_none());
        assert_eq!(resolved[1].field.name.as_str(), "name");
        assert_eq!(resolved[1].alias.as_deref(), Some("user_name"));
        assert_eq!(resolved[1].cast.as_deref(), Some("text"));
    }

    #[test]
    fn test_resolve_filters() {
        use smallvec::SmallVec;
        use crate::api_request::types::{Filter, OpExpr, Operation, QuantOperator};

        let filters = vec![Filter {
            field: ("id".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "5".into()),
            },
        }];

        let resolved = resolve_filters(&filters, None).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(matches!(resolved[0], CoercibleLogicTree::Stmnt(_)));
    }

    #[test]
    fn test_resolve_filters_with_computed_field() {
        use smallvec::SmallVec;
        use crate::api_request::types::{Filter, OpExpr, Operation, QuantOperator};
        use crate::schema_cache::table::ComputedField;
        use crate::types::QualifiedIdentifier;

        // Create a table with a computed field
        let mut table = test_table()
            .schema("test_api")
            .name("users")
            .column(test_column().name("id").data_type("integer").build())
            .build();

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi,
            return_type: "text".into(),
            returns_set: false,
        };
        table.computed_fields.insert("full_name".into(), computed);

        let filters = vec![Filter {
            field: ("full_name".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "John Doe".into()),
            },
        }];

        let resolved = resolve_filters(&filters, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 1);
        
        if let CoercibleLogicTree::Stmnt(CoercibleFilter::Filter { field, .. }) = &resolved[0] {
            assert!(field.is_computed);
            assert_eq!(field.name.as_str(), "full_name");
            assert_eq!(field.base_type.as_deref(), Some("text"));
        } else {
            panic!("Expected Filter variant");
        }
    }

    #[test]
    fn test_resolve_select_with_computed_field() {
        use crate::api_request::types::SelectItem;
        use crate::schema_cache::table::ComputedField;
        use crate::types::QualifiedIdentifier;

        // Create a table with a computed field
        let mut table = test_table()
            .schema("test_api")
            .name("users")
            .column(test_column().name("id").data_type("integer").build())
            .column(test_column().name("name").data_type("text").build())
            .build();

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi,
            return_type: "text".into(),
            returns_set: false,
        };
        table.computed_fields.insert("full_name".into(), computed);

        let items = vec![
            SelectItem::Field {
                field: ("id".into(), Default::default()),
                alias: None,
                cast: None,
                aggregate: None,
                aggregate_cast: None,
            },
            SelectItem::Field {
                field: ("full_name".into(), Default::default()),
                alias: None,
                cast: None,
                aggregate: None,
                aggregate_cast: None,
            },
        ];

        let resolved = resolve_select(&items, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 2);
        
        // First field (id) should be a regular column
        assert!(!resolved[0].field.is_computed);
        assert_eq!(resolved[0].field.name.as_str(), "id");
        
        // Second field (full_name) should be a computed field
        assert!(resolved[1].field.is_computed);
        assert_eq!(resolved[1].field.name.as_str(), "full_name");
        assert_eq!(resolved[1].field.base_type.as_deref(), Some("text"));
    }

    #[test]
    fn test_resolve_select_computed_field_with_cast() {
        use crate::api_request::types::SelectItem;
        use crate::schema_cache::table::ComputedField;
        use crate::types::QualifiedIdentifier;

        let mut table = test_table()
            .schema("test_api")
            .name("users")
            .column(test_column().name("id").data_type("integer").build())
            .build();

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi,
            return_type: "text".into(),
            returns_set: false,
        };
        table.computed_fields.insert("full_name".into(), computed);

        let items = vec![SelectItem::Field {
            field: ("full_name".into(), Default::default()),
            alias: None,
            cast: Some("varchar".into()),
            aggregate: None,
            aggregate_cast: None,
        }];

        let resolved = resolve_select(&items, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].field.is_computed);
        assert_eq!(resolved[0].cast.as_deref(), Some("varchar"));
    }

    #[test]
    fn test_resolve_order_with_computed_field() {
        use crate::api_request::types::OrderTerm;
        use crate::schema_cache::table::ComputedField;
        use crate::types::QualifiedIdentifier;

        let mut table = test_table()
            .schema("test_api")
            .name("users")
            .column(test_column().name("id").data_type("integer").build())
            .build();

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi,
            return_type: "text".into(),
            returns_set: false,
        };
        table.computed_fields.insert("full_name".into(), computed);

        let terms = vec![OrderTerm::Term {
            field: ("full_name".into(), Default::default()),
            direction: Some(crate::api_request::types::OrderDirection::Asc),
            nulls: None,
        }];

        let resolved = resolve_order_terms(&terms, Some(&table));
        assert_eq!(resolved.len(), 1);
        
        if let crate::plan::types::CoercibleOrderTerm::Term { field, .. } = &resolved[0] {
            assert!(field.is_computed);
            assert_eq!(field.name.as_str(), "full_name");
            assert_eq!(field.base_type.as_deref(), Some("text"));
        } else {
            panic!("Expected Term variant");
        }
    }

    #[test]
    fn test_resolve_filters_with_computed_field_operators() {
        use smallvec::SmallVec;
        use crate::api_request::types::{Filter, OpExpr, Operation, QuantOperator};
        use crate::schema_cache::table::ComputedField;
        use crate::types::QualifiedIdentifier;

        let mut table = test_table()
            .schema("test_api")
            .name("users")
            .column(test_column().name("id").data_type("integer").build())
            .build();

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi,
            return_type: "text".into(),
            returns_set: false,
        };
        table.computed_fields.insert("full_name".into(), computed);

        // Test LIKE operator
        let filters = vec![Filter {
            field: ("full_name".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Like, None, "John*".into()),
            },
        }];

        let resolved = resolve_filters(&filters, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 1);
        
        if let crate::plan::types::CoercibleLogicTree::Stmnt(crate::plan::types::CoercibleFilter::Filter { field, .. }) = &resolved[0] {
            assert!(field.is_computed);
            assert_eq!(field.name.as_str(), "full_name");
        } else {
            panic!("Expected Filter variant");
        }
    }

    #[test]
    fn test_resolve_filters_computed_field_vs_column() {
        use smallvec::SmallVec;
        use crate::api_request::types::{Filter, OpExpr, Operation, QuantOperator};
        use crate::schema_cache::table::ComputedField;
        use crate::types::QualifiedIdentifier;

        let mut table = test_table()
            .schema("test_api")
            .name("users")
            .column(test_column().name("name").data_type("text").build())
            .build();

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let computed = ComputedField {
            function: func_qi,
            return_type: "text".into(),
            returns_set: false,
        };
        table.computed_fields.insert("full_name".into(), computed);

        // Filter by regular column
        let filters1 = vec![Filter {
            field: ("name".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "John".into()),
            },
        }];
        let resolved1 = resolve_filters(&filters1, Some(&table)).unwrap();
        assert_eq!(resolved1.len(), 1);
        if let crate::plan::types::CoercibleLogicTree::Stmnt(crate::plan::types::CoercibleFilter::Filter { field, .. }) = &resolved1[0] {
            assert!(!field.is_computed);
            assert_eq!(field.name.as_str(), "name");
        }

        // Filter by computed field
        let filters2 = vec![Filter {
            field: ("full_name".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "John Doe".into()),
            },
        }];
        let resolved2 = resolve_filters(&filters2, Some(&table)).unwrap();
        assert_eq!(resolved2.len(), 1);
        if let crate::plan::types::CoercibleLogicTree::Stmnt(crate::plan::types::CoercibleFilter::Filter { field, .. }) = &resolved2[0] {
            assert!(field.is_computed);
            assert_eq!(field.name.as_str(), "full_name");
        }
    }

    #[test]
    fn test_action_plan_relation_read() {
        let config = test_config();
        let cache = test_schema_cache();
        let prefs = Preferences::default();
        let body = bytes::Bytes::new();

        let api_req = crate::api_request::from_request(
            &config,
            &prefs,
            "GET",
            "/users",
            "select=id,name",
            &[("accept".to_string(), "application/json".to_string())],
            body,
        )
        .unwrap();

        let plan = action_plan(&config, &api_req, &cache).unwrap();
        assert!(matches!(
            plan,
            ActionPlan::Db(DbActionPlan::DbCrud {
                plan: CrudPlan::WrappedReadPlan { .. },
                ..
            })
        ));
    }

    #[test]
    fn test_action_plan_relation_delete() {
        let config = test_config();
        let cache = test_schema_cache();
        let prefs = Preferences::default();
        let body = bytes::Bytes::new();

        let api_req = crate::api_request::from_request(
            &config,
            &prefs,
            "DELETE",
            "/users",
            "id=eq.1",
            &[],
            body,
        )
        .unwrap();

        let plan = action_plan(&config, &api_req, &cache).unwrap();
        assert!(matches!(
            plan,
            ActionPlan::Db(DbActionPlan::DbCrud {
                plan: CrudPlan::MutateReadPlan {
                    mutation: Mutation::MutationDelete,
                    ..
                },
                ..
            })
        ));
    }

    #[test]
    fn test_action_plan_schema_info() {
        let config = test_config();
        let cache = test_schema_cache();
        let prefs = Preferences::default();
        let body = bytes::Bytes::new();

        let api_req = crate::api_request::from_request(
            &config,
            &prefs,
            "OPTIONS",
            "/",
            "",
            &[],
            body,
        )
        .unwrap();

        let plan = action_plan(&config, &api_req, &cache).unwrap();
        assert!(matches!(plan, ActionPlan::NoDb(InfoPlan::SchemaInfoPlan)));
    }

    #[test]
    fn test_action_plan_relation_info() {
        let config = test_config();
        let cache = test_schema_cache();
        let prefs = Preferences::default();
        let body = bytes::Bytes::new();

        let api_req = crate::api_request::from_request(
            &config,
            &prefs,
            "OPTIONS",
            "/users",
            "",
            &[],
            body,
        )
        .unwrap();

        let plan = action_plan(&config, &api_req, &cache).unwrap();
        assert!(matches!(plan, ActionPlan::NoDb(InfoPlan::RelInfoPlan(_))));
    }

    #[test]
    fn test_action_plan_with_embed() {
        let config = test_config();
        let cache = test_schema_cache();
        let prefs = Preferences::default();
        let body = bytes::Bytes::new();

        let api_req = crate::api_request::from_request(
            &config,
            &prefs,
            "GET",
            "/users",
            "select=id,name,posts(id,title)",
            &[("accept".to_string(), "application/json".to_string())],
            body,
        )
        .unwrap();

        let plan = action_plan(&config, &api_req, &cache).unwrap();
        if let ActionPlan::Db(DbActionPlan::DbCrud {
            plan: CrudPlan::WrappedReadPlan { read_plan, .. },
            ..
        }) = plan
        {
            assert_eq!(read_plan.node_count(), 2); // root + posts child
            assert_eq!(read_plan.children().len(), 1);
            assert_eq!(read_plan.children()[0].node.rel_name.as_str(), "posts");
        } else {
            panic!("Expected WrappedReadPlan");
        }
    }

    #[test]
    fn test_action_plan_rpc() {
        let config = test_config();
        let cache = test_schema_cache();
        let prefs = Preferences::default();
        let body = bytes::Bytes::new();

        let api_req = crate::api_request::from_request(
            &config,
            &prefs,
            "GET",
            "/rpc/get_user",
            "user_id=1",
            &[("accept".to_string(), "application/json".to_string())],
            body,
        )
        .unwrap();

        let plan = action_plan(&config, &api_req, &cache).unwrap();
        assert!(matches!(
            plan,
            ActionPlan::Db(DbActionPlan::DbCrud {
                plan: CrudPlan::CallReadPlan { .. },
                ..
            })
        ));
    }

    #[test]
    fn test_action_plan_table_not_found() {
        let config = test_config();
        let cache = test_schema_cache();
        let prefs = Preferences::default();
        let body = bytes::Bytes::new();

        let api_req = crate::api_request::from_request(
            &config,
            &prefs,
            "GET",
            "/nonexistent",
            "",
            &[],
            body,
        )
        .unwrap();

        let result = action_plan(&config, &api_req, &cache);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::TableNotFound { .. }));
    }

    #[test]
    fn test_tx_mode_default() {
        let config = test_config();
        let prefs = Preferences::default();
        let tx = resolve_tx_mode(&config, &prefs, true);
        assert!(!tx.rollback);
    }

    #[test]
    fn test_tx_mode_rollback_all() {
        let mut config = test_config();
        config.db_tx_rollback_all = true;
        let prefs = Preferences::default();
        let tx = resolve_tx_mode(&config, &prefs, false);
        assert!(tx.rollback);
    }

    #[test]
    fn test_resolve_mutation_columns_all() {
        let table = test_table()
            .column(test_column().name("id").data_type("integer").build())
            .column(test_column().name("name").data_type("text").build())
            .build();

        let cols = resolve_mutation_columns(&table, &std::collections::HashSet::new());
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn test_resolve_mutation_columns_subset() {
        let table = test_table()
            .column(test_column().name("id").data_type("integer").build())
            .column(test_column().name("name").data_type("text").build())
            .column(test_column().name("email").data_type("text").build())
            .build();

        let mut payload_cols = std::collections::HashSet::new();
        payload_cols.insert(CompactString::from("name"));
        payload_cols.insert(CompactString::from("email"));

        let cols = resolve_mutation_columns(&table, &payload_cols);
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn test_resolve_select_composite_type() {
        use crate::api_request::types::SelectItem;
        use crate::api_request::types::{JsonOperation, JsonOperand, JsonPath};
        use smallvec::SmallVec;

        let mut table = test_table()
            .schema("test_api")
            .name("countries")
            .column(
                test_column()
                    .name("location")
                    .data_type("test_api.coordinates")
                    .build(),
            )
            .build();

        // Mark column as composite
        {
            use std::sync::Arc;
            use indexmap::IndexMap;
            let mut new_columns = IndexMap::new();
            for (k, v) in table.columns.iter() {
                if k.as_str() == "location" {
                    let mut new_col = v.clone();
                    new_col.is_composite = true;
                    new_col.composite_type_schema = Some("test_api".into());
                    new_col.composite_type_name = Some("coordinates".into());
                    new_columns.insert(k.clone(), new_col);
                } else {
                    new_columns.insert(k.clone(), v.clone());
                }
            }
            table.columns = Arc::new(new_columns);
        }

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow2(JsonOperand::Key("lat".into())));

        let items = vec![SelectItem::Field {
            field: ("location".into(), json_path),
            alias: None,
            cast: None,
            aggregate: None,
            aggregate_cast: None,
        }];

        let resolved = resolve_select(&items, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].field.to_json, "Composite type with JSON path should have to_json=true");
    }

    #[test]
    fn test_resolve_select_array_type() {
        use crate::api_request::types::SelectItem;
        use crate::api_request::types::{JsonOperation, JsonOperand, JsonPath};
        use smallvec::SmallVec;

        let table = test_table()
            .schema("test_api")
            .name("countries")
            .column(
                test_column()
                    .name("languages")
                    .data_type("text[]")
                    .build(),
            )
            .build();

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow(JsonOperand::Idx("0".into())));

        let items = vec![SelectItem::Field {
            field: ("languages".into(), json_path),
            alias: None,
            cast: None,
            aggregate: None,
            aggregate_cast: None,
        }];

        let resolved = resolve_select(&items, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(resolved[0].field.to_json, "Array type with JSON path should have to_json=true");
    }

    #[test]
    fn test_resolve_select_json_type_no_wrapper() {
        use crate::api_request::types::SelectItem;
        use crate::api_request::types::{JsonOperation, JsonOperand, JsonPath};
        use smallvec::SmallVec;

        let table = test_table()
            .schema("test_api")
            .name("posts")
            .column(
                test_column()
                    .name("metadata")
                    .data_type("jsonb")
                    .build(),
            )
            .build();

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow2(JsonOperand::Key("title".into())));

        let items = vec![SelectItem::Field {
            field: ("metadata".into(), json_path),
            alias: None,
            cast: None,
            aggregate: None,
            aggregate_cast: None,
        }];

        let resolved = resolve_select(&items, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 1);
        assert!(!resolved[0].field.to_json, "JSON/JSONB type should have to_json=false");
    }

    #[test]
    fn test_resolve_filters_composite_type() {
        use smallvec::SmallVec;
        use crate::api_request::types::{Filter, OpExpr, Operation, QuantOperator};
        use crate::api_request::types::{JsonOperation, JsonOperand, JsonPath};

        let mut table = test_table()
            .schema("test_api")
            .name("countries")
            .column(
                test_column()
                    .name("location")
                    .data_type("test_api.coordinates")
                    .build(),
            )
            .build();

        // Mark column as composite
        {
            use std::sync::Arc;
            use indexmap::IndexMap;
            let mut new_columns = IndexMap::new();
            for (k, v) in table.columns.iter() {
                if k.as_str() == "location" {
                    let mut new_col = v.clone();
                    new_col.is_composite = true;
                    new_col.composite_type_schema = Some("test_api".into());
                    new_col.composite_type_name = Some("coordinates".into());
                    new_columns.insert(k.clone(), new_col);
                } else {
                    new_columns.insert(k.clone(), v.clone());
                }
            }
            table.columns = Arc::new(new_columns);
        }

        let mut json_path: JsonPath = SmallVec::new();
        json_path.push(JsonOperation::Arrow2(JsonOperand::Key("lat".into())));

        let filters = vec![Filter {
            field: ("location".into(), json_path),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::GreaterThanEqual, None, "19.0".into()),
            },
        }];

        let resolved = resolve_filters(&filters, Some(&table)).unwrap();
        assert_eq!(resolved.len(), 1);
        
        if let crate::plan::types::CoercibleLogicTree::Stmnt(crate::plan::types::CoercibleFilter::Filter { field, .. }) = &resolved[0] {
            assert!(field.to_json, "Composite type filter with JSON path should have to_json=true");
        } else {
            panic!("Expected Filter variant");
        }
    }

    #[test]
    fn test_validate_cast_type() {
        // Valid cast types
        assert!(validate_cast_type("text").is_ok());
        assert!(validate_cast_type("integer").is_ok());
        assert!(validate_cast_type("bigint").is_ok());
        assert!(validate_cast_type("character varying").is_ok());
        assert!(validate_cast_type("numeric(10,2)").is_ok());
        assert!(validate_cast_type("text[]").is_ok());
        assert!(validate_cast_type("_int4").is_ok());

        // Invalid cast types
        assert!(validate_cast_type("").is_err());
        assert!(validate_cast_type("invalid@type").is_err());
        assert!(validate_cast_type("type;drop table").is_err());
    }

    #[test]
    fn test_resolve_filters_rejects_cast() {
        // Note: Cast validation now happens in query_params parsing (parse_tree_path),
        // not in resolve_filters. This test verifies that resolve_filters correctly
        // handles fields that have already been validated.
        // If a field with "::" somehow gets through, it would be treated as an unknown
        // field name, which would fail column lookup.
        use smallvec::SmallVec;
        use crate::api_request::types::{Filter, OpExpr, Operation, QuantOperator};

        let table = test_table()
            .schema("test_api")
            .name("users")
            .column(test_column().name("id").data_type("integer").build())
            .build();

        // Even if "::" is in the field name, it won't match "id" column
        let filters = vec![Filter {
            field: ("id::text".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
            },
        }];

        let result = resolve_filters(&filters, Some(&table));
        // Should fail because "id::text" doesn't match column "id"
        assert!(result.is_err(), "Should fail when column name doesn't match");
        
        if let Err(Error::ColumnNotFound { column, .. }) = result {
            assert_eq!(column, "id::text");
        } else {
            panic!("Expected ColumnNotFound error, got: {:?}", result);
        }
    }
}
