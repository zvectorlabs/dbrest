//! Query builder module — transforms typed plans into parameterized SQL.
//!
//! This module sits between the plan layer (`crate::plan`) and the database
//! executor. It consumes `ActionPlan` / `CrudPlan` values and produces
//! parameterized SQL strings ready for execution via sqlx.
//!
//! # Pipeline
//!
//! ```text
//! ActionPlan ──▶ main_query() ──▶ MainQuery { tx_vars, pre_req, main }
//!                                    │
//!                                    ├─ tx_var_query()     → SET search_path, role, …
//!                                    ├─ pre_req_query()    → SELECT pre_request()
//!                                    └─ main_read/write/call → CTE wrapper query
//! ```
//!
//! # SQL Example
//!
//! A simple GET /users produces:
//!
//! ```sql
//! -- tx_vars:
//! SELECT set_config('search_path', '"test_api"', true),
//!        set_config('role', 'web_anon', true), …
//!
//! -- main:
//! WITH pgrst_source AS (
//!   SELECT "test_api"."users"."id", "test_api"."users"."name"
//!   FROM "test_api"."users"
//! )
//! SELECT NULL AS total_result_set,
//!        pg_catalog.count(_pgrest_t) AS page_total,
//!        coalesce(json_agg(_pgrest_t), '[]')::text AS body,
//!        …
//! FROM (SELECT * FROM pgrst_source) AS _pgrest_t
//! ```

pub mod builder;
pub mod fragment;
pub mod pre_query;
pub mod sql_builder;
pub mod statements;

// Re-export key types
pub use sql_builder::{SqlBuilder, SqlParam};

use crate::backend::SqlDialect;
use crate::config::AppConfig;
use crate::plan::{ActionPlan, CrudPlan, DbActionPlan};

// ==========================================================================
// MainQuery — the final query bundle
// ==========================================================================

/// A bundle of SQL queries to execute for a single API request.
///
/// Created by [`main_query`]. The executor runs these queries in order:
///
/// 1. `tx_vars` — sets session variables (search_path, role, request context)
/// 2. `pre_req` — calls the pre-request function (if configured)
/// 3. `main` — the actual data query (read / mutate / call)
///
/// Each field is `Option<SqlBuilder>` because not every request needs all three
/// queries (e.g., info requests have no main query).
#[derive(Debug)]
pub struct MainQuery {
    /// Session variable setup query (`SELECT set_config(…)`).
    pub tx_vars: Option<SqlBuilder>,
    /// Pre-request function call (`SELECT schema.pre_request()`).
    pub pre_req: Option<SqlBuilder>,
    /// Mutation statement (INSERT/UPDATE/DELETE with RETURNING).
    ///
    /// Only set for backends that don't support DML in CTEs (e.g. SQLite).
    /// When set, `main` contains only the aggregation SELECT over a temp table
    /// populated by this mutation.
    pub mutation: Option<SqlBuilder>,
    /// Main data query (CTE-wrapped SELECT / INSERT / UPDATE / DELETE / CALL).
    pub main: Option<SqlBuilder>,
}

impl MainQuery {
    /// Create an empty query bundle.
    pub fn empty() -> Self {
        Self {
            tx_vars: None,
            pre_req: None,
            mutation: None,
            main: None,
        }
    }
}

// ==========================================================================
// main_query — entry point
// ==========================================================================

/// Build all queries for an API request.
///
/// Routes the `ActionPlan` to the appropriate statement builder and assembles
/// the full query bundle including session variables and pre-request calls.
///
/// # Behaviour
///
/// - `ActionPlan::Db(DbCrud { .. })` routes to `main_read`, `main_write`, or
///   `main_call` depending on the `CrudPlan` variant
/// - `ActionPlan::Db(MayUseDb(InspectPlan))` generates a schema inspection
///   query (not yet implemented — placeholder for future phases)
/// - `ActionPlan::NoDb(InfoPlan)` generates no SQL (handled at the HTTP layer)
///
/// # Returns
///
/// A [`MainQuery`] with the session setup, pre-request, and main query SQL.
#[allow(clippy::too_many_arguments)]
pub fn main_query(
    action_plan: &ActionPlan,
    config: &AppConfig,
    dialect: &dyn SqlDialect,
    method: &str,
    path: &str,
    role: Option<&str>,
    headers_json: Option<&str>,
    cookies_json: Option<&str>,
    claims_json: Option<&str>,
) -> MainQuery {
    // Session variables
    let tx_vars = Some(pre_query::tx_var_query(
        config,
        dialect,
        method,
        path,
        role,
        headers_json,
        cookies_json,
        claims_json,
    ));

    // Pre-request function
    let pre_req = config.db_pre_request.as_ref().map(pre_query::pre_req_query);

    // Main query based on action plan type
    let (mutation, main) = match action_plan {
        ActionPlan::Db(db_plan) => match db_plan {
            DbActionPlan::DbCrud { plan, .. } => {
                let (m, q) = build_crud_query(plan, config, dialect);
                (m, Some(q))
            }
            DbActionPlan::MayUseDb(_inspect) => {
                // Schema inspection — placeholder for future phases
                (None, None)
            }
        },
        ActionPlan::NoDb(_) => {
            // Info plans (OPTIONS) are handled at the HTTP layer, no SQL needed
            (None, None)
        }
    };

    MainQuery {
        tx_vars,
        pre_req,
        mutation,
        main,
    }
}

/// Build the main SQL query for a CRUD plan.
///
/// Dispatches to the appropriate CTE-wrapping statement builder based on the
/// plan variant:
///
/// | Variant           | Builder function       | Query shape              |
/// |-------------------|------------------------|--------------------------|
/// | `WrappedReadPlan` | `statements::main_read`  | CTE SELECT with aggregation |
/// | `MutateReadPlan`  | `statements::main_write` | CTE INSERT/UPDATE/DELETE |
/// | `CallReadPlan`    | `statements::main_call`  | CTE function call        |
///
/// Returns the assembled `SqlBuilder` ready for execution.
fn build_crud_query(plan: &CrudPlan, config: &AppConfig, dialect: &dyn SqlDialect) -> (Option<SqlBuilder>, SqlBuilder) {
    match plan {
        CrudPlan::WrappedReadPlan {
            read_plan,
            headers_only,
            handler,
            ..
        } => {
            (None, statements::main_read(
                read_plan,
                None,
                config.db_max_rows,
                *headers_only,
                Some(&handler.0),
                dialect,
            ))
        }
        CrudPlan::MutateReadPlan {
            read_plan,
            mutate_plan,
            handler,
            ..
        } => {
            let return_representation = !mutate_plan.returning().is_empty();
            if dialect.supports_dml_cte() {
                (None, statements::main_write(
                    mutate_plan,
                    read_plan,
                    return_representation,
                    Some(&handler.0),
                    dialect,
                ))
            } else {
                let (mutation, agg) = statements::main_write_split(
                    mutate_plan,
                    read_plan,
                    return_representation,
                    Some(&handler.0),
                    dialect,
                );
                (Some(mutation), agg)
            }
        }
        CrudPlan::CallReadPlan {
            call_plan, handler, ..
        } => (None, statements::main_call(call_plan, None, config.db_max_rows, Some(&handler.0), dialect)),
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_request::types::{InvokeMethod, Mutation, Payload};
    use crate::test_helpers::TestPgDialect;
    use crate::plan::TxMode;
    use crate::plan::call_plan::{CallArgs, CallParams, CallPlan};
    use crate::plan::mutate_plan::{InsertPlan, MutatePlan};
    use crate::plan::read_plan::{ReadPlan, ReadPlanTree};
    use crate::plan::types::*;
    use crate::schema_cache::media_handler::{MediaHandler, ResolvedHandler};
    use crate::types::identifiers::QualifiedIdentifier;
    use crate::types::media::MediaType;
    use bytes::Bytes;
    use smallvec::SmallVec;

    fn dialect() -> &'static dyn SqlDialect {
        &TestPgDialect
    }

    fn test_qi() -> QualifiedIdentifier {
        QualifiedIdentifier::new("test_api", "users")
    }

    fn test_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.db_schemas = vec!["test_api".to_string()];
        config.db_anon_role = Some("web_anon".to_string());
        config
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

    fn default_handler() -> ResolvedHandler {
        (MediaHandler::BuiltinOvAggJson, MediaType::ApplicationJson)
    }

    fn make_read_plan() -> ActionPlan {
        let mut rp = ReadPlan::root(test_qi());
        rp.select = vec![select_field("id"), select_field("name")];
        let tree = ReadPlanTree::leaf(rp);

        ActionPlan::Db(DbActionPlan::DbCrud {
            is_explain: false,
            plan: CrudPlan::WrappedReadPlan {
                read_plan: tree,
                tx_mode: TxMode::default_mode(),
                handler: default_handler(),
                media: MediaType::ApplicationJson,
                headers_only: false,
                qi: test_qi(),
            },
        })
    }

    fn make_mutate_plan() -> ActionPlan {
        let rp = ReadPlan::root(test_qi());
        let tree = ReadPlanTree::leaf(rp);

        let mutate = MutatePlan::Insert(InsertPlan {
            into: test_qi(),
            columns: vec![CoercibleField::from_column(
                "name".into(),
                SmallVec::new(),
                "text".into(),
            )],
            body: Payload::RawJSON(Bytes::from(r#"[{"name":"Alice"}]"#)),
            on_conflict: None,
            where_: vec![],
            returning: vec![select_field("id"), select_field("name")],
            pk_cols: vec!["id".into()],
            apply_defaults: false,
        });

        ActionPlan::Db(DbActionPlan::DbCrud {
            is_explain: false,
            plan: CrudPlan::MutateReadPlan {
                read_plan: tree,
                mutate_plan: mutate,
                tx_mode: TxMode::default_mode(),
                handler: default_handler(),
                media: MediaType::ApplicationJson,
                mutation: Mutation::MutationCreate,
                qi: test_qi(),
            },
        })
    }

    fn make_call_plan() -> ActionPlan {
        use crate::schema_cache::routine::{ReturnType, Routine, Volatility};
        use smallvec::smallvec;

        let rp = ReadPlan::root(test_qi());
        let tree = ReadPlanTree::leaf(rp);

        let call = CallPlan {
            qi: QualifiedIdentifier::new("test_api", "get_time"),
            params: CallParams::KeyParams(vec![]),
            args: CallArgs::JsonArgs(None),
            scalar: true,
            set_of_scalar: false,
            filter_fields: vec![],
            returning: vec![],
        };

        ActionPlan::Db(DbActionPlan::DbCrud {
            is_explain: false,
            plan: CrudPlan::CallReadPlan {
                read_plan: tree,
                call_plan: call,
                tx_mode: TxMode::default_mode(),
                proc: Routine {
                    schema: "test_api".into(),
                    name: "get_time".into(),
                    params: smallvec![],
                    return_type: ReturnType::Single(crate::schema_cache::routine::PgType::Scalar(
                        QualifiedIdentifier::new("pg_catalog", "timestamptz"),
                    )),
                    is_variadic: false,
                    volatility: Volatility::Stable,
                    description: None,
                    executable: true,
                },
                handler: default_handler(),
                media: MediaType::ApplicationJson,
                inv_method: InvokeMethod::InvRead(false),
                qi: QualifiedIdentifier::new("test_api", "get_time"),
            },
        })
    }

    // ------------------------------------------------------------------
    // main_query tests
    // ------------------------------------------------------------------

    #[test]
    fn test_main_query_read() {
        let plan = make_read_plan();
        let config = test_config();

        let mq = main_query(&plan, &config, dialect(), "GET", "/users", None, None, None, None);

        assert!(mq.tx_vars.is_some());
        assert!(mq.pre_req.is_none()); // No pre-request configured
        assert!(mq.main.is_some());

        let main_sql = mq.main.unwrap().sql().to_string();
        assert!(main_sql.contains("pgrst_source"));
        assert!(main_sql.contains("\"users\""));
    }

    #[test]
    fn test_main_query_mutate() {
        let plan = make_mutate_plan();
        let config = test_config();

        let mq = main_query(&plan, &config, dialect(), "POST", "/users", None, None, None, None);

        assert!(mq.main.is_some());
        let main_sql = mq.main.unwrap().sql().to_string();
        assert!(main_sql.contains("INSERT INTO"));
    }

    #[test]
    fn test_main_query_call() {
        let plan = make_call_plan();
        let config = test_config();

        let mq = main_query(
            &plan,
            &config,
            dialect(),
            "POST",
            "/rpc/get_time",
            None,
            None,
            None,
            None,
        );

        assert!(mq.main.is_some());
        let main_sql = mq.main.unwrap().sql().to_string();
        assert!(main_sql.contains("get_time"));
    }

    #[test]
    fn test_main_query_with_pre_request() {
        let plan = make_read_plan();
        let mut config = test_config();
        config.db_pre_request = Some(QualifiedIdentifier::new("test_api", "check_request"));

        let mq = main_query(&plan, &config, dialect(), "GET", "/users", None, None, None, None);

        assert!(mq.pre_req.is_some());
        let pre_sql = mq.pre_req.unwrap().sql().to_string();
        assert!(pre_sql.contains("check_request"));
    }

    #[test]
    fn test_main_query_info_plan() {
        let plan = ActionPlan::NoDb(crate::plan::InfoPlan::SchemaInfoPlan);
        let config = test_config();

        let mq = main_query(&plan, &config, dialect(), "OPTIONS", "/", None, None, None, None);

        // Info plans have no main SQL
        assert!(mq.main.is_none());
        // But still set tx vars
        assert!(mq.tx_vars.is_some());
    }

    #[test]
    fn test_main_query_empty() {
        let mq = MainQuery::empty();
        assert!(mq.tx_vars.is_none());
        assert!(mq.pre_req.is_none());
        assert!(mq.main.is_none());
    }
}
