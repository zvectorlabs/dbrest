//! Routines (functions/procedures) introspection query
//!
//! Query for loading all callable functions and procedures from the database.

/// SQL query to fetch all functions and procedures
///
/// This query returns:
/// - Function metadata (schema, name, description)
/// - Parameters as JSON
/// - Return type as JSON
/// - Volatility
pub const ROUTINES_QUERY: &str = r#"
WITH
callable_procs AS (
    SELECT
        p.oid,
        pn.nspname AS proc_schema,
        p.proname AS proc_name,
        d.description,
        p.provolatile AS volatility,
        p.provariadic != 0 AS is_variadic,
        p.prorettype,
        p.proretset
    FROM pg_proc p
    JOIN pg_namespace pn ON p.pronamespace = pn.oid
    LEFT JOIN pg_description d ON d.objoid = p.oid AND d.classoid = 'pg_proc'::regclass
    WHERE pn.nspname = ANY($1)
        AND p.prokind IN ('f', 'p')  -- functions and procedures
        AND NOT p.prorettype = 'pg_catalog.trigger'::regtype
),
proc_params AS (
    SELECT
        p.oid,
        jsonb_agg(jsonb_build_object(
            'name', COALESCE(pn.name, 'arg' || pn.ord::text),
            'pg_type', format_type(pn.type_oid, NULL),
            'type_max_length', format_type(pn.type_oid, pn.typmod),
            'required', pn.ord <= (p.pronargs - p.pronargdefaults),
            'is_variadic', pn.ord = p.pronargs AND p.provariadic != 0
        ) ORDER BY pn.ord) AS params
    FROM pg_proc p
    CROSS JOIN LATERAL (
        SELECT
            a.attname AS name,
            a.atttypid AS type_oid,
            a.atttypmod AS typmod,
            row_number() OVER () AS ord
        FROM unnest(p.proargtypes) WITH ORDINALITY AS t(type_oid, ord)
        LEFT JOIN pg_attribute a ON a.attrelid = p.oid AND a.attnum = t.ord
    ) AS pn
    WHERE p.oid IN (SELECT oid FROM callable_procs)
    GROUP BY p.oid, p.pronargs, p.pronargdefaults, p.provariadic
),
return_types AS (
    SELECT
        p.oid,
        jsonb_build_object(
            'kind', CASE WHEN p.proretset THEN 'setof' ELSE 'single' END,
            'type_kind', CASE 
                WHEN t.typtype = 'c' OR t.typrelid != 0 THEN 'composite'
                ELSE 'scalar'
            END,
            'type_schema', tn.nspname,
            'type_name', t.typname,
            'is_alias', t.typtype = 'd'
        ) AS return_type
    FROM pg_proc p
    JOIN pg_type t ON p.prorettype = t.oid
    JOIN pg_namespace tn ON t.typnamespace = tn.oid
    WHERE p.oid IN (SELECT oid FROM callable_procs)
)
SELECT
    cp.proc_schema AS routine_schema,
    cp.proc_name AS routine_name,
    cp.description,
    COALESCE(pp.params, '[]'::jsonb)::text AS params_json,
    COALESCE(rt.return_type, '{"kind":"single","type_kind":"scalar","type_schema":"pg_catalog","type_name":"void","is_alias":false}'::jsonb)::text AS return_type_json,
    cp.volatility,
    cp.is_variadic,
    has_function_privilege(cp.oid, 'EXECUTE') AS executable
FROM callable_procs cp
LEFT JOIN proc_params pp ON cp.oid = pp.oid
LEFT JOIN return_types rt ON cp.oid = rt.oid
ORDER BY cp.proc_schema, cp.proc_name
"#;

/// Build the schemas array parameter for the routines query
pub fn build_schemas_param(schemas: &[String]) -> Vec<&str> {
    schemas.iter().map(|s| s.as_str()).collect()
}

/// Parse volatility character from PostgreSQL
pub fn parse_volatility(v: &str) -> &'static str {
    match v {
        "i" => "immutable",
        "s" => "stable",
        "v" => "volatile",
        _ => "volatile",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_routines_query_not_empty() {
        assert!(!ROUTINES_QUERY.is_empty());
        assert!(ROUTINES_QUERY.contains("SELECT"));
        assert!(ROUTINES_QUERY.contains("pg_proc"));
    }

    #[test]
    fn test_routines_query_has_required_columns() {
        assert!(ROUTINES_QUERY.contains("routine_schema"));
        assert!(ROUTINES_QUERY.contains("routine_name"));
        assert!(ROUTINES_QUERY.contains("params_json"));
        assert!(ROUTINES_QUERY.contains("return_type_json"));
        assert!(ROUTINES_QUERY.contains("volatility"));
    }

    #[test]
    fn test_routines_query_excludes_triggers() {
        assert!(ROUTINES_QUERY.contains("trigger"));
    }

    #[test]
    fn test_routines_query_filters_by_schema() {
        assert!(ROUTINES_QUERY.contains("= ANY($1)"));
    }

    #[test]
    fn test_build_schemas_param() {
        let schemas = vec!["public".to_string(), "api".to_string()];
        let param = build_schemas_param(&schemas);
        assert_eq!(param, vec!["public", "api"]);
    }

    #[test]
    fn test_parse_volatility() {
        assert_eq!(parse_volatility("i"), "immutable");
        assert_eq!(parse_volatility("s"), "stable");
        assert_eq!(parse_volatility("v"), "volatile");
        assert_eq!(parse_volatility("x"), "volatile"); // unknown defaults to volatile
    }
}
