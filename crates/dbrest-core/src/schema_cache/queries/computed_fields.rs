//! Computed fields introspection query
//!
//! Query for detecting functions that can be used as computed fields.
//! These are functions that take a table row type as the first parameter
//! and return a scalar value (not a composite type).

/// SQL query to find computed field functions
///
/// Finds functions that:
/// - Take exactly 1 parameter (a table row type)
/// - Return a scalar value (not a composite type)
/// - Are in exposed schemas or db-extra-search-path
///
/// Returns:
/// - table_schema, table_name: The table the function operates on
/// - function_schema, function_name: The function that computes the field
/// - return_type: The PostgreSQL type returned by the function
/// - returns_set: Whether the function returns SETOF
pub const COMPUTED_FIELDS_QUERY: &str = r#"
SELECT
    n.nspname AS table_schema,
    t.relname AS table_name,
    pn.nspname AS function_schema,
    p.proname AS function_name,
    format_type(p.prorettype, NULL) AS return_type,
    p.proretset AS returns_set
FROM pg_proc p
JOIN pg_namespace pn ON p.pronamespace = pn.oid
JOIN pg_type pt ON p.proargtypes[0] = pt.oid
JOIN pg_class t ON pt.typrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
JOIN pg_type rt ON p.prorettype = rt.oid
WHERE array_length(p.proargtypes, 1) = 1
    AND t.relkind IN ('r', 'v', 'm', 'f', 'p')
    AND rt.typtype != 'c'  -- Not a composite type (scalar return)
    AND pn.nspname = ANY($1::text[])  -- Exposed schemas + extra-search-path
ORDER BY n.nspname, t.relname, p.proname
"#;

// Note: ComputedFieldRow is defined in schema_cache::db module

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_computed_fields_query_not_empty() {
        assert!(!COMPUTED_FIELDS_QUERY.is_empty());
        assert!(COMPUTED_FIELDS_QUERY.contains("SELECT"));
        assert!(COMPUTED_FIELDS_QUERY.contains("pg_proc"));
    }

    #[test]
    fn test_computed_fields_query_has_required_columns() {
        assert!(COMPUTED_FIELDS_QUERY.contains("table_schema"));
        assert!(COMPUTED_FIELDS_QUERY.contains("table_name"));
        assert!(COMPUTED_FIELDS_QUERY.contains("function_schema"));
        assert!(COMPUTED_FIELDS_QUERY.contains("function_name"));
        assert!(COMPUTED_FIELDS_QUERY.contains("return_type"));
        assert!(COMPUTED_FIELDS_QUERY.contains("returns_set"));
    }

    #[test]
    fn test_computed_fields_query_filters_scalar_returns() {
        assert!(COMPUTED_FIELDS_QUERY.contains("rt.typtype != 'c'"));
    }

    #[test]
    fn test_computed_fields_query_filters_single_param() {
        assert!(COMPUTED_FIELDS_QUERY.contains("array_length(p.proargtypes, 1) = 1"));
    }

    #[test]
    fn test_computed_fields_query_filters_schemas() {
        assert!(COMPUTED_FIELDS_QUERY.contains("pn.nspname = ANY($1::text[])"));
    }
}
