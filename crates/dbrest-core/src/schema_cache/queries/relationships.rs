//! Relationships introspection queries
//!
//! Queries for loading foreign key relationships between tables.

/// SQL query to fetch all M2O and O2O relationships (foreign keys)
///
/// This query returns:
/// - Source and target table information
/// - Constraint name
/// - Column mappings
/// - Whether it's a one-to-one relationship
pub const RELATIONSHIPS_QUERY: &str = r#"
WITH pks_uniques_cols AS (
    SELECT conrelid, array_agg(key ORDER BY key) AS cols
    FROM pg_constraint, LATERAL unnest(conkey) AS _(key)
    WHERE contype IN ('p', 'u') AND connamespace <> 'pg_catalog'::regnamespace
    GROUP BY oid, conrelid
)
SELECT
    ns1.nspname AS table_schema,
    tab.relname AS table_name,
    ns2.nspname AS foreign_table_schema,
    other.relname AS foreign_table_name,
    traint.conrelid = traint.confrelid AS is_self,
    traint.conname AS constraint_name,
    column_info.cols_and_fcols,
    (column_info.cols IN (SELECT cols FROM pks_uniques_cols WHERE conrelid = traint.conrelid)) AS one_to_one
FROM pg_constraint traint
JOIN LATERAL (
    SELECT
        array_agg(row(cols.attname, refs.attname) ORDER BY ord) AS cols_and_fcols,
        array_agg(cols.attnum ORDER BY cols.attnum) AS cols
    FROM unnest(traint.conkey, traint.confkey) WITH ORDINALITY AS _(col, ref, ord)
    JOIN pg_attribute cols ON cols.attrelid = traint.conrelid AND cols.attnum = col
    JOIN pg_attribute refs ON refs.attrelid = traint.confrelid AND refs.attnum = ref
) AS column_info ON TRUE
JOIN pg_namespace ns1 ON ns1.oid = traint.connamespace
JOIN pg_class tab ON tab.oid = traint.conrelid
JOIN pg_class other ON other.oid = traint.confrelid
JOIN pg_namespace ns2 ON ns2.oid = other.relnamespace
WHERE traint.contype = 'f' AND traint.conparentid = 0
ORDER BY traint.conrelid, traint.conname
"#;

/// SQL query to find computed relationships (function-based)
///
/// This finds functions that take a table row type as first argument
/// and return a related table type.
pub const COMPUTED_RELS_QUERY: &str = r#"
SELECT
    n.nspname AS table_schema,
    t.relname AS table_name,
    pn.nspname AS function_schema,
    p.proname AS function_name,
    rn.nspname AS foreign_table_schema,
    rt.relname AS foreign_table_name,
    t.oid = rt.oid AS is_self,
    p.proretset = false AS single_row
FROM pg_proc p
JOIN pg_namespace pn ON p.pronamespace = pn.oid
JOIN pg_type pt ON p.proargtypes[0] = pt.oid
JOIN pg_class t ON pt.typrelid = t.oid
JOIN pg_namespace n ON t.relnamespace = n.oid
JOIN pg_type rt_type ON p.prorettype = rt_type.oid
JOIN pg_class rt ON rt_type.typrelid = rt.oid
JOIN pg_namespace rn ON rt.relnamespace = rn.oid
WHERE array_length(p.proargtypes, 1) = 1
    AND t.relkind IN ('r', 'v', 'm', 'f', 'p')
    AND rt.relkind IN ('r', 'v', 'm', 'f', 'p')
    AND NOT p.proretset OR p.proretset
ORDER BY n.nspname, t.relname, p.proname
"#;

/// Parse the column mappings from the query result
///
/// The query returns columns as an array of records.
pub fn parse_cols_and_fcols(cols: &[(String, String)]) -> Vec<(String, String)> {
    cols.to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relationships_query_not_empty() {
        assert!(!RELATIONSHIPS_QUERY.is_empty());
        assert!(RELATIONSHIPS_QUERY.contains("SELECT"));
        assert!(RELATIONSHIPS_QUERY.contains("pg_constraint"));
    }

    #[test]
    fn test_relationships_query_has_required_columns() {
        assert!(RELATIONSHIPS_QUERY.contains("table_schema"));
        assert!(RELATIONSHIPS_QUERY.contains("table_name"));
        assert!(RELATIONSHIPS_QUERY.contains("foreign_table_schema"));
        assert!(RELATIONSHIPS_QUERY.contains("foreign_table_name"));
        assert!(RELATIONSHIPS_QUERY.contains("constraint_name"));
        assert!(RELATIONSHIPS_QUERY.contains("one_to_one"));
    }

    #[test]
    fn test_relationships_query_filters_foreign_keys() {
        assert!(RELATIONSHIPS_QUERY.contains("contype = 'f'"));
    }

    #[test]
    fn test_computed_rels_query_not_empty() {
        assert!(!COMPUTED_RELS_QUERY.is_empty());
        assert!(COMPUTED_RELS_QUERY.contains("pg_proc"));
    }

    #[test]
    fn test_computed_rels_query_has_required_columns() {
        assert!(COMPUTED_RELS_QUERY.contains("function_schema"));
        assert!(COMPUTED_RELS_QUERY.contains("function_name"));
        assert!(COMPUTED_RELS_QUERY.contains("single_row"));
    }

    #[test]
    fn test_parse_cols_and_fcols() {
        let cols = vec![
            ("user_id".to_string(), "id".to_string()),
            ("org_id".to_string(), "id".to_string()),
        ];

        let result = parse_cols_and_fcols(&cols);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], ("user_id".to_string(), "id".to_string()));
    }

    #[test]
    fn test_parse_cols_and_fcols_empty() {
        let cols: Vec<(String, String)> = vec![];
        let result = parse_cols_and_fcols(&cols);
        assert!(result.is_empty());
    }
}
