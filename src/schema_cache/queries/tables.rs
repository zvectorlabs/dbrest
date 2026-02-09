//! Tables introspection query
//!
//! Query for loading all tables, views, materialized views, and foreign tables
//! from the database.

/// SQL query to fetch all tables and views with their columns
///
/// This query returns:
/// - Table/view metadata (schema, name, description, permissions)
/// - Primary key columns
/// - Column information as JSON (name, type, nullable, default, etc.)
pub const TABLES_QUERY: &str = r#"
WITH
base_types AS (
    WITH RECURSIVE recurse AS (
        SELECT oid, typbasetype, typnamespace AS base_namespace,
               COALESCE(NULLIF(typbasetype, 0), oid) AS base_type
        FROM pg_type
        UNION
        SELECT t.oid, b.typbasetype, b.typnamespace AS base_namespace,
               COALESCE(NULLIF(b.typbasetype, 0), b.oid) AS base_type
        FROM recurse t
        JOIN pg_type b ON t.typbasetype = b.oid
    )
    SELECT oid, base_namespace, base_type FROM recurse WHERE typbasetype = 0
),
columns AS (
    SELECT
        c.oid AS relid,
        jsonb_agg(jsonb_build_object(
            'name', a.attname,
            'description', d.description,
            'nullable', NOT (a.attnotnull OR t.typtype = 'd' AND t.typnotnull),
            'data_type', CASE
                WHEN t.typtype = 'd' THEN
                    CASE WHEN bt.base_namespace = 'pg_catalog'::regnamespace 
                         THEN format_type(bt.base_type, NULL)
                         ELSE format_type(a.atttypid, a.atttypmod)
                    END
                ELSE CASE WHEN t.typnamespace = 'pg_catalog'::regnamespace 
                          THEN format_type(a.atttypid, NULL)
                          ELSE format_type(a.atttypid, a.atttypmod)
                     END
            END,
            'nominal_type', format_type(a.atttypid, a.atttypmod),
            'max_length', information_schema._pg_char_max_length(
                information_schema._pg_truetypid(a.*, t.*),
                information_schema._pg_truetypmod(a.*, t.*)
            ),
            'default', CASE
                WHEN (t.typbasetype != 0) AND (ad.adbin IS NULL) THEN pg_get_expr(t.typdefaultbin, 0)
                WHEN a.attidentity = 'd' THEN format('nextval(%L)', seq.objid::regclass)
                WHEN a.attgenerated = 's' THEN null
                ELSE pg_get_expr(ad.adbin, ad.adrelid)
            END,
            'enum_values', COALESCE(
                (SELECT jsonb_agg(enumlabel ORDER BY enumsortorder) FROM pg_enum WHERE enumtypid = bt.base_type),
                '[]'::jsonb
            ),
            'is_composite', CASE WHEN t.typtype = 'c' THEN true ELSE false END,
            'composite_type_schema', CASE WHEN t.typtype = 'c' THEN tn.nspname ELSE NULL END,
            'composite_type_name', CASE WHEN t.typtype = 'c' THEN t.typname ELSE NULL END
        ) ORDER BY a.attnum) AS columns
    FROM pg_attribute a
    LEFT JOIN pg_description d ON d.objoid = a.attrelid AND d.objsubid = a.attnum AND d.classoid = 'pg_class'::regclass
    LEFT JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
    JOIN pg_class c ON a.attrelid = c.oid
    JOIN pg_type t ON a.atttypid = t.oid
    LEFT JOIN pg_namespace tn ON t.typnamespace = tn.oid
    LEFT JOIN base_types bt ON t.oid = bt.oid
    LEFT JOIN pg_depend seq ON seq.refobjid = a.attrelid AND seq.refobjsubid = a.attnum AND seq.deptype = 'i'
    WHERE NOT pg_is_other_temp_schema(c.relnamespace)
        AND a.attnum > 0
        AND NOT a.attisdropped
        AND c.relkind IN ('r', 'v', 'f', 'm', 'p')
        AND c.relnamespace = ANY($1::regnamespace[])
    GROUP BY c.oid
),
tbl_pk_cols AS (
    SELECT r.oid AS relid, array_agg(a.attname ORDER BY a.attname) AS pk_cols
    FROM pg_class r
    JOIN pg_constraint c ON r.oid = c.conrelid
    JOIN pg_attribute a ON a.attrelid = r.oid AND a.attnum = ANY(c.conkey)
    WHERE c.contype = 'p' AND r.relkind IN ('r', 'p')
        AND r.relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
        AND NOT pg_is_other_temp_schema(r.relnamespace)
        AND NOT a.attisdropped
    GROUP BY r.oid
)
SELECT
    n.nspname AS table_schema,
    c.relname AS table_name,
    d.description AS table_description,
    c.relkind IN ('v', 'm') AS is_view,
    (c.relkind IN ('r', 'p') OR (c.relkind IN ('v', 'f') AND (pg_relation_is_updatable(c.oid, TRUE) & 8) = 8)) AS insertable,
    (c.relkind IN ('r', 'p') OR (c.relkind IN ('v', 'f') AND (pg_relation_is_updatable(c.oid, TRUE) & 4) = 4)) AS updatable,
    (c.relkind IN ('r', 'p') OR (c.relkind IN ('v', 'f') AND (pg_relation_is_updatable(c.oid, TRUE) & 16) = 16)) AS deletable,
    has_table_privilege(c.oid, 'SELECT') AS readable,
    COALESCE(tpks.pk_cols, '{}') AS pk_cols,
    COALESCE(cols.columns, '[]'::jsonb)::text AS columns_json
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = 0 AND d.classoid = 'pg_class'::regclass
LEFT JOIN tbl_pk_cols tpks ON c.oid = tpks.relid
LEFT JOIN columns cols ON c.oid = cols.relid
WHERE c.relkind IN ('v', 'r', 'm', 'f', 'p')
    AND c.relnamespace NOT IN ('pg_catalog'::regnamespace, 'information_schema'::regnamespace)
    AND NOT c.relispartition
    AND n.nspname = ANY($1)
ORDER BY table_schema, table_name
"#;

/// Build the schemas array parameter for the tables query
pub fn build_schemas_param(schemas: &[String]) -> Vec<&str> {
    schemas.iter().map(|s| s.as_str()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tables_query_not_empty() {
        assert!(!TABLES_QUERY.is_empty());
        assert!(TABLES_QUERY.contains("SELECT"));
        assert!(TABLES_QUERY.contains("pg_class"));
    }

    #[test]
    fn test_tables_query_has_required_columns() {
        assert!(TABLES_QUERY.contains("table_schema"));
        assert!(TABLES_QUERY.contains("table_name"));
        assert!(TABLES_QUERY.contains("is_view"));
        assert!(TABLES_QUERY.contains("pk_cols"));
        assert!(TABLES_QUERY.contains("columns_json"));
    }

    #[test]
    fn test_tables_query_excludes_system_schemas() {
        assert!(TABLES_QUERY.contains("pg_catalog"));
        assert!(TABLES_QUERY.contains("information_schema"));
    }

    #[test]
    fn test_build_schemas_param() {
        let schemas = vec!["public".to_string(), "api".to_string()];
        let param = build_schemas_param(&schemas);
        assert_eq!(param, vec!["public", "api"]);
    }

    #[test]
    fn test_build_schemas_param_empty() {
        let schemas: Vec<String> = vec![];
        let param = build_schemas_param(&schemas);
        assert!(param.is_empty());
    }
}
