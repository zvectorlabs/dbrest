//! Schema Cache module
//!
//! The schema cache is the heart of PgREST. It introspects the PostgreSQL database
//! and caches:
//! - Tables/Views metadata
//! - Column information
//! - Foreign key relationships
//! - Functions/Procedures
//!
//! # Architecture
//!
//! The cache is immutable and wrapped in `ArcSwap` for lock-free reads and atomic
//! replacement during schema reload.
//!
//! ```text
//! ┌─────────────────────────────────────────────────┐
//! │                  SchemaCache                     │
//! ├─────────────────────────────────────────────────┤
//! │  tables: HashMap<QualifiedIdentifier, Table>    │
//! │  relationships: HashMap<..., Vec<Relationship>> │
//! │  routines: HashMap<QualifiedIdentifier, Vec>    │
//! │  timezones: HashSet<String>                     │
//! └─────────────────────────────────────────────────┘
//! ```

pub mod db;
pub mod media_handler;
pub mod queries;
pub mod relationship;
pub mod representations;
pub mod routine;
pub mod table;

// Re-export main types
pub use db::{ComputedFieldRow, DbIntrospector, RelationshipRow, RoutineRow, SqlxIntrospector, TableRow};
pub use media_handler::{MediaHandler, MediaHandlerMap, ResolvedHandler};
pub use relationship::{AnyRelationship, Cardinality, ComputedRelationship, Junction, Relationship};
pub use representations::{DataRepresentation, RepresentationsMap};
pub use routine::{PgType, ReturnType, Routine, RoutineParam, Volatility};
pub use table::{Column, ComputedField, Table};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::config::AppConfig;
use crate::error::Error;
use crate::types::QualifiedIdentifier;

/// Type alias for the tables map
pub type TablesMap = HashMap<QualifiedIdentifier, Table>;

/// Type alias for the relationships map
/// Key: (source_table, schema) -> list of relationships from that table
pub type RelationshipsMap = HashMap<(QualifiedIdentifier, String), Vec<AnyRelationship>>;

/// Type alias for the routines map
/// Key: QualifiedIdentifier -> list of overloaded functions
pub type RoutinesMap = HashMap<QualifiedIdentifier, Vec<Routine>>;

/// Immutable schema cache
///
/// This structure holds all introspected database metadata. It is designed to be
/// immutable and wrapped in `ArcSwap` for lock-free reads.
#[derive(Debug, Clone)]
pub struct SchemaCache {
    /// All tables and views by qualified name
    pub tables: Arc<TablesMap>,
    /// Relationships indexed by source table
    pub relationships: Arc<RelationshipsMap>,
    /// Functions/procedures indexed by qualified name
    pub routines: Arc<RoutinesMap>,
    /// Available PostgreSQL timezones
    pub timezones: Arc<HashSet<String>>,
    /// Data representation mappings
    pub representations: Arc<RepresentationsMap>,
    /// Media handler mappings
    pub media_handlers: Arc<MediaHandlerMap>,
}

impl Default for SchemaCache {
    fn default() -> Self {
        Self::empty()
    }
}

impl SchemaCache {
    /// Create an empty schema cache
    pub fn empty() -> Self {
        Self {
            tables: Arc::new(HashMap::new()),
            relationships: Arc::new(HashMap::new()),
            routines: Arc::new(HashMap::new()),
            timezones: Arc::new(HashSet::new()),
            representations: Arc::new(HashMap::new()),
            media_handlers: Arc::new(HashMap::new()),
        }
    }

    /// Load schema cache from database using the provided introspector
    pub async fn load<I: DbIntrospector>(
        introspector: &I,
        config: &AppConfig,
    ) -> Result<Self, Error> {
        let schemas = &config.db_schemas;
        
        tracing::info!("Loading schema cache for schemas: {:?}", schemas);
        
        // Combine exposed schemas with extra search path for computed fields query
        let mut all_schemas = config.db_schemas.clone();
        for extra_schema in &config.db_extra_search_path {
            if !all_schemas.contains(extra_schema) {
                all_schemas.push(extra_schema.clone());
            }
        }
        
        tracing::debug!("All schemas for computed fields query: {:?}", all_schemas);

        // Query all data concurrently
        let (tables_rows, rel_rows, routine_rows, computed_fields_rows, timezones) = tokio::try_join!(
            introspector.query_tables(schemas),
            introspector.query_relationships(),
            introspector.query_routines(schemas),
            introspector.query_computed_fields(&all_schemas),
            introspector.query_timezones(),
        )?;

        tracing::debug!(
            "Loaded: {} tables, {} relationships, {} routines, {} computed fields, {} timezones",
            tables_rows.len(),
            rel_rows.len(),
            routine_rows.len(),
            computed_fields_rows.len(),
            timezones.len()
        );

        // Build tables map
        let mut tables = HashMap::with_capacity(tables_rows.len());
        for row in tables_rows {
            let table = row.into_table()?;
            let qi = table.qi();
            tables.insert(qi.clone(), table);
        }

        // Group computed fields by table and attach them
        use crate::schema_cache::table::ComputedField;
        use crate::types::QualifiedIdentifier as QI;
        
        let mut attached_count = 0;
        let mut not_found_count = 0;
        
        for row in computed_fields_rows {
            let table_qi = QI::new(&row.table_schema, &row.table_name);
            if let Some(table) = tables.get_mut(&table_qi) {
                let function_qi = QI::new(&row.function_schema, &row.function_name);
                let computed_field = ComputedField {
                    function: function_qi,
                    return_type: row.return_type.into(),
                    returns_set: row.returns_set,
                };
                // Use function name as the key (not qualified, matching PostgREST behavior)
                table.computed_fields.insert(row.function_name.clone().into(), computed_field);
                tracing::trace!(
                    "Attached computed field '{}' to table {}.{}",
                    row.function_name,
                    row.table_schema,
                    row.table_name
                );
                attached_count += 1;
            } else {
                tracing::warn!(
                    "Computed field function {}.{} references non-existent table {}.{}",
                    row.function_schema,
                    row.function_name,
                    row.table_schema,
                    row.table_name
                );
                not_found_count += 1;
            }
        }
        
        tracing::debug!(
            "Attached {} computed fields to tables, {} referenced non-existent tables",
            attached_count,
            not_found_count
        );

        // Build relationships map — store both forward (M2O) and reverse (O2M)
        // directions so that resource embedding works in either direction.
        let mut relationships: RelationshipsMap = HashMap::new();
        for row in rel_rows {
            let rel = row.into_relationship();

            // Forward direction (M2O / O2O): keyed under the FK-holding table
            let fwd_key = (rel.table.clone(), rel.table.schema.to_string());
            let reverse = rel.reverse();
            relationships
                .entry(fwd_key)
                .or_default()
                .push(AnyRelationship::ForeignKey(rel));

            // Reverse direction (O2M / O2O-parent): keyed under the referenced table
            let rev_key = (reverse.table.clone(), reverse.table.schema.to_string());
            relationships
                .entry(rev_key)
                .or_default()
                .push(AnyRelationship::ForeignKey(reverse));
        }

        // Build routines map
        let mut routines: RoutinesMap = HashMap::new();
        for row in routine_rows {
            let routine = row.into_routine()?;
            let qi = routine.qi();
            routines.entry(qi).or_default().push(routine);
        }

        // Convert timezones to HashSet, ensuring UTC is always included
        let mut timezone_set: HashSet<String> = timezones.into_iter().collect();
        timezone_set.insert("UTC".to_string());
        
        Ok(Self {
            tables: Arc::new(tables),
            relationships: Arc::new(relationships),
            routines: Arc::new(routines),
            timezones: Arc::new(timezone_set),
            representations: Arc::new(HashMap::new()),
            media_handlers: Arc::new(HashMap::new()),
        })
    }

    /// Get a table by qualified identifier
    pub fn get_table(&self, qi: &QualifiedIdentifier) -> Option<&Table> {
        self.tables.get(qi)
    }

    /// Get a table by schema and name
    pub fn get_table_by_name(&self, schema: &str, name: &str) -> Option<&Table> {
        let qi = QualifiedIdentifier::new(schema, name);
        self.tables.get(&qi)
    }

    /// Find relationships from a source table
    pub fn find_relationships(&self, source: &QualifiedIdentifier) -> &[AnyRelationship] {
        let key = (source.clone(), source.schema.to_string());
        self.relationships
            .get(&key)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Find relationships from source to a specific target
    pub fn find_relationships_to(
        &self,
        source: &QualifiedIdentifier,
        target_name: &str,
    ) -> Vec<&AnyRelationship> {
        self.find_relationships(source)
            .iter()
            .filter(|r| r.foreign_table().name.as_str() == target_name)
            .collect()
    }

    /// Get a routine by qualified identifier
    pub fn get_routines(&self, qi: &QualifiedIdentifier) -> Option<&[Routine]> {
        self.routines.get(qi).map(|v| v.as_slice())
    }

    /// Get a routine by schema and name
    pub fn get_routines_by_name(&self, schema: &str, name: &str) -> Option<&[Routine]> {
        let qi = QualifiedIdentifier::new(schema, name);
        self.routines.get(&qi).map(|v| v.as_slice())
    }

    /// Check if a timezone is valid
    pub fn is_valid_timezone(&self, tz: &str) -> bool {
        self.timezones.contains(tz)
    }

    /// Get the number of tables
    pub fn table_count(&self) -> usize {
        self.tables.len()
    }

    /// Get the number of relationships
    pub fn relationship_count(&self) -> usize {
        self.relationships.values().map(|v| v.len()).sum()
    }

    /// Get the number of routines
    pub fn routine_count(&self) -> usize {
        self.routines.values().map(|v| v.len()).sum()
    }

    /// Get a summary string for logging
    pub fn summary(&self) -> String {
        format!(
            "{} tables, {} relationships, {} routines, {} timezones",
            self.table_count(),
            self.relationship_count(),
            self.routine_count(),
            self.timezones.len(),
        )
    }

    /// Iterate over all tables
    pub fn tables_iter(&self) -> impl Iterator<Item = (&QualifiedIdentifier, &Table)> {
        self.tables.iter()
    }

    /// Iterate over all tables in a specific schema
    pub fn tables_in_schema(&self, schema: &str) -> impl Iterator<Item = &Table> {
        self.tables
            .values()
            .filter(move |t| t.schema.as_str() == schema)
    }
}

/// Schema cache holder with atomic swap capability
///
/// Wraps the schema cache in `ArcSwap` for lock-free reads and atomic updates.
#[derive(Debug)]
pub struct SchemaCacheHolder {
    inner: ArcSwap<Option<SchemaCache>>,
}

impl Default for SchemaCacheHolder {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaCacheHolder {
    /// Create a new empty holder
    pub fn new() -> Self {
        Self {
            inner: ArcSwap::from_pointee(None),
        }
    }

    /// Create a holder with an initial cache
    pub fn with_cache(cache: SchemaCache) -> Self {
        Self {
            inner: ArcSwap::from_pointee(Some(cache)),
        }
    }

    /// Get a reference to the current cache
    ///
    /// Returns None if the cache hasn't been loaded yet.
    pub fn get(&self) -> Option<arc_swap::Guard<Arc<Option<SchemaCache>>>> {
        let guard = self.inner.load();
        if guard.is_some() {
            Some(guard)
        } else {
            None
        }
    }

    /// Replace the cache with a new one
    pub fn replace(&self, cache: SchemaCache) {
        self.inner.store(Arc::new(Some(cache)));
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.inner.store(Arc::new(None));
    }

    /// Check if the cache is loaded
    pub fn is_loaded(&self) -> bool {
        self.inner.load().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;

    fn create_test_cache() -> SchemaCache {
        let mut tables = HashMap::new();

        let users_table = test_table()
            .schema("public")
            .name("users")
            .pk_col("id")
            .column(test_column().name("id").data_type("integer").build())
            .column(test_column().name("name").data_type("text").build())
            .build();

        let posts_table = test_table()
            .schema("public")
            .name("posts")
            .pk_col("id")
            .column(test_column().name("id").data_type("integer").build())
            .column(test_column().name("user_id").data_type("integer").build())
            .column(test_column().name("title").data_type("text").build())
            .build();

        tables.insert(users_table.qi(), users_table);
        tables.insert(posts_table.qi(), posts_table);

        // Create relationship posts -> users
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

        // Create routine
        let routine = test_routine()
            .schema("public")
            .name("get_user")
            .param(test_param().name("user_id").pg_type("integer").build())
            .returns_setof_composite("public", "users")
            .build();

        let mut routines = HashMap::new();
        routines.insert(routine.qi(), vec![routine]);

        let mut timezones = HashSet::new();
        timezones.insert("UTC".to_string());
        timezones.insert("America/New_York".to_string());

        SchemaCache {
            tables: Arc::new(tables),
            relationships: Arc::new(relationships),
            routines: Arc::new(routines),
            timezones: Arc::new(timezones),
            representations: Arc::new(HashMap::new()),
            media_handlers: Arc::new(HashMap::new()),
        }
    }

    #[test]
    fn test_schema_cache_empty() {
        let cache = SchemaCache::empty();
        assert_eq!(cache.table_count(), 0);
        assert_eq!(cache.relationship_count(), 0);
        assert_eq!(cache.routine_count(), 0);
    }

    #[test]
    fn test_schema_cache_get_table() {
        let cache = create_test_cache();

        let qi = QualifiedIdentifier::new("public", "users");
        let table = cache.get_table(&qi).unwrap();
        assert_eq!(table.name.as_str(), "users");
    }

    #[test]
    fn test_schema_cache_get_table_by_name() {
        let cache = create_test_cache();

        let table = cache.get_table_by_name("public", "posts").unwrap();
        assert_eq!(table.name.as_str(), "posts");
        assert!(table.has_pk());
    }

    #[test]
    fn test_schema_cache_get_table_not_found() {
        let cache = create_test_cache();

        let qi = QualifiedIdentifier::new("public", "nonexistent");
        assert!(cache.get_table(&qi).is_none());
    }

    #[test]
    fn test_schema_cache_find_relationships() {
        let cache = create_test_cache();

        let source = QualifiedIdentifier::new("public", "posts");
        let rels = cache.find_relationships(&source);
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].foreign_table().name.as_str(), "users");
    }

    #[test]
    fn test_schema_cache_find_relationships_to() {
        let cache = create_test_cache();

        let source = QualifiedIdentifier::new("public", "posts");
        let rels = cache.find_relationships_to(&source, "users");
        assert_eq!(rels.len(), 1);

        let rels = cache.find_relationships_to(&source, "nonexistent");
        assert!(rels.is_empty());
    }

    #[test]
    fn test_schema_cache_get_routines() {
        let cache = create_test_cache();

        let qi = QualifiedIdentifier::new("public", "get_user");
        let routines = cache.get_routines(&qi).unwrap();
        assert_eq!(routines.len(), 1);
        assert!(routines[0].returns_set());
    }

    #[test]
    fn test_schema_cache_get_routines_by_name() {
        let cache = create_test_cache();

        let routines = cache.get_routines_by_name("public", "get_user").unwrap();
        assert_eq!(routines.len(), 1);
    }

    #[test]
    fn test_schema_cache_is_valid_timezone() {
        let cache = create_test_cache();

        assert!(cache.is_valid_timezone("UTC"));
        assert!(cache.is_valid_timezone("America/New_York"));
        assert!(!cache.is_valid_timezone("Invalid/Zone"));
    }

    #[test]
    fn test_schema_cache_counts() {
        let cache = create_test_cache();

        assert_eq!(cache.table_count(), 2);
        assert_eq!(cache.relationship_count(), 1);
        assert_eq!(cache.routine_count(), 1);
    }

    #[test]
    fn test_schema_cache_summary() {
        let cache = create_test_cache();

        let summary = cache.summary();
        assert!(summary.contains("2 tables"));
        assert!(summary.contains("1 relationships"));
        assert!(summary.contains("1 routines"));
    }

    #[test]
    fn test_schema_cache_tables_iter() {
        let cache = create_test_cache();

        let table_names: Vec<_> = cache
            .tables_iter()
            .map(|(_, t)| t.name.as_str())
            .collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"posts"));
    }

    #[test]
    fn test_schema_cache_tables_in_schema() {
        let cache = create_test_cache();

        let public_tables: Vec<_> = cache.tables_in_schema("public").collect();
        assert_eq!(public_tables.len(), 2);

        let other_tables: Vec<_> = cache.tables_in_schema("other").collect();
        assert!(other_tables.is_empty());
    }

    // ========================================================================
    // SchemaCacheHolder Tests
    // ========================================================================

    #[test]
    fn test_schema_cache_holder_new() {
        let holder = SchemaCacheHolder::new();
        assert!(!holder.is_loaded());
        assert!(holder.get().is_none());
    }

    #[test]
    fn test_schema_cache_holder_with_cache() {
        let cache = create_test_cache();
        let holder = SchemaCacheHolder::with_cache(cache);
        assert!(holder.is_loaded());
        assert!(holder.get().is_some());
    }

    #[test]
    fn test_schema_cache_holder_replace() {
        let holder = SchemaCacheHolder::new();
        assert!(!holder.is_loaded());

        let cache = create_test_cache();
        holder.replace(cache);
        assert!(holder.is_loaded());
    }

    #[test]
    fn test_schema_cache_holder_clear() {
        let cache = create_test_cache();
        let holder = SchemaCacheHolder::with_cache(cache);
        assert!(holder.is_loaded());

        holder.clear();
        assert!(!holder.is_loaded());
    }

    // ========================================================================
    // Mock-based Tests
    // ========================================================================

    #[tokio::test]
    async fn test_schema_cache_load_with_mock() {
        use db::MockDbIntrospector;

        let mut mock = MockDbIntrospector::new();

        // Set up mock expectations
        mock.expect_query_tables().returning(|_| {
            Ok(vec![TableRow {
                table_schema: "public".to_string(),
                table_name: "test_table".to_string(),
                table_description: None,
                is_view: false,
                insertable: true,
                updatable: true,
                deletable: true,
                readable: true,
                pk_cols: vec!["id".to_string()],
                columns_json: r#"[{"name":"id","description":null,"nullable":false,"data_type":"integer","nominal_type":"integer","max_length":null,"default":null,"enum_values":[]}]"#.to_string(),
            }])
        });

        mock.expect_query_relationships().returning(|| Ok(vec![]));
        mock.expect_query_routines().returning(|_| Ok(vec![]));
        mock.expect_query_computed_fields().returning(|_| Ok(vec![]));
        mock.expect_query_timezones()
            .returning(|| Ok(vec!["UTC".to_string()]));

        let config = AppConfig::default();
        let cache = SchemaCache::load(&mock, &config).await.unwrap();

        assert_eq!(cache.table_count(), 1);
        let table = cache.get_table_by_name("public", "test_table").unwrap();
        assert!(table.has_pk());
    }

    #[tokio::test]
    async fn test_schema_cache_load_with_relationships() {
        use db::MockDbIntrospector;

        let mut mock = MockDbIntrospector::new();

        mock.expect_query_tables().returning(|_| {
            Ok(vec![
                TableRow {
                    table_schema: "public".to_string(),
                    table_name: "users".to_string(),
                    table_description: None,
                    is_view: false,
                    insertable: true,
                    updatable: true,
                    deletable: true,
                    readable: true,
                    pk_cols: vec!["id".to_string()],
                    columns_json: r#"[{"name":"id","description":null,"nullable":false,"data_type":"integer","nominal_type":"integer","max_length":null,"default":null,"enum_values":[]}]"#.to_string(),
                },
                TableRow {
                    table_schema: "public".to_string(),
                    table_name: "posts".to_string(),
                    table_description: None,
                    is_view: false,
                    insertable: true,
                    updatable: true,
                    deletable: true,
                    readable: true,
                    pk_cols: vec!["id".to_string()],
                    columns_json: r#"[{"name":"id","description":null,"nullable":false,"data_type":"integer","nominal_type":"integer","max_length":null,"default":null,"enum_values":[]},{"name":"user_id","description":null,"nullable":false,"data_type":"integer","nominal_type":"integer","max_length":null,"default":null,"enum_values":[]}]"#.to_string(),
                },
            ])
        });

        mock.expect_query_relationships().returning(|| {
            Ok(vec![RelationshipRow {
                table_schema: "public".to_string(),
                table_name: "posts".to_string(),
                foreign_table_schema: "public".to_string(),
                foreign_table_name: "users".to_string(),
                is_self: false,
                constraint_name: "fk_posts_user".to_string(),
                cols_and_fcols: vec![("user_id".to_string(), "id".to_string())],
                one_to_one: false,
            }])
        });

        mock.expect_query_routines().returning(|_| Ok(vec![]));
        mock.expect_query_computed_fields().returning(|_| Ok(vec![]));
        mock.expect_query_timezones().returning(|| Ok(vec![]));

        let config = AppConfig::default();
        let cache = SchemaCache::load(&mock, &config).await.unwrap();

        assert_eq!(cache.table_count(), 2);
        // 2 relationships: forward M2O (posts→users) + reverse O2M (users→posts)
        assert_eq!(cache.relationship_count(), 2);

        // Forward: posts → users (M2O)
        let source = QualifiedIdentifier::new("public", "posts");
        let rels = cache.find_relationships(&source);
        assert_eq!(rels.len(), 1);
        assert!(rels[0].is_to_one()); // M2O

        // Reverse: users → posts (O2M)
        let source_rev = QualifiedIdentifier::new("public", "users");
        let rels_rev = cache.find_relationships(&source_rev);
        assert_eq!(rels_rev.len(), 1);
        assert!(!rels_rev[0].is_to_one()); // O2M is not to-one
    }
}
