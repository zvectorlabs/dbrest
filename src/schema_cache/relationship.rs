//! Relationship types for schema cache
//!
//! This module defines types for representing PostgreSQL foreign key relationships
//! and computed (function-based) relationships.

use compact_str::CompactString;
use smallvec::SmallVec;

use crate::types::QualifiedIdentifier;

/// Foreign key relationship between two tables
///
/// Represents the relationship from one table to another via a foreign key constraint.
#[derive(Debug, Clone)]
pub struct Relationship {
    /// Source table (the table containing the FK columns)
    pub table: QualifiedIdentifier,
    /// Target table (the table being referenced)
    pub foreign_table: QualifiedIdentifier,
    /// Whether this is a self-referencing relationship
    pub is_self: bool,
    /// Relationship cardinality (M2O, O2M, O2O, M2M)
    pub cardinality: Cardinality,
    /// Whether the source table is a view
    pub table_is_view: bool,
    /// Whether the target table is a view
    pub foreign_table_is_view: bool,
}

impl Relationship {
    /// Check if this is a to-one relationship (M2O or O2O)
    ///
    /// Returns true if following this relationship yields at most one row.
    pub fn is_to_one(&self) -> bool {
        matches!(
            self.cardinality,
            Cardinality::M2O { .. } | Cardinality::O2O { .. }
        )
    }

    /// Check if this is a to-many relationship (O2M or M2M)
    pub fn is_to_many(&self) -> bool {
        matches!(
            self.cardinality,
            Cardinality::O2M { .. } | Cardinality::M2M(_)
        )
    }

    /// Get the constraint name for this relationship
    pub fn constraint_name(&self) -> &str {
        match &self.cardinality {
            Cardinality::M2O { constraint, .. } => constraint,
            Cardinality::O2M { constraint, .. } => constraint,
            Cardinality::O2O { constraint, .. } => constraint,
            Cardinality::M2M(j) => &j.constraint1,
        }
    }

    /// Get the column mappings for this relationship
    ///
    /// Returns pairs of (source_column, target_column).
    pub fn columns(&self) -> &[(CompactString, CompactString)] {
        match &self.cardinality {
            Cardinality::M2O { columns, .. } => columns,
            Cardinality::O2M { columns, .. } => columns,
            Cardinality::O2O { columns, .. } => columns,
            Cardinality::M2M(j) => &j.cols_source,
        }
    }

    /// Get the source column names
    pub fn source_columns(&self) -> impl Iterator<Item = &str> {
        self.columns().iter().map(|(src, _)| src.as_str())
    }

    /// Get the target column names
    pub fn target_columns(&self) -> impl Iterator<Item = &str> {
        self.columns().iter().map(|(_, tgt)| tgt.as_str())
    }

    /// Check if this relationship uses a specific column
    pub fn uses_column(&self, col_name: &str) -> bool {
        self.columns()
            .iter()
            .any(|(src, tgt)| src.as_str() == col_name || tgt.as_str() == col_name)
    }

    /// Check if this is a many-to-many relationship
    pub fn is_m2m(&self) -> bool {
        matches!(self.cardinality, Cardinality::M2M(_))
    }

    /// Get the junction table if this is an M2M relationship
    pub fn junction(&self) -> Option<&Junction> {
        match &self.cardinality {
            Cardinality::M2M(j) => Some(j),
            _ => None,
        }
    }

    /// Create the reverse direction of this relationship.
    ///
    /// Swaps `table` / `foreign_table` and flips cardinality:
    /// - M2O → O2M (and vice-versa)
    /// - O2O child → O2O parent (and vice-versa)
    ///
    /// Column pairs are swapped so `(src, tgt)` becomes `(tgt, src)`.
    pub fn reverse(&self) -> Self {
        let rev_cardinality = match &self.cardinality {
            Cardinality::M2O { constraint, columns } => Cardinality::O2M {
                constraint: constraint.clone(),
                columns: columns.iter().map(|(a, b)| (b.clone(), a.clone())).collect(),
            },
            Cardinality::O2M { constraint, columns } => Cardinality::M2O {
                constraint: constraint.clone(),
                columns: columns.iter().map(|(a, b)| (b.clone(), a.clone())).collect(),
            },
            Cardinality::O2O { constraint, columns, is_parent } => Cardinality::O2O {
                constraint: constraint.clone(),
                columns: columns.iter().map(|(a, b)| (b.clone(), a.clone())).collect(),
                is_parent: !is_parent,
            },
            Cardinality::M2M(j) => Cardinality::M2M(j.clone()), // M2M is symmetric
        };

        Relationship {
            table: self.foreign_table.clone(),
            foreign_table: self.table.clone(),
            is_self: self.is_self,
            cardinality: rev_cardinality,
            table_is_view: self.foreign_table_is_view,
            foreign_table_is_view: self.table_is_view,
        }
    }

    /// Check if this is an O2O relationship where we are the parent side
    pub fn is_o2o_parent(&self) -> bool {
        matches!(
            &self.cardinality,
            Cardinality::O2O { is_parent: true, .. }
        )
    }

    /// Check if this is an O2O relationship where we are the child side
    pub fn is_o2o_child(&self) -> bool {
        matches!(
            &self.cardinality,
            Cardinality::O2O {
                is_parent: false,
                ..
            }
        )
    }
}

/// Relationship cardinality
///
/// Describes the cardinality of a relationship between two tables.
#[derive(Debug, Clone)]
pub enum Cardinality {
    /// Many-to-One: the source table has an FK pointing to the target's PK
    ///
    /// Following this relationship from source yields at most one target row.
    M2O {
        /// Foreign key constraint name
        constraint: CompactString,
        /// Column mappings: (source_column, target_column)
        columns: SmallVec<[(CompactString, CompactString); 2]>,
    },

    /// One-to-Many: the source table's PK is referenced by the target's FK
    ///
    /// Following this relationship from source yields potentially many target rows.
    O2M {
        /// Foreign key constraint name (on the target table)
        constraint: CompactString,
        /// Column mappings: (source_column, target_column)
        columns: SmallVec<[(CompactString, CompactString); 2]>,
    },

    /// One-to-One: like M2O but the FK columns are also unique
    ///
    /// Following this relationship from either side yields at most one row.
    O2O {
        /// Foreign key constraint name
        constraint: CompactString,
        /// Column mappings: (source_column, target_column)
        columns: SmallVec<[(CompactString, CompactString); 2]>,
        /// Whether this is the parent side (referenced) or child side (referencing)
        is_parent: bool,
    },

    /// Many-to-Many: relationship via a junction table
    ///
    /// Both tables are connected through an intermediate junction table.
    M2M(Junction),
}

impl Cardinality {
    /// Get a short string representation of this cardinality
    pub fn as_str(&self) -> &'static str {
        match self {
            Cardinality::M2O { .. } => "M2O",
            Cardinality::O2M { .. } => "O2M",
            Cardinality::O2O { .. } => "O2O",
            Cardinality::M2M(_) => "M2M",
        }
    }
}

/// Junction table for many-to-many relationships
///
/// Represents the intermediate table that connects two tables in an M2M relationship.
#[derive(Debug, Clone)]
pub struct Junction {
    /// The junction table
    pub table: QualifiedIdentifier,
    /// FK constraint from junction to source table
    pub constraint1: CompactString,
    /// FK constraint from junction to target table
    pub constraint2: CompactString,
    /// Column mappings from junction to source: (junction_col, source_col)
    pub cols_source: SmallVec<[(CompactString, CompactString); 2]>,
    /// Column mappings from junction to target: (junction_col, target_col)
    pub cols_target: SmallVec<[(CompactString, CompactString); 2]>,
}

impl Junction {
    /// Get all junction table columns used in the relationship
    pub fn junction_columns(&self) -> impl Iterator<Item = &str> {
        self.cols_source
            .iter()
            .chain(self.cols_target.iter())
            .map(|(junc_col, _)| junc_col.as_str())
    }

    /// Get the source table column names
    pub fn source_columns(&self) -> impl Iterator<Item = &str> {
        self.cols_source.iter().map(|(_, src_col)| src_col.as_str())
    }

    /// Get the target table column names
    pub fn target_columns(&self) -> impl Iterator<Item = &str> {
        self.cols_target.iter().map(|(_, tgt_col)| tgt_col.as_str())
    }
}

/// Computed relationship (function-based)
///
/// A relationship defined by a function that takes a row from the source table
/// and returns related rows.
#[derive(Debug, Clone)]
pub struct ComputedRelationship {
    /// Source table
    pub table: QualifiedIdentifier,
    /// Function that computes the relationship
    pub function: QualifiedIdentifier,
    /// Target table (return type of the function)
    pub foreign_table: QualifiedIdentifier,
    /// Alias for the source table in the function context
    pub table_alias: QualifiedIdentifier,
    /// Whether this is a self-referencing relationship
    pub is_self: bool,
    /// Whether the function returns a single row
    pub single_row: bool,
}

impl ComputedRelationship {
    /// Check if this computed relationship returns multiple rows
    pub fn returns_set(&self) -> bool {
        !self.single_row
    }
}

/// Either a FK relationship or computed relationship
///
/// Used to represent any type of relationship in a unified way.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)] // Relationship is the common case, boxing hurts ergonomics
pub enum AnyRelationship {
    /// Standard foreign key relationship
    ForeignKey(Relationship),
    /// Function-based computed relationship
    Computed(ComputedRelationship),
}

impl AnyRelationship {
    /// Get the source table
    pub fn table(&self) -> &QualifiedIdentifier {
        match self {
            AnyRelationship::ForeignKey(r) => &r.table,
            AnyRelationship::Computed(r) => &r.table,
        }
    }

    /// Get the target/foreign table
    pub fn foreign_table(&self) -> &QualifiedIdentifier {
        match self {
            AnyRelationship::ForeignKey(r) => &r.foreign_table,
            AnyRelationship::Computed(r) => &r.foreign_table,
        }
    }

    /// Check if this is a self-referencing relationship
    pub fn is_self(&self) -> bool {
        match self {
            AnyRelationship::ForeignKey(r) => r.is_self,
            AnyRelationship::Computed(r) => r.is_self,
        }
    }

    /// Check if this relationship yields at most one row
    pub fn is_to_one(&self) -> bool {
        match self {
            AnyRelationship::ForeignKey(r) => r.is_to_one(),
            AnyRelationship::Computed(r) => r.single_row,
        }
    }

    /// Check if this is a foreign key relationship
    pub fn is_fk(&self) -> bool {
        matches!(self, AnyRelationship::ForeignKey(_))
    }

    /// Check if this is a computed relationship
    pub fn is_computed(&self) -> bool {
        matches!(self, AnyRelationship::Computed(_))
    }

    /// Get the foreign key relationship if this is one
    pub fn as_fk(&self) -> Option<&Relationship> {
        match self {
            AnyRelationship::ForeignKey(r) => Some(r),
            AnyRelationship::Computed(_) => None,
        }
    }

    /// Get the computed relationship if this is one
    pub fn as_computed(&self) -> Option<&ComputedRelationship> {
        match self {
            AnyRelationship::ForeignKey(_) => None,
            AnyRelationship::Computed(r) => Some(r),
        }
    }
}

impl From<Relationship> for AnyRelationship {
    fn from(r: Relationship) -> Self {
        AnyRelationship::ForeignKey(r)
    }
}

impl From<ComputedRelationship> for AnyRelationship {
    fn from(r: ComputedRelationship) -> Self {
        AnyRelationship::Computed(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;

    // ========================================================================
    // Relationship Tests
    // ========================================================================

    #[test]
    fn test_relationship_is_to_one_m2o() {
        let rel = test_relationship()
            .m2o("fk_user", &[("user_id", "id")])
            .build();
        assert!(rel.is_to_one());
        assert!(!rel.is_to_many());
    }

    #[test]
    fn test_relationship_is_to_one_o2o() {
        let rel = test_relationship()
            .o2o("fk_profile", &[("user_id", "id")], false)
            .build();
        assert!(rel.is_to_one());
        assert!(!rel.is_to_many());
    }

    #[test]
    fn test_relationship_is_to_many_o2m() {
        let rel = test_relationship()
            .o2m("fk_posts", &[("id", "user_id")])
            .build();
        assert!(!rel.is_to_one());
        assert!(rel.is_to_many());
    }

    #[test]
    fn test_relationship_is_to_many_m2m() {
        let junction = test_junction()
            .table("public", "user_roles")
            .cols_source(&[("user_id", "id")])
            .cols_target(&[("role_id", "id")])
            .build();

        let rel = test_relationship().m2m(junction).build();
        assert!(!rel.is_to_one());
        assert!(rel.is_to_many());
    }

    #[test]
    fn test_relationship_constraint_name() {
        let rel = test_relationship()
            .m2o("my_constraint", &[("fk_col", "pk_col")])
            .build();
        assert_eq!(rel.constraint_name(), "my_constraint");
    }

    #[test]
    fn test_relationship_columns() {
        let rel = test_relationship()
            .m2o("fk_test", &[("col_a", "col_b"), ("col_c", "col_d")])
            .build();

        let cols = rel.columns();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].0.as_str(), "col_a");
        assert_eq!(cols[0].1.as_str(), "col_b");
    }

    #[test]
    fn test_relationship_source_columns() {
        let rel = test_relationship()
            .m2o("fk", &[("src1", "tgt1"), ("src2", "tgt2")])
            .build();

        let sources: Vec<_> = rel.source_columns().collect();
        assert_eq!(sources, vec!["src1", "src2"]);
    }

    #[test]
    fn test_relationship_target_columns() {
        let rel = test_relationship()
            .m2o("fk", &[("src1", "tgt1"), ("src2", "tgt2")])
            .build();

        let targets: Vec<_> = rel.target_columns().collect();
        assert_eq!(targets, vec!["tgt1", "tgt2"]);
    }

    #[test]
    fn test_relationship_uses_column() {
        let rel = test_relationship()
            .m2o("fk", &[("user_id", "id")])
            .build();

        assert!(rel.uses_column("user_id"));
        assert!(rel.uses_column("id"));
        assert!(!rel.uses_column("name"));
    }

    #[test]
    fn test_relationship_is_m2m() {
        let junction = test_junction().build();
        let m2m_rel = test_relationship().m2m(junction).build();
        assert!(m2m_rel.is_m2m());

        let m2o_rel = test_relationship().m2o("fk", &[("a", "b")]).build();
        assert!(!m2o_rel.is_m2m());
    }

    #[test]
    fn test_relationship_junction() {
        let junction = test_junction()
            .table("public", "user_roles")
            .build();
        let rel = test_relationship().m2m(junction).build();

        let j = rel.junction().unwrap();
        assert_eq!(j.table.name.as_str(), "user_roles");
    }

    #[test]
    fn test_relationship_o2o_parent_child() {
        let parent_rel = test_relationship()
            .o2o("fk", &[("id", "user_id")], true)
            .build();
        assert!(parent_rel.is_o2o_parent());
        assert!(!parent_rel.is_o2o_child());

        let child_rel = test_relationship()
            .o2o("fk", &[("user_id", "id")], false)
            .build();
        assert!(!child_rel.is_o2o_parent());
        assert!(child_rel.is_o2o_child());
    }

    #[test]
    fn test_relationship_is_self() {
        let self_rel = test_relationship()
            .table("public", "employees")
            .foreign_table("public", "employees")
            .is_self(true)
            .build();
        assert!(self_rel.is_self);

        let normal_rel = test_relationship()
            .table("public", "posts")
            .foreign_table("public", "users")
            .is_self(false)
            .build();
        assert!(!normal_rel.is_self);
    }

    // ========================================================================
    // Cardinality Tests
    // ========================================================================

    #[test]
    fn test_cardinality_as_str() {
        assert_eq!(
            Cardinality::M2O {
                constraint: "fk".into(),
                columns: smallvec::smallvec![]
            }
            .as_str(),
            "M2O"
        );
        assert_eq!(
            Cardinality::O2M {
                constraint: "fk".into(),
                columns: smallvec::smallvec![]
            }
            .as_str(),
            "O2M"
        );
        assert_eq!(
            Cardinality::O2O {
                constraint: "fk".into(),
                columns: smallvec::smallvec![],
                is_parent: false
            }
            .as_str(),
            "O2O"
        );
        assert_eq!(Cardinality::M2M(test_junction().build()).as_str(), "M2M");
    }

    // ========================================================================
    // Junction Tests
    // ========================================================================

    #[test]
    fn test_junction_columns() {
        let junction = test_junction()
            .cols_source(&[("user_id", "id")])
            .cols_target(&[("role_id", "id")])
            .build();

        let junc_cols: Vec<_> = junction.junction_columns().collect();
        assert_eq!(junc_cols, vec!["user_id", "role_id"]);
    }

    #[test]
    fn test_junction_source_columns() {
        let junction = test_junction()
            .cols_source(&[("user_id", "id")])
            .build();

        let cols: Vec<_> = junction.source_columns().collect();
        assert_eq!(cols, vec!["id"]);
    }

    #[test]
    fn test_junction_target_columns() {
        let junction = test_junction()
            .cols_target(&[("role_id", "id")])
            .build();

        let cols: Vec<_> = junction.target_columns().collect();
        assert_eq!(cols, vec!["id"]);
    }

    // ========================================================================
    // ComputedRelationship Tests
    // ========================================================================

    #[test]
    fn test_computed_rel_returns_set() {
        let single_row = test_computed_rel().single_row(true).build();
        assert!(!single_row.returns_set());

        let multi_row = test_computed_rel().single_row(false).build();
        assert!(multi_row.returns_set());
    }

    // ========================================================================
    // AnyRelationship Tests
    // ========================================================================

    #[test]
    fn test_any_relationship_table() {
        let fk_rel: AnyRelationship = test_relationship()
            .table("api", "posts")
            .build()
            .into();

        assert_eq!(fk_rel.table().schema.as_str(), "api");
        assert_eq!(fk_rel.table().name.as_str(), "posts");

        let computed_rel: AnyRelationship = test_computed_rel()
            .table("api", "users")
            .build()
            .into();

        assert_eq!(computed_rel.table().schema.as_str(), "api");
        assert_eq!(computed_rel.table().name.as_str(), "users");
    }

    #[test]
    fn test_any_relationship_foreign_table() {
        let fk_rel: AnyRelationship = test_relationship()
            .foreign_table("api", "users")
            .build()
            .into();

        assert_eq!(fk_rel.foreign_table().name.as_str(), "users");
    }

    #[test]
    fn test_any_relationship_is_self() {
        let self_rel: AnyRelationship = test_relationship().is_self(true).build().into();
        assert!(self_rel.is_self());

        let computed_self: AnyRelationship = test_computed_rel().is_self(true).build().into();
        assert!(computed_self.is_self());
    }

    #[test]
    fn test_any_relationship_is_to_one() {
        let m2o: AnyRelationship = test_relationship()
            .m2o("fk", &[("a", "b")])
            .build()
            .into();
        assert!(m2o.is_to_one());

        let o2m: AnyRelationship = test_relationship()
            .o2m("fk", &[("a", "b")])
            .build()
            .into();
        assert!(!o2m.is_to_one());

        let computed_single: AnyRelationship = test_computed_rel().single_row(true).build().into();
        assert!(computed_single.is_to_one());

        let computed_multi: AnyRelationship = test_computed_rel().single_row(false).build().into();
        assert!(!computed_multi.is_to_one());
    }

    #[test]
    fn test_any_relationship_is_fk_computed() {
        let fk_rel: AnyRelationship = test_relationship().build().into();
        assert!(fk_rel.is_fk());
        assert!(!fk_rel.is_computed());

        let computed_rel: AnyRelationship = test_computed_rel().build().into();
        assert!(!computed_rel.is_fk());
        assert!(computed_rel.is_computed());
    }

    #[test]
    fn test_any_relationship_as_fk_computed() {
        let fk_rel: AnyRelationship = test_relationship().build().into();
        assert!(fk_rel.as_fk().is_some());
        assert!(fk_rel.as_computed().is_none());

        let computed_rel: AnyRelationship = test_computed_rel().build().into();
        assert!(computed_rel.as_fk().is_none());
        assert!(computed_rel.as_computed().is_some());
    }
}
