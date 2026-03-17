//! MutatePlan types for PgREST
//!
//! Defines the plan types for INSERT, UPDATE, DELETE, and UPSERT operations.
//! Matches the Haskell `MutatePlan` data type.

use crate::api_request::types::{FieldName, Payload};
use crate::types::identifiers::QualifiedIdentifier;

use super::types::*;

// ==========================================================================
// MutatePlan -- top-level mutation plan
// ==========================================================================

/// A mutation plan
///
/// Matches the Haskell `MutatePlan` data type with three variants.
#[derive(Debug, Clone)]
pub enum MutatePlan {
    /// INSERT plan
    Insert(InsertPlan),
    /// UPDATE plan
    Update(UpdatePlan),
    /// DELETE plan
    Delete(DeletePlan),
}

impl MutatePlan {
    /// Get the target table identifier
    pub fn qi(&self) -> &QualifiedIdentifier {
        match self {
            MutatePlan::Insert(p) => &p.into,
            MutatePlan::Update(p) => &p.into,
            MutatePlan::Delete(p) => &p.from,
        }
    }

    /// Get the RETURNING columns
    pub fn returning(&self) -> &[CoercibleSelectField] {
        match self {
            MutatePlan::Insert(p) => &p.returning,
            MutatePlan::Update(p) => &p.returning,
            MutatePlan::Delete(p) => &p.returning,
        }
    }
}

// ==========================================================================
// Insert / Update / Delete plans
// ==========================================================================

/// INSERT plan details
#[derive(Debug, Clone)]
pub struct InsertPlan {
    /// Target table
    pub into: QualifiedIdentifier,
    /// Typed columns to insert
    pub columns: Vec<CoercibleField>,
    /// Request body
    pub body: Payload,
    /// ON CONFLICT handling
    pub on_conflict: Option<OnConflict>,
    /// WHERE clause for conditional insert
    pub where_: Vec<CoercibleLogicTree>,
    /// RETURNING columns
    pub returning: Vec<CoercibleSelectField>,
    /// Primary key columns
    pub pk_cols: Vec<FieldName>,
    /// Whether to apply column defaults
    pub apply_defaults: bool,
}

/// UPDATE plan details
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    /// Target table
    pub into: QualifiedIdentifier,
    /// Typed columns to update
    pub columns: Vec<CoercibleField>,
    /// Request body
    pub body: Payload,
    /// WHERE clause for filtering rows to update
    pub where_: Vec<CoercibleLogicTree>,
    /// RETURNING columns
    pub returning: Vec<CoercibleSelectField>,
    /// Whether to apply column defaults
    pub apply_defaults: bool,
}

/// DELETE plan details
#[derive(Debug, Clone)]
pub struct DeletePlan {
    /// Target table
    pub from: QualifiedIdentifier,
    /// WHERE clause for filtering rows to delete
    pub where_: Vec<CoercibleLogicTree>,
    /// RETURNING columns
    pub returning: Vec<CoercibleSelectField>,
}

/// ON CONFLICT clause for upsert
#[derive(Debug, Clone)]
pub struct OnConflict {
    /// Conflict target columns
    pub columns: Vec<FieldName>,
    /// Whether to merge duplicates (DO UPDATE) vs ignore (DO NOTHING)
    pub merge_duplicates: bool,
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_qi(name: &str) -> QualifiedIdentifier {
        QualifiedIdentifier::new("public", name)
    }

    #[test]
    fn test_mutate_plan_qi() {
        let insert = MutatePlan::Insert(InsertPlan {
            into: test_qi("users"),
            columns: vec![],
            body: Payload::RawJSON(bytes::Bytes::new()),
            on_conflict: None,
            where_: vec![],
            returning: vec![],
            pk_cols: vec![],
            apply_defaults: false,
        });
        assert_eq!(insert.qi().name.as_str(), "users");

        let update = MutatePlan::Update(UpdatePlan {
            into: test_qi("posts"),
            columns: vec![],
            body: Payload::RawJSON(bytes::Bytes::new()),
            where_: vec![],
            returning: vec![],
            apply_defaults: false,
        });
        assert_eq!(update.qi().name.as_str(), "posts");

        let delete = MutatePlan::Delete(DeletePlan {
            from: test_qi("comments"),
            where_: vec![],
            returning: vec![],
        });
        assert_eq!(delete.qi().name.as_str(), "comments");
    }

    #[test]
    fn test_on_conflict() {
        let oc = OnConflict {
            columns: vec!["id".into()],
            merge_duplicates: true,
        };
        assert!(oc.merge_duplicates);
        assert_eq!(oc.columns.len(), 1);
    }

    #[test]
    fn test_insert_plan_with_on_conflict() {
        let plan = InsertPlan {
            into: test_qi("users"),
            columns: vec![],
            body: Payload::RawJSON(bytes::Bytes::from("{}")),
            on_conflict: Some(OnConflict {
                columns: vec!["id".into()],
                merge_duplicates: true,
            }),
            where_: vec![],
            returning: vec![],
            pk_cols: vec!["id".into()],
            apply_defaults: true,
        };
        assert!(plan.on_conflict.is_some());
        assert!(plan.apply_defaults);
        assert_eq!(plan.pk_cols.len(), 1);
    }
}
