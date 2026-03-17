//! ReadPlan types for PgREST
//!
//! Defines the ReadPlan struct and ReadPlanTree (rose tree) for representing
//! query plans for reading data from the database, including embedded relations.

use compact_str::CompactString;

use crate::api_request::range::Range;
use crate::api_request::types::{Alias, FieldName, Hint, JoinType};
use crate::schema_cache::relationship::AnyRelationship;
use crate::types::identifiers::QualifiedIdentifier;

use super::types::*;

/// Alias for node names in the plan tree
pub type NodeName = CompactString;

// ==========================================================================
// ReadPlanTree -- rose tree of read plans
// ==========================================================================

/// A tree of read plans (rose tree)
///
/// Each node represents a table/view to read from, with children
/// representing embedded (joined) relations.
#[derive(Debug, Clone)]
pub struct ReadPlanTree {
    /// This node's read plan
    pub node: ReadPlan,
    /// Child read plans (embedded relations)
    pub forest: Vec<ReadPlanTree>,
}

impl ReadPlanTree {
    /// Create a new tree with no children
    pub fn leaf(node: ReadPlan) -> Self {
        Self {
            node,
            forest: Vec::new(),
        }
    }

    /// Create a tree with children
    pub fn with_children(node: ReadPlan, forest: Vec<ReadPlanTree>) -> Self {
        Self { node, forest }
    }

    /// Get a mutable reference to the root node
    pub fn node_mut(&mut self) -> &mut ReadPlan {
        &mut self.node
    }

    /// Get a reference to the children
    pub fn children(&self) -> &[ReadPlanTree] {
        &self.forest
    }

    /// Get mutable references to the children
    pub fn children_mut(&mut self) -> &mut Vec<ReadPlanTree> {
        &mut self.forest
    }

    /// Depth-first iteration over all nodes
    pub fn iter(&self) -> ReadPlanTreeIter<'_> {
        ReadPlanTreeIter { stack: vec![self] }
    }

    /// Get the total number of nodes in the tree
    pub fn node_count(&self) -> usize {
        1 + self.forest.iter().map(|c| c.node_count()).sum::<usize>()
    }

    /// Get the maximum depth of the tree
    pub fn max_depth(&self) -> usize {
        if self.forest.is_empty() {
            self.node.depth
        } else {
            self.forest.iter().map(|c| c.max_depth()).max().unwrap_or(0)
        }
    }
}

/// Depth-first iterator over a ReadPlanTree
pub struct ReadPlanTreeIter<'a> {
    stack: Vec<&'a ReadPlanTree>,
}

impl<'a> Iterator for ReadPlanTreeIter<'a> {
    type Item = &'a ReadPlan;

    fn next(&mut self) -> Option<Self::Item> {
        let tree = self.stack.pop()?;
        // Push children in reverse order so leftmost child is processed first
        for child in tree.forest.iter().rev() {
            self.stack.push(child);
        }
        Some(&tree.node)
    }
}

// ==========================================================================
// JoinCondition
// ==========================================================================

/// A join condition between parent and child in the plan tree
#[derive(Debug, Clone)]
pub struct JoinCondition {
    /// (table, column) on the parent side
    pub parent: (QualifiedIdentifier, FieldName),
    /// (table, column) on the child side
    pub child: (QualifiedIdentifier, FieldName),
}

// ==========================================================================
// ReadPlan -- plan for a single table/view
// ==========================================================================

/// A read plan for a single table/view
///
/// Matches the Haskell `ReadPlan` data type. The root node has `depth=0`
/// and `rel_to_parent=None`; child nodes represent embedded relations.
#[derive(Debug, Clone)]
pub struct ReadPlan {
    /// Fields to select
    pub select: Vec<CoercibleSelectField>,
    /// Table/view to read from
    pub from: QualifiedIdentifier,
    /// Optional alias for the FROM clause
    pub from_alias: Option<Alias>,
    /// WHERE conditions (logic trees)
    pub where_: Vec<CoercibleLogicTree>,
    /// ORDER BY terms
    pub order: Vec<CoercibleOrderTerm>,
    /// Range (LIMIT/OFFSET)
    pub range: Range,
    /// Node name in the plan tree (usually the relation name)
    pub rel_name: NodeName,
    /// Relationship to parent node (None for root)
    pub rel_to_parent: Option<AnyRelationship>,
    /// Join conditions to parent
    pub rel_join_conds: Vec<JoinCondition>,
    /// Alias for the relation in the join
    pub rel_alias: Option<Alias>,
    /// Aggregate alias for subquery
    pub rel_agg_alias: Alias,
    /// Disambiguation hint
    pub rel_hint: Option<Hint>,
    /// Join type (INNER/LEFT)
    pub rel_join_type: Option<JoinType>,
    /// Spread type if this is a spread relation
    pub rel_spread: Option<SpreadType>,
    /// How this relation appears in the parent's select
    pub rel_select: Vec<RelSelectField>,
    /// Depth in the tree (0 for root)
    pub depth: usize,
}

impl ReadPlan {
    /// Create a new root read plan for a table
    pub fn root(qi: QualifiedIdentifier) -> Self {
        let name = qi.name.clone();
        Self {
            select: Vec::new(),
            from: qi,
            from_alias: None,
            where_: Vec::new(),
            order: Vec::new(),
            range: Range::all(),
            rel_name: name,
            rel_to_parent: None,
            rel_join_conds: Vec::new(),
            rel_alias: None,
            rel_agg_alias: CompactString::from("pgrst_agg"),
            rel_hint: None,
            rel_join_type: None,
            rel_spread: None,
            rel_select: Vec::new(),
            depth: 0,
        }
    }

    /// Create a child read plan for an embedded relation
    pub fn child(qi: QualifiedIdentifier, rel_name: NodeName, depth: usize) -> Self {
        Self {
            select: Vec::new(),
            from: qi,
            from_alias: None,
            where_: Vec::new(),
            order: Vec::new(),
            range: Range::all(),
            rel_name,
            rel_to_parent: None,
            rel_join_conds: Vec::new(),
            rel_alias: None,
            rel_agg_alias: CompactString::from(format!("pgrst_agg_{}", depth)),
            rel_hint: None,
            rel_join_type: None,
            rel_spread: None,
            rel_select: Vec::new(),
            depth,
        }
    }
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
    fn test_read_plan_root() {
        let plan = ReadPlan::root(test_qi("users"));
        assert_eq!(plan.from.name.as_str(), "users");
        assert_eq!(plan.depth, 0);
        assert!(plan.rel_to_parent.is_none());
        assert_eq!(plan.rel_name.as_str(), "users");
    }

    #[test]
    fn test_read_plan_child() {
        let plan = ReadPlan::child(test_qi("posts"), "posts".into(), 1);
        assert_eq!(plan.depth, 1);
        assert_eq!(plan.rel_agg_alias.as_str(), "pgrst_agg_1");
    }

    #[test]
    fn test_read_plan_tree_leaf() {
        let tree = ReadPlanTree::leaf(ReadPlan::root(test_qi("users")));
        assert_eq!(tree.node_count(), 1);
        assert!(tree.children().is_empty());
    }

    #[test]
    fn test_read_plan_tree_with_children() {
        let root = ReadPlan::root(test_qi("users"));
        let child1 = ReadPlanTree::leaf(ReadPlan::child(test_qi("posts"), "posts".into(), 1));
        let child2 = ReadPlanTree::leaf(ReadPlan::child(test_qi("comments"), "comments".into(), 1));

        let tree = ReadPlanTree::with_children(root, vec![child1, child2]);
        assert_eq!(tree.node_count(), 3);
        assert_eq!(tree.children().len(), 2);
    }

    #[test]
    fn test_read_plan_tree_nested() {
        let grandchild = ReadPlanTree::leaf(ReadPlan::child(test_qi("tags"), "tags".into(), 2));
        let child = ReadPlanTree::with_children(
            ReadPlan::child(test_qi("posts"), "posts".into(), 1),
            vec![grandchild],
        );
        let tree = ReadPlanTree::with_children(ReadPlan::root(test_qi("users")), vec![child]);

        assert_eq!(tree.node_count(), 3);
        assert_eq!(tree.max_depth(), 2);
    }

    #[test]
    fn test_read_plan_tree_iter() {
        let child = ReadPlanTree::leaf(ReadPlan::child(test_qi("posts"), "posts".into(), 1));
        let tree = ReadPlanTree::with_children(ReadPlan::root(test_qi("users")), vec![child]);

        let names: Vec<&str> = tree.iter().map(|p| p.rel_name.as_str()).collect();
        assert_eq!(names, vec!["users", "posts"]);
    }

    #[test]
    fn test_join_condition() {
        let jc = JoinCondition {
            parent: (test_qi("users"), "id".into()),
            child: (test_qi("posts"), "user_id".into()),
        };
        assert_eq!(jc.parent.1.as_str(), "id");
        assert_eq!(jc.child.1.as_str(), "user_id");
    }
}
