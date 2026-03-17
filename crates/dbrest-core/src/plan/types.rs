//! Coercible types for the plan module
//!
//! These types represent "resolved" versions of the api_request types,
//! where field names have been resolved against the schema cache to include
//! type information, transforms, and other metadata needed for SQL generation.

use compact_str::CompactString;

use crate::api_request::types::{
    AggregateFunction, Alias, Cast, FieldName, JsonPath, LogicOperator, OpExpr, OrderDirection,
    OrderNulls,
};
use crate::schema_cache::representations::DataRepresentation;
use crate::types::QualifiedIdentifier;

// ==========================================================================
// Type aliases
// ==========================================================================

/// A transformer procedure name (PostgreSQL function)
pub type TransformerProc = CompactString;

/// Language for tsvector conversion (e.g., "english")
pub type TsVectorLanguage = CompactString;

// ==========================================================================
// CoercibleField -- resolved field with type information
// ==========================================================================

/// A field that has been resolved against the schema cache
///
/// Contains type information, transforms, and other metadata that
/// the query builder needs to generate correct SQL.
#[derive(Debug, Clone)]
pub struct CoercibleField {
    /// Field name (column name)
    pub name: FieldName,
    /// JSON path operations on the field
    pub json_path: JsonPath,
    /// Whether to convert the output to JSON
    pub to_json: bool,
    /// Optional tsvector language for full-text search
    pub to_tsvector: Option<TsVectorLanguage>,
    /// The intermediate representation type (for casting)
    pub ir_type: Option<CompactString>,
    /// The base database type of this field
    pub base_type: Option<CompactString>,
    /// Optional data representation transform
    pub transform: Option<DataRepresentation>,
    /// Default value expression
    pub default: Option<CompactString>,
    /// Whether this represents the full row (*)
    pub full_row: bool,
    /// Whether this is a computed field (function call)
    pub is_computed: bool,
    /// Function qualified identifier for computed fields
    pub computed_function: Option<QualifiedIdentifier>,
}

impl CoercibleField {
    /// Create an unknown field (when the column is not found in schema cache)
    pub fn unknown(name: FieldName, json_path: JsonPath) -> Self {
        Self {
            name,
            json_path,
            to_json: false,
            to_tsvector: None,
            ir_type: None,
            base_type: None,
            transform: None,
            default: None,
            full_row: false,
            is_computed: false,
            computed_function: None,
        }
    }

    /// Create a field for a known column with a resolved base type
    pub fn from_column(name: FieldName, json_path: JsonPath, base_type: CompactString) -> Self {
        Self {
            name,
            json_path,
            to_json: false,
            to_tsvector: None,
            ir_type: Some(base_type.clone()),
            base_type: Some(base_type),
            transform: None,
            default: None,
            full_row: false,
            is_computed: false,
            computed_function: None,
        }
    }

    /// Create a field for a computed field (function call)
    pub fn from_computed_field(
        name: FieldName,
        json_path: JsonPath,
        function: QualifiedIdentifier,
        return_type: CompactString,
    ) -> Self {
        Self {
            name,
            json_path,
            to_json: false,
            to_tsvector: None,
            ir_type: Some(return_type.clone()),
            base_type: Some(return_type),
            transform: None,
            default: None,
            full_row: false,
            is_computed: true,
            computed_function: Some(function),
        }
    }

    /// Create a full-row field (*)
    pub fn full_row() -> Self {
        Self {
            name: "*".into(),
            json_path: Default::default(),
            to_json: false,
            to_tsvector: None,
            ir_type: None,
            base_type: None,
            transform: None,
            default: None,
            full_row: true,
            is_computed: false,
            computed_function: None,
        }
    }

    /// Set the to_json flag based on column type and JSON path presence
    pub fn with_to_json(mut self, column: Option<&crate::schema_cache::table::Column>) -> Self {
        // Only set to_json = true when JSON path is present
        if !self.json_path.is_empty() && !self.is_computed {
            if let Some(col) = column {
                // json/jsonb columns don't need wrapper
                if col.is_json_type() {
                    self.to_json = false;
                } else if col.is_composite_type() || col.is_array_type() {
                    // composites and arrays need wrapper
                    self.to_json = true;
                } else {
                    // For unknown types with JSON path, default to true (will wrap)
                    self.to_json = true;
                }
            } else {
                // Unknown column with JSON path - default to true
                self.to_json = true;
            }
        } else {
            // No JSON path or computed field - no wrapper needed
            self.to_json = false;
        }
        self
    }
}

// ==========================================================================
// CoercibleFilter -- filter with resolved field types
// ==========================================================================

/// A filter with resolved field types
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum CoercibleFilter {
    /// Standard filter with a coercible field and operator expression
    Filter {
        field: CoercibleField,
        op_expr: OpExpr,
    },
    /// Null embed filter (for checking if an embedded relation is null)
    /// The bool indicates negation; the FieldName is the embed name.
    NullEmbed(bool, FieldName),
}

// ==========================================================================
// CoercibleLogicTree -- logic tree with resolved field types
// ==========================================================================

/// A logic tree with resolved field types
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum CoercibleLogicTree {
    /// Logic expression: negated, operator, children
    Expr(bool, LogicOperator, Vec<CoercibleLogicTree>),
    /// Leaf statement (a single filter)
    Stmnt(CoercibleFilter),
}

// ==========================================================================
// CoercibleOrderTerm -- order term with resolved field types
// ==========================================================================

/// An order term with resolved field types
#[derive(Debug, Clone)]
pub enum CoercibleOrderTerm {
    /// Order on a resolved field
    Term {
        field: CoercibleField,
        direction: Option<OrderDirection>,
        nulls: Option<OrderNulls>,
    },
    /// Order on a field within an embedded relation
    RelationTerm {
        relation: FieldName,
        rel_term: CoercibleField,
        direction: Option<OrderDirection>,
        nulls: Option<OrderNulls>,
    },
}

// ==========================================================================
// CoercibleSelectField -- select field with resolved type information
// ==========================================================================

/// A select field with resolved type information
#[derive(Debug, Clone)]
pub struct CoercibleSelectField {
    /// The resolved field
    pub field: CoercibleField,
    /// Optional aggregate function
    pub agg_function: Option<AggregateFunction>,
    /// Cast for the aggregate result
    pub agg_cast: Option<Cast>,
    /// Cast for the field itself
    pub cast: Option<Cast>,
    /// Alias for this select field
    pub alias: Option<Alias>,
}

// ==========================================================================
// Relation select types -- how embedded relations appear in output
// ==========================================================================

/// How to embed a relation in JSON
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelJsonEmbedMode {
    /// Embed as a JSON object
    JsonObject,
    /// Embed as a JSON array
    JsonArray,
}

/// A relation select field (how embedded relations appear in the parent's output)
#[derive(Debug, Clone)]
pub enum RelSelectField {
    /// Embed as JSON (object or array)
    JsonEmbed {
        /// The relation name in the output
        sel_name: FieldName,
        /// The aggregate alias
        agg_alias: Alias,
        /// Whether to embed as object or array
        embed_mode: RelJsonEmbedMode,
        /// Whether to use empty JSON when no rows match
        empty_embed: bool,
    },
    /// Spread the relation's columns into the parent
    Spread {
        /// Spread select field details
        spread_sel: SpreadSelectField,
        /// The aggregate alias
        agg_alias: Alias,
    },
}

/// Details for a spread relation select
#[derive(Debug, Clone)]
pub struct SpreadSelectField {
    /// Column name in the output
    pub sel_name: FieldName,
    /// Optional aggregate function
    pub sel_agg_function: Option<AggregateFunction>,
    /// Cast for the aggregate result
    pub sel_agg_cast: Option<Cast>,
    /// Alias for this spread field
    pub sel_alias: Option<Alias>,
}

/// How a spread relation handles multiple rows
#[derive(Debug, Clone)]
pub enum SpreadType {
    /// Spread a to-one relation (single row)
    ToOneSpread,
    /// Spread a to-many relation (multiple rows)
    ToManySpread {
        /// Extra select fields for the spread
        extra_select: Vec<SpreadSelectField>,
        /// Order terms for the spread
        order: Vec<CoercibleOrderTerm>,
    },
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use smallvec::SmallVec;

    #[test]
    fn test_coercible_field_unknown() {
        let f = CoercibleField::unknown("name".into(), SmallVec::new());
        assert_eq!(f.name.as_str(), "name");
        assert!(f.json_path.is_empty());
        assert!(f.ir_type.is_none());
        assert!(f.base_type.is_none());
        assert!(!f.full_row);
    }

    #[test]
    fn test_coercible_field_from_column() {
        let f = CoercibleField::from_column("age".into(), SmallVec::new(), "integer".into());
        assert_eq!(f.name.as_str(), "age");
        assert_eq!(f.base_type.as_deref(), Some("integer"));
        assert_eq!(f.ir_type.as_deref(), Some("integer"));
        assert!(!f.full_row);
    }

    #[test]
    fn test_coercible_field_full_row() {
        let f = CoercibleField::full_row();
        assert_eq!(f.name.as_str(), "*");
        assert!(f.full_row);
    }

    #[test]
    fn test_coercible_select_field() {
        let csf = CoercibleSelectField {
            field: CoercibleField::unknown("id".into(), SmallVec::new()),
            agg_function: Some(AggregateFunction::Count),
            agg_cast: None,
            cast: Some("bigint".into()),
            alias: Some("total".into()),
        };
        assert_eq!(csf.alias.as_deref(), Some("total"));
        assert_eq!(csf.cast.as_deref(), Some("bigint"));
        assert!(matches!(csf.agg_function, Some(AggregateFunction::Count)));
    }

    #[test]
    fn test_coercible_filter_variants() {
        let filter = CoercibleFilter::Filter {
            field: CoercibleField::unknown("id".into(), SmallVec::new()),
            op_expr: OpExpr::NoOp("5".into()),
        };
        assert!(matches!(filter, CoercibleFilter::Filter { .. }));

        let null_embed = CoercibleFilter::NullEmbed(false, "clients".into());
        assert!(matches!(null_embed, CoercibleFilter::NullEmbed(false, _)));
    }

    #[test]
    fn test_coercible_order_term() {
        let term = CoercibleOrderTerm::Term {
            field: CoercibleField::unknown("created_at".into(), SmallVec::new()),
            direction: Some(OrderDirection::Desc),
            nulls: Some(OrderNulls::Last),
        };
        assert!(matches!(term, CoercibleOrderTerm::Term { .. }));

        let rel_term = CoercibleOrderTerm::RelationTerm {
            relation: "clients".into(),
            rel_term: CoercibleField::unknown("name".into(), SmallVec::new()),
            direction: None,
            nulls: None,
        };
        assert!(matches!(rel_term, CoercibleOrderTerm::RelationTerm { .. }));
    }

    #[test]
    fn test_rel_json_embed_mode() {
        assert_ne!(RelJsonEmbedMode::JsonObject, RelJsonEmbedMode::JsonArray);
    }

    #[test]
    fn test_rel_select_field_json_embed() {
        let rsf = RelSelectField::JsonEmbed {
            sel_name: "clients".into(),
            agg_alias: "pgrst_agg_0".into(),
            embed_mode: RelJsonEmbedMode::JsonArray,
            empty_embed: false,
        };
        assert!(matches!(rsf, RelSelectField::JsonEmbed { .. }));
    }

    #[test]
    fn test_spread_type() {
        let to_one = SpreadType::ToOneSpread;
        assert!(matches!(to_one, SpreadType::ToOneSpread));

        let to_many = SpreadType::ToManySpread {
            extra_select: vec![],
            order: vec![],
        };
        assert!(matches!(to_many, SpreadType::ToManySpread { .. }));
    }

    #[test]
    fn test_coercible_logic_tree() {
        let leaf = CoercibleLogicTree::Stmnt(CoercibleFilter::NullEmbed(false, "x".into()));
        assert!(matches!(leaf, CoercibleLogicTree::Stmnt(_)));

        let expr = CoercibleLogicTree::Expr(false, LogicOperator::And, vec![leaf]);
        assert!(matches!(
            expr,
            CoercibleLogicTree::Expr(false, LogicOperator::And, _)
        ));
    }

    #[test]
    fn test_coercible_field_from_computed_field() {
        use crate::types::QualifiedIdentifier;

        let func_qi = QualifiedIdentifier::new("test_api", "full_name");
        let field = CoercibleField::from_computed_field(
            "full_name".into(),
            Default::default(),
            func_qi.clone(),
            "text".into(),
        );

        assert_eq!(field.name.as_str(), "full_name");
        assert!(field.is_computed);
        assert_eq!(field.computed_function.as_ref(), Some(&func_qi));
        assert_eq!(field.base_type.as_deref(), Some("text"));
        assert_eq!(field.ir_type.as_deref(), Some("text"));
    }

    #[test]
    fn test_coercible_field_from_column_not_computed() {
        let field = CoercibleField::from_column("name".into(), Default::default(), "text".into());

        assert_eq!(field.name.as_str(), "name");
        assert!(!field.is_computed);
        assert!(field.computed_function.is_none());
        assert_eq!(field.base_type.as_deref(), Some("text"));
    }
}
