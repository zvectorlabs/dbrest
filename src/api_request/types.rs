//! Core types for the API request module
//!
//! Core types for the API request module.
//! They represent the domain-specific language for translating HTTP requests
//! into structured operations.

use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::fmt;

use crate::types::identifiers::QualifiedIdentifier;

// ==========================================================================
// Type aliases
// ==========================================================================

/// A field name (column name or "*")
pub type FieldName = CompactString;

/// A type cast (e.g., "::text")
pub type Cast = CompactString;

/// An alias (e.g., "alias:field")
pub type Alias = CompactString;

/// A disambiguation hint (e.g., "!hint")
pub type Hint = CompactString;

/// A single value in a filter (e.g., `id=eq.singleval`)
pub type SingleVal = CompactString;

/// A list value in a filter (e.g., `id=in.(val1,val2,val3)`)
pub type ListVal = Vec<CompactString>;

/// A language for full-text search (e.g., "english")
pub type Language = CompactString;

/// A JSON path: sequence of JSON operations
pub type JsonPath = SmallVec<[JsonOperation; 2]>;

/// A field: (name, json_path)
pub type Field = (FieldName, JsonPath);

/// Embed path: path of embedded levels (e.g., `["clients", "projects"]`)
pub type EmbedPath = Vec<FieldName>;

// ==========================================================================
// Action / Resource types
// ==========================================================================

/// How a routine is invoked
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum InvokeMethod {
    /// Volatile invocation (POST)
    Inv,
    /// Safe/read-only invocation (GET/HEAD). Bool = headers_only
    InvRead(bool),
}

/// Mutation type for relation operations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    MutationCreate,
    MutationDelete,
    MutationSingleUpsert,
    MutationUpdate,
}

/// The resource identified by the URL path
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Resource {
    /// A table or view (e.g., `/items`)
    Relation(CompactString),
    /// An RPC routine (e.g., `/rpc/my_func`)
    Routine(CompactString),
    /// The root schema info (e.g., `/`)
    Schema,
}

/// Database-level action
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DbAction {
    /// Read from a relation (table/view). `headers_only` = HEAD request.
    RelationRead {
        qi: QualifiedIdentifier,
        headers_only: bool,
    },
    /// Mutate a relation (INSERT/UPDATE/DELETE/UPSERT)
    RelationMut {
        qi: QualifiedIdentifier,
        mutation: Mutation,
    },
    /// Invoke a routine (function/procedure)
    Routine {
        qi: QualifiedIdentifier,
        inv_method: InvokeMethod,
    },
    /// Read schema info (GET /)
    SchemaRead {
        schema: CompactString,
        headers_only: bool,
    },
}

/// Top-level action combining DB actions and info actions
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    /// A database action
    Db(DbAction),
    /// OPTIONS on a relation
    RelationInfo(QualifiedIdentifier),
    /// OPTIONS on a routine
    RoutineInfo(QualifiedIdentifier, InvokeMethod),
    /// OPTIONS on root schema
    SchemaInfo,
}

impl Action {
    /// Check if this action is a safe (read-only) routine invocation
    pub fn is_invoke_safe(&self) -> bool {
        matches!(
            self,
            Action::Db(DbAction::Routine {
                inv_method: InvokeMethod::InvRead(_),
                ..
            })
        )
    }
}

// ==========================================================================
// Select types
// ==========================================================================

/// Aggregate functions available in select
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Max,
    Min,
}

impl fmt::Display for AggregateFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregateFunction::Count => write!(f, "count"),
            AggregateFunction::Sum => write!(f, "sum"),
            AggregateFunction::Avg => write!(f, "avg"),
            AggregateFunction::Max => write!(f, "max"),
            AggregateFunction::Min => write!(f, "min"),
        }
    }
}

/// Join type for embedded resources
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
}

/// Embed parameter: hint or join type specified with `!`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmbedParam {
    Hint(Hint),
    JoinType(JoinType),
}

/// A select item in the `select=` parameter
///
/// A parsed select item (field, relation, or spread).
/// Children (sub-selects) are stored in the `children` field for relation types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelectItem {
    /// A field selection: `alias:field->json_path.aggregate()::cast`
    Field {
        field: Field,
        alias: Option<Alias>,
        cast: Option<Cast>,
        aggregate: Option<AggregateFunction>,
        aggregate_cast: Option<Cast>,
    },
    /// A relation embedding: `alias:relation!hint!inner(*)`
    Relation {
        relation: FieldName,
        alias: Option<Alias>,
        hint: Option<Hint>,
        join_type: Option<JoinType>,
        children: Vec<SelectItem>,
    },
    /// A spread relation: `...relation!hint!inner(*)`
    Spread {
        relation: FieldName,
        hint: Option<Hint>,
        join_type: Option<JoinType>,
        children: Vec<SelectItem>,
    },
}

// ==========================================================================
// JSON operations
// ==========================================================================

/// JSON operand: key or index
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonOperand {
    /// A key access: `->'key'`
    Key(CompactString),
    /// An index access: `->0`
    Idx(CompactString),
}

/// JSON arrow operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JsonOperation {
    /// Single arrow `->` (returns JSON)
    Arrow(JsonOperand),
    /// Double arrow `->>` (returns text)
    Arrow2(JsonOperand),
}

// ==========================================================================
// Order types
// ==========================================================================

/// Order direction
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderDirection {
    Asc,
    Desc,
}

/// Nulls ordering
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderNulls {
    First,
    Last,
}

/// An order term for the `order=` parameter
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderTerm {
    /// Order on a field
    Term {
        field: Field,
        direction: Option<OrderDirection>,
        nulls: Option<OrderNulls>,
    },
    /// Order on a field within an embedded relation
    RelationTerm {
        relation: FieldName,
        field: Field,
        direction: Option<OrderDirection>,
        nulls: Option<OrderNulls>,
    },
}

// ==========================================================================
// Filter / Operation types
// ==========================================================================

/// Simple operators (always take a single value, no quantifiers)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SimpleOperator {
    NotEqual,    // neq
    Contains,    // cs
    Contained,   // cd
    Overlap,     // ov
    StrictlyLeft,     // sl
    StrictlyRight,    // sr
    NotExtendsRight,  // nxr
    NotExtendsLeft,   // nxl
    Adjacent,    // adj
}

/// Quantifiable operators (can use `any`/`all` modifiers)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuantOperator {
    Equal,            // eq
    GreaterThanEqual, // gte
    GreaterThan,      // gt
    LessThanEqual,    // lte
    LessThan,         // lt
    Like,             // like
    ILike,            // ilike
    Match,            // match
    IMatch,           // imatch
}

/// Full-text search operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FtsOperator {
    Fts,          // fts
    FtsPlain,     // plfts
    FtsPhrase,    // phfts
    FtsWebsearch, // wfts
}

/// Quantifier for operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpQuantifier {
    Any,
    All,
}

/// IS value variants
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IsValue {
    Null,
    NotNull,
    True,
    False,
    Unknown,
}

/// A filter operation (the right-hand side of a filter expression)
///
/// A parsed filter operation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Operation {
    /// Simple operator with single value: `neq.value`, `cs.value`, etc.
    Simple(SimpleOperator, SingleVal),
    /// Quantifiable operator with optional quantifier: `eq.value`, `eq(any).value`
    Quant(QuantOperator, Option<OpQuantifier>, SingleVal),
    /// IN list: `in.(val1,val2,val3)`
    In(ListVal),
    /// IS value: `is.null`, `is.true`, etc.
    Is(IsValue),
    /// IS DISTINCT FROM: `isdistinct.value`
    IsDistinctFrom(SingleVal),
    /// Full-text search: `fts(lang).value`
    Fts(FtsOperator, Option<Language>, SingleVal),
}

/// An operator expression, possibly negated
///
/// An operator expression (optionally negated).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpExpr {
    /// A normal operation, possibly negated (first bool = is_negated)
    Expr { negated: bool, operation: Operation },
    /// A value without an operator (used for RPC GET params)
    NoOp(CompactString),
}

/// A filter: field + operator expression
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    pub field: Field,
    pub op_expr: OpExpr,
}

// ==========================================================================
// Logic tree
// ==========================================================================

/// Logic operators for boolean combinations
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicOperator {
    And,
    Or,
}

/// Boolean logic expression tree
///
/// A tree of AND/OR logic conditions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicTree {
    /// A logic expression: `negated`, `operator`, `children`
    Expr {
        negated: bool,
        operator: LogicOperator,
        children: Vec<LogicTree>,
    },
    /// A leaf filter statement
    Stmnt(Filter),
}

// ==========================================================================
// Payload types
// ==========================================================================

/// Parsed request body
///
/// Request payload variants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Payload {
    /// Processed JSON with cached keys
    ProcessedJSON {
        raw: bytes::Bytes,
        keys: std::collections::HashSet<CompactString>,
    },
    /// URL-encoded form data
    ProcessedUrlEncoded {
        params: Vec<(CompactString, CompactString)>,
        keys: std::collections::HashSet<CompactString>,
    },
    /// Raw JSON (when &columns is specified)
    RawJSON(bytes::Bytes),
    /// Raw payload (text, XML, binary for RPC)
    RawPayload(bytes::Bytes),
}

impl Payload {
    /// Get the set of keys from the payload
    pub fn keys(&self) -> &std::collections::HashSet<CompactString> {
        static EMPTY: std::sync::LazyLock<std::collections::HashSet<CompactString>> =
            std::sync::LazyLock::new(std::collections::HashSet::new);
        match self {
            Payload::ProcessedJSON { keys, .. } => keys,
            Payload::ProcessedUrlEncoded { keys, .. } => keys,
            _ => &EMPTY,
        }
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_is_invoke_safe() {
        let safe = Action::Db(DbAction::Routine {
            qi: QualifiedIdentifier::new("public", "my_func"),
            inv_method: InvokeMethod::InvRead(false),
        });
        assert!(safe.is_invoke_safe());

        let volatile = Action::Db(DbAction::Routine {
            qi: QualifiedIdentifier::new("public", "my_func"),
            inv_method: InvokeMethod::Inv,
        });
        assert!(!volatile.is_invoke_safe());

        let read = Action::Db(DbAction::RelationRead {
            qi: QualifiedIdentifier::new("public", "items"),
            headers_only: false,
        });
        assert!(!read.is_invoke_safe());
    }

    #[test]
    fn test_resource_variants() {
        let rel = Resource::Relation("items".into());
        let rpc = Resource::Routine("my_func".into());
        let schema = Resource::Schema;

        assert_eq!(rel, Resource::Relation("items".into()));
        assert_eq!(rpc, Resource::Routine("my_func".into()));
        assert_eq!(schema, Resource::Schema);
    }

    #[test]
    fn test_mutation_variants() {
        assert_ne!(Mutation::MutationCreate, Mutation::MutationDelete);
        assert_ne!(Mutation::MutationUpdate, Mutation::MutationSingleUpsert);
    }

    #[test]
    fn test_select_item_field() {
        let item = SelectItem::Field {
            field: ("id".into(), SmallVec::new()),
            alias: None,
            cast: None,
            aggregate: None,
            aggregate_cast: None,
        };
        if let SelectItem::Field { field, .. } = &item {
            assert_eq!(field.0.as_str(), "id");
            assert!(field.1.is_empty());
        }
    }

    #[test]
    fn test_select_item_relation() {
        let item = SelectItem::Relation {
            relation: "posts".into(),
            alias: Some("my_posts".into()),
            hint: None,
            join_type: Some(JoinType::Inner),
            children: vec![SelectItem::Field {
                field: ("*".into(), SmallVec::new()),
                alias: None,
                cast: None,
                aggregate: None,
                aggregate_cast: None,
            }],
        };
        if let SelectItem::Relation {
            relation,
            alias,
            join_type,
            children,
            ..
        } = &item
        {
            assert_eq!(relation.as_str(), "posts");
            assert_eq!(alias.as_deref(), Some("my_posts"));
            assert_eq!(*join_type, Some(JoinType::Inner));
            assert_eq!(children.len(), 1);
        }
    }

    #[test]
    fn test_select_item_spread() {
        let item = SelectItem::Spread {
            relation: "details".into(),
            hint: Some("fk_detail".into()),
            join_type: None,
            children: vec![],
        };
        if let SelectItem::Spread { relation, hint, .. } = &item {
            assert_eq!(relation.as_str(), "details");
            assert_eq!(hint.as_deref(), Some("fk_detail"));
        }
    }

    #[test]
    fn test_json_operations() {
        let op = JsonOperation::Arrow(JsonOperand::Key("name".into()));
        assert_eq!(
            op,
            JsonOperation::Arrow(JsonOperand::Key("name".into()))
        );

        let op2 = JsonOperation::Arrow2(JsonOperand::Idx("+0".into()));
        assert_eq!(
            op2,
            JsonOperation::Arrow2(JsonOperand::Idx("+0".into()))
        );
    }

    #[test]
    fn test_order_term() {
        let term = OrderTerm::Term {
            field: ("created_at".into(), SmallVec::new()),
            direction: Some(OrderDirection::Desc),
            nulls: Some(OrderNulls::First),
        };
        if let OrderTerm::Term {
            field,
            direction,
            nulls,
        } = &term
        {
            assert_eq!(field.0.as_str(), "created_at");
            assert_eq!(*direction, Some(OrderDirection::Desc));
            assert_eq!(*nulls, Some(OrderNulls::First));
        }
    }

    #[test]
    fn test_order_relation_term() {
        let term = OrderTerm::RelationTerm {
            relation: "clients".into(),
            field: ("name".into(), SmallVec::new()),
            direction: None,
            nulls: None,
        };
        if let OrderTerm::RelationTerm { relation, field, .. } = &term {
            assert_eq!(relation.as_str(), "clients");
            assert_eq!(field.0.as_str(), "name");
        }
    }

    #[test]
    fn test_operation_simple() {
        let op = Operation::Simple(SimpleOperator::NotEqual, "5".into());
        assert_eq!(
            op,
            Operation::Simple(SimpleOperator::NotEqual, "5".into())
        );
    }

    #[test]
    fn test_operation_quant() {
        let op = Operation::Quant(QuantOperator::Equal, Some(OpQuantifier::Any), "5".into());
        assert_eq!(
            op,
            Operation::Quant(QuantOperator::Equal, Some(OpQuantifier::Any), "5".into())
        );
    }

    #[test]
    fn test_operation_in() {
        let op = Operation::In(vec!["1".into(), "2".into(), "3".into()]);
        if let Operation::In(vals) = &op {
            assert_eq!(vals.len(), 3);
        }
    }

    #[test]
    fn test_operation_is() {
        assert_eq!(Operation::Is(IsValue::Null), Operation::Is(IsValue::Null));
        assert_ne!(
            Operation::Is(IsValue::True),
            Operation::Is(IsValue::False)
        );
    }

    #[test]
    fn test_operation_fts() {
        let op = Operation::Fts(
            FtsOperator::FtsWebsearch,
            Some("english".into()),
            "search term".into(),
        );
        if let Operation::Fts(fts_op, lang, val) = &op {
            assert_eq!(*fts_op, FtsOperator::FtsWebsearch);
            assert_eq!(lang.as_deref(), Some("english"));
            assert_eq!(val.as_str(), "search term");
        }
    }

    #[test]
    fn test_op_expr_negated() {
        let expr = OpExpr::Expr {
            negated: true,
            operation: Operation::Quant(QuantOperator::Equal, None, "5".into()),
        };
        if let OpExpr::Expr { negated, .. } = &expr {
            assert!(*negated);
        }
    }

    #[test]
    fn test_op_expr_no_op() {
        let expr = OpExpr::NoOp("raw_value".into());
        assert_eq!(expr, OpExpr::NoOp("raw_value".into()));
    }

    #[test]
    fn test_filter() {
        let filter = Filter {
            field: ("id".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
            },
        };
        assert_eq!(filter.field.0.as_str(), "id");
    }

    #[test]
    fn test_logic_tree_stmnt() {
        let tree = LogicTree::Stmnt(Filter {
            field: ("name".into(), SmallVec::new()),
            op_expr: OpExpr::Expr {
                negated: false,
                operation: Operation::Quant(QuantOperator::Equal, None, "John".into()),
            },
        });
        assert!(matches!(tree, LogicTree::Stmnt(_)));
    }

    #[test]
    fn test_logic_tree_expr() {
        let tree = LogicTree::Expr {
            negated: false,
            operator: LogicOperator::And,
            children: vec![
                LogicTree::Stmnt(Filter {
                    field: ("a".into(), SmallVec::new()),
                    op_expr: OpExpr::Expr {
                        negated: false,
                        operation: Operation::Quant(QuantOperator::Equal, None, "1".into()),
                    },
                }),
                LogicTree::Stmnt(Filter {
                    field: ("b".into(), SmallVec::new()),
                    op_expr: OpExpr::Expr {
                        negated: false,
                        operation: Operation::Quant(QuantOperator::GreaterThan, None, "5".into()),
                    },
                }),
            ],
        };
        if let LogicTree::Expr { children, .. } = &tree {
            assert_eq!(children.len(), 2);
        }
    }

    #[test]
    fn test_payload_keys() {
        let mut keys = std::collections::HashSet::new();
        keys.insert(CompactString::from("id"));
        keys.insert(CompactString::from("name"));

        let payload = Payload::ProcessedJSON {
            raw: bytes::Bytes::from(r#"{"id":1,"name":"test"}"#),
            keys: keys.clone(),
        };
        assert_eq!(payload.keys().len(), 2);
        assert!(payload.keys().contains("id"));

        let raw = Payload::RawJSON(bytes::Bytes::from("{}"));
        assert!(raw.keys().is_empty());
    }

    #[test]
    fn test_aggregate_function_display() {
        assert_eq!(AggregateFunction::Count.to_string(), "count");
        assert_eq!(AggregateFunction::Sum.to_string(), "sum");
        assert_eq!(AggregateFunction::Avg.to_string(), "avg");
        assert_eq!(AggregateFunction::Max.to_string(), "max");
        assert_eq!(AggregateFunction::Min.to_string(), "min");
    }

    #[test]
    fn test_is_distinct_from() {
        let op = Operation::IsDistinctFrom("value".into());
        assert_eq!(
            op,
            Operation::IsDistinctFrom("value".into())
        );
    }

    #[test]
    fn test_all_simple_operators() {
        let ops = [
            SimpleOperator::NotEqual,
            SimpleOperator::Contains,
            SimpleOperator::Contained,
            SimpleOperator::Overlap,
            SimpleOperator::StrictlyLeft,
            SimpleOperator::StrictlyRight,
            SimpleOperator::NotExtendsRight,
            SimpleOperator::NotExtendsLeft,
            SimpleOperator::Adjacent,
        ];
        // Ensure all 9 variants exist and are distinct
        for (i, a) in ops.iter().enumerate() {
            for (j, b) in ops.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn test_all_quant_operators() {
        let ops = [
            QuantOperator::Equal,
            QuantOperator::GreaterThanEqual,
            QuantOperator::GreaterThan,
            QuantOperator::LessThanEqual,
            QuantOperator::LessThan,
            QuantOperator::Like,
            QuantOperator::ILike,
            QuantOperator::Match,
            QuantOperator::IMatch,
        ];
        for (i, a) in ops.iter().enumerate() {
            for (j, b) in ops.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn test_all_fts_operators() {
        let ops = [
            FtsOperator::Fts,
            FtsOperator::FtsPlain,
            FtsOperator::FtsPhrase,
            FtsOperator::FtsWebsearch,
        ];
        for (i, a) in ops.iter().enumerate() {
            for (j, b) in ops.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn test_db_action_variants() {
        let read = DbAction::RelationRead {
            qi: QualifiedIdentifier::new("public", "items"),
            headers_only: false,
        };
        let head = DbAction::RelationRead {
            qi: QualifiedIdentifier::new("public", "items"),
            headers_only: true,
        };
        assert_ne!(read, head);

        let create = DbAction::RelationMut {
            qi: QualifiedIdentifier::new("public", "items"),
            mutation: Mutation::MutationCreate,
        };
        let delete = DbAction::RelationMut {
            qi: QualifiedIdentifier::new("public", "items"),
            mutation: Mutation::MutationDelete,
        };
        assert_ne!(create, delete);

        let schema = DbAction::SchemaRead {
            schema: "public".into(),
            headers_only: false,
        };
        assert!(matches!(schema, DbAction::SchemaRead { .. }));
    }
}
