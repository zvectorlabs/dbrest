//! Routine (function/procedure) types for schema cache
//!
//! This module defines types for representing PostgreSQL functions and procedures
//! in the schema cache.

use compact_str::CompactString;
use smallvec::SmallVec;

use crate::types::QualifiedIdentifier;

/// PostgreSQL function or procedure
///
/// Represents a callable routine with its parameters and return type.
#[derive(Debug, Clone)]
pub struct Routine {
    /// Schema name
    pub schema: CompactString,
    /// Function/procedure name
    pub name: CompactString,
    /// Description from pg_description
    pub description: Option<String>,
    /// Function parameters
    pub params: SmallVec<[RoutineParam; 4]>,
    /// Return type
    pub return_type: ReturnType,
    /// Volatility (immutable, stable, volatile)
    pub volatility: Volatility,
    /// Whether the function has a variadic parameter
    pub is_variadic: bool,
    /// Whether EXECUTE is allowed (for current role)
    pub executable: bool,
}

impl Routine {
    /// Get the qualified identifier for this routine
    pub fn qi(&self) -> QualifiedIdentifier {
        QualifiedIdentifier::new(self.schema.clone(), self.name.clone())
    }

    /// Check if function returns a scalar value
    pub fn returns_scalar(&self) -> bool {
        matches!(self.return_type, ReturnType::Single(PgType::Scalar(_)))
    }

    /// Check if function returns a set of scalar values
    pub fn returns_set_of_scalar(&self) -> bool {
        matches!(self.return_type, ReturnType::SetOf(PgType::Scalar(_)))
    }

    /// Check if function returns a single row (not a set)
    pub fn returns_single(&self) -> bool {
        matches!(self.return_type, ReturnType::Single(_))
    }

    /// Check if function returns a set of rows
    pub fn returns_set(&self) -> bool {
        matches!(self.return_type, ReturnType::SetOf(_))
    }

    /// Check if function returns a composite type (table row)
    pub fn returns_composite(&self) -> bool {
        matches!(
            &self.return_type,
            ReturnType::Single(PgType::Composite(_, _)) | ReturnType::SetOf(PgType::Composite(_, _))
        )
    }

    /// Get the table name if function returns a composite type
    pub fn table_name(&self) -> Option<&str> {
        match &self.return_type {
            ReturnType::Single(PgType::Composite(qi, _)) => Some(&qi.name),
            ReturnType::SetOf(PgType::Composite(qi, _)) => Some(&qi.name),
            _ => None,
        }
    }

    /// Get the table QI if function returns a composite type
    pub fn table_qi(&self) -> Option<&QualifiedIdentifier> {
        match &self.return_type {
            ReturnType::Single(PgType::Composite(qi, _)) => Some(qi),
            ReturnType::SetOf(PgType::Composite(qi, _)) => Some(qi),
            _ => None,
        }
    }

    /// Check if the return type is an alias (domain type)
    pub fn is_return_type_alias(&self) -> bool {
        match &self.return_type {
            ReturnType::Single(PgType::Composite(_, is_alias)) => *is_alias,
            ReturnType::SetOf(PgType::Composite(_, is_alias)) => *is_alias,
            _ => false,
        }
    }

    /// Get required parameters (non-variadic, no default)
    pub fn required_params(&self) -> impl Iterator<Item = &RoutineParam> {
        self.params.iter().filter(|p| p.required && !p.is_variadic)
    }

    /// Get optional parameters (has default)
    pub fn optional_params(&self) -> impl Iterator<Item = &RoutineParam> {
        self.params.iter().filter(|p| !p.required && !p.is_variadic)
    }

    /// Get the variadic parameter if present
    pub fn variadic_param(&self) -> Option<&RoutineParam> {
        self.params.iter().find(|p| p.is_variadic)
    }

    /// Get parameter by name
    pub fn get_param(&self, name: &str) -> Option<&RoutineParam> {
        self.params.iter().find(|p| p.name.as_str() == name)
    }

    /// Count of all parameters
    pub fn param_count(&self) -> usize {
        self.params.len()
    }

    /// Count of required parameters
    pub fn required_param_count(&self) -> usize {
        self.params
            .iter()
            .filter(|p| p.required && !p.is_variadic)
            .count()
    }

    /// Check if this is a volatile function
    pub fn is_volatile(&self) -> bool {
        matches!(self.volatility, Volatility::Volatile)
    }

    /// Check if this is a stable function
    pub fn is_stable(&self) -> bool {
        matches!(self.volatility, Volatility::Stable)
    }

    /// Check if this is an immutable function
    pub fn is_immutable(&self) -> bool {
        matches!(self.volatility, Volatility::Immutable)
    }
}

/// Function parameter
#[derive(Debug, Clone)]
pub struct RoutineParam {
    /// Parameter name
    pub name: CompactString,
    /// PostgreSQL type name
    pub pg_type: CompactString,
    /// Type with max length info (e.g., "character varying(255)")
    pub type_max_length: CompactString,
    /// Whether this parameter is required (no default value)
    pub required: bool,
    /// Whether this is a variadic parameter
    pub is_variadic: bool,
}

impl RoutineParam {
    /// Check if this is a text-like parameter
    pub fn is_text_type(&self) -> bool {
        matches!(
            self.pg_type.as_str(),
            "text" | "character varying" | "character" | "varchar" | "char" | "name"
        )
    }

    /// Check if this is a numeric parameter
    pub fn is_numeric_type(&self) -> bool {
        matches!(
            self.pg_type.as_str(),
            "integer"
                | "bigint"
                | "smallint"
                | "numeric"
                | "decimal"
                | "real"
                | "double precision"
                | "int"
                | "int4"
                | "int8"
                | "int2"
        )
    }

    /// Check if this is a JSON parameter
    pub fn is_json_type(&self) -> bool {
        matches!(self.pg_type.as_str(), "json" | "jsonb")
    }
}

/// Function return type
#[derive(Debug, Clone)]
pub enum ReturnType {
    /// Returns a single value/row
    Single(PgType),
    /// Returns a set of values/rows (SETOF)
    SetOf(PgType),
}

impl ReturnType {
    /// Get the underlying type
    pub fn inner_type(&self) -> &PgType {
        match self {
            ReturnType::Single(t) => t,
            ReturnType::SetOf(t) => t,
        }
    }

    /// Check if this is a set-returning type
    pub fn is_set(&self) -> bool {
        matches!(self, ReturnType::SetOf(_))
    }
}

/// PostgreSQL type classification
#[derive(Debug, Clone)]
pub enum PgType {
    /// Scalar type (integer, text, etc.)
    Scalar(QualifiedIdentifier),
    /// Composite type (table row type)
    ///
    /// The bool indicates whether this is an alias (domain type)
    Composite(QualifiedIdentifier, bool),
}

impl PgType {
    /// Check if this is a scalar type
    pub fn is_scalar(&self) -> bool {
        matches!(self, PgType::Scalar(_))
    }

    /// Check if this is a composite type
    pub fn is_composite(&self) -> bool {
        matches!(self, PgType::Composite(_, _))
    }

    /// Get the type's qualified identifier
    pub fn qi(&self) -> &QualifiedIdentifier {
        match self {
            PgType::Scalar(qi) => qi,
            PgType::Composite(qi, _) => qi,
        }
    }
}

/// Function volatility category
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Volatility {
    /// Function always returns same result for same arguments
    Immutable,
    /// Function returns same result within a single query
    Stable,
    /// Function may return different results even within same query
    #[default]
    Volatile,
}

impl Volatility {
    /// Parse volatility from PostgreSQL string
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "i" | "immutable" => Some(Volatility::Immutable),
            "s" | "stable" => Some(Volatility::Stable),
            "v" | "volatile" => Some(Volatility::Volatile),
            _ => None,
        }
    }

    /// Get SQL keyword for this volatility
    pub fn as_sql(&self) -> &'static str {
        match self {
            Volatility::Immutable => "IMMUTABLE",
            Volatility::Stable => "STABLE",
            Volatility::Volatile => "VOLATILE",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;

    // ========================================================================
    // Routine Tests
    // ========================================================================

    #[test]
    fn test_routine_qi() {
        let routine = test_routine().schema("api").name("get_user").build();

        let qi = routine.qi();
        assert_eq!(qi.schema.as_str(), "api");
        assert_eq!(qi.name.as_str(), "get_user");
    }

    #[test]
    fn test_routine_returns_scalar() {
        let scalar_func = test_routine().returns_scalar("integer").build();
        assert!(scalar_func.returns_scalar());
        assert!(!scalar_func.returns_composite());

        let composite_func = test_routine()
            .returns_composite("public", "users")
            .build();
        assert!(!composite_func.returns_scalar());
        assert!(composite_func.returns_composite());
    }

    #[test]
    fn test_routine_returns_set() {
        let single_func = test_routine().returns_scalar("integer").build();
        assert!(single_func.returns_single());
        assert!(!single_func.returns_set());

        let set_func = test_routine().returns_setof_scalar("integer").build();
        assert!(!set_func.returns_single());
        assert!(set_func.returns_set());
    }

    #[test]
    fn test_routine_returns_set_of_scalar() {
        let func = test_routine().returns_setof_scalar("text").build();
        assert!(func.returns_set_of_scalar());

        let composite_func = test_routine()
            .returns_setof_composite("public", "users")
            .build();
        assert!(!composite_func.returns_set_of_scalar());
    }

    #[test]
    fn test_routine_table_name() {
        let scalar_func = test_routine().returns_scalar("integer").build();
        assert!(scalar_func.table_name().is_none());

        let composite_func = test_routine()
            .returns_composite("api", "users")
            .build();
        assert_eq!(composite_func.table_name(), Some("users"));
    }

    #[test]
    fn test_routine_required_params() {
        let p1 = test_param().name("id").required(true).build();
        let p2 = test_param().name("name").required(false).build();
        let p3 = test_param().name("extra").required(true).build();

        let routine = test_routine().params([p1, p2, p3]).build();

        let required: Vec<_> = routine.required_params().map(|p| p.name.as_str()).collect();
        assert_eq!(required, vec!["id", "extra"]);
    }

    #[test]
    fn test_routine_optional_params() {
        let p1 = test_param().name("id").required(true).build();
        let p2 = test_param().name("limit").required(false).build();

        let routine = test_routine().params([p1, p2]).build();

        let optional: Vec<_> = routine.optional_params().map(|p| p.name.as_str()).collect();
        assert_eq!(optional, vec!["limit"]);
    }

    #[test]
    fn test_routine_variadic_param() {
        let p1 = test_param().name("id").build();
        let p2 = test_param().name("args").is_variadic(true).build();

        let routine = test_routine().params([p1, p2]).build();

        let variadic = routine.variadic_param().unwrap();
        assert_eq!(variadic.name.as_str(), "args");
    }

    #[test]
    fn test_routine_get_param() {
        let p1 = test_param().name("user_id").build();

        let routine = test_routine().param(p1).build();

        assert!(routine.get_param("user_id").is_some());
        assert!(routine.get_param("nonexistent").is_none());
    }

    #[test]
    fn test_routine_param_counts() {
        let p1 = test_param().name("a").required(true).build();
        let p2 = test_param().name("b").required(true).build();
        let p3 = test_param().name("c").required(false).build();

        let routine = test_routine().params([p1, p2, p3]).build();

        assert_eq!(routine.param_count(), 3);
        assert_eq!(routine.required_param_count(), 2);
    }

    #[test]
    fn test_routine_volatility() {
        let volatile_func = test_routine().volatility(Volatility::Volatile).build();
        assert!(volatile_func.is_volatile());
        assert!(!volatile_func.is_stable());
        assert!(!volatile_func.is_immutable());

        let stable_func = test_routine().volatility(Volatility::Stable).build();
        assert!(!stable_func.is_volatile());
        assert!(stable_func.is_stable());

        let immutable_func = test_routine().volatility(Volatility::Immutable).build();
        assert!(immutable_func.is_immutable());
    }

    // ========================================================================
    // RoutineParam Tests
    // ========================================================================

    #[test]
    fn test_routine_param_is_text_type() {
        assert!(test_param().pg_type("text").build().is_text_type());
        assert!(test_param()
            .pg_type("character varying")
            .build()
            .is_text_type());
        assert!(!test_param().pg_type("integer").build().is_text_type());
    }

    #[test]
    fn test_routine_param_is_numeric_type() {
        assert!(test_param().pg_type("integer").build().is_numeric_type());
        assert!(test_param().pg_type("bigint").build().is_numeric_type());
        assert!(!test_param().pg_type("text").build().is_numeric_type());
    }

    #[test]
    fn test_routine_param_is_json_type() {
        assert!(test_param().pg_type("json").build().is_json_type());
        assert!(test_param().pg_type("jsonb").build().is_json_type());
        assert!(!test_param().pg_type("text").build().is_json_type());
    }

    // ========================================================================
    // ReturnType Tests
    // ========================================================================

    #[test]
    fn test_return_type_inner_type() {
        let single = ReturnType::Single(PgType::Scalar(QualifiedIdentifier::new(
            "pg_catalog",
            "int4",
        )));
        assert!(single.inner_type().is_scalar());

        let setof = ReturnType::SetOf(PgType::Composite(
            QualifiedIdentifier::new("public", "users"),
            false,
        ));
        assert!(setof.inner_type().is_composite());
    }

    #[test]
    fn test_return_type_is_set() {
        let single = ReturnType::Single(PgType::Scalar(QualifiedIdentifier::new(
            "pg_catalog",
            "int4",
        )));
        assert!(!single.is_set());

        let setof = ReturnType::SetOf(PgType::Scalar(QualifiedIdentifier::new(
            "pg_catalog",
            "int4",
        )));
        assert!(setof.is_set());
    }

    // ========================================================================
    // PgType Tests
    // ========================================================================

    #[test]
    fn test_pg_type_is_scalar_composite() {
        let scalar = PgType::Scalar(QualifiedIdentifier::new("pg_catalog", "int4"));
        assert!(scalar.is_scalar());
        assert!(!scalar.is_composite());

        let composite = PgType::Composite(QualifiedIdentifier::new("public", "users"), false);
        assert!(!composite.is_scalar());
        assert!(composite.is_composite());
    }

    #[test]
    fn test_pg_type_qi() {
        let scalar = PgType::Scalar(QualifiedIdentifier::new("pg_catalog", "text"));
        assert_eq!(scalar.qi().name.as_str(), "text");

        let composite = PgType::Composite(QualifiedIdentifier::new("api", "users"), false);
        assert_eq!(composite.qi().schema.as_str(), "api");
        assert_eq!(composite.qi().name.as_str(), "users");
    }

    // ========================================================================
    // Volatility Tests
    // ========================================================================

    #[test]
    fn test_volatility_parse() {
        assert_eq!(Volatility::parse("i"), Some(Volatility::Immutable));
        assert_eq!(Volatility::parse("immutable"), Some(Volatility::Immutable));
        assert_eq!(Volatility::parse("s"), Some(Volatility::Stable));
        assert_eq!(Volatility::parse("stable"), Some(Volatility::Stable));
        assert_eq!(Volatility::parse("v"), Some(Volatility::Volatile));
        assert_eq!(Volatility::parse("volatile"), Some(Volatility::Volatile));
        assert_eq!(Volatility::parse("invalid"), None);
    }

    #[test]
    fn test_volatility_as_sql() {
        assert_eq!(Volatility::Immutable.as_sql(), "IMMUTABLE");
        assert_eq!(Volatility::Stable.as_sql(), "STABLE");
        assert_eq!(Volatility::Volatile.as_sql(), "VOLATILE");
    }
}
