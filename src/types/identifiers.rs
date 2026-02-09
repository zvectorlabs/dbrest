//! Database identifiers
//!
//! Types for representing PostgreSQL identifiers with proper SQL escaping.

use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use std::fmt;

/// A schema-qualified identifier (schema.name)
///
/// # Examples
///
/// ```
/// use pgrest::types::identifiers::QualifiedIdentifier;
///
/// let qi = QualifiedIdentifier::new("public", "users");
/// assert_eq!(qi.to_string(), "public.users");
/// assert_eq!(qi.to_sql(), "\"public\".\"users\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct QualifiedIdentifier {
    /// Schema name (can be empty for unqualified identifiers)
    pub schema: CompactString,
    /// Object name (table, function, etc.)
    pub name: CompactString,
}

impl QualifiedIdentifier {
    /// Create a new qualified identifier.
    pub fn new(schema: impl Into<CompactString>, name: impl Into<CompactString>) -> Self {
        Self {
            schema: schema.into(),
            name: name.into(),
        }
    }

    /// Create an unqualified identifier (no schema).
    pub fn unqualified(name: impl Into<CompactString>) -> Self {
        Self {
            schema: CompactString::default(),
            name: name.into(),
        }
    }

    /// Parse from "schema.name" or just "name".
    ///
    /// # Examples
    ///
    /// ```
    /// use pgrest::types::identifiers::QualifiedIdentifier;
    ///
    /// let qi = QualifiedIdentifier::parse("public.users").unwrap();
    /// assert_eq!(qi.schema, "public");
    /// assert_eq!(qi.name, "users");
    ///
    /// let qi = QualifiedIdentifier::parse("users").unwrap();
    /// assert!(qi.schema.is_empty());
    /// assert_eq!(qi.name, "users");
    /// ```
    pub fn parse(input: &str) -> Result<Self, ParseError> {
        if input.is_empty() {
            return Err(ParseError::EmptyIdentifier);
        }

        if let Some((schema, name)) = input.split_once('.') {
            if name.is_empty() {
                return Err(ParseError::EmptyName);
            }
            Ok(Self::new(schema, name))
        } else {
            Ok(Self::unqualified(input))
        }
    }

    /// Check if this identifier has a schema.
    pub fn is_qualified(&self) -> bool {
        !self.schema.is_empty()
    }

    /// Format for SQL with proper quoting: "schema"."name"
    pub fn to_sql(&self) -> String {
        if self.schema.is_empty() {
            quote_ident(&self.name)
        } else {
            format!("{}.{}", quote_ident(&self.schema), quote_ident(&self.name))
        }
    }

    /// Get a reference to the schema.
    pub fn schema(&self) -> &str {
        &self.schema
    }

    /// Get a reference to the name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for QualifiedIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.schema.is_empty() {
            write!(f, "{}", self.name)
        } else {
            write!(f, "{}.{}", self.schema, self.name)
        }
    }
}

/// Identifier for relationship lookups
///
/// Can be either a table reference or the special "anyelement" type
/// used for polymorphic functions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RelIdentifier {
    /// A specific table
    Table(QualifiedIdentifier),
    /// The "anyelement" pseudo-type (for polymorphic functions)
    AnyElement,
}

impl RelIdentifier {
    /// Create a table identifier.
    pub fn table(qi: QualifiedIdentifier) -> Self {
        Self::Table(qi)
    }

    /// Create an "anyelement" identifier.
    pub fn any_element() -> Self {
        Self::AnyElement
    }

    /// Check if this is the "anyelement" type.
    pub fn is_any_element(&self) -> bool {
        matches!(self, RelIdentifier::AnyElement)
    }

    /// Get the qualified identifier if this is a table.
    pub fn as_table(&self) -> Option<&QualifiedIdentifier> {
        match self {
            RelIdentifier::Table(qi) => Some(qi),
            RelIdentifier::AnyElement => None,
        }
    }
}

impl fmt::Display for RelIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RelIdentifier::Table(qi) => write!(f, "{}", qi),
            RelIdentifier::AnyElement => write!(f, "anyelement"),
        }
    }
}

impl From<QualifiedIdentifier> for RelIdentifier {
    fn from(qi: QualifiedIdentifier) -> Self {
        RelIdentifier::Table(qi)
    }
}

/// Parse errors for identifiers
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Empty identifier string
    EmptyIdentifier,
    /// Empty name part (e.g., "schema.")
    EmptyName,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EmptyIdentifier => write!(f, "identifier cannot be empty"),
            ParseError::EmptyName => write!(f, "name part cannot be empty"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Escape a SQL identifier by doubling quotes.
///
/// # Examples
///
/// ```
/// use pgrest::types::identifiers::escape_ident;
///
/// assert_eq!(escape_ident("users"), "users");
/// assert_eq!(escape_ident("user\"name"), "user\"\"name");
/// ```
pub fn escape_ident(s: &str) -> String {
    s.replace('"', "\"\"")
}

/// Quote a SQL identifier with double quotes.
///
/// # Examples
///
/// ```
/// use pgrest::types::identifiers::quote_ident;
///
/// assert_eq!(quote_ident("users"), "\"users\"");
/// assert_eq!(quote_ident("user\"name"), "\"user\"\"name\"");
/// ```
pub fn quote_ident(s: &str) -> String {
    format!("\"{}\"", escape_ident(s))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qualified_identifier_new() {
        let qi = QualifiedIdentifier::new("public", "users");
        assert_eq!(qi.schema, "public");
        assert_eq!(qi.name, "users");
        assert!(qi.is_qualified());
    }

    #[test]
    fn test_qualified_identifier_unqualified() {
        let qi = QualifiedIdentifier::unqualified("users");
        assert!(qi.schema.is_empty());
        assert_eq!(qi.name, "users");
        assert!(!qi.is_qualified());
    }

    #[test]
    fn test_qualified_identifier_parse() {
        let qi = QualifiedIdentifier::parse("public.users").unwrap();
        assert_eq!(qi.schema, "public");
        assert_eq!(qi.name, "users");

        let qi = QualifiedIdentifier::parse("users").unwrap();
        assert!(qi.schema.is_empty());
        assert_eq!(qi.name, "users");
    }

    #[test]
    fn test_qualified_identifier_parse_errors() {
        assert!(matches!(
            QualifiedIdentifier::parse(""),
            Err(ParseError::EmptyIdentifier)
        ));
        assert!(matches!(
            QualifiedIdentifier::parse("schema."),
            Err(ParseError::EmptyName)
        ));
    }

    #[test]
    fn test_qualified_identifier_to_sql() {
        let qi = QualifiedIdentifier::new("public", "users");
        assert_eq!(qi.to_sql(), "\"public\".\"users\"");

        let qi = QualifiedIdentifier::new("public", "user\"s");
        assert_eq!(qi.to_sql(), "\"public\".\"user\"\"s\"");

        let qi = QualifiedIdentifier::unqualified("users");
        assert_eq!(qi.to_sql(), "\"users\"");
    }

    #[test]
    fn test_qualified_identifier_display() {
        let qi = QualifiedIdentifier::new("public", "users");
        assert_eq!(qi.to_string(), "public.users");

        let qi = QualifiedIdentifier::unqualified("users");
        assert_eq!(qi.to_string(), "users");
    }

    #[test]
    fn test_rel_identifier() {
        let ri = RelIdentifier::table(QualifiedIdentifier::new("public", "users"));
        assert!(!ri.is_any_element());
        assert!(ri.as_table().is_some());
        assert_eq!(ri.to_string(), "public.users");

        let ri = RelIdentifier::any_element();
        assert!(ri.is_any_element());
        assert!(ri.as_table().is_none());
        assert_eq!(ri.to_string(), "anyelement");
    }

    #[test]
    fn test_escape_ident() {
        assert_eq!(escape_ident("simple"), "simple");
        assert_eq!(escape_ident("with\"quote"), "with\"\"quote");
        assert_eq!(escape_ident("\"\""), "\"\"\"\"");
    }

    #[test]
    fn test_quote_ident() {
        assert_eq!(quote_ident("simple"), "\"simple\"");
        assert_eq!(quote_ident("with\"quote"), "\"with\"\"quote\"");
    }
}
