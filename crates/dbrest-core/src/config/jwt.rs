//! JWT configuration utilities
//!
//! This module provides JSPath parsing for extracting role claims from JWTs.

use compact_str::CompactString;

use super::error::ConfigError;

/// JSON path expression for accessing JWT claims
///
/// Used to configure how to extract the role from JWT claims.
///
/// # Examples
///
/// - `.role` → `[Key("role")]`
/// - `.realm_access.roles[0]` → `[Key("realm_access"), Key("roles"), Index(0)]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JsPathExp {
    /// Object key access: `.key`
    Key(CompactString),
    /// Array index access: `[0]`
    Index(usize),
}

impl JsPathExp {
    /// Check if this is a key expression
    pub fn is_key(&self) -> bool {
        matches!(self, JsPathExp::Key(_))
    }

    /// Check if this is an index expression
    pub fn is_index(&self) -> bool {
        matches!(self, JsPathExp::Index(_))
    }

    /// Get the key if this is a Key variant
    pub fn as_key(&self) -> Option<&str> {
        match self {
            JsPathExp::Key(k) => Some(k.as_str()),
            JsPathExp::Index(_) => None,
        }
    }

    /// Get the index if this is an Index variant
    pub fn as_index(&self) -> Option<usize> {
        match self {
            JsPathExp::Key(_) => None,
            JsPathExp::Index(i) => Some(*i),
        }
    }
}

/// Parse a JWT role claim key path
///
/// Parses a path expression like `.role` or `.realm_access.roles[0]` into
/// a list of path segments.
///
/// # Examples
///
/// ```
/// use dbrest::config::jwt::{parse_js_path, JsPathExp};
///
/// let path = parse_js_path(".role").unwrap();
/// assert_eq!(path, vec![JsPathExp::Key("role".into())]);
///
/// let path = parse_js_path(".realm_access.roles[0]").unwrap();
/// assert_eq!(path.len(), 3);
/// ```
pub fn parse_js_path(input: &str) -> Result<Vec<JsPathExp>, ConfigError> {
    let mut result = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                chars.next(); // consume '.'
                let key = parse_key(&mut chars);
                if !key.is_empty() {
                    result.push(JsPathExp::Key(key.into()));
                }
            }
            '[' => {
                chars.next(); // consume '['
                let index = parse_index(&mut chars)?;
                result.push(JsPathExp::Index(index));
            }
            _ => {
                // Assume it's a key without leading dot
                let key = parse_key(&mut chars);
                if !key.is_empty() {
                    result.push(JsPathExp::Key(key.into()));
                }
            }
        }
    }

    if result.is_empty() {
        // Default to "role" if empty input
        result.push(JsPathExp::Key("role".into()));
    }

    Ok(result)
}

/// Parse a key (identifier) from the input
fn parse_key(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    let mut key = String::new();
    while let Some(&c) = chars.peek() {
        if c == '.' || c == '[' {
            break;
        }
        key.push(c);
        chars.next();
    }
    key
}

/// Parse an array index from the input
fn parse_index(chars: &mut std::iter::Peekable<std::str::Chars>) -> Result<usize, ConfigError> {
    let mut num = String::new();

    while let Some(&c) = chars.peek() {
        if c == ']' {
            chars.next(); // consume ']'
            break;
        }
        if !c.is_ascii_digit() {
            return Err(ConfigError::InvalidJsPath(format!(
                "Invalid character '{}' in array index",
                c
            )));
        }
        num.push(c);
        chars.next();
    }

    if num.is_empty() {
        return Err(ConfigError::InvalidJsPath("Empty array index".to_string()));
    }

    num.parse()
        .map_err(|_| ConfigError::InvalidJsPath(format!("Invalid array index: {}", num)))
}

/// Extract a value from JSON using a JSPath
///
/// # Examples
///
/// ```
/// use dbrest::config::jwt::{parse_js_path, extract_from_json};
/// use serde_json::json;
///
/// let data = json!({
///     "role": "admin",
///     "realm_access": {
///         "roles": ["user", "moderator"]
///     }
/// });
///
/// let path = parse_js_path(".role").unwrap();
/// let value = extract_from_json(&data, &path);
/// assert_eq!(value.and_then(|v| v.as_str()), Some("admin"));
///
/// let path = parse_js_path(".realm_access.roles[1]").unwrap();
/// let value = extract_from_json(&data, &path);
/// assert_eq!(value.and_then(|v| v.as_str()), Some("moderator"));
/// ```
pub fn extract_from_json<'a>(
    value: &'a serde_json::Value,
    path: &[JsPathExp],
) -> Option<&'a serde_json::Value> {
    let mut current = value;

    for exp in path {
        current = match exp {
            JsPathExp::Key(key) => current.get(key.as_str())?,
            JsPathExp::Index(idx) => current.get(*idx)?,
        };
    }

    Some(current)
}

/// Extract a string value from JSON using a JSPath
pub fn extract_string_from_json(value: &serde_json::Value, path: &[JsPathExp]) -> Option<String> {
    let extracted = extract_from_json(value, path)?;

    match extracted {
        serde_json::Value::String(s) => Some(s.clone()),
        // Also handle numbers and booleans as strings
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_simple_key() {
        let path = parse_js_path(".role").unwrap();
        assert_eq!(path, vec![JsPathExp::Key("role".into())]);
    }

    #[test]
    fn test_parse_nested_keys() {
        let path = parse_js_path(".realm_access.roles").unwrap();
        assert_eq!(
            path,
            vec![
                JsPathExp::Key("realm_access".into()),
                JsPathExp::Key("roles".into()),
            ]
        );
    }

    #[test]
    fn test_parse_with_index() {
        let path = parse_js_path(".realm_access.roles[0]").unwrap();
        assert_eq!(
            path,
            vec![
                JsPathExp::Key("realm_access".into()),
                JsPathExp::Key("roles".into()),
                JsPathExp::Index(0),
            ]
        );
    }

    #[test]
    fn test_parse_multiple_indices() {
        let path = parse_js_path(".data[0][1]").unwrap();
        assert_eq!(
            path,
            vec![
                JsPathExp::Key("data".into()),
                JsPathExp::Index(0),
                JsPathExp::Index(1),
            ]
        );
    }

    #[test]
    fn test_parse_without_leading_dot() {
        let path = parse_js_path("role").unwrap();
        assert_eq!(path, vec![JsPathExp::Key("role".into())]);
    }

    #[test]
    fn test_parse_empty_defaults_to_role() {
        let path = parse_js_path("").unwrap();
        assert_eq!(path, vec![JsPathExp::Key("role".into())]);
    }

    #[test]
    fn test_parse_invalid_index() {
        let result = parse_js_path(".roles[abc]");
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_simple() {
        let data = json!({ "role": "admin" });
        let path = parse_js_path(".role").unwrap();
        let value = extract_from_json(&data, &path);
        assert_eq!(value.and_then(|v| v.as_str()), Some("admin"));
    }

    #[test]
    fn test_extract_nested() {
        let data = json!({
            "realm_access": {
                "roles": ["user", "admin"]
            }
        });
        let path = parse_js_path(".realm_access.roles[1]").unwrap();
        let value = extract_from_json(&data, &path);
        assert_eq!(value.and_then(|v| v.as_str()), Some("admin"));
    }

    #[test]
    fn test_extract_missing() {
        let data = json!({ "role": "admin" });
        let path = parse_js_path(".missing.path").unwrap();
        let value = extract_from_json(&data, &path);
        assert!(value.is_none());
    }

    #[test]
    fn test_extract_string_from_number() {
        let data = json!({ "id": 123 });
        let path = parse_js_path(".id").unwrap();
        let value = extract_string_from_json(&data, &path);
        assert_eq!(value, Some("123".to_string()));
    }

    #[test]
    fn test_js_path_exp_methods() {
        let key = JsPathExp::Key("test".into());
        assert!(key.is_key());
        assert!(!key.is_index());
        assert_eq!(key.as_key(), Some("test"));
        assert_eq!(key.as_index(), None);

        let idx = JsPathExp::Index(5);
        assert!(!idx.is_key());
        assert!(idx.is_index());
        assert_eq!(idx.as_key(), None);
        assert_eq!(idx.as_index(), Some(5));
    }
}
