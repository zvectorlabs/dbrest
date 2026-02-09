//! Authentication result types
//!
//! The [`AuthResult`] struct is the output of the JWT validation pipeline.
//! It is stored in axum request extensions so downstream handlers can read
//! the authenticated role and claims.

use compact_str::CompactString;

/// Outcome of successful JWT authentication or anonymous access.
///
/// Stored in the request extensions by the auth middleware. Downstream
/// handlers retrieve it via `request.extensions().get::<AuthResult>()`.
///
/// # Fields
///
/// - `role` — the PostgreSQL role to use for the current request. Either
///   extracted from the JWT claims or the configured anonymous role.
/// - `claims` — the full JWT claims as a JSON key-value map. For anonymous
///   access this map is empty. The `"role"` key is always present in the
///   map when a token was provided.
#[derive(Debug, Clone)]
pub struct AuthResult {
    /// The resolved PostgreSQL role name.
    pub role: CompactString,
    /// JWT claims (empty map for anonymous requests).
    pub claims: serde_json::Map<String, serde_json::Value>,
}

impl AuthResult {
    /// Create an anonymous auth result with the given role.
    pub fn anonymous(role: &str) -> Self {
        Self {
            role: CompactString::from(role),
            claims: serde_json::Map::new(),
        }
    }

    /// Check if this is an anonymous (no-token) authentication.
    pub fn is_anonymous(&self) -> bool {
        self.claims.is_empty()
    }

    /// Get the claims as a JSON `Value::Object` (for `set_config`).
    pub fn claims_json(&self) -> String {
        serde_json::Value::Object(self.claims.clone()).to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_anonymous() {
        let auth = AuthResult::anonymous("web_anon");
        assert_eq!(auth.role.as_str(), "web_anon");
        assert!(auth.is_anonymous());
        assert_eq!(auth.claims_json(), "{}");
    }

    #[test]
    fn test_authenticated() {
        let mut claims = serde_json::Map::new();
        claims.insert("role".to_string(), serde_json::json!("admin"));
        claims.insert("sub".to_string(), serde_json::json!("user123"));

        let auth = AuthResult {
            role: CompactString::from("admin"),
            claims,
        };
        assert!(!auth.is_anonymous());
        assert!(auth.claims_json().contains("admin"));
        assert!(auth.claims_json().contains("user123"));
    }
}
