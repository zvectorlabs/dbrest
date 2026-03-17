//! Namespace routing abstractions.
//!
//! Defines the [`Router`] trait for deciding whether a namespace should be
//! served locally or forwarded to another instance. The default
//! [`LocalRouter`] always serves locally (single-instance mode).

use std::sync::Arc;

// ---------------------------------------------------------------------------
// NamespaceId
// ---------------------------------------------------------------------------

/// A namespace identifier. Cheaply cloneable.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct NamespaceId(Arc<str>);

impl NamespaceId {
    pub fn new(s: impl Into<Arc<str>>) -> Self {
        Self(s.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for NamespaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

// ---------------------------------------------------------------------------
// RoutingError
// ---------------------------------------------------------------------------

/// Errors that can occur during namespace routing.
#[derive(Debug, thiserror::Error)]
pub enum RoutingError {
    #[error("routing failed: {0}")]
    RoutingFailed(String),
}

// ---------------------------------------------------------------------------
// Route & Router trait
// ---------------------------------------------------------------------------

/// The outcome of a routing decision.
#[derive(Debug, Clone)]
pub enum Route {
    /// Serve from this instance.
    Local,
    // Remote(InstanceAddr) — added later for multi-instance
}

/// Decides whether a namespace should be served locally or remotely.
///
/// The default [`LocalRouter`] always returns [`Route::Local`].
/// Custom implementations can check a shared registry, raft state,
/// or any other logic to decide routing.
pub trait Router: Send + Sync {
    fn route(&self, ns: &NamespaceId) -> Result<Route, RoutingError>;
}

// ---------------------------------------------------------------------------
// LocalRouter (default)
// ---------------------------------------------------------------------------

/// Default router — always serves locally.
///
/// Used for single-instance deployments where no routing is needed.
pub struct LocalRouter;

impl Router for LocalRouter {
    fn route(&self, _ns: &NamespaceId) -> Result<Route, RoutingError> {
        Ok(Route::Local)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_namespace_id_new_and_display() {
        let ns = NamespaceId::new("tenant_a");
        assert_eq!(ns.as_str(), "tenant_a");
        assert_eq!(ns.to_string(), "tenant_a");
    }

    #[test]
    fn test_namespace_id_clone_is_cheap() {
        let ns = NamespaceId::new("tenant_b");
        let ns2 = ns.clone();
        assert_eq!(ns, ns2);
    }

    #[test]
    fn test_namespace_id_hash_eq() {
        let mut set = HashSet::new();
        set.insert(NamespaceId::new("a"));
        set.insert(NamespaceId::new("a"));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn test_local_router_returns_local() {
        let router = LocalRouter;
        let ns = NamespaceId::new("any_namespace");
        let result = router.route(&ns).unwrap();
        assert!(matches!(result, Route::Local));
    }

    #[test]
    fn test_local_router_returns_local_for_different_namespaces() {
        let router = LocalRouter;
        for name in ["ns1", "ns2", "tenant_abc", ""] {
            let ns = NamespaceId::new(name);
            let result = router.route(&ns).unwrap();
            assert!(matches!(result, Route::Local));
        }
    }
}
