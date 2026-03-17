//! dbrest-core — database-agnostic core for the dbrest REST API
//!
//! This crate provides the core traits, query generation, API handling,
//! authentication, configuration, and schema cache logic. It does not
//! contain any database-specific code (e.g. no sqlx, no PostgreSQL).

#![cfg_attr(test, allow(clippy::field_reassign_with_default))]
#![cfg_attr(test, allow(clippy::const_is_empty))]
#![cfg_attr(test, allow(clippy::unnecessary_get_then_check))]

pub mod api_request;
pub mod app;
pub mod auth;
pub mod backend;
pub mod config;
pub mod error;
pub mod openapi;
pub mod plan;
pub mod query;
pub mod routing;
pub mod schema_cache;
pub mod types;

// Test helpers (only available in test builds)
#[cfg(test)]
pub mod test_helpers;

// Re-export commonly used types
pub use api_request::ApiRequest;
pub use app::{AppState, start_server};
pub use auth::{AuthResult, AuthState, JwtCache};
pub use backend::{DatabaseBackend, DbVersion, SqlDialect};
pub use config::{AppConfig, load_config};
pub use error::Error;
pub use plan::action_plan;
pub use schema_cache::{SchemaCache, SchemaCacheHolder};
pub use routing::{LocalRouter, NamespaceId, Route, Router, RoutingError};
pub use types::identifiers::{QualifiedIdentifier, RelIdentifier};
pub use types::media::MediaType;
