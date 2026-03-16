//! dbrest — a high-performance REST API for databases
//!
//! This crate provides a REST API layer for databases, written in Rust
//! for speed and safety. It currently supports PostgreSQL, with a
//! pluggable backend architecture for future database support.

#![cfg_attr(test, allow(clippy::field_reassign_with_default))]
#![cfg_attr(test, allow(clippy::const_is_empty))]
#![cfg_attr(test, allow(clippy::unnecessary_get_then_check))]
//!
//! # Architecture
//!
//! The crate is organized into the following modules:
//!
//! - [`api_request`] - HTTP request parsing into domain types
//! - [`auth`] - JWT authentication, caching, and role resolution
//! - [`backend`] - Database backend abstraction (traits + implementations)
//! - [`config`] - Configuration loading and validation
//! - [`error`] - Error types with PGRST-compatible error codes
//! - [`plan`] - Query planning (read, mutate, call plans)
//! - [`query`] - SQL generation from execution plans
//! - [`schema_cache`] - Database schema introspection and caching
//! - [`types`] - Core types (identifiers, media types, etc.)

pub mod api_request;
pub mod app;
pub mod auth;
pub mod backend;
pub mod config;
pub mod error;
pub mod openapi;
pub mod plan;
pub mod query;
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
pub use types::identifiers::{QualifiedIdentifier, RelIdentifier};
pub use types::media::MediaType;
