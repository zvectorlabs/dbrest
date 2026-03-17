//! dbrest-sqlite — SQLite backend for the dbrest REST API
//!
//! This crate implements the [`DatabaseBackend`](dbrest_core::backend::DatabaseBackend)
//! and [`SqlDialect`](dbrest_core::backend::SqlDialect) traits for SQLite
//! via `sqlx::SqlitePool`.

pub mod dialect;
pub mod executor;
pub mod introspector;

pub use dialect::SqliteDialect;
pub use executor::SqliteBackend;
pub use introspector::SqliteIntrospector;
