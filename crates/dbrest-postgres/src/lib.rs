//! dbrest-postgres — PostgreSQL backend for the dbrest REST API
//!
//! This crate implements the [`DatabaseBackend`](dbrest_core::backend::DatabaseBackend)
//! and [`SqlDialect`](dbrest_core::backend::SqlDialect) traits for PostgreSQL
//! via `sqlx::PgPool`.

pub mod dialect;
pub mod executor;
pub mod introspector;

pub use dialect::PgDialect;
pub use executor::PgBackend;
pub use introspector::SqlxIntrospector;
