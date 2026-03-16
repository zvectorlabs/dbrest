//! PostgreSQL backend implementation.
//!
//! Implements [`DatabaseBackend`] and [`SqlDialect`] for PostgreSQL via
//! `sqlx::PgPool`.

mod dialect;
mod executor;

pub use dialect::PgDialect;
pub use executor::PgBackend;
