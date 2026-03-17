//! dbrest — a high-performance REST API for databases
//!
//! This crate provides a REST API layer for databases, written in Rust
//! for speed and safety. It currently supports PostgreSQL, with a
//! pluggable backend architecture for future database support.
//!
//! This is the root binary crate that re-exports types from
//! `dbrest-core` (database-agnostic core) and `dbrest-postgres`
//! (PostgreSQL-specific backend).

#![cfg_attr(test, allow(clippy::field_reassign_with_default))]
#![cfg_attr(test, allow(clippy::const_is_empty))]
#![cfg_attr(test, allow(clippy::unnecessary_get_then_check))]

// Re-export core modules for backwards compatibility
pub use dbrest_core::api_request;
pub use dbrest_core::app;
pub use dbrest_core::auth;
pub use dbrest_core::backend;
pub use dbrest_core::config;
pub use dbrest_core::error;
pub use dbrest_core::openapi;
pub use dbrest_core::plan;
pub use dbrest_core::query;
pub use dbrest_core::schema_cache;
pub use dbrest_core::types;

// Re-export commonly used types from core
pub use dbrest_core::ApiRequest;
pub use dbrest_core::app::{AppState, start_server};
pub use dbrest_core::auth::{AuthResult, AuthState, JwtCache};
pub use dbrest_core::backend::{DatabaseBackend, DbVersion, SqlDialect};
pub use dbrest_core::config::{AppConfig, load_config};
pub use dbrest_core::error::Error;
pub use dbrest_core::plan::action_plan;
pub use dbrest_core::schema_cache::{SchemaCache, SchemaCacheHolder};
pub use dbrest_core::types::identifiers::{QualifiedIdentifier, RelIdentifier};
pub use dbrest_core::types::media::MediaType;

// Re-export postgres backend types
pub use dbrest_postgres::{PgBackend, PgDialect, SqlxIntrospector};

// Re-export sqlite backend types
pub use dbrest_sqlite::{SqliteBackend, SqliteDialect, SqliteIntrospector};

/// Compatibility module providing legacy constructors that depend on both
/// `dbrest-core` and `dbrest-postgres`.
pub mod compat {
    use std::sync::Arc;

    use dbrest_core::app::state::{AppState, PgVersion};
    use dbrest_core::backend::{DatabaseBackend, DbVersion, SqlDialect};
    use dbrest_core::config::AppConfig;
    use dbrest_postgres::{PgBackend, PgDialect};

    /// Create a new `AppState` from a pool, config, and PG version.
    ///
    /// Legacy constructor — uses `PgBackend` and `PgDialect` automatically.
    /// The schema cache starts as `None` — call `reload_schema_cache()`
    /// after construction.
    pub fn app_state_from_pool(
        pool: sqlx::PgPool,
        config: AppConfig,
        pg_version: PgVersion,
    ) -> AppState {
        let db_version = DbVersion {
            major: pg_version.major,
            minor: pg_version.minor,
            patch: pg_version.patch,
            engine: "PostgreSQL".to_string(),
        };
        let backend: Arc<dyn DatabaseBackend> = Arc::new(PgBackend::from_pool(pool));
        let dialect: Arc<dyn SqlDialect> = Arc::new(PgDialect);

        AppState::new_with_backend(backend, dialect, config, db_version)
    }
}
