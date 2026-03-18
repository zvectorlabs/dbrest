//! Application state shared across all handlers.
//!
//! [`AppState`] is the central struct holding the database backend,
//! configuration, schema cache, authentication state, metrics, and
//! database version info. It is cheaply cloneable (all fields are
//! `Arc`-wrapped) and passed to every axum handler via `State<AppState>`.

use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::auth::JwtCache;
use crate::auth::middleware::AuthState;
use crate::backend::{DatabaseBackend, DbVersion, SqlDialect};
use crate::config::AppConfig;
use crate::error::Error;
use crate::schema_cache::SchemaCache;

// Keep PgVersion as a compatibility alias during migration
/// PostgreSQL server version.
///
/// Deprecated — use [`DbVersion`] from the backend module instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgVersion {
    pub major: u32,
    pub minor: u32,
    pub patch: u32,
}

impl std::fmt::Display for PgVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl From<&DbVersion> for PgVersion {
    fn from(v: &DbVersion) -> Self {
        Self {
            major: v.major,
            minor: v.minor,
            patch: v.patch,
        }
    }
}

/// Central application state.
///
/// Constructed once at startup and shared across all handlers.
/// The `config` and `schema_cache` fields use `ArcSwap` for
/// lock-free reads and atomic replacement during live reload.
#[derive(Clone)]
pub struct AppState {
    /// Database backend (connection pool + execution + introspection).
    pub db: Arc<dyn DatabaseBackend>,
    /// SQL dialect for the active backend.
    pub dialect: Arc<dyn SqlDialect>,
    /// Atomically swappable configuration.
    pub config: Arc<ArcSwap<AppConfig>>,
    /// Atomically swappable schema cache.
    pub schema_cache: Arc<ArcSwap<Option<SchemaCache>>>,
    /// Authentication state (JWT cache + config reference).
    pub auth: AuthState,
    /// JWT validation cache (shared with AuthState).
    pub jwt_cache: JwtCache,
    /// Database version.
    pub db_version: DbVersion,
    /// PostgreSQL version (legacy — prefer `db_version` field).
    pub pg_version: PgVersion,
}

impl AppState {
    /// Create a new `AppState` from a database backend, dialect, config, and version.
    ///
    /// The schema cache starts as `None` — call [`Self::reload_schema_cache`]
    /// after construction.
    pub fn new_with_backend(
        db: Arc<dyn DatabaseBackend>,
        dialect: Arc<dyn SqlDialect>,
        config: AppConfig,
        db_version: DbVersion,
    ) -> Self {
        let pg_version = PgVersion::from(&db_version);
        let config_swap = Arc::new(ArcSwap::new(Arc::new(config)));
        let auth = AuthState::with_shared_config(config_swap.clone());
        let jwt_cache = auth.cache.clone();
        Self {
            db,
            dialect,
            config: config_swap,
            schema_cache: Arc::new(ArcSwap::new(Arc::new(None))),
            auth,
            jwt_cache,
            db_version,
            pg_version,
        }
    }

    /// Get the current config snapshot.
    pub fn config(&self) -> arc_swap::Guard<Arc<AppConfig>> {
        self.config.load()
    }

    /// Get the current schema cache (may be `None` if not loaded).
    pub fn schema_cache_guard(&self) -> arc_swap::Guard<Arc<Option<SchemaCache>>> {
        self.schema_cache.load()
    }

    /// Reload configuration from file and environment.
    #[tracing::instrument(name = "reload_config", skip(self))]
    pub async fn reload_config(&self) -> Result<(), Error> {
        let current = self.config.load();
        let file_path = current.config_file_path.clone();
        let new_config =
            crate::config::load_config(file_path.as_deref(), std::collections::HashMap::new())
                .await
                .map_err(|e| Error::InvalidConfig {
                    message: e.to_string(),
                })?;

        self.config.store(Arc::new(new_config));

        tracing::info!("Configuration reloaded successfully");
        Ok(())
    }

    /// Load or reload the schema cache from the database.
    #[tracing::instrument(name = "reload_schema_cache", skip(self))]
    pub async fn reload_schema_cache(&self) -> Result<(), Error> {
        let config = self.config.load();
        let introspector = self.db.introspector();
        let cache = SchemaCache::load(&*introspector, &config).await?;

        metrics::counter!("schema_cache.reload.total").increment(1);
        self.schema_cache.store(Arc::new(Some(cache)));

        tracing::info!("Schema cache reloaded successfully");
        Ok(())
    }
}
