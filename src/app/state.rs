//! Application state shared across all handlers.
//!
//! [`AppState`] is the central struct holding the database pool,
//! configuration, schema cache, authentication state, metrics, and
//! PostgreSQL version info. It is cheaply cloneable (all fields are
//! `Arc`-wrapped) and passed to every axum handler via `State<AppState>`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arc_swap::ArcSwap;
use sqlx::PgPool;

use crate::auth::JwtCache;
use crate::auth::middleware::AuthState;
use crate::config::AppConfig;
use crate::error::Error;
use crate::schema_cache::SchemaCache;

/// PostgreSQL server version.
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

/// Application-level metrics (atomic counters).
#[derive(Debug, Default)]
pub struct Metrics {
    pub requests_total: AtomicU64,
    pub requests_success: AtomicU64,
    pub requests_error: AtomicU64,
    pub db_queries_total: AtomicU64,
    pub schema_cache_reloads: AtomicU64,
    pub jwt_cache_hits: AtomicU64,
    pub jwt_cache_misses: AtomicU64,
}

/// Central application state.
///
/// Constructed once at startup and shared across all handlers.
/// The `config` and `schema_cache` fields use `ArcSwap` for
/// lock-free reads and atomic replacement during live reload.
#[derive(Clone)]
pub struct AppState {
    /// Database connection pool.
    pub pool: PgPool,
    /// Atomically swappable configuration.
    pub config: Arc<ArcSwap<AppConfig>>,
    /// Atomically swappable schema cache.
    pub schema_cache: Arc<ArcSwap<Option<SchemaCache>>>,
    /// Authentication state (JWT cache + config reference).
    pub auth: AuthState,
    /// JWT validation cache (shared with AuthState).
    pub jwt_cache: JwtCache,
    /// Application metrics.
    pub metrics: Arc<Metrics>,
    /// PostgreSQL version.
    pub pg_version: PgVersion,
}

impl AppState {
    /// Create a new `AppState` from a pool, config, and PG version.
    ///
    /// The schema cache starts as `None` — call [`reload_schema_cache`]
    /// after construction.
    pub fn new(pool: PgPool, config: AppConfig, pg_version: PgVersion) -> Self {
        let config_swap = Arc::new(ArcSwap::new(Arc::new(config)));
        let auth = AuthState::with_shared_config(config_swap.clone());
        let jwt_cache = auth.cache.clone();
        Self {
            pool,
            config: config_swap,
            schema_cache: Arc::new(ArcSwap::new(Arc::new(None))),
            auth,
            jwt_cache,
            metrics: Arc::new(Metrics::default()),
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
    ///
    /// Re-reads the config file (if one was specified at startup) and
    /// environment variables, then atomically swaps in the new config.
    /// Because `AuthState` shares the same `ArcSwap`, the auth middleware
    /// automatically picks up the new JWT secret / audience / role settings.
    pub async fn reload_config(&self) -> Result<(), Error> {
        let current = self.config.load();
        let file_path = current.config_file_path.clone();
        let new_config = crate::config::load_config(
            file_path.as_deref(),
            std::collections::HashMap::new(),
        )
        .await
        .map_err(|e| Error::InvalidConfig { message: e.to_string() })?;

        self.config.store(Arc::new(new_config));

        tracing::info!("Configuration reloaded successfully");
        Ok(())
    }

    /// Load or reload the schema cache from the database.
    pub async fn reload_schema_cache(&self) -> Result<(), Error> {
        let config = self.config.load();
        let introspector = crate::schema_cache::SqlxIntrospector::new(&self.pool);
        let cache = SchemaCache::load(&introspector, &config).await?;

        self.metrics
            .schema_cache_reloads
            .fetch_add(1, Ordering::Relaxed);
        self.schema_cache.store(Arc::new(Some(cache)));

        tracing::info!("Schema cache reloaded successfully");
        Ok(())
    }
}

/// Query the PostgreSQL version from the connection pool.
pub async fn query_pg_version(pool: &PgPool) -> Result<PgVersion, Error> {
    let row: (String,) = sqlx::query_as("SHOW server_version")
        .fetch_one(pool)
        .await
        .map_err(|e| Error::DbConnection(format!("Failed to query PG version: {}", e)))?;

    let version_str = &row.0;
    let parts: Vec<&str> = version_str.split('.').collect();
    Ok(PgVersion {
        major: parts
            .first()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        minor: parts
            .get(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        patch: parts
            .get(2)
            .and_then(|s| {
                // Handle "15.4 (Debian 15.4-1.pgdg120+1)" style
                s.split_whitespace().next().and_then(|v| v.parse().ok())
            })
            .unwrap_or(0),
    })
}
