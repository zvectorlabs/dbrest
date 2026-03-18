//! `DbrestApp` — fluent builder for constructing a dbrest router.
//!
//! This is the primary API for using dbrest-core as a library. It lets
//! consumers create an `axum::Router` that can be nested, merged, or
//! served standalone — instead of handing over the entire server lifecycle.
//!
//! # Single datasource
//!
//! ```rust,no_run
//! # async fn example() -> Result<(), dbrest_core::error::Error> {
//! use std::sync::Arc;
//! use dbrest_core::app::builder::DbrestApp;
//! use dbrest_core::config::AppConfig;
//! // Assume `MyBackend` and `MyDialect` implement the required traits.
//! # use dbrest_core::backend::{DatabaseBackend, SqlDialect};
//! # todo!()
//! # }
//! ```
//!
//! # Multi-datasource (future)
//!
//! Register multiple backends keyed by a profile name. The incoming
//! `Accept-Profile` / `Content-Profile` header selects which datasource
//! handles the request.
//!
//! ```rust,ignore
//! let app = DbrestApp::new()
//!     .datasource("pg", pg_backend, PgDialect, pg_config)
//!     .datasource("sqlite", sqlite_backend, SqliteDialect, sqlite_config)
//!     .build()
//!     .await?;
//! ```

use std::sync::Arc;

use axum::Router;

use crate::backend::{DatabaseBackend, DbVersion, SqlDialect};
use crate::config::AppConfig;
use crate::error::Error;

use super::admin::create_admin_router;
use super::router::create_router;
use super::state::AppState;

// =========================================================================
// Datasource — a named (backend, dialect, config) tuple
// =========================================================================

/// A single database datasource with its backend, dialect, config, and version.
pub struct Datasource {
    /// Profile name used for `Accept-Profile` / `Content-Profile` routing.
    pub name: String,
    pub backend: Arc<dyn DatabaseBackend>,
    pub dialect: Arc<dyn SqlDialect>,
    pub config: AppConfig,
    pub version: DbVersion,
}

impl Datasource {
    /// Create a datasource from its parts.
    pub fn new(
        name: impl Into<String>,
        backend: Arc<dyn DatabaseBackend>,
        dialect: Arc<dyn SqlDialect>,
        config: AppConfig,
        version: DbVersion,
    ) -> Self {
        Self {
            name: name.into(),
            backend,
            dialect,
            config,
            version,
        }
    }

    /// Build an `AppState` and load the schema cache for this datasource.
    pub async fn into_state(self) -> Result<AppState, Error> {
        let state =
            AppState::new_with_backend(self.backend, self.dialect, self.config, self.version);
        state.reload_schema_cache().await?;
        Ok(state)
    }
}

// =========================================================================
// DbrestApp builder
// =========================================================================

/// Fluent builder for constructing a dbrest `axum::Router`.
///
/// Use this when you want to embed dbrest in a larger application or
/// need fine-grained control over the server lifecycle.
pub struct DbrestApp {
    datasources: Vec<Datasource>,
    include_admin: bool,
    /// Optional path prefix for nesting (e.g. "/api").
    prefix: Option<String>,
}

impl DbrestApp {
    /// Create a new builder with no datasources.
    pub fn new() -> Self {
        Self {
            datasources: Vec::new(),
            include_admin: false,
            prefix: None,
        }
    }

    /// Add a datasource.
    pub fn datasource(mut self, ds: Datasource) -> Self {
        self.datasources.push(ds);
        self
    }

    /// Convenience: add a single datasource from its raw parts.
    pub fn with_backend(
        self,
        backend: Arc<dyn DatabaseBackend>,
        dialect: Arc<dyn SqlDialect>,
        config: AppConfig,
        version: DbVersion,
    ) -> Self {
        self.datasource(Datasource::new(
            "default", backend, dialect, config, version,
        ))
    }

    /// Include the admin router (health, ready, metrics, config) at `/admin`.
    pub fn with_admin(mut self) -> Self {
        self.include_admin = true;
        self
    }

    /// Set a path prefix for all dbrest routes (e.g. "/api/v1").
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Build the `axum::Router`.
    ///
    /// Loads schema caches for all datasources and wires up routing.
    /// For a single datasource, returns the standard dbrest router.
    /// For multiple datasources (future), routes by profile header.
    pub async fn build(self) -> Result<DbrestRouters, Error> {
        if self.datasources.is_empty() {
            return Err(Error::InvalidConfig {
                message: "DbrestApp requires at least one datasource".to_string(),
            });
        }

        // Build AppState for the first (or only) datasource
        // Safety: `self.datasources.is_empty()` is checked above, so this
        // will always succeed. Using `expect` to be explicit about the
        // invariant rather than panicking with an opaque unwrap message.
        let primary = self
            .datasources
            .into_iter()
            .next()
            .expect("BUG: datasources checked non-empty above");
        let state = primary.into_state().await?;

        let mut api_router = create_router(state.clone());
        if let Some(ref prefix) = self.prefix {
            api_router = Router::new().nest(prefix, api_router);
        }

        let admin_router = if self.include_admin {
            Some(create_admin_router(state.clone()))
        } else {
            None
        };

        Ok(DbrestRouters {
            api: api_router,
            admin: admin_router,
            state,
        })
    }

    /// Build and return only the API router (convenience for embedding).
    pub async fn into_router(self) -> Result<Router, Error> {
        Ok(self.build().await?.api)
    }
}

impl Default for DbrestApp {
    fn default() -> Self {
        Self::new()
    }
}

// =========================================================================
// DbrestRouters — the build output
// =========================================================================

/// Output of [`DbrestApp::build`].
///
/// Contains the API router, optional admin router, and the `AppState` for
/// advanced use (e.g. manually starting a NOTIFY listener or reloading
/// the schema cache).
pub struct DbrestRouters {
    /// The main REST API router.
    pub api: Router,
    /// Optional admin router (health, metrics, config).
    pub admin: Option<Router>,
    /// The constructed application state (for lifecycle management).
    pub state: AppState,
}

impl DbrestRouters {
    /// Merge the API and admin routers into a single router.
    ///
    /// Admin routes are nested under `/admin`.
    pub fn merged(self) -> Router {
        let mut router = self.api;
        if let Some(admin) = self.admin {
            router = router.nest("/admin", admin);
        }
        router
    }

    /// Start the NOTIFY listener in the background (if the backend supports it).
    ///
    /// Returns a cancel handle — send `true` to shut down the listener.
    pub fn start_listener(&self) -> tokio::sync::watch::Sender<bool> {
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        let config = self.state.config();

        if config.db_channel_enabled {
            let db = self.state.db.clone();
            let state = self.state.clone();
            let channel = config.db_channel.clone();
            tokio::spawn(async move {
                super::server::start_notify_listener_public(db, state, &channel, cancel_rx).await;
            });
        }

        cancel_tx
    }
}
