//! dbrest binary entry point
//!
//! Parses CLI arguments, loads configuration, initialises logging,
//! and starts the HTTP server. Supports both PostgreSQL and SQLite backends.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;

use dbrest_core::backend::{DatabaseBackend, SqlDialect};
use dbrest_core::config::AppConfig;
use dbrest_core::error::Error;
use dbrest_postgres::{PgBackend, PgDialect};
use dbrest_sqlite::{SqliteBackend, SqliteDialect};

/// dbrest — high-performance REST API for PostgreSQL and SQLite
#[derive(Parser, Debug)]
#[command(name = "dbrest", version, about)]
struct Args {
    /// Path to a configuration file
    #[arg(short, long, env = "DBREST_CONFIG")]
    config: Option<String>,

    /// Database connection URI (overrides config file)
    ///
    /// PostgreSQL: postgres://user:pass@host/db
    /// SQLite:     sqlite:path/to/db.sqlite or sqlite::memory:
    #[arg(long, env = "DBREST_DB_URI")]
    db_uri: Option<String>,

    /// Server bind port (overrides config file)
    #[arg(short, long, env = "DBREST_SERVER_PORT")]
    port: Option<u16>,
}

#[tokio::main]
async fn main() {
    // Initialise logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Load config (from file or defaults)
    let file_path = args.config.as_deref().map(Path::new);
    let mut config = match dbrest_core::load_config(file_path, HashMap::new()).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    // CLI overrides
    if let Some(ref uri) = args.db_uri {
        config.db_uri = uri.clone();
    }
    if let Some(port) = args.port {
        config.server_port = port;
    }

    // Detect backend from URI and start
    let result = if is_sqlite_uri(&config.db_uri) {
        start_sqlite_server(config).await
    } else {
        start_pg_server(config).await
    };

    if let Err(e) = result {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}

/// Detect whether a URI targets SQLite.
fn is_sqlite_uri(uri: &str) -> bool {
    uri.starts_with("sqlite:") || uri.ends_with(".sqlite") || uri.ends_with(".db")
}

/// Create a PostgreSQL backend and start the server.
async fn start_pg_server(config: AppConfig) -> Result<(), Error> {
    tracing::info!("Connecting to PostgreSQL database...");
    let backend = PgBackend::connect(
        &config.db_uri,
        config.db_pool_size as u32,
        config.db_pool_acquisition_timeout,
        config.db_pool_max_lifetime,
        config.db_pool_max_idletime,
    )
    .await?;

    let db_version = backend.version().await?;
    tracing::info!(db_version = %db_version, "Connected to database");

    let (min_major, min_minor) = backend.min_version();
    if db_version.major < min_major
        || (db_version.major == min_major && db_version.minor < min_minor)
    {
        return Err(Error::UnsupportedPgVersion {
            major: db_version.major,
            minor: db_version.minor,
        });
    }

    let db: Arc<dyn DatabaseBackend> = Arc::new(backend);
    let dialect: Arc<dyn SqlDialect> = Arc::new(PgDialect);

    dbrest_core::app::server::start_server_with_backend(db, dialect, db_version, config).await
}

/// Create a SQLite backend and start the server.
async fn start_sqlite_server(config: AppConfig) -> Result<(), Error> {
    tracing::info!("Connecting to SQLite database...");
    let backend = SqliteBackend::connect(
        &config.db_uri,
        config.db_pool_size as u32,
        config.db_pool_acquisition_timeout,
        config.db_pool_max_lifetime,
        config.db_pool_max_idletime,
    )
    .await?;

    let db_version = backend.version().await?;
    tracing::info!(db_version = %db_version, "Connected to database");

    let (min_major, min_minor) = backend.min_version();
    if db_version.major < min_major
        || (db_version.major == min_major && db_version.minor < min_minor)
    {
        return Err(Error::UnsupportedPgVersion {
            major: db_version.major,
            minor: db_version.minor,
        });
    }

    let db: Arc<dyn DatabaseBackend> = Arc::new(backend);
    let dialect: Arc<dyn SqlDialect> = Arc::new(SqliteDialect);

    dbrest_core::app::server::start_server_with_backend(db, dialect, db_version, config).await
}
