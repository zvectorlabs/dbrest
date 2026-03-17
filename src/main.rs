//! PgREST binary entry point
//!
//! Parses CLI arguments, loads configuration, initialises logging,
//! and starts the HTTP server.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use clap::Parser;

use dbrest_core::backend::{DatabaseBackend, SqlDialect};
use dbrest_core::config::AppConfig;
use dbrest_core::error::Error;
use dbrest_postgres::{PgBackend, PgDialect};

/// PgREST — high-performance REST API for PostgreSQL
#[derive(Parser, Debug)]
#[command(name = "pgrest", version, about)]
struct Args {
    /// Path to a configuration file
    #[arg(short, long, env = "PGREST_CONFIG")]
    config: Option<String>,

    /// Database connection URI (overrides config file)
    #[arg(long, env = "PGREST_DB_URI")]
    db_uri: Option<String>,

    /// Server bind port (overrides config file)
    #[arg(short, long, env = "PGREST_SERVER_PORT")]
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

    // Start the server with the PostgreSQL backend
    if let Err(e) = start_pg_server(config).await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}

/// Create a PostgreSQL backend and start the server.
async fn start_pg_server(config: AppConfig) -> Result<(), Error> {
    tracing::info!("Connecting to database...");
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
