//! PgREST binary entry point
//!
//! Parses CLI arguments, loads configuration, initialises logging,
//! and starts the HTTP server.

use std::collections::HashMap;
use std::path::Path;

use clap::Parser;

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
    let mut config = match pgrest::load_config(file_path, HashMap::new()).await {
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

    // Start the server
    if let Err(e) = pgrest::start_server(config).await {
        eprintln!("Server error: {}", e);
        std::process::exit(1);
    }
}
