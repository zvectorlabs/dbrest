//! dbrest binary entry point
//!
//! Parses CLI arguments, loads configuration, initialises logging,
//! and starts the HTTP server. Supports both PostgreSQL and SQLite backends.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

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

    // Initialize tracing subscriber (fmt + optional OTLP span export)
    let _tracer_provider = init_tracing(&config);

    // Initialize metrics exporter (OTLP push) if enabled
    let _meter_provider = init_metrics(&config);

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

/// Initialize the tracing subscriber with optional OTLP span export.
///
/// When `tracing_enabled` is true, spans are exported via OTLP alongside
/// the console fmt output. When false, only console logging is active.
fn init_tracing(config: &AppConfig) -> Option<opentelemetry_sdk::trace::SdkTracerProvider> {
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let fmt_layer = tracing_subscriber::fmt::layer();
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if !config.tracing_enabled {
        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
        return None;
    }

    use opentelemetry::trace::TracerProvider;
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::trace::Sampler;

    // Set W3C TraceContext propagator for traceparent header extraction
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.metrics_otlp_endpoint)
        .build()
        .expect("Failed to build OTLP span exporter");

    let service_name: &'static str =
        Box::leak(config.metrics_service_name.clone().into_boxed_str());

    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_sampler(Sampler::TraceIdRatioBased(config.tracing_sampling_ratio))
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name)
                .build(),
        )
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("dbrest"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    tracing::info!(
        endpoint = %config.metrics_otlp_endpoint,
        sampling_ratio = config.tracing_sampling_ratio,
        "OTLP tracing exporter initialized"
    );

    Some(tracer_provider)
}

/// Initialize the `metrics` global recorder backed by an OTLP exporter.
///
/// Returns the `SdkMeterProvider` so it can be kept alive (and flushed on
/// shutdown). Returns `None` when metrics are disabled.
fn init_metrics(config: &AppConfig) -> Option<opentelemetry_sdk::metrics::SdkMeterProvider> {
    use opentelemetry_otlp::WithExportConfig;

    if !config.metrics_enabled {
        return None;
    }

    let exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_tonic()
        .with_endpoint(&config.metrics_otlp_endpoint)
        .build()
        .expect("Failed to build OTLP metric exporter");

    let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
        .with_interval(Duration::from_secs(config.metrics_export_interval_secs))
        .build();

    let service_name: &'static str =
        Box::leak(config.metrics_service_name.clone().into_boxed_str());
    let (provider, _recorder) = metrics_exporter_opentelemetry::Recorder::builder(service_name)
        .with_meter_provider(|builder| builder.with_reader(reader))
        .install()
        .expect("Failed to install metrics recorder");

    tracing::info!(
        endpoint = %config.metrics_otlp_endpoint,
        interval_secs = config.metrics_export_interval_secs,
        service_name = %config.metrics_service_name,
        "OTLP metrics exporter initialized"
    );

    Some(provider)
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
        config.db_busy_timeout_ms,
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

    if config.metrics_enabled {
        dbrest_core::app::metrics::start_pool_metrics_reporter(db.clone());
    }

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
        config.db_busy_timeout_ms,
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

    if config.metrics_enabled {
        dbrest_core::app::metrics::start_pool_metrics_reporter(db.clone());
    }

    dbrest_core::app::server::start_server_with_backend(db, dialect, db_version, config).await
}
