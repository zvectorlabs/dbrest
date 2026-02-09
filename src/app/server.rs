//! HTTP server setup and lifecycle
//!
//! Creates the main API server and the admin server, wires up graceful
//! shutdown, and starts the NOTIFY listener.
//!
//! # Startup Sequence
//!
//! 1. Create `AppState` (connect to DB, query version).
//! 2. Load schema cache.
//! 3. Start admin server (separate port).
//! 4. Start NOTIFY listener (background task).
//! 5. Start main API server.
//!
//! # Graceful Shutdown
//!
//! Listens for `SIGTERM` and `Ctrl+C`. On receipt, stops accepting new
//! connections and drains in-flight requests before exiting.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::net::TcpListener;

use crate::config::AppConfig;
use crate::error::Error;

use super::admin::create_admin_router;
use super::listener::start_notify_listener;
use super::router::create_router;
use super::state::{AppState, query_pg_version};

/// Start the PgREST server.
///
/// This is the main entry point for the application. It initializes all
/// components and starts serving HTTP requests.
///
/// # Arguments
///
/// * `config` — the parsed application configuration.
///
/// # Returns
///
/// Returns `Ok(())` when the server shuts down gracefully, or an error
/// if initialization fails.
pub async fn start_server(config: AppConfig) -> Result<(), Error> {
    // 1. Create database pool
    tracing::info!("Connecting to database…");
    let pool = PgPoolOptions::new()
        .max_connections(config.db_pool_size as u32)
        .acquire_timeout(Duration::from_secs(config.db_pool_acquisition_timeout))
        .max_lifetime(Duration::from_secs(config.db_pool_max_lifetime))
        .idle_timeout(Duration::from_secs(config.db_pool_max_idletime))
        .connect(&config.db_uri)
        .await
        .map_err(|e| Error::DbConnection(e.to_string()))?;

    // 2. Query PostgreSQL version
    let pg_version = query_pg_version(&pool).await?;
    tracing::info!(pg_version = %pg_version, "Connected to PostgreSQL");

    if pg_version.major < 12 {
        return Err(Error::UnsupportedPgVersion {
            major: pg_version.major,
            minor: pg_version.minor,
        });
    }

    // 3. Initialize application state
    let state = AppState::new(pool, config.clone(), pg_version);

    // 4. Load schema cache
    tracing::info!("Loading schema cache…");
    state.reload_schema_cache().await?;

    // 5. Build routers
    let main_router = create_router(state.clone());
    let admin_router = create_admin_router(state.clone());

    // 6. Cancellation channel for background tasks
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    // 7. Start NOTIFY listener
    if config.db_channel_enabled {
        let notify_state = state.clone();
        tokio::spawn(async move {
            start_notify_listener(notify_state, cancel_rx).await;
        });
    }

    // 8. Start admin server (if configured)
    if let Some(admin_port) = config.admin_server_port {
        let admin_ip = parse_address(&config.admin_server_host)?;
        let admin_addr = SocketAddr::new(admin_ip, admin_port);
        let admin_listener = TcpListener::bind(admin_addr)
            .await
            .map_err(|e| Error::Internal(format!("Failed to bind admin server: {}", e)))?;

        tracing::info!(addr = %admin_addr, "Admin server listening");

        tokio::spawn(async move {
            if let Err(e) = axum::serve(admin_listener, admin_router).await {
                tracing::error!(error = %e, "Admin server error");
            }
        });
    }

    // 9. Start main server — Unix socket or TCP
    #[cfg(unix)]
    if let Some(ref socket_path) = config.server_unix_socket {
        serve_unix_socket(main_router, socket_path, config.server_unix_socket_mode).await?;
    } else {
        serve_tcp(main_router, &config).await?;
    }

    #[cfg(not(unix))]
    {
        if config.server_unix_socket.is_some() {
            return Err(Error::InvalidConfig {
                message: "Unix sockets are not supported on this platform".to_string(),
            });
        }
        serve_tcp(main_router, &config).await?;
    }

    // 10. Cleanup
    tracing::info!("Shutting down…");
    let _ = cancel_tx.send(true);

    Ok(())
}

/// Start the main server on a TCP socket.
async fn serve_tcp(router: axum::Router, config: &AppConfig) -> Result<(), Error> {
    let server_ip = parse_address(&config.server_host)?;
    let server_addr = SocketAddr::new(server_ip, config.server_port);
    let main_listener = TcpListener::bind(server_addr)
        .await
        .map_err(|e| Error::Internal(format!("Failed to bind main server: {}", e)))?;

    tracing::info!(addr = %server_addr, "PgREST server listening");

    axum::serve(main_listener, router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| Error::Internal(format!("Server error: {}", e)))
}

/// Start the main server on a Unix domain socket.
///
/// Uses a manual accept loop since `axum::serve` in 0.7 only accepts
/// `TcpListener`. Each accepted connection is served with `hyper_util`.
#[cfg(unix)]
async fn serve_unix_socket(
    router: axum::Router,
    socket_path: &std::path::Path,
    mode: u32,
) -> Result<(), Error> {
    use hyper_util::rt::TokioIo;
    use std::os::unix::fs::PermissionsExt;

    // Remove stale socket file if it exists
    let _ = std::fs::remove_file(socket_path);

    let uds = tokio::net::UnixListener::bind(socket_path).map_err(|e| {
        Error::Internal(format!(
            "Failed to bind Unix socket '{}': {}",
            socket_path.display(),
            e
        ))
    })?;

    // Set file permissions
    std::fs::set_permissions(socket_path, std::fs::Permissions::from_mode(mode)).map_err(|e| {
        Error::Internal(format!(
            "Failed to set socket permissions on '{}': {}",
            socket_path.display(),
            e
        ))
    })?;

    tracing::info!(path = %socket_path.display(), "PgREST server listening (Unix socket)");

    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            result = uds.accept() => {
                let (stream, _addr) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        tracing::warn!(error = %e, "Unix socket accept error");
                        continue;
                    }
                };

                // Clone router for this connection — Router is cheaply clonable
                let svc = router.clone();
                tokio::spawn(async move {
                    let io = TokioIo::new(stream);
                    let hyper_svc = hyper_util::service::TowerToHyperService::new(svc);
                    let conn = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    );
                    if let Err(e) = conn.serve_connection(io, hyper_svc).await {
                        tracing::debug!(error = %e, "Connection error");
                    }
                });
            }
            _ = &mut shutdown => {
                tracing::info!("Shutting down Unix socket server");
                break;
            }
        }
    }

    // Clean up socket file
    let _ = std::fs::remove_file(socket_path);
    Ok(())
}

/// Parse a PgREST host string into an `IpAddr`.
///
/// Supported formats:
///
/// | Input       | Result                            |
/// |-------------|-----------------------------------|
/// | `!4`        | `0.0.0.0` (IPv4 any)              |
/// | `!6`        | `::` (IPv6 any)                   |
/// | `*` / `*4`  | `0.0.0.0` (IPv4 any, alias)       |
/// | `*6`        | `::` (IPv6 any, alias)            |
/// | `127.0.0.1` | Parsed as literal IPv4            |
/// | `::1`       | Parsed as literal IPv6            |
/// | `localhost` | `127.0.0.1`                       |
///
/// Returns an error if the host string is unrecognised.
pub fn parse_address(host: &str) -> Result<IpAddr, Error> {
    match host {
        "!4" | "*" | "*4" => Ok(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
        "!6" | "*6" => Ok(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
        "localhost" => Ok(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        other => other.parse::<IpAddr>().map_err(|_| Error::InvalidConfig {
            message: format!("Invalid server host: '{other}'"),
        }),
    }
}

/// Wait for a shutdown signal (SIGTERM or Ctrl+C).
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address_ipv4_any() {
        assert_eq!(
            parse_address("!4").unwrap(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn test_parse_address_ipv6_any() {
        assert_eq!(
            parse_address("!6").unwrap(),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn test_parse_address_star() {
        assert_eq!(
            parse_address("*").unwrap(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn test_parse_address_star4() {
        assert_eq!(
            parse_address("*4").unwrap(),
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn test_parse_address_star6() {
        assert_eq!(
            parse_address("*6").unwrap(),
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        );
    }

    #[test]
    fn test_parse_address_localhost() {
        assert_eq!(
            parse_address("localhost").unwrap(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
    }

    #[test]
    fn test_parse_address_literal_ipv4() {
        let addr = parse_address("192.168.1.1").unwrap();
        assert_eq!(addr, IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)));
    }

    #[test]
    fn test_parse_address_literal_ipv6() {
        let addr = parse_address("::1").unwrap();
        assert_eq!(addr, IpAddr::V6(Ipv6Addr::LOCALHOST));
    }

    #[test]
    fn test_parse_address_invalid() {
        let err = parse_address("not-an-ip");
        assert!(err.is_err());
    }

    #[test]
    fn test_parse_address_loopback() {
        assert_eq!(
            parse_address("127.0.0.1").unwrap(),
            IpAddr::V4(Ipv4Addr::LOCALHOST)
        );
    }
}
