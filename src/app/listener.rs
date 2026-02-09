//! PostgreSQL NOTIFY listener
//!
//! Listens for `NOTIFY` messages on a configurable channel. When a notification
//! is received, the schema cache and/or configuration are reloaded.
//!
//! # Usage
//!
//! PgREST-compatible clients can trigger a reload with:
//!
//! ```sql
//! NOTIFY pgrst, 'reload schema';
//! NOTIFY pgrst, 'reload config';
//! ```
//!
//! The listener runs in a background task and reconnects on failure.

use std::time::Duration;

use super::state::AppState;

/// Start the PostgreSQL NOTIFY listener in a background task.
///
/// The listener subscribes to the configured channel (default: `pgrst`) and
/// triggers schema cache or config reloads when notifications arrive.
///
/// # Reconnection
///
/// If the connection drops, the listener waits 5 seconds and retries.
/// This loop runs until the provided cancellation token is triggered.
pub async fn start_notify_listener(
    state: AppState,
    cancel: tokio::sync::watch::Receiver<bool>,
) {
    let channel = {
        let config = state.config.load();
        config.db_channel.clone()
    };

    tracing::info!(channel = %channel, "Starting NOTIFY listener");

    loop {
        if *cancel.borrow() {
            tracing::info!("NOTIFY listener shutting down");
            return;
        }

        match listen_loop(&state, &channel, &cancel).await {
            Ok(()) => {
                tracing::info!("NOTIFY listener exiting normally");
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "NOTIFY listener disconnected, reconnecting in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }
}

/// Inner listen loop — subscribes and processes notifications until
/// the connection drops or cancellation is requested.
async fn listen_loop(
    state: &AppState,
    channel: &str,
    cancel: &tokio::sync::watch::Receiver<bool>,
) -> Result<(), sqlx::Error> {
    let mut listener = sqlx::postgres::PgListener::connect_with(&state.pool).await?;
    listener.listen(channel).await?;

    tracing::info!(channel = channel, "Subscribed to NOTIFY channel");

    loop {
        if *cancel.borrow() {
            return Ok(());
        }

        // Wait for a notification with a timeout so we can check cancellation
        let notification = tokio::time::timeout(
            Duration::from_secs(30),
            listener.recv(),
        )
        .await;

        match notification {
            Ok(Ok(msg)) => {
                let payload = msg.payload();
                tracing::info!(payload = payload, "Received NOTIFY");

                if payload.contains("schema") || payload.contains("reload") {
                    if let Err(e) = state.reload_schema_cache().await {
                        tracing::error!(error = %e, "Failed to reload schema cache");
                    }
                }

                if payload.contains("config") {
                    if let Err(e) = state.reload_config().await {
                        tracing::error!(error = %e, "Failed to reload config");
                    }
                }
            }
            Ok(Err(e)) => {
                // Connection error
                return Err(e);
            }
            Err(_) => {
                // Timeout — loop back and check cancellation
                continue;
            }
        }
    }
}
