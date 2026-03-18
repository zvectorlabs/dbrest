//! Metrics helpers for background reporting.

use std::sync::Arc;

use crate::backend::DatabaseBackend;

/// Spawn a background task that periodically reports connection pool gauges.
///
/// Reports `db.pool.connections.active`, `db.pool.connections.idle`, and
/// `db.pool.connections.max` every 15 seconds via the `metrics` facade.
///
/// Does nothing if the backend's `pool_status()` returns `None`.
pub fn start_pool_metrics_reporter(db: Arc<dyn DatabaseBackend>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(15));
        loop {
            interval.tick().await;
            if let Some(status) = db.pool_status() {
                metrics::gauge!("db.pool.connections.active").set(status.active as f64);
                metrics::gauge!("db.pool.connections.idle").set(status.idle as f64);
                metrics::gauge!("db.pool.connections.max").set(status.max_size as f64);
            }
        }
    });
}
