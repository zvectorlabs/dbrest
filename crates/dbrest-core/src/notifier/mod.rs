pub mod app_level;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub table: String,
    pub schema: String,
    pub event: ChangeOp,
    pub new: Option<serde_json::Value>,
    pub old: Option<serde_json::Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum ChangeOp {
    Insert,
    Update,
    Delete,
}

#[async_trait]
pub trait ChangeNotifier: Send + Sync + 'static {
    /// Subscribe to change events. Returns a broadcast receiver.
    fn subscribe(&self) -> broadcast::Receiver<ChangeEvent>;

    /// Notify all subscribers of a change event.
    async fn notify(&self, event: ChangeEvent);

    /// Whether this notifier catches writes from external sources (not via this API).
    /// PgChangeNotifier returns true (uses LISTEN/NOTIFY), AppLevelNotifier returns false.
    fn catches_external_writes(&self) -> bool;
}
