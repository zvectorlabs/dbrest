use async_trait::async_trait;
use dbrest_core::notifier::{ChangeEvent, ChangeNotifier};
use sqlx::PgPool;
use sqlx::postgres::PgListener;
use tokio::sync::broadcast;
use tracing::{error, warn};

pub struct PgChangeNotifier {
    sender: broadcast::Sender<ChangeEvent>,
}

impl PgChangeNotifier {
    /// Create a new PgChangeNotifier that listens on the given channel.
    /// Spawns a background task to receive NOTIFY events.
    pub async fn new(pool: PgPool, channel: &str, capacity: usize) -> Result<Self, sqlx::Error> {
        let (sender, _) = broadcast::channel(capacity);
        let tx = sender.clone();
        let channel = channel.to_string();

        // Create PgListener and subscribe
        let mut listener = PgListener::connect_with(&pool).await?;
        listener.listen(&channel).await?;

        // Spawn background receive loop
        tokio::spawn(async move {
            Self::receive_loop(listener, tx, channel).await;
        });

        Ok(Self { sender })
    }

    async fn receive_loop(
        mut listener: PgListener,
        sender: broadcast::Sender<ChangeEvent>,
        channel: String,
    ) {
        loop {
            match listener.recv().await {
                Ok(notification) => {
                    let payload = notification.payload();
                    match serde_json::from_str::<ChangeEvent>(payload) {
                        Ok(event) => {
                            // Ignore send errors (no active subscribers)
                            let _ = sender.send(event);
                        }
                        Err(e) => {
                            warn!(
                                channel = %channel,
                                payload = %payload,
                                error = %e,
                                "Failed to parse NOTIFY payload as ChangeEvent"
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        channel = %channel,
                        error = %e,
                        "PgListener error, reconnecting in 5s"
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    // PgListener auto-reconnects on next recv() call
                }
            }
        }
    }
}

#[async_trait]
impl ChangeNotifier for PgChangeNotifier {
    fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    async fn notify(&self, event: ChangeEvent) {
        let _ = self.sender.send(event);
    }

    fn catches_external_writes(&self) -> bool {
        true
    }
}
