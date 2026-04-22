use super::{ChangeEvent, ChangeNotifier};
use async_trait::async_trait;
use tokio::sync::broadcast;

pub struct AppLevelNotifier {
    sender: broadcast::Sender<ChangeEvent>,
}

impl AppLevelNotifier {
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }
}

#[async_trait]
impl ChangeNotifier for AppLevelNotifier {
    fn subscribe(&self) -> broadcast::Receiver<ChangeEvent> {
        self.sender.subscribe()
    }

    async fn notify(&self, event: ChangeEvent) {
        // Ignore send errors (no active subscribers)
        let _ = self.sender.send(event);
    }

    fn catches_external_writes(&self) -> bool {
        false
    }
}
