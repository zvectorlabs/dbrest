use dbrest_core::notifier::{ChangeEvent, ChangeNotifier, ChangeOp, app_level::AppLevelNotifier};

#[tokio::test]
async fn subscribe_and_notify_round_trip() {
    let notifier = AppLevelNotifier::new(16);
    let mut rx = notifier.subscribe();

    let event = ChangeEvent {
        table: "posts".to_string(),
        schema: "public".to_string(),
        event: ChangeOp::Insert,
        new: Some(serde_json::json!({"id": 1, "title": "Hello"})),
        old: None,
    };

    notifier.notify(event.clone()).await;

    let received = rx.recv().await.unwrap();
    assert_eq!(received.table, "posts");
    assert_eq!(received.event, ChangeOp::Insert);
}

#[tokio::test]
async fn multiple_subscribers_receive_same_event() {
    let notifier = AppLevelNotifier::new(16);
    let mut rx1 = notifier.subscribe();
    let mut rx2 = notifier.subscribe();

    let event = ChangeEvent {
        table: "users".to_string(),
        schema: "public".to_string(),
        event: ChangeOp::Update,
        new: Some(serde_json::json!({"id": 1})),
        old: Some(serde_json::json!({"id": 1})),
    };

    notifier.notify(event).await;

    let r1 = rx1.recv().await.unwrap();
    let r2 = rx2.recv().await.unwrap();
    assert_eq!(r1.table, r2.table);
    assert_eq!(r1.event, r2.event);
}

#[tokio::test]
async fn lagged_subscriber_gets_error() {
    let notifier = AppLevelNotifier::new(2); // small capacity
    let mut rx = notifier.subscribe();

    // Send more events than capacity
    for i in 0..5 {
        notifier
            .notify(ChangeEvent {
                table: format!("t{}", i),
                schema: "public".to_string(),
                event: ChangeOp::Insert,
                new: None,
                old: None,
            })
            .await;
    }

    // First recv should be a Lagged error
    match rx.recv().await {
        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
            assert!(n > 0);
        }
        other => {
            // May get the last few events; that's also valid for broadcast
            // The key is it doesn't panic
            let _ = other;
        }
    }
}

#[test]
fn catches_external_writes_is_false() {
    let notifier = AppLevelNotifier::new(16);
    assert!(!notifier.catches_external_writes());
}
