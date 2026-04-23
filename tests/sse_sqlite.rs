//! SSE (Server-Sent Events) integration tests for the SQLite backend.
//!
//! Tests the `/listen/:resource` endpoint, mutation→SSE notification flow,
//! concurrent subscribers, and edge cases using an in-memory SQLite database.
//! No Docker required.

#![allow(clippy::field_reassign_with_default)]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dbrest::app::router::create_router;
use dbrest::app::state::AppState;
use dbrest::backend::{DatabaseBackend, DbVersion, SqlDialect};
use dbrest::config::AppConfig;
use dbrest_core::notifier::app_level::AppLevelNotifier;
use dbrest_core::notifier::{ChangeEvent, ChangeNotifier, ChangeOp};
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::time::timeout;

use dbrest_sqlite::{SqliteBackend, SqliteDialect};

// ==========================================================================
// SSE Test Client — parses SSE text protocol from reqwest streaming response
// ==========================================================================

#[derive(Debug)]
struct SseEvent {
    event_type: String,
    data: String,
}

impl SseEvent {
    /// Parse the data field as a ChangeEvent.
    fn change_event(&self) -> ChangeEvent {
        serde_json::from_str(&self.data).expect("SSE data should be valid ChangeEvent JSON")
    }
}

struct SseTestClient {
    response: reqwest::Response,
    buffer: String,
}

impl SseTestClient {
    /// Connect to the SSE endpoint. Returns after verifying 200 OK.
    async fn connect(client: &Client, url: &str) -> Self {
        let response = client
            .get(url)
            .header("accept", "text/event-stream")
            .send()
            .await
            .expect("SSE connection failed");
        assert_eq!(response.status(), StatusCode::OK);
        Self {
            response,
            buffer: String::new(),
        }
    }

    /// Read the next SSE event, skipping heartbeat comments.
    /// Returns None on timeout.
    async fn next_event(&mut self, dur: Duration) -> Option<SseEvent> {
        timeout(dur, async {
            loop {
                // Try to parse a complete event from the buffer
                if let Some(event) = self.try_parse_event() {
                    return event;
                }
                // Read more data
                let chunk = self
                    .response
                    .chunk()
                    .await
                    .expect("SSE stream error")
                    .expect("SSE stream ended unexpectedly");
                self.buffer.push_str(&String::from_utf8_lossy(&chunk));
            }
        })
        .await
        .ok()
    }

    /// Try to extract a complete SSE event from the buffer.
    /// SSE events are terminated by a blank line (\n\n).
    fn try_parse_event(&mut self) -> Option<SseEvent> {
        loop {
            let separator = self.buffer.find("\n\n")?;
            let block = self.buffer[..separator].to_string();
            self.buffer = self.buffer[separator + 2..].to_string();

            let mut event_type = String::new();
            let mut data_parts = Vec::new();
            let mut is_comment_only = true;

            for line in block.lines() {
                if line.starts_with(':') {
                    // Comment (heartbeat), skip
                    continue;
                }
                is_comment_only = false;
                if let Some(val) = line.strip_prefix("event:") {
                    event_type = val.trim().to_string();
                } else if let Some(val) = line.strip_prefix("data:") {
                    data_parts.push(val.trim().to_string());
                }
            }

            if is_comment_only {
                // Pure comment block (heartbeat), skip and try next block
                continue;
            }

            return Some(SseEvent {
                event_type,
                data: data_parts.join("\n"),
            });
        }
    }

    /// Collect exactly `n` events with a total timeout.
    async fn collect_events(&mut self, n: usize, dur: Duration) -> Vec<SseEvent> {
        let mut events = Vec::with_capacity(n);
        let deadline = tokio::time::Instant::now() + dur;
        for _ in 0..n {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match self.next_event(remaining).await {
                Some(e) => events.push(e),
                None => break,
            }
        }
        events
    }

    /// Assert that no event arrives within the given duration.
    async fn expect_no_event(&mut self, dur: Duration) {
        let result = self.next_event(dur).await;
        assert!(
            result.is_none(),
            "Expected no SSE event but received: {:?}",
            result
        );
    }
}

// ==========================================================================
// TestServer helper (SQLite variant with notifier support)
// ==========================================================================

struct TestServer {
    client: Client,
    base_url: String,
    #[allow(dead_code)]
    db: common::TestSqliteDb,
    state: AppState,
}

impl TestServer {
    /// Start a test server with AppLevelNotifier wired in.
    async fn start() -> Self {
        Self::start_inner(true).await
    }

    /// Start a test server WITHOUT a notifier (for 503 test).
    async fn start_without_notifier() -> Self {
        Self::start_inner(false).await
    }

    async fn start_inner(with_notifier: bool) -> Self {
        let db = common::TestSqliteDb::new()
            .await
            .expect("Failed to create SQLite test database");

        let pool = db.pool().clone();

        let mut config = AppConfig::default();
        config.db_schemas = vec!["main".to_string()];
        config.db_anon_role = Some("anon".to_string());

        let backend: Arc<dyn DatabaseBackend> = Arc::new(SqliteBackend::from_pool(pool));
        let dialect: Arc<dyn SqlDialect> = Arc::new(SqliteDialect);
        let db_version = DbVersion {
            major: 3,
            minor: 45,
            patch: 0,
            engine: "SQLite".to_string(),
        };

        let mut state = AppState::new_with_backend(backend, dialect, config, db_version);

        if with_notifier {
            let notifier: Arc<dyn ChangeNotifier> = Arc::new(AppLevelNotifier::new(1024));
            state = state.with_notifier(notifier);
        }

        state
            .reload_schema_cache()
            .await
            .expect("Failed to load SQLite schema cache");

        let router = create_router(state.clone());

        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .expect("Failed to bind TCP listener");
        let addr = listener.local_addr().unwrap();
        let base_url = format!("http://{}", addr);

        tokio::spawn(async move {
            axum::serve(listener, router).await.expect("Server error");
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let client = Client::new();

        Self {
            client,
            base_url,
            db,
            state,
        }
    }

    fn listen_url(&self, resource: &str) -> String {
        format!("{}/listen/{}", self.base_url, resource)
    }

    async fn post_json(&self, path: &str, body: &Value) -> reqwest::Response {
        self.client
            .post(format!("{}{}", self.base_url, path))
            .header("content-type", "application/json")
            .header("prefer", "return=representation")
            .json(body)
            .send()
            .await
            .unwrap()
    }

    async fn patch_json(&self, path: &str, body: &Value) -> reqwest::Response {
        self.client
            .patch(format!("{}{}", self.base_url, path))
            .header("content-type", "application/json")
            .header("prefer", "return=representation")
            .json(body)
            .send()
            .await
            .unwrap()
    }

    async fn delete(&self, path: &str) -> reqwest::Response {
        self.client
            .delete(format!("{}{}", self.base_url, path))
            .send()
            .await
            .unwrap()
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await
            .unwrap()
    }
}

// ==========================================================================
// Category A: SSE Endpoint Basics
// ==========================================================================

#[tokio::test]
async fn sse_returns_503_when_no_notifier() {
    let server = TestServer::start_without_notifier().await;
    let resp = server
        .client
        .get(server.listen_url("users"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn sse_connection_returns_200_event_stream() {
    let server = TestServer::start().await;
    let resp = server
        .client
        .get(server.listen_url("products"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(
        ct.contains("text/event-stream"),
        "Expected text/event-stream content-type, got: {}",
        ct
    );
}

#[tokio::test]
async fn sse_filters_events_by_table_name() {
    let server = TestServer::start().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fire a notification for a DIFFERENT table — should be filtered out
    let notifier = server.state.notifier.as_ref().unwrap();
    notifier
        .notify(ChangeEvent {
            table: "users".to_string(),
            schema: "main".to_string(),
            event: ChangeOp::Insert,
            new: None,
            old: None,
        })
        .await;

    // Fire a notification for the MATCHING table
    notifier
        .notify(ChangeEvent {
            table: "products".to_string(),
            schema: "main".to_string(),
            event: ChangeOp::Update,
            new: None,
            old: None,
        })
        .await;

    let event = sse
        .next_event(Duration::from_secs(2))
        .await
        .expect("Should receive products event");
    assert_eq!(event.event_type, "UPDATE");
    let ce = event.change_event();
    assert_eq!(ce.table, "products");
}

// ==========================================================================
// Category B: Mutation → SSE Full-Stack Flow
// ==========================================================================

#[tokio::test]
async fn sse_post_produces_insert_event() {
    let server = TestServer::start().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server
        .post_json("/products", &json!({"name": "SSE Widget", "price": 5.99}))
        .await;
    assert!(resp.status().is_success(), "POST failed: {}", resp.status());

    let event = sse
        .next_event(Duration::from_secs(2))
        .await
        .expect("Should receive INSERT event");
    assert_eq!(event.event_type, "INSERT");
    let ce = event.change_event();
    assert_eq!(ce.table, "products");
    assert_eq!(ce.event, ChangeOp::Insert);
    assert_eq!(ce.schema, "public"); // notify_change hardcodes "public"
}

#[tokio::test]
async fn sse_patch_produces_update_event() {
    let server = TestServer::start().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server
        .patch_json("/products?name=eq.Widget", &json!({"price": 19.99}))
        .await;
    assert!(
        resp.status().is_success(),
        "PATCH failed: {}",
        resp.status()
    );

    let event = sse
        .next_event(Duration::from_secs(2))
        .await
        .expect("Should receive UPDATE event");
    assert_eq!(event.event_type, "UPDATE");
    let ce = event.change_event();
    assert_eq!(ce.table, "products");
    assert_eq!(ce.event, ChangeOp::Update);
}

#[tokio::test]
async fn sse_delete_produces_delete_event() {
    let server = TestServer::start().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server.delete("/products?name=eq.Doohickey").await;
    assert!(
        resp.status().is_success(),
        "DELETE failed: {}",
        resp.status()
    );

    let event = sse
        .next_event(Duration::from_secs(2))
        .await
        .expect("Should receive DELETE event");
    assert_eq!(event.event_type, "DELETE");
    let ce = event.change_event();
    assert_eq!(ce.table, "products");
    assert_eq!(ce.event, ChangeOp::Delete);
}

#[tokio::test]
async fn sse_multiple_mutations_arrive_in_order() {
    let server = TestServer::start().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // INSERT
    let resp = server
        .post_json("/products", &json!({"name": "OrderTest", "price": 1.00}))
        .await;
    assert!(resp.status().is_success());

    // UPDATE
    let resp = server
        .patch_json("/products?name=eq.OrderTest", &json!({"price": 2.00}))
        .await;
    assert!(resp.status().is_success());

    // DELETE
    let resp = server.delete("/products?name=eq.OrderTest").await;
    assert!(resp.status().is_success());

    let events = sse.collect_events(3, Duration::from_secs(5)).await;
    assert_eq!(events.len(), 3, "Expected 3 events, got {}", events.len());
    assert_eq!(events[0].event_type, "INSERT");
    assert_eq!(events[1].event_type, "UPDATE");
    assert_eq!(events[2].event_type, "DELETE");
}

#[tokio::test]
async fn sse_mutations_different_tables_filtered() {
    let server = TestServer::start().await;
    let mut sse_products =
        SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    let mut sse_tasks = SseTestClient::connect(&server.client, &server.listen_url("tasks")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Mutate products
    let resp = server
        .post_json("/products", &json!({"name": "FilterTest", "price": 3.00}))
        .await;
    assert!(resp.status().is_success());

    // Mutate tasks
    let resp = server
        .post_json("/tasks", &json!({"title": "FilterTask", "priority": "low"}))
        .await;
    assert!(resp.status().is_success());

    // products client should get products INSERT only
    let event = sse_products
        .next_event(Duration::from_secs(2))
        .await
        .expect("products client should receive event");
    assert_eq!(event.event_type, "INSERT");
    assert_eq!(event.change_event().table, "products");

    // tasks client should get tasks INSERT only
    let event = sse_tasks
        .next_event(Duration::from_secs(2))
        .await
        .expect("tasks client should receive event");
    assert_eq!(event.event_type, "INSERT");
    assert_eq!(event.change_event().table, "tasks");

    // Neither should get the other's event
    sse_products
        .expect_no_event(Duration::from_millis(300))
        .await;
    sse_tasks.expect_no_event(Duration::from_millis(300)).await;
}

// ==========================================================================
// Category D: Concurrent Subscribers
// ==========================================================================

#[tokio::test]
async fn sse_multiple_clients_same_resource_all_receive() {
    let server = TestServer::start().await;
    let mut sse1 = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    let mut sse2 = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    let mut sse3 = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server
        .post_json(
            "/products",
            &json!({"name": "BroadcastTest", "price": 7.00}),
        )
        .await;
    assert!(resp.status().is_success());

    // All three should receive the event
    for (i, sse) in [&mut sse1, &mut sse2, &mut sse3].iter_mut().enumerate() {
        let event = sse
            .next_event(Duration::from_secs(2))
            .await
            .unwrap_or_else(|| panic!("Client {} should receive event", i + 1));
        assert_eq!(event.event_type, "INSERT");
        assert_eq!(event.change_event().table, "products");
    }
}

#[tokio::test]
async fn sse_multiple_clients_different_resources_correct_filtering() {
    let server = TestServer::start().await;
    let mut sse_products =
        SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    let mut sse_tasks = SseTestClient::connect(&server.client, &server.listen_url("tasks")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Only mutate products
    let resp = server
        .post_json(
            "/products",
            &json!({"name": "IsolationTest", "price": 1.00}),
        )
        .await;
    assert!(resp.status().is_success());

    // products client receives event
    let event = sse_products
        .next_event(Duration::from_secs(2))
        .await
        .expect("products client should receive event");
    assert_eq!(event.event_type, "INSERT");

    // tasks client should NOT receive anything
    sse_tasks.expect_no_event(Duration::from_millis(500)).await;
}

#[tokio::test]
async fn sse_late_subscriber_misses_old_events() {
    let server = TestServer::start().await;

    // Fire mutation BEFORE connecting SSE
    let resp = server
        .post_json("/products", &json!({"name": "LateJoiner", "price": 2.00}))
        .await;
    assert!(resp.status().is_success());

    // Small delay to ensure the event has been broadcast
    tokio::time::sleep(Duration::from_millis(100)).await;

    // NOW connect — should not receive the old event
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    sse.expect_no_event(Duration::from_millis(500)).await;
}

// ==========================================================================
// Category E: Edge Cases & Robustness
// ==========================================================================

#[tokio::test]
async fn sse_rapid_fire_mutations_all_delivered() {
    let server = TestServer::start().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let count = 20;
    for i in 0..count {
        let resp = server
            .post_json(
                "/products",
                &json!({"name": format!("Rapid{}", i), "price": 1.00}),
            )
            .await;
        assert!(resp.status().is_success(), "POST {} failed", i);
    }

    let events = sse.collect_events(count, Duration::from_secs(10)).await;
    assert_eq!(
        events.len(),
        count,
        "Expected {} INSERT events, got {}",
        count,
        events.len()
    );
    for event in &events {
        assert_eq!(event.event_type, "INSERT");
    }
}

#[tokio::test]
async fn sse_client_disconnect_no_panic() {
    let server = TestServer::start().await;

    // Connect then immediately drop the SSE client
    {
        let _sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
        // _sse is dropped here, closing the connection
    }

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Fire a mutation — server should not panic
    let resp = server
        .post_json(
            "/products",
            &json!({"name": "AfterDisconnect", "price": 1.00}),
        )
        .await;
    assert!(resp.status().is_success());

    // Verify server is still healthy
    let resp = server.get("/products").await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn sse_listen_nonexistent_table_connects() {
    let server = TestServer::start().await;
    let mut sse =
        SseTestClient::connect(&server.client, &server.listen_url("nonexistent_table_xyz")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // The handler does not validate table existence — it just filters by name.
    // Manually fire an event for this nonexistent table.
    let notifier = server.state.notifier.as_ref().unwrap();
    notifier
        .notify(ChangeEvent {
            table: "nonexistent_table_xyz".to_string(),
            schema: "main".to_string(),
            event: ChangeOp::Insert,
            new: None,
            old: None,
        })
        .await;

    let event = sse
        .next_event(Duration::from_secs(2))
        .await
        .expect("Should receive event for nonexistent table");
    assert_eq!(event.event_type, "INSERT");
    assert_eq!(event.change_event().table, "nonexistent_table_xyz");
}
