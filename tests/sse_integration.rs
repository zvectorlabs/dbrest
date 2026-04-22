//! SSE (Server-Sent Events) integration tests for the PostgreSQL backend.
//!
//! Tests the `/listen/:resource` endpoint, mutation→SSE notification flow,
//! PostgreSQL LISTEN/NOTIFY integration, and edge cases using testcontainers.
//! All tests require Docker and are marked `#[ignore]`.

#![allow(clippy::field_reassign_with_default)]
#![allow(dead_code)]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use dbrest::app::router::create_router;
use dbrest::app::state::{AppState, PgVersion};
use dbrest::config::AppConfig;
use dbrest_core::notifier::app_level::AppLevelNotifier;
use dbrest_core::notifier::{ChangeEvent, ChangeNotifier, ChangeOp};
use dbrest_postgres::notifier::PgChangeNotifier;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use sqlx::Executor;
use tokio::net::TcpListener;
use tokio::time::timeout;

// ==========================================================================
// SSE Test Client — parses SSE text protocol from reqwest streaming response
// ==========================================================================

#[derive(Debug)]
struct SseEvent {
    event_type: String,
    data: String,
}

impl SseEvent {
    fn change_event(&self) -> ChangeEvent {
        serde_json::from_str(&self.data).expect("SSE data should be valid ChangeEvent JSON")
    }
}

struct SseTestClient {
    response: reqwest::Response,
    buffer: String,
}

impl SseTestClient {
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

    async fn next_event(&mut self, dur: Duration) -> Option<SseEvent> {
        timeout(dur, async {
            loop {
                if let Some(event) = self.try_parse_event() {
                    return event;
                }
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
                continue;
            }

            return Some(SseEvent {
                event_type,
                data: data_parts.join("\n"),
            });
        }
    }

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
// TestServer helper (PostgreSQL variant with notifier support)
// ==========================================================================

const NOTIFY_CHANNEL: &str = "dbrest_test_changes";

struct TestServer {
    client: Client,
    base_url: String,
    #[allow(dead_code)]
    db: common::TestDb,
    state: AppState,
}

impl TestServer {
    /// Start with AppLevelNotifier (mutation→SSE flow, no LISTEN/NOTIFY).
    async fn start_with_app_notifier() -> Self {
        let db = common::TestDb::new()
            .await
            .expect("Failed to create test database: Docker required");

        let pool = db.pool().clone();

        let mut config = AppConfig::default();
        config.db_schemas = vec!["test_api".to_string()];
        config.db_anon_role = Some("web_anon".to_string());

        let state = dbrest::compat::app_state_from_pool(
            pool,
            config,
            PgVersion { major: 16, minor: 0, patch: 0 },
        );

        let notifier: Arc<dyn ChangeNotifier> = Arc::new(AppLevelNotifier::new(1024));
        let state = state.with_notifier(notifier);

        Self::boot(db, state).await
    }

    /// Start with PgChangeNotifier (listens on PostgreSQL NOTIFY channel).
    async fn start_with_pg_notifier() -> Self {
        let db = common::TestDb::new()
            .await
            .expect("Failed to create test database: Docker required");

        let pool = db.pool().clone();

        let mut config = AppConfig::default();
        config.db_schemas = vec!["test_api".to_string()];
        config.db_anon_role = Some("web_anon".to_string());

        let state = dbrest::compat::app_state_from_pool(
            pool.clone(),
            config,
            PgVersion { major: 16, minor: 0, patch: 0 },
        );

        let notifier = PgChangeNotifier::new(pool, NOTIFY_CHANNEL, 1024)
            .await
            .expect("Failed to create PgChangeNotifier");
        let notifier: Arc<dyn ChangeNotifier> = Arc::new(notifier);
        let state = state.with_notifier(notifier);

        Self::boot(db, state).await
    }

    /// Start WITHOUT a notifier (for 503 test).
    async fn start_without_notifier() -> Self {
        let db = common::TestDb::new()
            .await
            .expect("Failed to create test database: Docker required");

        let pool = db.pool().clone();

        let mut config = AppConfig::default();
        config.db_schemas = vec!["test_api".to_string()];
        config.db_anon_role = Some("web_anon".to_string());

        let state = dbrest::compat::app_state_from_pool(
            pool,
            config,
            PgVersion { major: 16, minor: 0, patch: 0 },
        );

        Self::boot(db, state).await
    }

    async fn boot(db: common::TestDb, state: AppState) -> Self {
        state
            .reload_schema_cache()
            .await
            .expect("Failed to load schema cache");

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

        Self {
            client: Client::new(),
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

    async fn put_json(&self, path: &str, body: &Value) -> reqwest::Response {
        self.client
            .put(format!("{}{}", self.base_url, path))
            .header("content-type", "application/json")
            .header("prefer", "return=representation,resolution=merge-duplicates")
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
// Category A: SSE Endpoint Basics (PG)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn sse_returns_503_when_no_notifier() {
    let server = TestServer::start_without_notifier().await;
    let resp = server.client
        .get(server.listen_url("users"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
#[ignore]
async fn sse_connection_returns_200_event_stream() {
    let server = TestServer::start_with_app_notifier().await;
    let resp = server.client
        .get(server.listen_url("products"))
        .header("accept", "text/event-stream")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp.headers().get("content-type").unwrap().to_str().unwrap();
    assert!(
        ct.contains("text/event-stream"),
        "Expected text/event-stream, got: {}",
        ct
    );
}

#[tokio::test]
#[ignore]
async fn sse_filters_events_by_table_name() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let notifier = server.state.notifier.as_ref().unwrap();
    notifier
        .notify(ChangeEvent {
            table: "users".to_string(),
            schema: "test_api".to_string(),
            event: ChangeOp::Insert,
            new: None,
            old: None,
        })
        .await;

    notifier
        .notify(ChangeEvent {
            table: "products".to_string(),
            schema: "test_api".to_string(),
            event: ChangeOp::Update,
            new: None,
            old: None,
        })
        .await;

    let event = sse.next_event(Duration::from_secs(2)).await.expect("Should receive products event");
    assert_eq!(event.event_type, "UPDATE");
    assert_eq!(event.change_event().table, "products");
}

// ==========================================================================
// Category B: Mutation → SSE Full-Stack Flow (PG)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn sse_post_produces_insert_event() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server
        .post_json("/products", &json!({"name": "PG SSE Widget", "price": 5.99}))
        .await;
    assert!(resp.status().is_success(), "POST failed: {}", resp.status());

    let event = sse.next_event(Duration::from_secs(2)).await.expect("Should receive INSERT event");
    assert_eq!(event.event_type, "INSERT");
    let ce = event.change_event();
    assert_eq!(ce.table, "products");
    assert_eq!(ce.event, ChangeOp::Insert);
}

#[tokio::test]
#[ignore]
async fn sse_patch_produces_update_event() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server
        .patch_json("/products?name=eq.Widget", &json!({"price": 19.99}))
        .await;
    assert!(resp.status().is_success(), "PATCH failed: {}", resp.status());

    let event = sse.next_event(Duration::from_secs(2)).await.expect("Should receive UPDATE event");
    assert_eq!(event.event_type, "UPDATE");
    assert_eq!(event.change_event().table, "products");
}

#[tokio::test]
#[ignore]
async fn sse_put_produces_update_event() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // PUT (upsert) — maps to ChangeOp::Update in notify_change
    let resp = server
        .put_json("/products", &json!({"name": "Widget", "price": 15.00}))
        .await;
    assert!(resp.status().is_success(), "PUT failed: {}", resp.status());

    let event = sse.next_event(Duration::from_secs(2)).await.expect("Should receive UPDATE event");
    assert_eq!(event.event_type, "UPDATE");
    assert_eq!(event.change_event().table, "products");
}

#[tokio::test]
#[ignore]
async fn sse_delete_produces_delete_event() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server.delete("/products?name=eq.Doohickey").await;
    assert!(resp.status().is_success(), "DELETE failed: {}", resp.status());

    let event = sse.next_event(Duration::from_secs(2)).await.expect("Should receive DELETE event");
    assert_eq!(event.event_type, "DELETE");
    assert_eq!(event.change_event().table, "products");
}

#[tokio::test]
#[ignore]
async fn sse_multiple_mutations_arrive_in_order() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server
        .post_json("/products", &json!({"name": "OrderPG", "price": 1.00}))
        .await;
    assert!(resp.status().is_success());

    let resp = server
        .patch_json("/products?name=eq.OrderPG", &json!({"price": 2.00}))
        .await;
    assert!(resp.status().is_success());

    let resp = server.delete("/products?name=eq.OrderPG").await;
    assert!(resp.status().is_success());

    let events = sse.collect_events(3, Duration::from_secs(5)).await;
    assert_eq!(events.len(), 3, "Expected 3 events, got {}", events.len());
    assert_eq!(events[0].event_type, "INSERT");
    assert_eq!(events[1].event_type, "UPDATE");
    assert_eq!(events[2].event_type, "DELETE");
}

#[tokio::test]
#[ignore]
async fn sse_mutations_different_tables_filtered() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse_products =
        SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    let mut sse_tasks =
        SseTestClient::connect(&server.client, &server.listen_url("tasks")).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let resp = server
        .post_json("/products", &json!({"name": "PGFilter", "price": 3.00}))
        .await;
    assert!(resp.status().is_success());

    let resp = server
        .post_json("/tasks", &json!({"title": "PGFilterTask", "priority": "low"}))
        .await;
    assert!(resp.status().is_success());

    let event = sse_products
        .next_event(Duration::from_secs(2))
        .await
        .expect("products client should receive event");
    assert_eq!(event.change_event().table, "products");

    let event = sse_tasks
        .next_event(Duration::from_secs(2))
        .await
        .expect("tasks client should receive event");
    assert_eq!(event.change_event().table, "tasks");

    sse_products.expect_no_event(Duration::from_millis(300)).await;
    sse_tasks.expect_no_event(Duration::from_millis(300)).await;
}

// ==========================================================================
// Category C: PostgreSQL LISTEN/NOTIFY
// ==========================================================================

#[tokio::test]
#[ignore]
async fn pg_notify_raw_sql_produces_sse_event() {
    let server = TestServer::start_with_pg_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("users")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send a raw NOTIFY with a valid ChangeEvent JSON payload
    let payload = json!({
        "table": "users",
        "schema": "test_api",
        "event": "INSERT",
        "new": {"id": 99, "email": "notify@test.com"},
        "old": null
    });
    let sql = format!(
        "SELECT pg_notify('{}', '{}')",
        NOTIFY_CHANNEL,
        payload.to_string().replace('\'', "''")
    );
    server.db.pool().execute(sql.as_str()).await.expect("pg_notify failed");

    let event = sse
        .next_event(Duration::from_secs(3))
        .await
        .expect("Should receive event from pg_notify");
    assert_eq!(event.event_type, "INSERT");
    let ce = event.change_event();
    assert_eq!(ce.table, "users");
    assert_eq!(ce.schema, "test_api");
    // PgChangeNotifier preserves the new/old data from the NOTIFY payload
    assert!(ce.new.is_some(), "new should be populated from NOTIFY payload");
    let new_val = ce.new.unwrap();
    assert_eq!(new_val["id"], 99);
    assert_eq!(new_val["email"], "notify@test.com");
}

#[tokio::test]
#[ignore]
async fn pg_notify_with_trigger_produces_event() {
    let server = TestServer::start_with_pg_notifier().await;

    // Install the trigger function and attach to users table
    let trigger_ddl = format!(
        r#"
        CREATE OR REPLACE FUNCTION test_api.dbrest_notify() RETURNS trigger AS $$
        BEGIN
          PERFORM pg_notify(
            '{}',
            json_build_object(
              'table', TG_TABLE_NAME,
              'schema', TG_TABLE_SCHEMA,
              'event', TG_OP,
              'new', CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW) ELSE NULL END,
              'old', CASE WHEN TG_OP IN ('DELETE', 'UPDATE') THEN row_to_json(OLD) ELSE NULL END
            )::text
          );
          RETURN COALESCE(NEW, OLD);
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS test_users_changes ON test_api.users;
        CREATE TRIGGER test_users_changes
          AFTER INSERT OR UPDATE OR DELETE ON test_api.users
          FOR EACH ROW EXECUTE FUNCTION test_api.dbrest_notify();
        "#,
        NOTIFY_CHANNEL
    );
    sqlx::raw_sql(&trigger_ddl)
        .execute(server.db.pool())
        .await
        .expect("Failed to install trigger");

    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("users")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Direct SQL INSERT — triggers the NOTIFY via trigger
    server
        .db
        .pool()
        .execute(
            sqlx::query(
                "INSERT INTO test_api.users (email, name) VALUES ('trigger@test.com', 'Trigger User')",
            ),
        )
        .await
        .expect("INSERT failed");

    let event = sse
        .next_event(Duration::from_secs(3))
        .await
        .expect("Should receive trigger-fired event");
    assert_eq!(event.event_type, "INSERT");
    let ce = event.change_event();
    assert_eq!(ce.table, "users");
    assert_eq!(ce.schema, "test_api");
    assert!(ce.new.is_some(), "Trigger should populate new row data");
    let new_val = ce.new.unwrap();
    assert_eq!(new_val["email"], "trigger@test.com");
    assert_eq!(new_val["name"], "Trigger User");
}

#[tokio::test]
#[ignore]
async fn pg_notify_malformed_payload_no_crash() {
    let server = TestServer::start_with_pg_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Send malformed JSON — should be logged as warning, not crash
    let bad_sql = format!(
        "SELECT pg_notify('{}', 'this is not valid json at all')",
        NOTIFY_CHANNEL
    );
    server.db.pool().execute(bad_sql.as_str()).await.expect("pg_notify failed");

    // Small delay to let the receive_loop process the bad payload
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Now send a valid event — should still arrive, proving the listener survived
    let valid_payload = json!({
        "table": "products",
        "schema": "test_api",
        "event": "UPDATE",
        "new": null,
        "old": null
    });
    let good_sql = format!(
        "SELECT pg_notify('{}', '{}')",
        NOTIFY_CHANNEL,
        valid_payload.to_string().replace('\'', "''")
    );
    server.db.pool().execute(good_sql.as_str()).await.expect("pg_notify failed");

    let event = sse
        .next_event(Duration::from_secs(3))
        .await
        .expect("Should receive valid event after malformed one");
    assert_eq!(event.event_type, "UPDATE");
    assert_eq!(event.change_event().table, "products");
}

#[tokio::test]
#[ignore]
async fn pg_catches_external_writes_flag() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker required");
    let notifier = PgChangeNotifier::new(db.pool().clone(), NOTIFY_CHANNEL, 64)
        .await
        .expect("Failed to create PgChangeNotifier");
    assert!(
        notifier.catches_external_writes(),
        "PgChangeNotifier should report catches_external_writes = true"
    );
}

// ==========================================================================
// Category E: Edge Cases (PG-specific)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn sse_large_notify_payload() {
    let server = TestServer::start_with_pg_notifier().await;
    let mut sse = SseTestClient::connect(&server.client, &server.listen_url("products")).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // PostgreSQL NOTIFY payload limit is ~8000 bytes.
    // Create a payload just under that limit.
    let large_value = "x".repeat(6000);
    let payload = json!({
        "table": "products",
        "schema": "test_api",
        "event": "INSERT",
        "new": {"data": large_value},
        "old": null
    });
    let sql = format!(
        "SELECT pg_notify('{}', '{}')",
        NOTIFY_CHANNEL,
        payload.to_string().replace('\'', "''")
    );
    server.db.pool().execute(sql.as_str()).await.expect("pg_notify with large payload failed");

    let event = sse
        .next_event(Duration::from_secs(3))
        .await
        .expect("Should receive large payload event");
    assert_eq!(event.event_type, "INSERT");
    let ce = event.change_event();
    assert_eq!(ce.table, "products");
    let new_val = ce.new.expect("new should contain the large payload");
    assert_eq!(new_val["data"].as_str().unwrap().len(), 6000);
}

#[tokio::test]
#[ignore]
async fn sse_listen_nonexistent_table_connects_pg() {
    let server = TestServer::start_with_app_notifier().await;
    let mut sse = SseTestClient::connect(
        &server.client,
        &server.listen_url("nonexistent_table_xyz"),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let notifier = server.state.notifier.as_ref().unwrap();
    notifier
        .notify(ChangeEvent {
            table: "nonexistent_table_xyz".to_string(),
            schema: "test_api".to_string(),
            event: ChangeOp::Delete,
            new: None,
            old: None,
        })
        .await;

    let event = sse
        .next_event(Duration::from_secs(2))
        .await
        .expect("Should receive event for nonexistent table");
    assert_eq!(event.event_type, "DELETE");
    assert_eq!(event.change_event().table, "nonexistent_table_xyz");
}
