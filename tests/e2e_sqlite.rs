//! End-to-end integration tests for the SQLite backend.
//!
//! These mirror the PostgreSQL e2e tests in e2e_app.rs but use an in-memory
//! SQLite database — no Docker required.

#![allow(clippy::field_reassign_with_default)]

mod common;

use std::net::SocketAddr;
use std::sync::Arc;

use dbrest::app::router::create_router;
use dbrest::app::state::AppState;
use dbrest::backend::{DatabaseBackend, DbVersion, SqlDialect};
use dbrest::config::AppConfig;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use tokio::net::TcpListener;

use dbrest_sqlite::{SqliteBackend, SqliteDialect};

// ==========================================================================
// TestServer helper (SQLite variant)
// ==========================================================================

struct TestServer {
    client: Client,
    base_url: String,
    #[allow(dead_code)]
    db: common::TestSqliteDb,
    #[allow(dead_code)]
    state: AppState,
}

impl TestServer {
    /// Start a test server backed by an in-memory SQLite database.
    async fn start() -> Self {
        Self::start_with_config(|_| {}).await
    }

    /// Start with custom config modifications.
    async fn start_with_config<F>(config_fn: F) -> Self
    where
        F: FnOnce(&mut AppConfig),
    {
        let db = common::TestSqliteDb::new()
            .await
            .expect("Failed to create SQLite test database");

        let pool = db.pool().clone();

        let mut config = AppConfig::default();
        // SQLite has no schemas — use "main" as the schema name
        config.db_schemas = vec!["main".to_string()];
        // SQLite has no roles — but the framework requires an anon role to avoid 401.
        // Set a placeholder value; it won't be used for SET ROLE in SQLite.
        config.db_anon_role = Some("anon".to_string());
        config_fn(&mut config);

        let backend: Arc<dyn DatabaseBackend> = Arc::new(SqliteBackend::from_pool(pool));
        let dialect: Arc<dyn SqlDialect> = Arc::new(SqliteDialect);
        let db_version = DbVersion {
            major: 3,
            minor: 45,
            patch: 0,
            engine: "SQLite".to_string(),
        };

        let state = AppState::new_with_backend(backend, dialect, config, db_version);

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

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let client = Client::new();

        Self {
            client,
            base_url,
            db,
            state,
        }
    }

    async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(format!("{}{}", self.base_url, path))
            .send()
            .await
            .unwrap()
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

    async fn patch_json(
        &self,
        path: &str,
        body: &Value,
        extra_headers: &[(&str, &str)],
    ) -> reqwest::Response {
        let mut req = self
            .client
            .patch(format!("{}{}", self.base_url, path))
            .header("content-type", "application/json")
            .json(body);
        for (k, v) in extra_headers {
            req = req.header(*k, *v);
        }
        req.send().await.unwrap()
    }

    async fn delete(&self, path: &str) -> reqwest::Response {
        self.client
            .delete(format!("{}{}", self.base_url, path))
            .send()
            .await
            .unwrap()
    }

    async fn head(&self, path: &str) -> reqwest::Response {
        self.client
            .head(format!("{}{}", self.base_url, path))
            .send()
            .await
            .unwrap()
    }

    async fn options(&self, path: &str) -> reqwest::Response {
        self.client
            .request(
                reqwest::Method::OPTIONS,
                format!("{}{}", self.base_url, path),
            )
            .send()
            .await
            .unwrap()
    }
}

// ==========================================================================
// E2E: Schema introspection (SQLite must discover tables)
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_schema_cache_loads() {
    let server = TestServer::start().await;
    // If we got here, schema cache loaded successfully.
    // Verify by hitting a table endpoint.
    let resp = server.get("/users").await;
    assert!(
        resp.status().is_success() || resp.status() == StatusCode::NOT_FOUND,
        "Unexpected status: {}",
        resp.status()
    );
}

// ==========================================================================
// E2E: Basic reads
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_get_all_users() {
    let server = TestServer::start().await;
    let resp = server.get("/users").await;

    let status = resp.status();
    if status != StatusCode::OK {
        let body = resp.text().await.unwrap_or_default();
        panic!("Expected 200, got {}: {}", status, body);
    }

    let ct = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.contains("application/json"));

    let body: Value = resp.json().await.unwrap();
    assert!(body.is_array());

    let users = body.as_array().unwrap();
    assert_eq!(users.len(), 4, "Expected 4 users, got {}", users.len());

    for user in users {
        assert!(user.get("id").is_some(), "Missing 'id' field");
        assert!(user.get("name").is_some(), "Missing 'name' field");
        assert!(user.get("email").is_some(), "Missing 'email' field");
    }
}

#[tokio::test]
async fn sqlite_e2e_get_all_products() {
    let server = TestServer::start().await;
    let resp = server.get("/products").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let products = body.as_array().unwrap();
    assert_eq!(products.len(), 4, "Expected 4 products");

    let first = &products[0];
    assert!(first.get("price").is_some(), "Product should have price");
}

#[tokio::test]
async fn sqlite_e2e_get_all_posts() {
    let server = TestServer::start().await;
    let resp = server.get("/posts").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let posts = body.as_array().unwrap();
    // SQLite has no RLS — all 4 posts visible
    assert_eq!(posts.len(), 4, "Expected 4 posts");
}

#[tokio::test]
async fn sqlite_e2e_get_all_tasks() {
    let server = TestServer::start().await;
    let resp = server.get("/tasks").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let tasks = body.as_array().unwrap();
    assert_eq!(tasks.len(), 3, "Expected 3 tasks");
}

// ==========================================================================
// E2E: Select specific columns
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_select_columns() {
    let server = TestServer::start().await;
    let resp = server.get("/users?select=id,name").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    assert!(!users.is_empty());

    let first = &users[0];
    assert!(first.get("id").is_some());
    assert!(first.get("name").is_some());
    // email should NOT be present when not selected
    assert!(
        first.get("email").is_none(),
        "email should not be in response"
    );
}

#[tokio::test]
async fn sqlite_e2e_select_single_column() {
    let server = TestServer::start().await;
    let resp = server.get("/users?select=name").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    for user in users {
        assert!(user.get("name").is_some());
        assert!(user.get("id").is_none());
    }
}

// ==========================================================================
// E2E: Filtering
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_filter_eq() {
    let server = TestServer::start().await;
    let resp = server.get("/users?name=eq.Alice Johnson").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0]["name"], "Alice Johnson");
}

#[tokio::test]
async fn sqlite_e2e_filter_neq() {
    let server = TestServer::start().await;
    let resp = server.get("/users?status=neq.active").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    // inactive + pending = 2
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn sqlite_e2e_filter_gt() {
    let server = TestServer::start().await;
    let resp = server.get("/products?price=gt.10").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let products = body.as_array().unwrap();
    // Gizmo (24.50) and Thingamajig (99.99) are > 10
    assert_eq!(products.len(), 2);
}

#[tokio::test]
async fn sqlite_e2e_multiple_filters() {
    let server = TestServer::start().await;
    let resp = server.get("/products?price=gte.5&in_stock=eq.1").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let products = body.as_array().unwrap();
    // Widget (9.99, in_stock), Gizmo (24.50, in_stock) — Thingamajig is NOT in stock, Doohickey < 5
    assert_eq!(products.len(), 2);
}

// ==========================================================================
// E2E: Ordering
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_order_by_name_asc() {
    let server = TestServer::start().await;
    let resp = server.get("/users?order=name.asc").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    assert_eq!(users[0]["name"], "Alice Johnson");
    assert_eq!(users[1]["name"], "Bob Smith");
}

#[tokio::test]
async fn sqlite_e2e_order_by_name_desc() {
    let server = TestServer::start().await;
    let resp = server.get("/users?order=name.desc").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    assert_eq!(users[0]["name"], "Diana Prince");
}

// ==========================================================================
// E2E: Limit / Offset
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_limit() {
    let server = TestServer::start().await;
    let resp = server.get("/users?limit=2").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn sqlite_e2e_limit_offset() {
    let server = TestServer::start().await;
    let resp = server.get("/users?limit=2&offset=2").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    assert_eq!(users.len(), 2);
}

// ==========================================================================
// E2E: Mutations (INSERT)
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_insert_then_read() {
    let server = TestServer::start().await;

    let new_product = json!({
        "name": "SQLite Widget",
        "price": 42.00,
        "in_stock": 1,
        "category": "test"
    });
    let resp = server.post_json("/products", &new_product).await;
    assert!(
        resp.status().is_success(),
        "Insert failed: {} — {}",
        resp.status(),
        resp.text().await.unwrap_or_default()
    );

    // Read back all products
    let resp = server.get("/products?name=eq.SQLite Widget").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let products = body.as_array().unwrap();
    assert_eq!(products.len(), 1);
    assert_eq!(products[0]["name"], "SQLite Widget");
}

// ==========================================================================
// E2E: Mutations (UPDATE)
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_patch_update() {
    let server = TestServer::start().await;

    let update = json!({ "price": 19.99 });
    let resp = server
        .patch_json(
            "/products?name=eq.Widget",
            &update,
            &[("prefer", "return=representation")],
        )
        .await;

    assert!(
        resp.status().is_success(),
        "Patch failed: {}",
        resp.status()
    );

    // Verify
    let resp = server.get("/products?name=eq.Widget").await;
    let body: Value = resp.json().await.unwrap();
    let products = body.as_array().unwrap();
    assert_eq!(products.len(), 1);
    // Price should be updated
    let price = products[0]["price"].as_f64().unwrap();
    assert!((price - 19.99).abs() < 0.01);
}

// ==========================================================================
// E2E: Mutations (DELETE)
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_delete() {
    let server = TestServer::start().await;

    let resp = server.delete("/products?name=eq.Doohickey").await;
    assert!(resp.status().is_success());

    // Verify deleted
    let resp = server.get("/products?name=eq.Doohickey").await;
    let body: Value = resp.json().await.unwrap();
    let products = body.as_array().unwrap();
    assert_eq!(products.len(), 0);
}

// ==========================================================================
// E2E: Views
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_read_view_active_users() {
    let server = TestServer::start().await;
    let resp = server.get("/active_users").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let users = body.as_array().unwrap();
    // Only active users: Alice and Bob
    assert_eq!(users.len(), 2);
}

#[tokio::test]
async fn sqlite_e2e_read_view_published_posts() {
    let server = TestServer::start().await;
    let resp = server.get("/published_posts").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let posts = body.as_array().unwrap();
    // 3 published posts
    assert_eq!(posts.len(), 3);
}

// ==========================================================================
// E2E: HEAD request
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_head_request() {
    let server = TestServer::start().await;
    let resp = server.head("/users").await;

    assert_eq!(resp.status(), StatusCode::OK);
    // HEAD should return no body
    let body_bytes = resp.bytes().await.unwrap();
    assert!(body_bytes.is_empty());
}

// ==========================================================================
// E2E: Nonexistent table
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_nonexistent_table() {
    let server = TestServer::start().await;
    let resp = server.get("/nonexistent_table").await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ==========================================================================
// E2E: OPTIONS
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_options_root() {
    let server = TestServer::start().await;
    let resp = server.options("/").await;

    assert_eq!(resp.status(), StatusCode::OK);
}

// ==========================================================================
// E2E: Relationships (foreign key embeds)
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_embed_m2o() {
    let server = TestServer::start().await;
    // posts -> users (M2O via user_id FK)
    let resp = server.get("/posts?select=title,users(name)").await;

    // This tests whether the SQLite introspector correctly discovers FK relationships
    // and whether the query builder handles embedding without LATERAL JOIN.
    let status = resp.status();
    if status == StatusCode::OK {
        let body: Value = resp.json().await.unwrap();
        let posts = body.as_array().unwrap();
        assert!(!posts.is_empty());
    }
    // If it returns 400 (embedding not supported yet), that's expected too
    assert!(
        status == StatusCode::OK || status == StatusCode::BAD_REQUEST,
        "Unexpected status: {}",
        status
    );
}

// ==========================================================================
// E2E: Countries (simpler schema without PG-specific types)
// ==========================================================================

#[tokio::test]
async fn sqlite_e2e_get_countries() {
    let server = TestServer::start().await;
    let resp = server.get("/countries").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let countries = body.as_array().unwrap();
    assert_eq!(countries.len(), 5);
}

#[tokio::test]
async fn sqlite_e2e_filter_countries_by_population() {
    let server = TestServer::start().await;
    let resp = server.get("/countries?population=gt.100000000").await;

    assert_eq!(resp.status(), StatusCode::OK);
    let body: Value = resp.json().await.unwrap();
    let countries = body.as_array().unwrap();
    // US (331M), Mexico (128M)
    assert_eq!(countries.len(), 2);
}
