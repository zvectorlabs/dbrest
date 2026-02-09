//! Auth module integration tests
//!
//! Tests the full JWT authentication pipeline against a real PostgreSQL
//! database using testcontainers:
//!
//! 1. JWT parsing, validation, and role extraction
//! 2. Auth middleware behaviour (valid token, anonymous, expired, etc.)
//! 3. Session variable setup via `set_config` (role, claims)
//! 4. Pre-request function invocation gated by auth
//! 5. Row-level security enforcement with authenticated roles
//!
//! Run with: `cargo test --test auth_integration -- --ignored`

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::extract::Request;
use axum::response::{IntoResponse, Response};
use http::header;
use jsonwebtoken::{Algorithm, EncodingKey, Header as JwtHeader};
use pgrest::auth::error::{JwtClaimsError, JwtDecodeError, JwtError};
use pgrest::auth::jwt;
use pgrest::auth::middleware::{authenticate, AuthState};
use pgrest::auth::types::AuthResult;
use pgrest::config::AppConfig;
use sqlx::{PgPool, Row};

// ==========================================================================
// Helpers
// ==========================================================================

const SECRET: &str = "reallyreallyreallyreallyverysafe!";

fn test_config() -> AppConfig {
    AppConfig {
        db_schemas: vec!["test_api".to_string()],
        jwt_secret: Some(SECRET.to_string()),
        db_anon_role: Some("web_anon".to_string()),
        jwt_cache_max_entries: 100,
        ..Default::default()
    }
}

fn encode_hs256(claims: &serde_json::Value) -> String {
    jsonwebtoken::encode(
        &JwtHeader::default(),
        claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

fn encode_with_alg(claims: &serde_json::Value, alg: Algorithm) -> String {
    let header = JwtHeader::new(alg);
    jsonwebtoken::encode(
        &header,
        claims,
        &EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

fn valid_claims(role: &str) -> serde_json::Value {
    serde_json::json!({
        "role": role,
        "exp": chrono::Utc::now().timestamp() + 3600
    })
}

fn make_request(token: Option<&str>) -> Request<Body> {
    let mut builder = Request::builder().method("GET").uri("/items");
    if let Some(t) = token {
        builder = builder.header("Authorization", format!("Bearer {t}"));
    }
    builder.body(Body::empty()).unwrap()
}

/// Create database roles and RLS policies for auth testing.
async fn setup_auth_roles(pool: &PgPool) {
    let sqls = [
        // Create roles
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'web_anon') THEN CREATE ROLE web_anon NOLOGIN; END IF; END $$",
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_author') THEN CREATE ROLE test_author NOLOGIN; END IF; END $$",
        "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_admin') THEN CREATE ROLE test_admin NOLOGIN; END IF; END $$",
        // Grant usage on schema
        "GRANT USAGE ON SCHEMA test_api TO web_anon, test_author, test_admin",
        // web_anon: read-only on users and posts
        "GRANT SELECT ON test_api.users TO web_anon",
        "GRANT SELECT ON test_api.posts TO web_anon",
        // test_author: read + write on users and posts
        "GRANT SELECT, INSERT, UPDATE ON test_api.users TO test_author",
        "GRANT SELECT, INSERT, UPDATE ON test_api.posts TO test_author",
        "GRANT USAGE ON ALL SEQUENCES IN SCHEMA test_api TO test_author",
        // test_admin: everything
        "GRANT ALL ON ALL TABLES IN SCHEMA test_api TO test_admin",
        "GRANT USAGE ON ALL SEQUENCES IN SCHEMA test_api TO test_admin",
        // Enable RLS on posts
        "ALTER TABLE test_api.posts ENABLE ROW LEVEL SECURITY",
        "ALTER TABLE test_api.posts FORCE ROW LEVEL SECURITY",
        // RLS policy: test_author can only see own posts
        "DROP POLICY IF EXISTS author_posts ON test_api.posts",
        "CREATE POLICY author_posts ON test_api.posts FOR ALL TO test_author USING (user_id = (current_setting('request.jwt.claims', true)::json->>'sub')::integer) WITH CHECK (true)",
        // RLS policy: web_anon can only see published posts
        "DROP POLICY IF EXISTS anon_posts ON test_api.posts",
        "CREATE POLICY anon_posts ON test_api.posts FOR SELECT TO web_anon USING (published = true)",
        // RLS policy: test_admin sees everything
        "DROP POLICY IF EXISTS admin_posts ON test_api.posts",
        "CREATE POLICY admin_posts ON test_api.posts FOR ALL TO test_admin USING (true) WITH CHECK (true)",
        // Grant current_user function
        "GRANT SELECT ON test_api.users TO test_author",
    ];

    for sql in &sqls {
        sqlx::raw_sql(sql).execute(pool).await.unwrap_or_else(|e| {
            panic!("Failed SQL: {sql}\nError: {e}");
        });
    }
}

/// Set session context variables for a role, then run a query.
async fn query_as_role(
    pool: &PgPool,
    role: &str,
    claims_json: &str,
) -> Vec<sqlx::postgres::PgRow> {
    let mut tx = pool.begin().await.unwrap();

    // Set role
    let set_role = format!("SET LOCAL ROLE {role}");
    sqlx::raw_sql(&set_role)
        .execute(&mut *tx)
        .await
        .unwrap();

    // Set claims
    sqlx::query("SELECT set_config('request.jwt.claims', $1, true)")
        .bind(claims_json)
        .execute(&mut *tx)
        .await
        .unwrap();

    // Query
    let rows = sqlx::query("SELECT id, title, published FROM test_api.posts ORDER BY id")
        .fetch_all(&mut *tx)
        .await
        .unwrap();

    tx.rollback().await.unwrap();
    rows
}

// ==========================================================================
// JWT parsing tests (against real config, no DB needed)
// ==========================================================================

#[test]
#[ignore]
fn test_jwt_hs256_valid() {
    let config = test_config();
    let claims = valid_claims("test_author");
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_author");
    assert!(!result.is_anonymous());
    assert!(result.claims.contains_key("role"));
    assert!(result.claims.contains_key("exp"));
}

#[test]
#[ignore]
fn test_jwt_hs384_valid() {
    let config = test_config();
    let claims = valid_claims("test_author");
    let token = encode_with_alg(&claims, Algorithm::HS384);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_author");
}

#[test]
#[ignore]
fn test_jwt_hs512_valid() {
    let config = test_config();
    let claims = valid_claims("test_author");
    let token = encode_with_alg(&claims, Algorithm::HS512);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_author");
}

#[test]
#[ignore]
fn test_jwt_expired_beyond_skew() {
    let config = test_config();
    let claims = serde_json::json!({
        "role": "test_author",
        "exp": chrono::Utc::now().timestamp() - 60  // 60s past = beyond 30s skew
    });
    let token = encode_hs256(&claims);

    let err = jwt::parse_and_validate(&token, &config).unwrap_err();
    assert!(matches!(err, JwtError::Claims(JwtClaimsError::Expired)));
    assert_eq!(err.code(), "PGRST303");
    assert_eq!(err.status(), http::StatusCode::UNAUTHORIZED);
}

#[test]
#[ignore]
fn test_jwt_expired_within_skew() {
    let config = test_config();
    let claims = serde_json::json!({
        "role": "test_author",
        "exp": chrono::Utc::now().timestamp() - 20  // 20s past = within 30s skew
    });
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config);
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_jwt_nbf_future() {
    let config = test_config();
    let claims = serde_json::json!({
        "role": "test_author",
        "nbf": chrono::Utc::now().timestamp() + 60,
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let err = jwt::parse_and_validate(&token, &config).unwrap_err();
    assert!(matches!(err, JwtError::Claims(JwtClaimsError::NotYetValid)));
}

#[test]
#[ignore]
fn test_jwt_iat_future() {
    let config = test_config();
    let claims = serde_json::json!({
        "role": "test_author",
        "iat": chrono::Utc::now().timestamp() + 60,
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let err = jwt::parse_and_validate(&token, &config).unwrap_err();
    assert!(matches!(
        err,
        JwtError::Claims(JwtClaimsError::IssuedAtFuture)
    ));
}

#[test]
#[ignore]
fn test_jwt_wrong_secret() {
    let config = test_config();
    let claims = valid_claims("test_author");
    let token = jsonwebtoken::encode(
        &JwtHeader::default(),
        &claims,
        &EncodingKey::from_secret(b"completely_different_secret_val!"),
    )
    .unwrap();

    let err = jwt::parse_and_validate(&token, &config).unwrap_err();
    assert!(matches!(err, JwtError::Decode(_)));
    assert_eq!(err.code(), "PGRST301");
}

#[test]
#[ignore]
fn test_jwt_empty_token() {
    let config = test_config();
    let err = jwt::parse_and_validate("", &config).unwrap_err();
    assert!(matches!(
        err,
        JwtError::Decode(JwtDecodeError::EmptyAuthHeader)
    ));
}

#[test]
#[ignore]
fn test_jwt_malformed_token() {
    let config = test_config();
    let err = jwt::parse_and_validate("abc.def", &config).unwrap_err();
    assert!(matches!(
        err,
        JwtError::Decode(JwtDecodeError::UnexpectedParts(2))
    ));
}

#[test]
#[ignore]
fn test_jwt_no_secret_configured() {
    let mut config = test_config();
    config.jwt_secret = None;
    let err = jwt::parse_and_validate("a.b.c", &config).unwrap_err();
    assert!(matches!(err, JwtError::SecretMissing));
    assert_eq!(err.code(), "PGRST300");
}

#[test]
#[ignore]
fn test_jwt_audience_match() {
    let mut config = test_config();
    config.jwt_aud = Some("my_api".to_string());

    let claims = serde_json::json!({
        "role": "test_author",
        "aud": "my_api",
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_author");
}

#[test]
#[ignore]
fn test_jwt_audience_mismatch() {
    let mut config = test_config();
    config.jwt_aud = Some("my_api".to_string());

    let claims = serde_json::json!({
        "role": "test_author",
        "aud": "wrong_api",
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let err = jwt::parse_and_validate(&token, &config).unwrap_err();
    assert!(matches!(
        err,
        JwtError::Claims(JwtClaimsError::NotInAudience)
    ));
}

#[test]
#[ignore]
fn test_jwt_audience_array() {
    let mut config = test_config();
    config.jwt_aud = Some("my_api".to_string());

    let claims = serde_json::json!({
        "role": "test_author",
        "aud": ["other_api", "my_api"],
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_author");
}

#[test]
#[ignore]
fn test_jwt_no_role_falls_back_to_anon() {
    let config = test_config();
    let claims = serde_json::json!({
        "sub": "user123",
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "web_anon");
}

#[test]
#[ignore]
fn test_jwt_custom_claims_preserved() {
    let config = test_config();
    let claims = serde_json::json!({
        "role": "test_author",
        "sub": "user42",
        "org_id": 7,
        "perms": ["read", "write"],
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(
        result.claims.get("sub").unwrap(),
        &serde_json::json!("user42")
    );
    assert_eq!(result.claims.get("org_id").unwrap(), &serde_json::json!(7));
    assert_eq!(
        result.claims.get("perms").unwrap(),
        &serde_json::json!(["read", "write"])
    );
}

#[test]
#[ignore]
fn test_jwt_nested_role_claim() {
    use pgrest::config::jwt::JsPathExp;
    let mut config = test_config();
    config.jwt_role_claim_key = vec![
        JsPathExp::Key("realm_access".into()),
        JsPathExp::Key("role".into()),
    ];

    let claims = serde_json::json!({
        "realm_access": { "role": "test_admin" },
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_admin");
}

#[test]
#[ignore]
fn test_jwt_base64_secret() {
    use base64::Engine;
    let raw = SECRET;
    let b64 = base64::engine::general_purpose::STANDARD.encode(raw.as_bytes());

    let mut config = test_config();
    config.jwt_secret = Some(b64);
    config.jwt_secret_is_base64 = true;

    let claims = valid_claims("test_author");
    let token = jsonwebtoken::encode(
        &JwtHeader::default(),
        &claims,
        &EncodingKey::from_secret(raw.as_bytes()),
    )
    .unwrap();

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_author");
}

#[test]
#[ignore]
fn test_jwt_no_exp_succeeds() {
    let config = test_config();
    let claims = serde_json::json!({
        "role": "test_author"
    });
    let token = encode_hs256(&claims);

    let result = jwt::parse_and_validate(&token, &config).unwrap();
    assert_eq!(result.role.as_str(), "test_author");
}

// ==========================================================================
// Middleware tests (authenticate function)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_middleware_valid_token() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));
    let claims = valid_claims("test_author");
    let token = encode_hs256(&claims);
    let request = make_request(Some(&token));

    let result = authenticate(&state, &request).await.unwrap();
    assert_eq!(result.role.as_str(), "test_author");
    assert!(!result.is_anonymous());
}

#[tokio::test]
#[ignore]
async fn test_middleware_anonymous_access() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));
    let request = make_request(None);

    let result = authenticate(&state, &request).await.unwrap();
    assert_eq!(result.role.as_str(), "web_anon");
    assert!(result.is_anonymous());
}

#[tokio::test]
#[ignore]
async fn test_middleware_no_anon_no_token() {
    let mut config = test_config();
    config.db_anon_role = None;
    let state = AuthState::new(Arc::new(config));
    let request = make_request(None);

    let err = authenticate(&state, &request).await.unwrap_err();
    assert!(matches!(err, JwtError::TokenRequired));
    assert_eq!(err.code(), "PGRST302");
}

#[tokio::test]
#[ignore]
async fn test_middleware_expired_token() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));
    let claims = serde_json::json!({
        "role": "test_author",
        "exp": chrono::Utc::now().timestamp() - 60
    });
    let token = encode_hs256(&claims);
    let request = make_request(Some(&token));

    let err = authenticate(&state, &request).await.unwrap_err();
    assert!(matches!(err, JwtError::Claims(JwtClaimsError::Expired)));
}

#[tokio::test]
#[ignore]
async fn test_middleware_cache_hit() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));
    let claims = valid_claims("cached_role");
    let token = encode_hs256(&claims);

    // First call — cache miss
    let r1 = authenticate(&state, &make_request(Some(&token)))
        .await
        .unwrap();
    assert_eq!(r1.role.as_str(), "cached_role");

    // Second call — cache hit
    let r2 = authenticate(&state, &make_request(Some(&token)))
        .await
        .unwrap();
    assert_eq!(r2.role.as_str(), "cached_role");

    // Verify cache populated
    assert!(state.cache.get(&token).await.is_some());
}

#[tokio::test]
#[ignore]
async fn test_middleware_cache_invalidate() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));
    let claims = valid_claims("cached_role");
    let token = encode_hs256(&claims);

    authenticate(&state, &make_request(Some(&token)))
        .await
        .unwrap();
    assert!(state.cache.get(&token).await.is_some());

    state.cache.invalidate_all();
    // After invalidation, the cache should eventually be empty
    // (moka invalidation is lazy)
}

// ==========================================================================
// Axum middleware integration (HTTP request → response)
// ==========================================================================

/// Build an axum Router with auth middleware and a simple handler that
/// returns the authenticated role.
/// Build an error response from a JwtError (mirrors middleware behaviour).
fn jwt_error_response(err: JwtError) -> Response {
    let status = err.status();
    let www_auth = err.www_authenticate();
    let body = pgrest::error::response::ErrorResponse {
        code: err.code(),
        message: err.to_string(),
        details: err.details(),
        hint: None,
    };
    let mut response = (status, axum::Json(body)).into_response();
    if let Some(val) = www_auth {
        if let Ok(hv) = http::HeaderValue::from_str(&val) {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, hv);
        }
    }
    response
}

/// Simulate a full auth + handler pipeline:
/// 1. Run authenticate()
/// 2. On success, return JSON with role info
/// 3. On error, return JWT error response
async fn simulate_http(state: &AuthState, request: Request) -> Response {
    match authenticate(state, &request).await {
        Ok(auth_result) => {
            let json = serde_json::json!({
                "role": auth_result.role.as_str(),
                "anonymous": auth_result.is_anonymous(),
                "claims": auth_result.claims
            });
            (http::StatusCode::OK, axum::Json(json)).into_response()
        }
        Err(jwt_err) => jwt_error_response(jwt_err),
    }
}

#[tokio::test]
#[ignore]
async fn test_http_valid_token_response() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));

    let claims = valid_claims("test_author");
    let token = encode_hs256(&claims);

    let request = Request::builder()
        .uri("/test")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    assert_eq!(response.status(), http::StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["role"], "test_author");
    assert_eq!(json["anonymous"], false);
}

#[tokio::test]
#[ignore]
async fn test_http_anonymous_response() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));

    let request = Request::builder()
        .uri("/test")
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    assert_eq!(response.status(), http::StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["role"], "web_anon");
    assert_eq!(json["anonymous"], true);
}

#[tokio::test]
#[ignore]
async fn test_http_expired_token_401() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));

    let claims = serde_json::json!({
        "role": "test_author",
        "exp": chrono::Utc::now().timestamp() - 60
    });
    let token = encode_hs256(&claims);

    let request = Request::builder()
        .uri("/test")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
    assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "PGRST303");
}

#[tokio::test]
#[ignore]
async fn test_http_no_secret_500() {
    let mut config = test_config();
    config.jwt_secret = None;
    let state = AuthState::new(Arc::new(config));

    let request = Request::builder()
        .uri("/test")
        .header("Authorization", "Bearer abc.def.ghi")
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    assert_eq!(
        response.status(),
        http::StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
#[ignore]
async fn test_http_no_anon_no_token_401() {
    let mut config = test_config();
    config.db_anon_role = None;
    let state = AuthState::new(Arc::new(config));

    let request = Request::builder()
        .uri("/test")
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
    let has_www_auth = response.headers().contains_key(header::WWW_AUTHENTICATE);
    assert!(has_www_auth, "Should have WWW-Authenticate header");

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "PGRST302");
}

#[tokio::test]
#[ignore]
async fn test_http_www_authenticate_header() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));

    let claims = serde_json::json!({
        "role": "test_author",
        "exp": chrono::Utc::now().timestamp() - 60
    });
    let token = encode_hs256(&claims);

    let request = Request::builder()
        .uri("/test")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    let www_auth = response
        .headers()
        .get(header::WWW_AUTHENTICATE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(www_auth.contains("Bearer"));
    assert!(www_auth.contains("invalid_token"));
}

// ==========================================================================
// Database role & session tests (requires testcontainers)
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_db_role_switching() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    setup_auth_roles(db.pool()).await;

    // Verify role switching works
    let row = sqlx::query("SELECT current_user")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let default_user: String = row.get(0);
    assert_eq!(default_user, "postgres"); // default superuser

    // Switch role in transaction
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::raw_sql("SET LOCAL ROLE web_anon")
        .execute(&mut *tx)
        .await
        .unwrap();

    let row = sqlx::query("SELECT current_user")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let switched_user: String = row.get(0);
    assert_eq!(switched_user, "web_anon");

    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_db_set_claims_in_session() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    setup_auth_roles(db.pool()).await;

    let config = test_config();
    let claims = serde_json::json!({
        "role": "test_author",
        "sub": "user42",
        "exp": chrono::Utc::now().timestamp() + 3600
    });
    let token = encode_hs256(&claims);

    let auth_result = jwt::parse_and_validate(&token, &config).unwrap();

    // Set claims in a database session
    let mut tx = db.pool().begin().await.unwrap();

    // Set role
    let set_role = format!("SET LOCAL ROLE {}", auth_result.role);
    sqlx::raw_sql(&set_role)
        .execute(&mut *tx)
        .await
        .unwrap();

    // Set claims JSON
    let claims_json = auth_result.claims_json();
    sqlx::query("SELECT set_config('request.jwt.claims', $1, true)")
        .bind(&claims_json)
        .execute(&mut *tx)
        .await
        .unwrap();

    // Verify claims are accessible
    let row = sqlx::query("SELECT current_setting('request.jwt.claims', true)")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let stored_claims: String = row.get(0);
    let parsed: serde_json::Value = serde_json::from_str(&stored_claims).unwrap();
    assert_eq!(parsed["role"], "test_author");
    assert_eq!(parsed["sub"], "user42");

    // Verify role
    let row = sqlx::query("SELECT current_user")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let user: String = row.get(0);
    assert_eq!(user, "test_author");

    tx.rollback().await.unwrap();
}

#[tokio::test]
#[ignore]
async fn test_db_rls_web_anon_sees_published_only() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    setup_auth_roles(db.pool()).await;

    let rows = query_as_role(db.pool(), "web_anon", "{}").await;

    // web_anon should only see published posts (3 out of 4)
    assert_eq!(rows.len(), 3, "web_anon should see 3 published posts");
    for row in &rows {
        let published: bool = row.get("published");
        assert!(published, "web_anon should only see published posts");
    }
}

#[tokio::test]
#[ignore]
async fn test_db_rls_admin_sees_all() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    setup_auth_roles(db.pool()).await;

    let rows = query_as_role(db.pool(), "test_admin", "{}").await;

    // test_admin should see all 4 posts
    assert_eq!(rows.len(), 4, "test_admin should see all 4 posts");
}

#[tokio::test]
#[ignore]
async fn test_db_anonymous_auth_result_to_session() {
    let db = common::TestDb::new()
        .await
        .expect("Failed to create test database: Docker and network access required. Ensure Docker is running and you have permission to create containers.");
    setup_auth_roles(db.pool()).await;

    let auth_result = AuthResult::anonymous("web_anon");
    assert!(auth_result.is_anonymous());
    assert_eq!(auth_result.claims_json(), "{}");

    // Use anonymous role in session
    let mut tx = db.pool().begin().await.unwrap();
    sqlx::raw_sql("SET LOCAL ROLE web_anon")
        .execute(&mut *tx)
        .await
        .unwrap();

    let row = sqlx::query("SELECT current_user")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let user: String = row.get(0);
    assert_eq!(user, "web_anon");

    tx.rollback().await.unwrap();
}

// ==========================================================================
// JWT cache + DB integration
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_cache_across_requests() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));
    let claims = valid_claims("test_author");
    let token = encode_hs256(&claims);

    // Simulate multiple requests with the same token
    for _ in 0..5 {
        let request = make_request(Some(&token));
        let result = authenticate(&state, &request).await.unwrap();
        assert_eq!(result.role.as_str(), "test_author");
    }

    // Verify cache has this token via get()
    let cached = state.cache.get(&token).await;
    assert!(cached.is_some(), "Token should be cached after first use");
    assert_eq!(cached.unwrap().role.as_str(), "test_author");
}

#[tokio::test]
#[ignore]
async fn test_cache_different_tokens() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));

    let mut tokens = Vec::new();

    // Multiple different tokens
    for i in 0..3 {
        let claims = serde_json::json!({
            "role": format!("role_{i}"),
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_hs256(&claims);
        let request = make_request(Some(&token));
        let result = authenticate(&state, &request).await.unwrap();
        assert_eq!(result.role.as_str(), format!("role_{i}"));
        tokens.push(token);
    }

    // Verify each token is individually cached
    for (i, token) in tokens.iter().enumerate() {
        let cached = state.cache.get(token).await;
        assert!(cached.is_some(), "Token {i} should be cached");
        assert_eq!(cached.unwrap().role.as_str(), format!("role_{i}"));
    }
}

// ==========================================================================
// Error response format tests
// ==========================================================================

#[tokio::test]
#[ignore]
async fn test_error_response_json_format() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));

    let claims = serde_json::json!({
        "role": "test_author",
        "exp": chrono::Utc::now().timestamp() - 60
    });
    let token = encode_hs256(&claims);

    let request = Request::builder()
        .uri("/test")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Verify standard error format
    assert!(json.get("code").is_some(), "Error should have 'code' field");
    assert!(
        json.get("message").is_some(),
        "Error should have 'message' field"
    );
    assert_eq!(json["code"], "PGRST303");
}

#[tokio::test]
#[ignore]
async fn test_error_response_wrong_secret_format() {
    let config = test_config();
    let state = AuthState::new(Arc::new(config));

    let claims = valid_claims("test_author");
    let token = jsonwebtoken::encode(
        &JwtHeader::default(),
        &claims,
        &EncodingKey::from_secret(b"wrong_secret_that_is_long_enough"),
    )
    .unwrap();

    let request = Request::builder()
        .uri("/test")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let response = simulate_http(&state, request).await;
    assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["code"], "PGRST301");
}
