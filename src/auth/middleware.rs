//! Axum auth middleware
//!
//! Extracts the `Authorization: Bearer <token>` header, validates the JWT,
//! caches the result, and inserts an [`AuthResult`] into the request
//! extensions for downstream handlers.
//!
//! # Flow
//!
//! 1. Extract token from `Authorization` header.
//! 2. If no token and anonymous role is configured → anonymous access.
//! 3. If no token and no anonymous role → 401 (`PGRST302`).
//! 4. Check the JWT cache for a previous validation result.
//! 5. On cache miss, validate via [`jwt::parse_and_validate`].
//! 6. Store the result in the cache and attach it to the request extensions.
//!
//! # Error Response
//!
//! JWT errors produce a JSON error body with the appropriate PGRST error
//! code and a `WWW-Authenticate` header when the status is 401.

use std::sync::Arc;

use arc_swap::ArcSwap;
use axum::{
    extract::Request,
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::header;

use crate::config::AppConfig;
use crate::error::response::ErrorResponse;

use super::cache::JwtCache;
use super::error::JwtError;
use super::jwt;
use super::types::AuthResult;

/// Shared authentication state passed to the middleware via axum `State`.
///
/// Contains the config and JWT cache. Cloned per-request (cheap — all
/// fields are `Arc` or `Clone`-cheap).
///
/// The `config` field is an `ArcSwap` so that live config reloads
/// (triggered via `NOTIFY pgrst, 'reload config'`) are visible to the
/// auth middleware without restarting the server.
#[derive(Debug, Clone)]
pub struct AuthState {
    pub config: Arc<ArcSwap<AppConfig>>,
    pub cache: JwtCache,
}

impl AuthState {
    /// Create a new `AuthState` wrapping the given config snapshot.
    ///
    /// The config is placed inside a fresh `ArcSwap`. Use
    /// [`with_shared_config`](Self::with_shared_config) to share the
    /// same `ArcSwap` with `AppState` for live-reload support.
    pub fn new(config: Arc<AppConfig>) -> Self {
        let max_entries = config.jwt_cache_max_entries;
        Self {
            config: Arc::new(ArcSwap::new(config)),
            cache: JwtCache::new(max_entries),
        }
    }

    /// Create an `AuthState` that shares an existing `ArcSwap<AppConfig>`.
    ///
    /// When the `ArcSwap` is updated (e.g. during config reload), the
    /// auth middleware automatically sees the new values.
    pub fn with_shared_config(config: Arc<ArcSwap<AppConfig>>) -> Self {
        let max_entries = config.load().jwt_cache_max_entries;
        Self {
            config,
            cache: JwtCache::new(max_entries),
        }
    }

    /// Get a snapshot of the current config.
    pub fn load_config(&self) -> arc_swap::Guard<Arc<AppConfig>> {
        self.config.load()
    }
}

/// Axum middleware function for JWT authentication.
///
/// Attach to a router via `axum::middleware::from_fn_with_state`:
///
/// ```ignore
/// use axum::{Router, middleware};
/// use pgrest::auth::middleware::{auth_middleware, AuthState};
///
/// let state = AuthState::new(config.into());
/// let app = Router::new()
///     .route("/items", get(handler))
///     .layer(middleware::from_fn_with_state(state, auth_middleware));
/// ```
pub async fn auth_middleware(
    axum::extract::State(state): axum::extract::State<AuthState>,
    mut request: Request,
    next: Next,
) -> Response {
    match authenticate(&state, &request).await {
        Ok(auth_result) => {
            request.extensions_mut().insert(auth_result);
            next.run(request).await
        }
        Err(jwt_err) => jwt_error_response(jwt_err),
    }
}

/// Core authentication logic, separated for testability.
pub async fn authenticate(
    state: &AuthState,
    request: &Request,
) -> Result<AuthResult, JwtError> {
    let token = extract_bearer_token(request);
    authenticate_token(state, token).await
}

/// Authenticate with an already-extracted token string.
///
/// This variant avoids borrowing the `Request` across await points, making
/// it usable in contexts where the `Request` body is not `Sync`.
pub async fn authenticate_token(
    state: &AuthState,
    token: Option<&str>,
) -> Result<AuthResult, JwtError> {
    let config = state.load_config();
    match token {
        Some(token) => {
            // Check cache first
            if let Some(cached) = state.cache.get(token).await {
                return Ok((*cached).clone());
            }

            // Validate
            let result = jwt::parse_and_validate(token, &config)?;

            // Cache the result
            state.cache.insert(token, result.clone()).await;

            Ok(result)
        }
        None => {
            // No token — check anonymous role
            if let Some(ref anon_role) = config.db_anon_role {
                Ok(AuthResult::anonymous(anon_role))
            } else {
                Err(JwtError::TokenRequired)
            }
        }
    }
}

/// Extract the Bearer token from the `Authorization` header.
///
/// Returns `None` if no `Authorization` header is present.
/// Returns `Some("")` if the header is `Bearer ` with an empty token,
/// which is then caught by `parse_and_validate` as `EmptyAuthHeader`.
fn extract_bearer_token(request: &Request) -> Option<&str> {
    let header_value = request.headers().get(header::AUTHORIZATION)?;
    let header_str = header_value.to_str().ok()?;

    if let Some(token) = header_str.strip_prefix("Bearer ") {
        Some(token)
    } else if let Some(token) = header_str.strip_prefix("bearer ") {
        Some(token)
    } else {
        // Not a Bearer token — ignore (might be Basic auth etc.)
        None
    }
}

/// Build an HTTP error response from a JWT error.
pub fn jwt_error_response(err: JwtError) -> Response {
    let status = err.status();
    let www_auth = err.www_authenticate();

    let body = ErrorResponse {
        code: err.code(),
        message: err.to_string(),
        details: err.details(),
        hint: None,
    };

    let mut response = (status, axum::Json(body)).into_response();

    if let Some(www_auth_value) = www_auth {
        if let Ok(hv) = http::HeaderValue::from_str(&www_auth_value) {
            response
                .headers_mut()
                .insert(header::WWW_AUTHENTICATE, hv);
        }
    }

    response
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use jsonwebtoken::{EncodingKey, Header as JwtHeader};

    fn test_state(secret: &str) -> AuthState {
        let mut config = AppConfig::default();
        config.jwt_secret = Some(secret.to_string());
        config.db_anon_role = Some("web_anon".to_string());
        config.jwt_cache_max_entries = 100;
        AuthState::new(Arc::new(config))
    }

    fn test_state_no_anon(secret: &str) -> AuthState {
        let mut config = AppConfig::default();
        config.jwt_secret = Some(secret.to_string());
        config.db_anon_role = None;
        config.jwt_cache_max_entries = 100;
        AuthState::new(Arc::new(config))
    }

    fn encode_token(claims: &serde_json::Value, secret: &str) -> String {
        jsonwebtoken::encode(
            &JwtHeader::default(),
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn make_request(token: Option<&str>) -> Request {
        let mut builder = Request::builder().method("GET").uri("/items");
        if let Some(t) = token {
            builder = builder.header("Authorization", format!("Bearer {t}"));
        }
        builder.body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn test_authenticate_valid_token() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let state = test_state(secret);
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);
        let request = make_request(Some(&token));

        let result = authenticate(&state, &request).await.unwrap();
        assert_eq!(result.role.as_str(), "test_author");
        assert!(!result.is_anonymous());
    }

    #[tokio::test]
    async fn test_authenticate_anonymous() {
        let state = test_state("secret");
        let request = make_request(None);

        let result = authenticate(&state, &request).await.unwrap();
        assert_eq!(result.role.as_str(), "web_anon");
        assert!(result.is_anonymous());
    }

    #[tokio::test]
    async fn test_authenticate_no_anon_no_token() {
        let state = test_state_no_anon("secret");
        let request = make_request(None);

        let err = authenticate(&state, &request).await.unwrap_err();
        assert!(matches!(err, JwtError::TokenRequired));
    }

    #[tokio::test]
    async fn test_authenticate_expired_token() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let state = test_state(secret);
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() - 60
        });
        let token = encode_token(&claims, secret);
        let request = make_request(Some(&token));

        let err = authenticate(&state, &request).await.unwrap_err();
        assert!(matches!(err, JwtError::Claims(_)));
    }

    #[tokio::test]
    async fn test_authenticate_wrong_secret() {
        let state = test_state("correct_secret_is_long_enough");
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, "wrong_secret_value_different");
        let request = make_request(Some(&token));

        let err = authenticate(&state, &request).await.unwrap_err();
        assert!(matches!(err, JwtError::Decode(_)));
    }

    #[tokio::test]
    async fn test_authenticate_cache_hit() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let state = test_state(secret);
        let claims = serde_json::json!({
            "role": "cached_role",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        // First request — cache miss
        let request = make_request(Some(&token));
        let result1 = authenticate(&state, &request).await.unwrap();
        assert_eq!(result1.role.as_str(), "cached_role");

        // Second request — cache hit
        let request = make_request(Some(&token));
        let result2 = authenticate(&state, &request).await.unwrap();
        assert_eq!(result2.role.as_str(), "cached_role");

        // Verify cache has the entry
        assert!(state.cache.get(&token).await.is_some());
    }

    #[tokio::test]
    async fn test_authenticate_empty_bearer() {
        let state = test_state("secret");
        let request = Request::builder()
            .method("GET")
            .uri("/items")
            .header("Authorization", "Bearer ")
            .body(Body::empty())
            .unwrap();

        let err = authenticate(&state, &request).await.unwrap_err();
        assert!(matches!(
            err,
            JwtError::Decode(super::super::error::JwtDecodeError::EmptyAuthHeader)
        ));
    }

    #[test]
    fn test_extract_bearer_token() {
        let req = make_request(Some("abc123"));
        assert_eq!(extract_bearer_token(&req), Some("abc123"));

        let req = make_request(None);
        assert!(extract_bearer_token(&req).is_none());

        // Case insensitive "bearer"
        let req = Request::builder()
            .method("GET")
            .uri("/")
            .header("Authorization", "bearer mytoken")
            .body(Body::empty())
            .unwrap();
        assert_eq!(extract_bearer_token(&req), Some("mytoken"));

        // Basic auth — should return None
        let req = Request::builder()
            .method("GET")
            .uri("/")
            .header("Authorization", "Basic dXNlcjpwYXNz")
            .body(Body::empty())
            .unwrap();
        assert!(extract_bearer_token(&req).is_none());
    }

    #[test]
    fn test_jwt_error_response_has_www_authenticate() {
        let err = JwtError::TokenRequired;
        let response = jwt_error_response(err);
        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        assert!(response.headers().contains_key(header::WWW_AUTHENTICATE));
        assert_eq!(
            response.headers().get(header::WWW_AUTHENTICATE).unwrap(),
            "Bearer"
        );
    }

    #[test]
    fn test_jwt_error_response_decode() {
        let err = JwtError::Decode(super::super::error::JwtDecodeError::BadCrypto);
        let response = jwt_error_response(err);
        assert_eq!(response.status(), http::StatusCode::UNAUTHORIZED);
        let www = response
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(www.contains("invalid_token"));
    }

    #[test]
    fn test_jwt_error_response_secret_missing() {
        let err = JwtError::SecretMissing;
        let response = jwt_error_response(err);
        assert_eq!(
            response.status(),
            http::StatusCode::INTERNAL_SERVER_ERROR
        );
        assert!(!response.headers().contains_key(header::WWW_AUTHENTICATE));
    }

    #[test]
    fn test_shared_config_swap_propagates() {
        let config = AppConfig::default();
        let swap = Arc::new(ArcSwap::new(Arc::new(config)));
        let auth = AuthState::with_shared_config(swap.clone());

        // Initial config
        assert_eq!(auth.load_config().server_port, 3000);

        // Swap in new config
        let mut new_config = AppConfig::default();
        new_config.server_port = 9999;
        swap.store(Arc::new(new_config));

        // Auth state sees the update immediately
        assert_eq!(auth.load_config().server_port, 9999);
    }

    #[test]
    fn test_new_constructor_creates_isolated_swap() {
        let config = AppConfig::default();
        let auth = AuthState::new(Arc::new(config));
        assert_eq!(auth.load_config().server_port, 3000);
    }
}
