//! Request routing
//!
//! Builds the main `axum::Router` with resource, RPC, and root routes.
//! The middleware stack (CORS, compression, tracing, auth, metrics) is
//! layered on top.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use arc_swap::ArcSwap;
use axum::{
    Router,
    extract::Request,
    middleware::{self, Next},
    response::Response,
    routing::{get, options, post},
};
use tower_http::{
    compression::CompressionLayer,
    cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer, ExposeHeaders},
    trace::TraceLayer,
};

use crate::auth::middleware::{authenticate_token, jwt_error_response};
use crate::config::AppConfig;

use super::handlers;
use super::state::AppState;

/// Create the main application router with all routes and middleware.
///
/// # Routes
///
/// | Method  | Path             | Handler               |
/// |---------|------------------|-----------------------|
/// | GET     | `/`              | schema_root_handler   |
/// | OPTIONS | `/`              | root_options_handler  |
/// | GET     | `/openapi.json`  | openapi_spec_handler  |
/// | GET     | `/:resource`     | read_handler          |
/// | HEAD    | `/:resource`     | read_handler          |
/// | POST    | `/:resource`     | create_handler        |
/// | PATCH   | `/:resource`     | update_handler        |
/// | PUT     | `/:resource`     | upsert_handler        |
/// | DELETE  | `/:resource`     | delete_handler        |
/// | OPTIONS | `/:resource`     | options_handler       |
/// | GET     | `/rpc/:function` | rpc_get_handler       |
/// | POST    | `/rpc/:function` | rpc_post_handler      |
///
/// # Middleware Stack (outer → inner)
///
/// 1. Tracing (request/response logging)
/// 2. CORS
/// 3. Compression (gzip, deflate, brotli)
pub fn create_router(state: AppState) -> Router {
    // RPC routes (must be before the catch-all /:resource)
    let rpc_routes = Router::new()
        .route("/:function", get(handlers::rpc_get_handler))
        .route("/:function", post(handlers::rpc_post_handler));

    // Resource routes
    let resource_routes = Router::new()
        .route(
            "/:resource",
            get(handlers::read_handler)
                .head(handlers::read_handler)
                .post(handlers::create_handler)
                .patch(handlers::update_handler)
                .put(handlers::upsert_handler)
                .delete(handlers::delete_handler),
        )
        .route("/:resource", options(handlers::options_handler));

    // Build auth middleware layer as a closure capturing the AuthState
    let auth_state = state.auth.clone();
    let auth_layer = middleware::from_fn(move |req: Request, next: Next| {
        let auth = auth_state.clone();
        async move { auth_middleware_inner(auth, req, next).await }
    });

    // Build metrics middleware layer
    let metrics = state.metrics.clone();
    let metrics_layer = middleware::from_fn(move |req: Request, next: Next| {
        let m = metrics.clone();
        async move {
            m.requests_total.fetch_add(1, Ordering::Relaxed);
            let response = next.run(req).await;
            if response.status().is_success() {
                m.requests_success.fetch_add(1, Ordering::Relaxed);
            } else {
                m.requests_error.fetch_add(1, Ordering::Relaxed);
            }
            response
        }
    });

    // Build CORS layer from config
    let config = state.config();
    let cors_layer = create_cors_layer(&config);

    // Build Server-Timing middleware (only active when config enables it)
    let timing_config = state.config.clone();
    let timing_layer = middleware::from_fn(move |req: Request, next: Next| {
        let cfg = timing_config.clone();
        async move { server_timing_middleware(cfg, req, next).await }
    });

    // Assemble main router (fixed paths before catch-all /:resource)
    Router::new()
        .route("/", get(handlers::schema_root_handler))
        .route("/", options(handlers::root_options_handler))
        .route("/openapi.json", get(handlers::openapi_spec_handler))
        .nest("/rpc", rpc_routes)
        .merge(resource_routes)
        .route_layer(auth_layer)
        .layer(metrics_layer)
        .layer(timing_layer)
        .layer(CompressionLayer::new())
        .layer(cors_layer)
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Middleware that adds a `Server-Timing` header when enabled in config.
///
/// The header format follows the W3C Server-Timing specification:
/// `Server-Timing: total;dur=123.456` (duration in milliseconds).
///
/// The config is read from the shared `ArcSwap` on each request, so
/// toggling `server_timing_enabled` via config reload takes effect
/// immediately.
async fn server_timing_middleware(
    config: Arc<ArcSwap<AppConfig>>,
    req: Request,
    next: Next,
) -> Response {
    let enabled = config.load().server_timing_enabled;
    if !enabled {
        return next.run(req).await;
    }

    let start = std::time::Instant::now();
    let mut response = next.run(req).await;
    let elapsed = start.elapsed();
    let dur_ms = elapsed.as_secs_f64() * 1000.0;

    if let Ok(value) = http::HeaderValue::from_str(&format!("total;dur={dur_ms:.3}")) {
        response
            .headers_mut()
            .insert(http::HeaderName::from_static("server-timing"), value);
    }

    response
}

/// Build a CORS layer from configuration.
///
/// Uses `config.server_cors_allowed_origins` if set, otherwise allows any origin.
/// Always allows the standard PgREST headers and HTTP methods.
pub fn create_cors_layer(config: &AppConfig) -> CorsLayer {
    use http::Method;

    let methods = AllowMethods::list([
        Method::GET,
        Method::POST,
        Method::PATCH,
        Method::PUT,
        Method::DELETE,
        Method::OPTIONS,
        Method::HEAD,
    ]);

    let headers = AllowHeaders::list([
        http::header::AUTHORIZATION,
        http::header::CONTENT_TYPE,
        http::header::ACCEPT,
        http::header::RANGE,
        http::HeaderName::from_static("prefer"),
        http::HeaderName::from_static("accept-profile"),
        http::HeaderName::from_static("content-profile"),
    ]);

    let expose = ExposeHeaders::list([
        http::HeaderName::from_static("content-range"),
        http::HeaderName::from_static("preference-applied"),
        http::header::LOCATION,
    ]);

    let origin = match config.server_cors_allowed_origins {
        Some(ref origins) => {
            let parsed: Vec<http::HeaderValue> =
                origins.iter().filter_map(|o| o.parse().ok()).collect();
            AllowOrigin::list(parsed)
        }
        None => AllowOrigin::any(),
    };

    CorsLayer::new()
        .allow_methods(methods)
        .allow_headers(headers)
        .expose_headers(expose)
        .allow_origin(origin)
}

/// Auth middleware logic: validates JWT and inserts `AuthResult` into extensions.
async fn auth_middleware_inner(
    auth: crate::auth::middleware::AuthState,
    mut request: Request,
    next: Next,
) -> Response {
    // Extract the bearer token from headers (synchronous) so we don't
    // borrow the non-Sync Request across an await boundary.
    let token = request
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
        })
        .map(|t| t.to_owned());

    match authenticate_token(&auth, token.as_deref()).await {
        Ok(auth_result) => {
            request.extensions_mut().insert(auth_result);
            next.run(request).await
        }
        Err(jwt_err) => jwt_error_response(jwt_err),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_cors_layer_default_origins() {
        let config = AppConfig::default();
        // Should not panic — produces an Any-origin layer
        let _layer = create_cors_layer(&config);
    }

    #[test]
    fn test_create_cors_layer_specific_origins() {
        let mut config = AppConfig::default();
        config.server_cors_allowed_origins = Some(vec![
            "http://localhost:3000".to_string(),
            "https://example.com".to_string(),
        ]);
        let _layer = create_cors_layer(&config);
    }

    #[tokio::test]
    async fn test_server_timing_header_when_enabled() {
        use axum::body::Body;
        use tower::ServiceExt;

        let mut config = AppConfig::default();
        config.server_timing_enabled = true;
        let config_swap = Arc::new(ArcSwap::new(Arc::new(config)));

        // Build a minimal router with just the timing middleware
        let cfg = config_swap.clone();
        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req: Request, next: Next| {
                let c = cfg.clone();
                async move { server_timing_middleware(c, req, next).await }
            }));

        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);

        let timing = resp
            .headers()
            .get("server-timing")
            .expect("Server-Timing header missing");
        let val = timing.to_str().unwrap();
        assert!(val.starts_with("total;dur="), "Unexpected format: {val}");
    }

    #[tokio::test]
    async fn test_server_timing_header_absent_when_disabled() {
        use axum::body::Body;
        use tower::ServiceExt;

        let config = AppConfig::default(); // server_timing_enabled = false
        let config_swap = Arc::new(ArcSwap::new(Arc::new(config)));

        let cfg = config_swap.clone();
        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req: Request, next: Next| {
                let c = cfg.clone();
                async move { server_timing_middleware(c, req, next).await }
            }));

        let req = Request::builder().uri("/test").body(Body::empty()).unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert!(resp.headers().get("server-timing").is_none());
    }
}
