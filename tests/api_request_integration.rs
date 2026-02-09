//! Integration tests for the api_request module.
//!
//! Tests use two approaches:
//! 1. **Tower ServiceExt tests** — mock HTTP requests through an axum router without
//!    a TCP listener, using `tower::ServiceExt::oneshot`.
//! 2. **Full HTTP server tests** — spin up a real `tokio::net::TcpListener`-backed
//!    axum server and hit it with `reqwest`.
//!
//! Both approaches verify that the api_request parsing pipeline correctly transforms
//! HTTP requests into typed ApiRequest structs.

use axum::{
    Router,
    body::Body,
    extract::Request,
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use compact_str::CompactString;
use http::StatusCode;
use serde_json::{Value, json};
use tower::ServiceExt;

use pgrest::api_request::{self, ApiRequest, preferences::Preferences};
use pgrest::config::AppConfig;
use pgrest::types::media::MediaType;

// ==========================================================================
// Shared test helpers
// ==========================================================================

fn test_config() -> AppConfig {
    AppConfig {
        db_schemas: vec!["public".to_string(), "api".to_string()],
        ..Default::default()
    }
}

/// Build an ApiRequest from raw parts (used by both tower and reqwest tests).
fn build_api_request(
    config: &AppConfig,
    method: &str,
    path: &str,
    query: &str,
    headers: &[(String, String)],
    body: Bytes,
) -> Result<ApiRequest, pgrest::Error> {
    let prefs = Preferences::default();
    api_request::from_request(config, &prefs, method, path, query, headers, body)
}

/// A test handler that parses the request and returns a JSON summary of the ApiRequest.
/// This is used by the tower and reqwest integration tests to verify parsing.
async fn test_handler(req: Request) -> Response {
    let config = test_config();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    let headers: Vec<(String, String)> = req
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, format!("body error: {}", e)).into_response();
        }
    };

    let prefs = Preferences::default();
    match api_request::from_request(&config, &prefs, &method, &path, &query, &headers, body) {
        Ok(api_req) => {
            let summary = json!({
                "method": api_req.method,
                "path": api_req.path,
                "schema": api_req.schema.as_str(),
                "negotiated_by_profile": api_req.negotiated_by_profile,
                "action_type": format!("{:?}", api_req.action),
                "select_count": api_req.query_params.select.len(),
                "filter_count": api_req.query_params.filters_root.len(),
                "order_count": api_req.query_params.order.len(),
                "has_payload": api_req.payload.is_some(),
                "column_count": api_req.columns.len(),
                "accept_types": api_req.accept_media_types.iter()
                    .map(|m| m.as_str().to_string())
                    .collect::<Vec<_>>(),
                "content_type": api_req.content_media_type.as_str(),
                "cookie_count": api_req.cookies.len(),
                "top_level_range_all": api_req.top_level_range.is_all(),
                "top_level_range_offset": api_req.top_level_range.offset,
                "top_level_range_limit": api_req.top_level_range.limit(),
            });
            Json(summary).into_response()
        }
        Err(e) => {
            let err_json = json!({
                "error": true,
                "code": e.code(),
                "message": e.to_string(),
            });
            (e.status(), Json(err_json)).into_response()
        }
    }
}

/// Build the test router used by both tower and reqwest tests.
/// Uses a fallback handler to capture all routes — the test handler
/// parses the URL path directly so no path extraction is needed.
fn test_router() -> Router {
    Router::new().fallback(test_handler)
}

// ==========================================================================
// Part 1: Tower ServiceExt tests (mock HTTP, no TCP listener)
// ==========================================================================

mod tower_tests {
    use super::*;

    #[tokio::test]
    async fn test_get_table_simple() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items?select=id,name")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["method"], "GET");
        assert_eq!(json["path"], "/items");
        assert_eq!(json["schema"], "public");
        assert_eq!(json["select_count"], 2);
        assert!(!json["negotiated_by_profile"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_get_table_with_filters() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items?select=*&id=eq.5&name=like.*test*")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["filter_count"], 2);
    }

    #[tokio::test]
    async fn test_get_table_with_order() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items?select=*&order=name.desc,id.asc")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["order_count"], 1); // One order param with 2 terms
    }

    #[tokio::test]
    async fn test_post_json_body() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/items")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":1,"name":"test","value":42}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["has_payload"].as_bool().unwrap());
        assert_eq!(json["column_count"], 3);
        assert!(
            json["action_type"]
                .as_str()
                .unwrap()
                .contains("MutationCreate")
        );
    }

    #[tokio::test]
    async fn test_put_request() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/items?select=*")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":1,"name":"updated"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(
            json["action_type"]
                .as_str()
                .unwrap()
                .contains("MutationSingleUpsert")
        );
    }

    #[tokio::test]
    async fn test_patch_request() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("PATCH")
                    .uri("/items?id=eq.1")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"name":"patched"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(
            json["action_type"]
                .as_str()
                .unwrap()
                .contains("MutationUpdate")
        );
    }

    #[tokio::test]
    async fn test_delete_request() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/items?id=eq.1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(
            json["action_type"]
                .as_str()
                .unwrap()
                .contains("MutationDelete")
        );
        assert!(!json["has_payload"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_head_request() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("HEAD")
                    .uri("/items")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(
            json["action_type"]
                .as_str()
                .unwrap()
                .contains("RelationRead")
        );
        assert!(json["action_type"].as_str().unwrap().contains("true")); // headers_only
    }

    #[tokio::test]
    async fn test_rpc_get() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/rpc/my_func?id=5&name=test")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["action_type"].as_str().unwrap().contains("Routine"));
        assert!(json["action_type"].as_str().unwrap().contains("InvRead"));
    }

    #[tokio::test]
    async fn test_rpc_post_with_json() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/my_func")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"arg1":"value1","arg2":42}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        // InvokeMethod::Inv debug format: "Inv" (no bool parameter unlike InvRead)
        let action = json["action_type"].as_str().unwrap();
        assert!(
            action.contains("Routine"),
            "expected Routine, got: {}",
            action
        );
        assert!(action.contains("Inv"), "expected Inv, got: {}", action);
        assert!(
            !action.contains("InvRead"),
            "should not be InvRead, got: {}",
            action
        );
        assert!(json["has_payload"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_schema_request() {
        let app = test_router();

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["action_type"].as_str().unwrap().contains("SchemaRead"));
    }

    #[tokio::test]
    async fn test_profile_header_get() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items")
                    .header("accept-profile", "api")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["schema"], "api");
        assert!(json["negotiated_by_profile"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_content_profile_post() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/items")
                    .header("content-profile", "api")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"id":1}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["schema"], "api");
    }

    #[tokio::test]
    async fn test_invalid_schema_rejected() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items")
                    .header("accept-profile", "nonexistent_schema")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should return an error status
        assert_ne!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["error"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_range_header() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items")
                    .header("range", "items=0-24")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(!json["top_level_range_all"].as_bool().unwrap());
        assert_eq!(json["top_level_range_offset"], 0);
        assert_eq!(json["top_level_range_limit"], 25); // 0-24 = 25 rows
    }

    #[tokio::test]
    async fn test_select_with_relation_embedding() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items?select=id,name,posts(id,title)")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["select_count"], 3); // id, name, posts(...)
    }

    #[tokio::test]
    async fn test_accept_header_csv() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items")
                    .header("accept", "text/csv")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        let accept_types = json["accept_types"].as_array().unwrap();
        assert_eq!(accept_types[0], "text/csv");
    }

    #[tokio::test]
    async fn test_cookies_parsing() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items")
                    .header("cookie", "session=abc123; lang=en")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["cookie_count"], 2);
    }

    #[tokio::test]
    async fn test_invalid_json_body() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/items")
                    .header("content-type", "application/json")
                    .body(Body::from("not valid json"))
                    .unwrap(),
            )
            .await
            .unwrap();

        // Should return an error
        assert_ne!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["error"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_limit_offset_query_params() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/items?select=*&limit=10&offset=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(!json["top_level_range_all"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_url_encoded_body_rpc() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/rpc/my_func")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("arg1=value1&arg2=42"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["has_payload"].as_bool().unwrap());
        assert_eq!(json["column_count"], 2);
    }

    #[tokio::test]
    async fn test_json_array_body() {
        let app = test_router();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/items")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();

        assert!(json["has_payload"].as_bool().unwrap());
        assert_eq!(json["column_count"], 2);
    }
}

// ==========================================================================
// Part 2: Direct api_request::from_request tests (no HTTP overhead)
// ==========================================================================

mod direct_tests {
    use super::*;

    #[test]
    fn test_get_simple() {
        let config = test_config();
        let req = build_api_request(
            &config,
            "GET",
            "/items",
            "select=id,name",
            &[],
            Bytes::new(),
        )
        .unwrap();

        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/items");
        assert_eq!(req.query_params.select.len(), 2);
        assert_eq!(req.schema.as_str(), "public");
    }

    #[test]
    fn test_post_with_body() {
        let config = test_config();
        let headers = vec![("content-type".to_string(), "application/json".to_string())];
        let body = Bytes::from(r#"{"x":1,"y":2}"#);

        let req = build_api_request(&config, "POST", "/items", "", &headers, body).unwrap();

        assert!(req.payload.is_some());
        assert_eq!(req.columns.len(), 2);
        assert!(req.columns.contains("x"));
        assert!(req.columns.contains("y"));
    }

    #[test]
    fn test_rpc_get_params() {
        let config = test_config();
        let req = build_api_request(
            &config,
            "GET",
            "/rpc/my_func",
            "a=1&b=hello",
            &[],
            Bytes::new(),
        )
        .unwrap();

        assert_eq!(req.query_params.params.len(), 2);
    }

    #[test]
    fn test_schema_root() {
        let config = test_config();
        let req = build_api_request(&config, "GET", "/", "", &[], Bytes::new()).unwrap();

        assert!(matches!(
            req.action,
            pgrest::api_request::Action::Db(
                pgrest::api_request::types::DbAction::SchemaRead { .. }
            )
        ));
    }

    #[test]
    fn test_invalid_path() {
        let config = test_config();
        let result = build_api_request(&config, "GET", "/a/b/c", "", &[], Bytes::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_schema() {
        let config = test_config();
        let headers = vec![("accept-profile".to_string(), "nonexistent".to_string())];
        let result = build_api_request(&config, "GET", "/items", "", &headers, Bytes::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_filters_parsed() {
        let config = test_config();
        let req = build_api_request(
            &config,
            "GET",
            "/items",
            "id=eq.5&name=like.*test*&active=is.true",
            &[],
            Bytes::new(),
        )
        .unwrap();

        assert_eq!(req.query_params.filters_root.len(), 3);
    }

    #[test]
    fn test_order_parsed() {
        let config = test_config();
        let req = build_api_request(
            &config,
            "GET",
            "/items",
            "order=name.asc,id.desc.nullsfirst",
            &[],
            Bytes::new(),
        )
        .unwrap();

        assert_eq!(req.query_params.order.len(), 1);
        assert_eq!(req.query_params.order[0].1.len(), 2);
    }

    #[test]
    fn test_accept_header_negotiation() {
        let config = test_config();
        let headers = vec![(
            "accept".to_string(),
            "text/csv, application/json;q=0.5".to_string(),
        )];
        let req = build_api_request(&config, "GET", "/items", "", &headers, Bytes::new()).unwrap();

        assert_eq!(req.accept_media_types.len(), 2);
        assert_eq!(req.accept_media_types[0], MediaType::TextCsv);
        assert_eq!(req.accept_media_types[1], MediaType::ApplicationJson);
    }

    #[test]
    fn test_default_accept() {
        let config = test_config();
        let req = build_api_request(&config, "GET", "/items", "", &[], Bytes::new()).unwrap();

        assert_eq!(req.accept_media_types.len(), 1);
        assert_eq!(req.accept_media_types[0], MediaType::Any);
    }

    #[test]
    fn test_content_type_default() {
        let config = test_config();
        let req = build_api_request(&config, "GET", "/items", "", &[], Bytes::new()).unwrap();

        assert_eq!(req.content_media_type, MediaType::ApplicationJson);
    }

    #[test]
    fn test_cookies_extracted() {
        let config = test_config();
        let headers = vec![("cookie".to_string(), "session=abc; lang=en".to_string())];
        let req = build_api_request(&config, "GET", "/items", "", &headers, Bytes::new()).unwrap();

        assert_eq!(req.cookies.len(), 2);
        assert_eq!(req.cookies[0].0, "session");
        assert_eq!(req.cookies[0].1, "abc");
        assert_eq!(req.cookies[1].0, "lang");
        assert_eq!(req.cookies[1].1, "en");
    }

    #[test]
    fn test_headers_lowercased() {
        let config = test_config();
        let headers = vec![
            ("X-Custom-Header".to_string(), "value123".to_string()),
            ("Authorization".to_string(), "Bearer token".to_string()),
        ];
        let req = build_api_request(&config, "GET", "/items", "", &headers, Bytes::new()).unwrap();

        // All header names should be lowercased
        for (name, _) in &req.headers {
            assert_eq!(name, &name.to_lowercase());
        }
    }

    #[test]
    fn test_range_from_header() {
        let config = test_config();
        let headers = vec![("range".to_string(), "items=5-19".to_string())];
        let req = build_api_request(&config, "GET", "/items", "", &headers, Bytes::new()).unwrap();

        assert_eq!(req.top_level_range.offset, 5);
        assert_eq!(req.top_level_range.limit(), Some(15));
    }

    #[test]
    fn test_range_header_ignored_for_post() {
        let config = test_config();
        let headers = vec![
            ("range".to_string(), "items=5-19".to_string()),
            ("content-type".to_string(), "application/json".to_string()),
        ];
        let body = Bytes::from(r#"{"id":1}"#);
        let req = build_api_request(&config, "POST", "/items", "", &headers, body).unwrap();

        assert!(req.top_level_range.is_all());
    }

    #[test]
    fn test_delete_no_payload() {
        let config = test_config();
        let req =
            build_api_request(&config, "DELETE", "/items", "id=eq.1", &[], Bytes::new()).unwrap();

        assert!(req.payload.is_none());
        assert!(req.columns.is_empty());
    }

    #[test]
    fn test_embedded_filters() {
        let config = test_config();
        let req = build_api_request(
            &config,
            "GET",
            "/items",
            "select=*,posts(*)&posts.status=eq.published",
            &[],
            Bytes::new(),
        )
        .unwrap();

        assert_eq!(req.query_params.filters_not_root.len(), 1);
        assert_eq!(
            req.query_params.filters_not_root[0].0,
            vec![CompactString::from("posts")]
        );
    }
}

// ==========================================================================
// Part 3: Full HTTP server tests (reqwest + TcpListener)
// ==========================================================================

mod reqwest_tests {
    use super::*;

    async fn start_test_server() -> String {
        let app = test_router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind TCP listener: network access required. Ensure you have permission to bind sockets.");
        let addr = listener
            .local_addr()
            .expect("Failed to get listener address");
        let url = format!("http://{}", addr);

        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // Give the server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        url
    }

    #[tokio::test]
    async fn test_reqwest_get_table() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/items?select=id,name", base_url))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert_eq!(json["method"], "GET");
        assert_eq!(json["path"], "/items");
        assert_eq!(json["select_count"], 2);
    }

    #[tokio::test]
    async fn test_reqwest_post_json() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{}/items", base_url))
            .header("content-type", "application/json")
            .body(r#"{"id":1,"name":"test"}"#)
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(json["has_payload"].as_bool().unwrap());
        assert_eq!(json["column_count"], 2);
    }

    #[tokio::test]
    async fn test_reqwest_rpc() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/rpc/my_func?param1=value1", base_url))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(json["action_type"].as_str().unwrap().contains("Routine"));
    }

    #[tokio::test]
    async fn test_reqwest_profile_header() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/items", base_url))
            .header("accept-profile", "api")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert_eq!(json["schema"], "api");
        assert!(json["negotiated_by_profile"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_reqwest_invalid_schema() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/items", base_url))
            .header("accept-profile", "nonexistent")
            .send()
            .await
            .unwrap();

        assert_ne!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(json["error"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_reqwest_range_header() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/items", base_url))
            .header("range", "items=0-9")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(!json["top_level_range_all"].as_bool().unwrap());
        assert_eq!(json["top_level_range_offset"], 0);
        assert_eq!(json["top_level_range_limit"], 10);
    }

    #[tokio::test]
    async fn test_reqwest_csv_accept() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/items", base_url))
            .header("accept", "text/csv")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        let accept_types = json["accept_types"].as_array().unwrap();
        assert_eq!(accept_types[0], "text/csv");
    }

    #[tokio::test]
    async fn test_reqwest_delete() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .delete(format!("{}/items?id=eq.1", base_url))
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(
            json["action_type"]
                .as_str()
                .unwrap()
                .contains("MutationDelete")
        );
    }

    #[tokio::test]
    async fn test_reqwest_post_url_encoded_rpc() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{}/rpc/my_func", base_url))
            .header("content-type", "application/x-www-form-urlencoded")
            .body("a=1&b=hello")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(json["has_payload"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_reqwest_invalid_json() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .post(format!("{}/items", base_url))
            .header("content-type", "application/json")
            .body("not json")
            .send()
            .await
            .unwrap();

        assert_ne!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(json["error"].as_bool().unwrap());
    }

    #[tokio::test]
    async fn test_reqwest_cookies() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client
            .get(format!("{}/items", base_url))
            .header("cookie", "token=xyz; locale=us")
            .send()
            .await
            .unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert_eq!(json["cookie_count"], 2);
    }

    #[tokio::test]
    async fn test_reqwest_schema_root() {
        let base_url = start_test_server().await;
        let client = reqwest::Client::new();

        let resp = client.get(format!("{}/", base_url)).send().await.unwrap();

        assert_eq!(resp.status(), 200);

        let json: Value = resp.json().await.unwrap();
        assert!(json["action_type"].as_str().unwrap().contains("SchemaRead"));
    }
}
