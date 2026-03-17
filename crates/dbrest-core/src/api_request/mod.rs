//! API Request module
//!
//! Transforms raw HTTP requests into domain-specific `ApiRequest` objects.
//! This is the bridge between HTTP and domain logic, mirroring the
//! Haskell `ApiRequest` module for full API compatibility.
//!
//! # Architecture
//!
//! ```text
//! HTTP Request
//!   ├─ URL path   → Resource + Action
//!   ├─ Query      → QueryParams (select, filter, order, logic)
//!   ├─ Headers    → Preferences, Range, Schema, MediaType
//!   └─ Body       → Payload
//!       ↓
//!   ApiRequest struct
//! ```

pub mod payload;
pub mod preferences;
pub mod query_params;
pub mod range;
pub mod types;

// Re-export key types
pub use preferences::Preferences;
pub use query_params::QueryParams;
pub use range::Range;
pub use types::{
    Action, AggregateFunction, DbAction, EmbedPath, Filter, InvokeMethod, JoinType, LogicTree,
    Mutation, OpExpr, Operation, OrderTerm, Payload, Resource, SelectItem,
};

use bytes::Bytes;
use compact_str::CompactString;
use std::collections::HashSet;

use crate::config::AppConfig;
use crate::error::Error;
use crate::types::identifiers::QualifiedIdentifier;
use crate::types::media::MediaType;

/// The core API request struct.
///
/// The core `ApiRequest` data type. Contains all parsed and
/// validated information from an HTTP request.
#[derive(Debug, Clone)]
pub struct ApiRequest {
    /// The resolved action to perform
    pub action: Action,
    /// Ranges keyed by embed level (e.g., "limit" for top-level)
    pub ranges: std::collections::HashMap<CompactString, Range>,
    /// The top-level range for the main query
    pub top_level_range: Range,
    /// Parsed request body
    pub payload: Option<Payload>,
    /// Parsed Prefer headers
    pub preferences: Preferences,
    /// Parsed query parameters
    pub query_params: QueryParams,
    /// Column names from payload or &columns parameter
    pub columns: HashSet<CompactString>,
    /// HTTP headers (lowercased name, value)
    pub headers: Vec<(String, String)>,
    /// Request cookies
    pub cookies: Vec<(String, String)>,
    /// Raw request path
    pub path: String,
    /// HTTP method
    pub method: String,
    /// The request schema (from profile headers or default)
    pub schema: CompactString,
    /// Whether the schema was negotiated via profile headers
    pub negotiated_by_profile: bool,
    /// Accepted media types from Accept header (sorted by quality)
    pub accept_media_types: Vec<MediaType>,
    /// Content-Type of the request body
    pub content_media_type: MediaType,
}

/// Build an `ApiRequest` from HTTP request parts.
///
/// Build an `ApiRequest` from raw HTTP components.
pub fn from_request(
    config: &AppConfig,
    prefs: &Preferences,
    method: &str,
    path: &str,
    query_string: &str,
    headers: &[(String, String)],
    body: Bytes,
) -> Result<ApiRequest, Error> {
    // 1. Parse resource from path
    let resource = get_resource(config, path)?;

    // 2. Get schema from profile headers
    let (schema, negotiated_by_profile) = get_schema(config, headers, method)?;

    // 3. Determine action
    let action = get_action(&resource, &schema, method)?;

    // 4. Parse query parameters
    let query_params = query_params::parse(action.is_invoke_safe(), query_string)?;

    // 5. Parse range (from Range header and limit/offset)
    let (top_level_range, ranges) = get_ranges(method, &query_params, headers)?;

    // 6. Get content type
    let content_media_type = get_content_type(headers);

    // 7. Parse payload
    let (payload, columns) =
        payload::get_payload(body, &content_media_type, &query_params, &action)?;

    // 8. Parse accept media types
    let accept_media_types = get_accept_media_types(headers);

    // 9. Extract headers and cookies
    let (req_headers, cookies) = extract_headers_and_cookies(headers);

    Ok(ApiRequest {
        action,
        ranges,
        top_level_range,
        payload,
        preferences: prefs.clone(),
        query_params,
        columns,
        headers: req_headers,
        cookies,
        path: path.to_string(),
        method: method.to_string(),
        schema,
        negotiated_by_profile,
        accept_media_types,
        content_media_type,
    })
}

// ==========================================================================
// Resource resolution
// ==========================================================================

/// Parse URL path into a Resource.
fn get_resource(config: &AppConfig, path: &str) -> Result<Resource, Error> {
    let path = path.trim_start_matches('/');
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    match segments.as_slice() {
        [] => {
            // Root path
            if let Some(ref root_spec) = config.db_root_spec {
                Ok(Resource::Routine(root_spec.name.clone()))
            } else {
                Ok(Resource::Schema)
            }
        }
        [table] => Ok(Resource::Relation(CompactString::from(*table))),
        ["rpc", func_name] => Ok(Resource::Routine(CompactString::from(*func_name))),
        _ => Err(Error::ParseError {
            location: "path".to_string(),
            message: format!("invalid resource path: /{}", segments.join("/")),
        }),
    }
}

// ==========================================================================
// Action resolution
// ==========================================================================

/// Determine the Action from resource, schema, and HTTP method.
fn get_action(resource: &Resource, schema: &str, method: &str) -> Result<Action, Error> {
    let qi = |name: &str| QualifiedIdentifier::new(schema, name);

    match (resource, method) {
        // Routines
        (Resource::Routine(name), "HEAD") => Ok(Action::Db(DbAction::Routine {
            qi: qi(name),
            inv_method: InvokeMethod::InvRead(true),
        })),
        (Resource::Routine(name), "GET") => Ok(Action::Db(DbAction::Routine {
            qi: qi(name),
            inv_method: InvokeMethod::InvRead(false),
        })),
        (Resource::Routine(name), "POST") => Ok(Action::Db(DbAction::Routine {
            qi: qi(name),
            inv_method: InvokeMethod::Inv,
        })),
        (Resource::Routine(name), "OPTIONS") => {
            Ok(Action::RoutineInfo(qi(name), InvokeMethod::InvRead(true)))
        }

        // Relations
        (Resource::Relation(name), "HEAD") => Ok(Action::Db(DbAction::RelationRead {
            qi: qi(name),
            headers_only: true,
        })),
        (Resource::Relation(name), "GET") => Ok(Action::Db(DbAction::RelationRead {
            qi: qi(name),
            headers_only: false,
        })),
        (Resource::Relation(name), "POST") => Ok(Action::Db(DbAction::RelationMut {
            qi: qi(name),
            mutation: Mutation::MutationCreate,
        })),
        (Resource::Relation(name), "PUT") => Ok(Action::Db(DbAction::RelationMut {
            qi: qi(name),
            mutation: Mutation::MutationSingleUpsert,
        })),
        (Resource::Relation(name), "PATCH") => Ok(Action::Db(DbAction::RelationMut {
            qi: qi(name),
            mutation: Mutation::MutationUpdate,
        })),
        (Resource::Relation(name), "DELETE") => Ok(Action::Db(DbAction::RelationMut {
            qi: qi(name),
            mutation: Mutation::MutationDelete,
        })),
        (Resource::Relation(name), "OPTIONS") => Ok(Action::RelationInfo(qi(name))),

        // Schema
        (Resource::Schema, "HEAD") => Ok(Action::Db(DbAction::SchemaRead {
            schema: CompactString::from(schema),
            headers_only: true,
        })),
        (Resource::Schema, "GET") => Ok(Action::Db(DbAction::SchemaRead {
            schema: CompactString::from(schema),
            headers_only: false,
        })),
        (Resource::Schema, "OPTIONS") => Ok(Action::SchemaInfo),

        // Unsupported
        (_, method) => Err(Error::ParseError {
            location: "method".to_string(),
            message: format!("unsupported method: {}", method),
        }),
    }
}

// ==========================================================================
// Schema resolution
// ==========================================================================

/// Determine the request schema from profile headers.
fn get_schema(
    config: &AppConfig,
    headers: &[(String, String)],
    method: &str,
) -> Result<(CompactString, bool), Error> {
    let profile = match method {
        "DELETE" | "PATCH" | "POST" | "PUT" => find_header(headers, "content-profile"),
        _ => find_header(headers, "accept-profile"),
    };

    match profile {
        Some(p) => {
            if config.db_schemas.iter().any(|s| s == &p) {
                Ok((CompactString::from(p.as_str()), true))
            } else {
                Err(Error::ParseError {
                    location: "schema".to_string(),
                    message: format!(
                        "schema '{}' not in allowed schemas: {:?}",
                        p, config.db_schemas
                    ),
                })
            }
        }
        None => {
            let default = config
                .db_schemas
                .first()
                .map(|s| s.as_str())
                .unwrap_or("public");
            Ok((CompactString::from(default), false))
        }
    }
}

// ==========================================================================
// Range resolution
// ==========================================================================

/// Resolve ranges from Range header and query parameters.
fn get_ranges(
    method: &str,
    query_params: &QueryParams,
    headers: &[(String, String)],
) -> Result<(Range, std::collections::HashMap<CompactString, Range>), Error> {
    // Range header only applies to GET
    let header_range = if method == "GET" {
        find_header(headers, "range")
            .and_then(|v| range::parse_range_header(&v))
            .unwrap_or_else(Range::all)
    } else {
        Range::all()
    };

    let limit_range = query_params
        .ranges
        .get("limit")
        .copied()
        .unwrap_or_else(Range::all);

    let header_and_limit = header_range.intersect(&limit_range);

    let mut ranges = query_params.ranges.clone();
    ranges.insert(
        "limit".into(),
        limit_range.convert_to_limit_zero(&header_and_limit),
    );

    let top_level = ranges.get("limit").copied().unwrap_or_else(Range::all);

    // Validate range
    if top_level.is_empty_range() && !limit_range.has_limit_zero() {
        return Err(Error::InvalidRange("invalid range".to_string()));
    }

    if method == "PUT" && !top_level.is_all() {
        return Err(Error::InvalidRange(
            "PUT with limit/offset is not allowed".to_string(),
        ));
    }

    Ok((top_level, ranges))
}

// ==========================================================================
// Header helpers
// ==========================================================================

fn find_header(headers: &[(String, String)], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.clone())
}

fn get_content_type(headers: &[(String, String)]) -> MediaType {
    find_header(headers, "content-type")
        .map(|v| MediaType::parse(&v))
        .unwrap_or(MediaType::ApplicationJson)
}

fn get_accept_media_types(headers: &[(String, String)]) -> Vec<MediaType> {
    find_header(headers, "accept")
        .map(|v| {
            crate::types::media::parse_accept_header(&v)
                .into_iter()
                .map(|item| item.media_type)
                .collect()
        })
        .unwrap_or_else(|| vec![MediaType::Any])
}

/// Headers list: (name, value) pairs
type HeaderList = Vec<(String, String)>;

fn extract_headers_and_cookies(headers: &[(String, String)]) -> (HeaderList, HeaderList) {
    let mut req_headers = Vec::new();
    let mut cookies = Vec::new();

    for (name, value) in headers {
        let lower = name.to_lowercase();
        if lower == "cookie" {
            // Parse cookies
            for cookie in value.split(';') {
                let cookie = cookie.trim();
                if let Some((k, v)) = cookie.split_once('=') {
                    cookies.push((k.trim().to_string(), v.trim().to_string()));
                }
            }
        } else {
            req_headers.push((lower, value.clone()));
        }
    }

    (req_headers, cookies)
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AppConfig {
        let mut config = AppConfig::default();
        config.db_schemas = vec!["public".to_string(), "api".to_string()];
        config
    }

    // ---------- Resource resolution tests ----------

    #[test]
    fn test_get_resource_root() {
        let config = test_config();
        let resource = get_resource(&config, "/").unwrap();
        assert_eq!(resource, Resource::Schema);
    }

    #[test]
    fn test_get_resource_table() {
        let config = test_config();
        let resource = get_resource(&config, "/items").unwrap();
        assert_eq!(resource, Resource::Relation("items".into()));
    }

    #[test]
    fn test_get_resource_rpc() {
        let config = test_config();
        let resource = get_resource(&config, "/rpc/my_func").unwrap();
        assert_eq!(resource, Resource::Routine("my_func".into()));
    }

    #[test]
    fn test_get_resource_invalid() {
        let config = test_config();
        let result = get_resource(&config, "/a/b/c");
        assert!(result.is_err());
    }

    // ---------- Action resolution tests ----------

    #[test]
    fn test_get_action_get_table() {
        let action = get_action(&Resource::Relation("items".into()), "public", "GET").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::RelationRead {
                headers_only: false,
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_head_table() {
        let action = get_action(&Resource::Relation("items".into()), "public", "HEAD").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::RelationRead {
                headers_only: true,
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_post_table() {
        let action = get_action(&Resource::Relation("items".into()), "public", "POST").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::RelationMut {
                mutation: Mutation::MutationCreate,
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_put_table() {
        let action = get_action(&Resource::Relation("items".into()), "public", "PUT").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::RelationMut {
                mutation: Mutation::MutationSingleUpsert,
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_patch_table() {
        let action = get_action(&Resource::Relation("items".into()), "public", "PATCH").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::RelationMut {
                mutation: Mutation::MutationUpdate,
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_delete_table() {
        let action = get_action(&Resource::Relation("items".into()), "public", "DELETE").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::RelationMut {
                mutation: Mutation::MutationDelete,
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_options_table() {
        let action = get_action(&Resource::Relation("items".into()), "public", "OPTIONS").unwrap();
        assert!(matches!(action, Action::RelationInfo(_)));
    }

    #[test]
    fn test_get_action_get_rpc() {
        let action = get_action(&Resource::Routine("func".into()), "public", "GET").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::Routine {
                inv_method: InvokeMethod::InvRead(false),
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_post_rpc() {
        let action = get_action(&Resource::Routine("func".into()), "public", "POST").unwrap();
        assert!(matches!(
            action,
            Action::Db(DbAction::Routine {
                inv_method: InvokeMethod::Inv,
                ..
            })
        ));
    }

    #[test]
    fn test_get_action_schema_get() {
        let action = get_action(&Resource::Schema, "public", "GET").unwrap();
        assert!(matches!(action, Action::Db(DbAction::SchemaRead { .. })));
    }

    #[test]
    fn test_get_action_schema_options() {
        let action = get_action(&Resource::Schema, "public", "OPTIONS").unwrap();
        assert!(matches!(action, Action::SchemaInfo));
    }

    #[test]
    fn test_get_action_unsupported() {
        let result = get_action(&Resource::Schema, "public", "TRACE");
        assert!(result.is_err());
    }

    // ---------- Schema resolution tests ----------

    #[test]
    fn test_get_schema_default() {
        let config = test_config();
        let headers: Vec<(String, String)> = vec![];
        let (schema, negotiated) = get_schema(&config, &headers, "GET").unwrap();
        assert_eq!(schema.as_str(), "public");
        assert!(!negotiated); // no profile header used
    }

    #[test]
    fn test_get_schema_accept_profile() {
        let config = test_config();
        let headers = vec![("accept-profile".to_string(), "api".to_string())];
        let (schema, negotiated) = get_schema(&config, &headers, "GET").unwrap();
        assert_eq!(schema.as_str(), "api");
        assert!(negotiated);
    }

    #[test]
    fn test_get_schema_content_profile_for_post() {
        let config = test_config();
        let headers = vec![("content-profile".to_string(), "api".to_string())];
        let (schema, negotiated) = get_schema(&config, &headers, "POST").unwrap();
        assert_eq!(schema.as_str(), "api");
        assert!(negotiated);
    }

    #[test]
    fn test_get_schema_invalid() {
        let config = test_config();
        let headers = vec![("accept-profile".to_string(), "nonexistent".to_string())];
        let result = get_schema(&config, &headers, "GET");
        assert!(result.is_err());
    }

    // ---------- Range tests ----------

    #[test]
    fn test_get_ranges_default() {
        let qp = QueryParams::default();
        let headers: Vec<(String, String)> = vec![];
        let (top, _) = get_ranges("GET", &qp, &headers).unwrap();
        assert!(top.is_all());
    }

    #[test]
    fn test_get_ranges_with_header() {
        let qp = QueryParams::default();
        let headers = vec![("range".to_string(), "items=0-24".to_string())];
        let (top, _) = get_ranges("GET", &qp, &headers).unwrap();
        assert_eq!(top.offset, 0);
        assert_eq!(top.limit_to, Some(24));
    }

    #[test]
    fn test_get_ranges_header_ignored_for_post() {
        let qp = QueryParams::default();
        let headers = vec![("range".to_string(), "items=0-24".to_string())];
        let (top, _) = get_ranges("POST", &qp, &headers).unwrap();
        assert!(top.is_all()); // Range header ignored for non-GET
    }

    // ---------- Header helper tests ----------

    #[test]
    fn test_find_header() {
        let headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Accept".to_string(), "text/csv".to_string()),
        ];
        assert_eq!(
            find_header(&headers, "content-type").as_deref(),
            Some("application/json")
        );
        assert_eq!(find_header(&headers, "accept").as_deref(), Some("text/csv"));
        assert!(find_header(&headers, "nonexistent").is_none());
    }

    #[test]
    fn test_get_content_type() {
        let headers = vec![("content-type".to_string(), "text/csv".to_string())];
        assert_eq!(get_content_type(&headers), MediaType::TextCsv);

        let empty: Vec<(String, String)> = vec![];
        assert_eq!(get_content_type(&empty), MediaType::ApplicationJson);
    }

    #[test]
    fn test_get_accept_media_types() {
        let headers = vec![(
            "accept".to_string(),
            "text/csv, application/json;q=0.5".to_string(),
        )];
        let types = get_accept_media_types(&headers);
        assert_eq!(types.len(), 2);
        // Sorted by quality
        assert_eq!(types[0], MediaType::TextCsv);
        assert_eq!(types[1], MediaType::ApplicationJson);
    }

    #[test]
    fn test_extract_headers_and_cookies() {
        let headers = vec![
            ("Content-Type".to_string(), "application/json".to_string()),
            ("Cookie".to_string(), "session=abc123; lang=en".to_string()),
            ("X-Custom".to_string(), "value".to_string()),
        ];
        let (hdrs, cookies) = extract_headers_and_cookies(&headers);
        assert_eq!(hdrs.len(), 2);
        assert_eq!(cookies.len(), 2);
        assert_eq!(cookies[0].0, "session");
        assert_eq!(cookies[0].1, "abc123");
    }

    // ---------- Full from_request tests ----------

    #[test]
    fn test_from_request_get() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers = vec![("accept".to_string(), "application/json".to_string())];
        let body = Bytes::new();

        let req = from_request(
            &config,
            &prefs,
            "GET",
            "/items",
            "select=id,name",
            &headers,
            body,
        )
        .unwrap();

        assert!(matches!(
            req.action,
            Action::Db(DbAction::RelationRead { .. })
        ));
        assert_eq!(req.query_params.select.len(), 2);
        assert_eq!(req.schema.as_str(), "public");
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/items");
    }

    #[test]
    fn test_from_request_post() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers = vec![("content-type".to_string(), "application/json".to_string())];
        let body = Bytes::from(r#"{"id":1,"name":"test"}"#);

        let req = from_request(&config, &prefs, "POST", "/items", "", &headers, body).unwrap();

        assert!(matches!(
            req.action,
            Action::Db(DbAction::RelationMut {
                mutation: Mutation::MutationCreate,
                ..
            })
        ));
        assert!(req.payload.is_some());
        assert_eq!(req.columns.len(), 2);
    }

    #[test]
    fn test_from_request_rpc_get() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers: Vec<(String, String)> = vec![];
        let body = Bytes::new();

        let req = from_request(
            &config,
            &prefs,
            "GET",
            "/rpc/my_func",
            "id=5",
            &headers,
            body,
        )
        .unwrap();

        assert!(matches!(
            req.action,
            Action::Db(DbAction::Routine {
                inv_method: InvokeMethod::InvRead(false),
                ..
            })
        ));
        // For RPC GET, params without operators become rpc params
        assert_eq!(req.query_params.params.len(), 1);
    }

    #[test]
    fn test_from_request_schema() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers: Vec<(String, String)> = vec![];
        let body = Bytes::new();

        let req = from_request(&config, &prefs, "GET", "/", "", &headers, body).unwrap();

        assert!(matches!(
            req.action,
            Action::Db(DbAction::SchemaRead { .. })
        ));
    }

    #[test]
    fn test_from_request_with_profile() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers = vec![("accept-profile".to_string(), "api".to_string())];
        let body = Bytes::new();

        let req = from_request(&config, &prefs, "GET", "/items", "", &headers, body).unwrap();

        assert_eq!(req.schema.as_str(), "api");
        assert!(req.negotiated_by_profile);
    }

    #[test]
    fn test_from_request_with_range() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers = vec![("range".to_string(), "items=0-24".to_string())];
        let body = Bytes::new();

        let req = from_request(&config, &prefs, "GET", "/items", "", &headers, body).unwrap();

        assert_eq!(req.top_level_range.offset, 0);
        assert_eq!(req.top_level_range.limit_to, Some(24));
    }

    #[test]
    fn test_from_request_with_filters() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers: Vec<(String, String)> = vec![];
        let body = Bytes::new();

        let req = from_request(
            &config,
            &prefs,
            "GET",
            "/items",
            "id=eq.5&name=like.*john*",
            &headers,
            body,
        )
        .unwrap();

        assert_eq!(req.query_params.filters_root.len(), 2);
    }

    #[test]
    fn test_from_request_delete() {
        let config = test_config();
        let prefs = Preferences::default();
        let headers: Vec<(String, String)> = vec![];
        let body = Bytes::new();

        let req = from_request(
            &config, &prefs, "DELETE", "/items", "id=eq.1", &headers, body,
        )
        .unwrap();

        assert!(matches!(
            req.action,
            Action::Db(DbAction::RelationMut {
                mutation: Mutation::MutationDelete,
                ..
            })
        ));
        assert!(req.payload.is_none()); // DELETE doesn't parse payload
    }
}
