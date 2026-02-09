//! Request body (payload) parsing
//!
//! Handles HTTP request payload parsing and validation.
//! Parses JSON, URL-encoded, and raw payloads based on content type and action.

use bytes::Bytes;
use compact_str::CompactString;
use std::collections::HashSet;

use crate::error::Error;
use crate::types::media::MediaType;

use super::query_params::QueryParams;
use super::types::{Action, DbAction, InvokeMethod, Mutation, Payload};

/// Parse the request body into a Payload based on content type and action.
///
/// Parse and validate the request payload.
///
/// Returns: `(Option<Payload>, columns)` where columns is the set of column names
/// derived from either the payload keys or the &columns parameter.
pub fn get_payload(
    body: Bytes,
    content_type: &MediaType,
    query_params: &QueryParams,
    action: &Action,
) -> Result<(Option<Payload>, HashSet<CompactString>), Error> {
    if !should_parse_payload(action) {
        return Ok((None, HashSet::new()));
    }

    let is_proc = is_procedure(action);
    let columns_param = &query_params.columns;

    let payload = parse_payload(&body, content_type, is_proc, columns_param)?;

    let cols = match (&payload, get_action_columns(action, &query_params.columns)) {
        (Some(Payload::ProcessedJSON { keys, .. }), _) => keys.clone(),
        (Some(Payload::ProcessedUrlEncoded { keys, .. }), _) => keys.clone(),
        (Some(Payload::RawJSON(_)), Some(cls)) => cls.clone(),
        _ => HashSet::new(),
    };

    Ok((payload, cols))
}

fn should_parse_payload(action: &Action) -> bool {
    matches!(
        action,
        Action::Db(DbAction::RelationMut {
            mutation: Mutation::MutationCreate | Mutation::MutationUpdate | Mutation::MutationSingleUpsert,
            ..
        }) | Action::Db(DbAction::Routine {
            inv_method: InvokeMethod::Inv,
            ..
        })
    )
}

fn is_procedure(action: &Action) -> bool {
    matches!(action, Action::Db(DbAction::Routine { .. }))
}

fn get_action_columns<'a>(
    action: &Action,
    columns: &'a Option<HashSet<CompactString>>,
) -> Option<&'a HashSet<CompactString>> {
    match action {
        Action::Db(DbAction::RelationMut {
            mutation: Mutation::MutationCreate | Mutation::MutationUpdate,
            ..
        })
        | Action::Db(DbAction::Routine {
            inv_method: InvokeMethod::Inv,
            ..
        }) => columns.as_ref(),
        _ => None,
    }
}

fn parse_payload(
    body: &Bytes,
    content_type: &MediaType,
    is_proc: bool,
    columns_param: &Option<HashSet<CompactString>>,
) -> Result<Option<Payload>, Error> {
    match (content_type, is_proc) {
        (MediaType::ApplicationJson, _) => {
            if columns_param.is_some() {
                // When &columns is specified, pass raw JSON through
                Ok(Some(Payload::RawJSON(body.clone())))
            } else {
                parse_json_payload(body, is_proc)
            }
        }
        (MediaType::ApplicationFormUrlEncoded, true) => {
            // URL-encoded for RPC
            let params: Vec<(CompactString, CompactString)> = form_urlencoded::parse(body)
                .map(|(k, v)| (CompactString::from(k.as_ref()), CompactString::from(v.as_ref())))
                .collect();
            let keys: HashSet<CompactString> = params.iter().map(|(k, _)| k.clone()).collect();
            Ok(Some(Payload::ProcessedUrlEncoded { params, keys }))
        }
        (MediaType::ApplicationFormUrlEncoded, false) => {
            // URL-encoded for non-RPC: convert to JSON-like structure
            let params: Vec<(CompactString, CompactString)> = form_urlencoded::parse(body)
                .map(|(k, v)| (CompactString::from(k.as_ref()), CompactString::from(v.as_ref())))
                .collect();
            let keys: HashSet<CompactString> = params.iter().map(|(k, _)| k.clone()).collect();
            // Build JSON from params
            let json_map: serde_json::Map<String, serde_json::Value> = params
                .iter()
                .map(|(k, v)| (k.to_string(), serde_json::Value::String(v.to_string())))
                .collect();
            let raw = serde_json::to_vec(&json_map)
                .map_err(|e| Error::InvalidBody(e.to_string()))?;
            Ok(Some(Payload::ProcessedJSON {
                raw: Bytes::from(raw),
                keys,
            }))
        }
        (MediaType::TextPlain, true) | (MediaType::ApplicationXml, true) | (MediaType::ApplicationOctetStream, true) => {
            Ok(Some(Payload::RawPayload(body.clone())))
        }
        (ct, _) => Err(Error::InvalidContentType(format!(
            "Content-Type not acceptable: {}",
            ct
        ))),
    }
}

fn parse_json_payload(body: &Bytes, is_proc: bool) -> Result<Option<Payload>, Error> {
    if body.is_empty() && is_proc {
        // Empty body for RPC is treated as empty object
        let keys = HashSet::new();
        return Ok(Some(Payload::ProcessedJSON {
            raw: Bytes::from_static(b"{}"),
            keys,
        }));
    }

    if body.is_empty() {
        return Err(Error::InvalidBody("Empty or invalid json".to_string()));
    }

    let parsed: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| Error::InvalidBody("Empty or invalid json".to_string()))?;

    match &parsed {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                return Ok(Some(Payload::ProcessedJSON {
                    raw: Bytes::from_static(b"[]"),
                    keys: HashSet::new(),
                }));
            }

            // Check that all objects have the same keys
            if let Some(serde_json::Value::Object(first)) = arr.first() {
                let canonical_keys: HashSet<CompactString> = first
                    .keys()
                    .map(|k| CompactString::from(k.as_str()))
                    .collect();

                let uniform = arr.iter().all(|item| {
                    if let serde_json::Value::Object(obj) = item {
                        let item_keys: HashSet<CompactString> = obj
                            .keys()
                            .map(|k| CompactString::from(k.as_str()))
                            .collect();
                        item_keys == canonical_keys
                    } else {
                        false
                    }
                });

                if uniform {
                    Ok(Some(Payload::ProcessedJSON {
                        raw: body.clone(),
                        keys: canonical_keys,
                    }))
                } else {
                    Err(Error::InvalidBody(
                        "All object keys must match".to_string(),
                    ))
                }
            } else {
                Err(Error::InvalidBody(
                    "All object keys must match".to_string(),
                ))
            }
        }
        serde_json::Value::Object(obj) => {
            let keys: HashSet<CompactString> = obj
                .keys()
                .map(|k| CompactString::from(k.as_str()))
                .collect();
            Ok(Some(Payload::ProcessedJSON {
                raw: body.clone(),
                keys,
            }))
        }
        _ => {
            // Non-object, non-array: treat as empty array
            Ok(Some(Payload::ProcessedJSON {
                raw: Bytes::from_static(b"[]"),
                keys: HashSet::new(),
            }))
        }
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::identifiers::QualifiedIdentifier;

    fn create_action() -> Action {
        Action::Db(DbAction::RelationMut {
            qi: QualifiedIdentifier::new("public", "items"),
            mutation: Mutation::MutationCreate,
        })
    }

    fn rpc_action() -> Action {
        Action::Db(DbAction::Routine {
            qi: QualifiedIdentifier::new("public", "my_func"),
            inv_method: InvokeMethod::Inv,
        })
    }

    fn read_action() -> Action {
        Action::Db(DbAction::RelationRead {
            qi: QualifiedIdentifier::new("public", "items"),
            headers_only: false,
        })
    }

    fn default_qp() -> QueryParams {
        QueryParams::default()
    }

    #[test]
    fn test_json_object_payload() {
        let body = Bytes::from(r#"{"id":1,"name":"test"}"#);
        let qp = default_qp();
        let (payload, cols) = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action()).unwrap();
        let payload = payload.unwrap();
        assert_eq!(cols.len(), 2);
        assert!(cols.contains("id"));
        assert!(cols.contains("name"));
        assert!(matches!(payload, Payload::ProcessedJSON { .. }));
    }

    #[test]
    fn test_json_array_payload() {
        let body = Bytes::from(r#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#);
        let qp = default_qp();
        let (payload, cols) = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action()).unwrap();
        let payload = payload.unwrap();
        assert_eq!(cols.len(), 2);
        assert!(matches!(payload, Payload::ProcessedJSON { .. }));
    }

    #[test]
    fn test_json_array_non_uniform_keys() {
        let body = Bytes::from(r#"[{"id":1},{"name":"b"}]"#);
        let qp = default_qp();
        let result = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action());
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_json_for_rpc() {
        let body = Bytes::new();
        let qp = default_qp();
        let (payload, _) = get_payload(body, &MediaType::ApplicationJson, &qp, &rpc_action()).unwrap();
        assert!(payload.is_some());
    }

    #[test]
    fn test_empty_json_non_rpc_error() {
        let body = Bytes::new();
        let qp = default_qp();
        let result = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action());
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_json() {
        let body = Bytes::from("not json");
        let qp = default_qp();
        let result = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action());
        assert!(result.is_err());
    }

    #[test]
    fn test_url_encoded_rpc() {
        let body = Bytes::from("id=1&name=test");
        let qp = default_qp();
        let (payload, cols) = get_payload(body, &MediaType::ApplicationFormUrlEncoded, &qp, &rpc_action()).unwrap();
        let payload = payload.unwrap();
        assert_eq!(cols.len(), 2);
        assert!(matches!(payload, Payload::ProcessedUrlEncoded { .. }));
    }

    #[test]
    fn test_url_encoded_non_rpc() {
        let body = Bytes::from("id=1&name=test");
        let qp = default_qp();
        let (payload, cols) = get_payload(body, &MediaType::ApplicationFormUrlEncoded, &qp, &create_action()).unwrap();
        let payload = payload.unwrap();
        assert_eq!(cols.len(), 2);
        assert!(matches!(payload, Payload::ProcessedJSON { .. }));
    }

    #[test]
    fn test_raw_payload_rpc() {
        let body = Bytes::from("raw text content");
        let qp = default_qp();
        let (payload, _) = get_payload(body, &MediaType::TextPlain, &qp, &rpc_action()).unwrap();
        assert!(matches!(payload.unwrap(), Payload::RawPayload(_)));
    }

    #[test]
    fn test_octet_stream_rpc() {
        let body = Bytes::from(vec![0u8, 1, 2, 3]);
        let qp = default_qp();
        let (payload, _) = get_payload(body, &MediaType::ApplicationOctetStream, &qp, &rpc_action()).unwrap();
        assert!(matches!(payload.unwrap(), Payload::RawPayload(_)));
    }

    #[test]
    fn test_unsupported_content_type() {
        let body = Bytes::from("data");
        let qp = default_qp();
        let result = get_payload(body, &MediaType::TextCsv, &qp, &create_action());
        assert!(result.is_err());
    }

    #[test]
    fn test_no_payload_for_read() {
        let body = Bytes::from("data");
        let qp = default_qp();
        let (payload, cols) = get_payload(body, &MediaType::ApplicationJson, &qp, &read_action()).unwrap();
        assert!(payload.is_none());
        assert!(cols.is_empty());
    }

    #[test]
    fn test_raw_json_with_columns() {
        let body = Bytes::from(r#"{"id":1,"name":"test"}"#);
        let mut qp = default_qp();
        let mut cols_set = HashSet::new();
        cols_set.insert(CompactString::from("id"));
        cols_set.insert(CompactString::from("name"));
        qp.columns = Some(cols_set.clone());

        let (payload, cols) = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action()).unwrap();
        assert!(matches!(payload.unwrap(), Payload::RawJSON(_)));
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn test_empty_json_array() {
        let body = Bytes::from("[]");
        let qp = default_qp();
        let (payload, cols) = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action()).unwrap();
        assert!(payload.is_some());
        assert!(cols.is_empty());
    }

    #[test]
    fn test_payload_keys() {
        let body = Bytes::from(r#"{"a":1,"b":2,"c":3}"#);
        let qp = default_qp();
        let (payload, cols) = get_payload(body, &MediaType::ApplicationJson, &qp, &create_action()).unwrap();
        let payload = payload.unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(payload.keys().len(), 3);
    }
}
