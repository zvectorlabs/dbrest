//! Content negotiation for dbrest
//!
//! Resolves accepted media types against available media handlers.
//! Matches the Haskell `Plan.Negotiate` module.

use crate::api_request::types::Action;
use crate::error::Error;
use crate::schema_cache::media_handler::{MediaHandler, MediaHandlerMap, ResolvedHandler};
use crate::types::identifiers::RelIdentifier;
use crate::types::media::MediaType;

// ==========================================================================
// Content negotiation
// ==========================================================================

/// Negotiate the content type for a response
///
/// Given the accepted media types (from the Accept header, sorted by quality),
/// the available media handlers, and the relation identifier, resolve the
/// handler and final media type.
///
/// Matches the Haskell `negotiateContent` function.
pub fn negotiate_content(
    accepted: &[MediaType],
    handlers: &MediaHandlerMap,
    rel_id: &RelIdentifier,
    action: &Action,
    plan_enabled: bool,
) -> Result<ResolvedHandler, Error> {
    // If the action is an info request (OPTIONS), return OpenAPI
    if matches!(
        action,
        Action::RelationInfo(_) | Action::RoutineInfo(_, _) | Action::SchemaInfo
    ) {
        return Ok((MediaHandler::NoAgg, MediaType::ApplicationOpenApi));
    }

    // Try to find a handler for each accepted media type (in quality order)
    for media_type in accepted {
        // Check for EXPLAIN (plan) output
        if plan_enabled && is_plan_media_type(media_type) {
            return Ok((MediaHandler::NoAgg, media_type.clone()));
        }

        // Try exact match first
        let key = (rel_id.clone(), media_type.clone());
        if let Some(handler) = handlers.get(&key) {
            return Ok(handler.clone());
        }

        // Try with AnyElement as fallback
        let any_key = (RelIdentifier::any_element(), media_type.clone());
        if let Some(handler) = handlers.get(&any_key) {
            return Ok(handler.clone());
        }

        // Handle wildcard (*/*) — return default JSON handler
        if *media_type == MediaType::Any {
            return Ok(default_json_handler());
        }

        // For well-known types, return built-in handlers
        if let Some(handler) = builtin_handler_for(media_type) {
            return Ok(handler);
        }
    }

    // If nothing matched but we have accepted types, try default
    if accepted.is_empty() {
        return Ok(default_json_handler());
    }

    // No acceptable media type found
    Err(Error::InvalidContentType(format!(
        "None of the accepted media types are available: {:?}",
        accepted.iter().map(|m| m.as_str()).collect::<Vec<_>>()
    )))
}

/// Check if a media type is for EXPLAIN output
fn is_plan_media_type(media_type: &MediaType) -> bool {
    matches!(media_type, MediaType::ApplicationOpenApi)
        || media_type.as_str().contains("vnd.dbrst.plan")
}

/// Get the default JSON handler
fn default_json_handler() -> ResolvedHandler {
    (MediaHandler::BuiltinOvAggJson, MediaType::ApplicationJson)
}

/// Get a built-in handler for well-known media types
fn builtin_handler_for(media_type: &MediaType) -> Option<ResolvedHandler> {
    match media_type {
        MediaType::ApplicationJson => {
            Some((MediaHandler::BuiltinOvAggJson, MediaType::ApplicationJson))
        }
        MediaType::ApplicationVndDbrstObject => Some((
            MediaHandler::BuiltinAggSingleJson(true),
            MediaType::ApplicationVndDbrstObject,
        )),
        MediaType::ApplicationVndDbrstArray => Some((
            MediaHandler::BuiltinAggArrayJsonStrip,
            MediaType::ApplicationVndDbrstArray,
        )),
        MediaType::TextCsv => Some((MediaHandler::BuiltinOvAggCsv, MediaType::TextCsv)),
        MediaType::ApplicationOctetStream => {
            Some((MediaHandler::NoAgg, MediaType::ApplicationOctetStream))
        }
        MediaType::TextPlain => Some((MediaHandler::NoAgg, MediaType::TextPlain)),
        _ => None,
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api_request::types::DbAction;
    use std::collections::HashMap;

    fn read_action() -> Action {
        Action::Db(DbAction::RelationRead {
            qi: crate::types::identifiers::QualifiedIdentifier::new("public", "items"),
            headers_only: false,
        })
    }

    fn empty_handlers() -> MediaHandlerMap {
        HashMap::new()
    }

    #[test]
    fn test_negotiate_json_default() {
        let accepted = vec![MediaType::ApplicationJson];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, MediaType::ApplicationJson);
        assert!(matches!(handler, MediaHandler::BuiltinOvAggJson));
    }

    #[test]
    fn test_negotiate_wildcard() {
        let accepted = vec![MediaType::Any];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, MediaType::ApplicationJson);
        assert!(matches!(handler, MediaHandler::BuiltinOvAggJson));
    }

    #[test]
    fn test_negotiate_csv() {
        let accepted = vec![MediaType::TextCsv];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, MediaType::TextCsv);
        assert!(matches!(handler, MediaHandler::BuiltinOvAggCsv));
    }

    #[test]
    fn test_negotiate_singular_object() {
        let accepted = vec![MediaType::ApplicationVndDbrstObject];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, MediaType::ApplicationVndDbrstObject);
        assert!(matches!(handler, MediaHandler::BuiltinAggSingleJson(true)));
    }

    #[test]
    fn test_negotiate_octet_stream() {
        let accepted = vec![MediaType::ApplicationOctetStream];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, MediaType::ApplicationOctetStream);
        assert!(matches!(handler, MediaHandler::NoAgg));
    }

    #[test]
    fn test_negotiate_info_action() {
        let accepted = vec![MediaType::ApplicationJson];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();
        let action = Action::SchemaInfo;

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &action, false).unwrap();
        assert_eq!(media, MediaType::ApplicationOpenApi);
        assert!(matches!(handler, MediaHandler::NoAgg));
    }

    #[test]
    fn test_negotiate_custom_handler() {
        let mut handlers: MediaHandlerMap = HashMap::new();
        let qi = crate::types::identifiers::QualifiedIdentifier::new("public", "items");
        let rel_id = RelIdentifier::Table(qi.clone());
        let custom_type = MediaType::Other(crate::types::media::OtherMediaType {
            full: "application/geo+json".to_string(),
            type_: "application".to_string(),
            subtype: "geo+json".to_string(),
        });

        handlers.insert(
            (rel_id.clone(), custom_type.clone()),
            (
                MediaHandler::CustomFunc(
                    crate::types::identifiers::QualifiedIdentifier::new("public", "to_geojson"),
                    rel_id.clone(),
                ),
                custom_type.clone(),
            ),
        );

        let accepted = vec![custom_type.clone()];
        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, custom_type);
        assert!(matches!(handler, MediaHandler::CustomFunc(_, _)));
    }

    #[test]
    fn test_negotiate_empty_accepted() {
        let accepted: Vec<MediaType> = vec![];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let (_, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, MediaType::ApplicationJson);
    }

    #[test]
    fn test_negotiate_unsupported_type() {
        let unknown_type = MediaType::Other(crate::types::media::OtherMediaType {
            full: "application/x-custom".to_string(),
            type_: "application".to_string(),
            subtype: "x-custom".to_string(),
        });
        let accepted = vec![unknown_type];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let result = negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false);
        assert!(result.is_err());
    }

    #[test]
    fn test_negotiate_quality_order() {
        // CSV first in quality, then JSON
        let accepted = vec![MediaType::TextCsv, MediaType::ApplicationJson];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::any_element();

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        // Should pick CSV since it's first (highest quality)
        assert_eq!(media, MediaType::TextCsv);
        assert!(matches!(handler, MediaHandler::BuiltinOvAggCsv));
    }

    #[test]
    fn test_negotiate_with_profile_schema() {
        let action = Action::Db(DbAction::RelationRead {
            qi: crate::types::identifiers::QualifiedIdentifier::new("api", "items"),
            headers_only: false,
        });
        let accepted = vec![MediaType::ApplicationJson];
        let handlers = empty_handlers();
        let rel_id = RelIdentifier::Table(crate::types::identifiers::QualifiedIdentifier::new(
            "api", "items",
        ));

        let (_, media) = negotiate_content(&accepted, &handlers, &rel_id, &action, false).unwrap();
        assert_eq!(media, MediaType::ApplicationJson);
    }

    #[test]
    fn test_negotiate_routine_info() {
        let action = Action::RoutineInfo(
            crate::types::identifiers::QualifiedIdentifier::new("public", "my_func"),
            crate::api_request::types::InvokeMethod::InvRead(true),
        );
        let accepted = vec![MediaType::ApplicationJson];
        let (_, media) = negotiate_content(
            &accepted,
            &empty_handlers(),
            &RelIdentifier::any_element(),
            &action,
            false,
        )
        .unwrap();
        assert_eq!(media, MediaType::ApplicationOpenApi);
    }

    #[test]
    fn test_negotiate_fallback_to_any_element() {
        let mut handlers: MediaHandlerMap = HashMap::new();
        // Register a handler for AnyElement + CSV
        handlers.insert(
            (RelIdentifier::any_element(), MediaType::TextCsv),
            (MediaHandler::BuiltinOvAggCsv, MediaType::TextCsv),
        );

        // Request as a specific table
        let rel_id = RelIdentifier::Table(crate::types::identifiers::QualifiedIdentifier::new(
            "public", "items",
        ));
        let accepted = vec![MediaType::TextCsv];

        let (handler, media) =
            negotiate_content(&accepted, &handlers, &rel_id, &read_action(), false).unwrap();
        assert_eq!(media, MediaType::TextCsv);
        assert!(matches!(handler, MediaHandler::BuiltinOvAggCsv));
    }
}
