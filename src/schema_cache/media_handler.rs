//! Media handler types for PgREST content negotiation
//!
//! Media handlers define how database results are aggregated and formatted
//! for different content types (JSON, CSV, binary, etc.).

use std::collections::HashMap;

use crate::types::identifiers::{QualifiedIdentifier, RelIdentifier};
use crate::types::media::MediaType;

/// Handler for aggregating/formatting database results into a specific media type
///
/// Matches the Haskell `MediaHandler` data type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaHandler {
    /// Built-in: aggregate into a single JSON object.
    /// The bool indicates whether to strip the array wrapper.
    BuiltinAggSingleJson(bool),
    /// Built-in: aggregate into a JSON array with stripping
    BuiltinAggArrayJsonStrip,
    /// Built-in overridable: JSON aggregation
    BuiltinOvAggJson,
    /// Built-in overridable: CSV aggregation
    BuiltinOvAggCsv,
    /// Built-in overridable: GeoJSON aggregation
    BuiltinOvAggGeoJson,
    /// Custom aggregate function
    CustomFunc(QualifiedIdentifier, RelIdentifier),
    /// No aggregation needed
    NoAgg,
}

impl MediaHandler {
    /// Check if this is a built-in handler
    pub fn is_builtin(&self) -> bool {
        !matches!(self, MediaHandler::CustomFunc(_, _) | MediaHandler::NoAgg)
    }

    /// Check if this is a custom function handler
    pub fn is_custom(&self) -> bool {
        matches!(self, MediaHandler::CustomFunc(_, _))
    }
}

/// A resolved handler: the media handler paired with its media type
pub type ResolvedHandler = (MediaHandler, MediaType);

/// Map from (relation_identifier, media_type) to the resolved handler
pub type MediaHandlerMap = HashMap<(RelIdentifier, MediaType), ResolvedHandler>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_media_handler_builtin() {
        assert!(MediaHandler::BuiltinOvAggJson.is_builtin());
        assert!(MediaHandler::BuiltinOvAggCsv.is_builtin());
        assert!(MediaHandler::BuiltinAggSingleJson(true).is_builtin());
        assert!(MediaHandler::BuiltinAggArrayJsonStrip.is_builtin());
        assert!(!MediaHandler::NoAgg.is_builtin());
    }

    #[test]
    fn test_media_handler_custom() {
        let custom = MediaHandler::CustomFunc(
            QualifiedIdentifier::new("public", "to_geojson"),
            RelIdentifier::any_element(),
        );
        assert!(custom.is_custom());
        assert!(!custom.is_builtin());
    }

    #[test]
    fn test_media_handler_map() {
        let mut map: MediaHandlerMap = HashMap::new();
        let key = (
            RelIdentifier::any_element(),
            MediaType::ApplicationJson,
        );
        let handler = (MediaHandler::BuiltinOvAggJson, MediaType::ApplicationJson);
        map.insert(key.clone(), handler);

        assert!(map.contains_key(&key));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_media_handler_equality() {
        assert_eq!(MediaHandler::BuiltinOvAggJson, MediaHandler::BuiltinOvAggJson);
        assert_ne!(MediaHandler::BuiltinOvAggJson, MediaHandler::BuiltinOvAggCsv);
        assert_eq!(
            MediaHandler::BuiltinAggSingleJson(true),
            MediaHandler::BuiltinAggSingleJson(true)
        );
        assert_ne!(
            MediaHandler::BuiltinAggSingleJson(true),
            MediaHandler::BuiltinAggSingleJson(false)
        );
    }
}
