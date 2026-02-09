//! Media types for content negotiation
//!
//! Handles HTTP content types for request/response processing.

use std::fmt;

/// Supported media types
///
/// Supported media types for content negotiation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum MediaType {
    /// application/json (default)
    #[default]
    ApplicationJson,
    /// application/vnd.pgrst.object+json (single object)
    ApplicationVndPgrstObject,
    /// application/vnd.pgrst.array+json (array of objects)
    ApplicationVndPgrstArray,
    /// text/csv
    TextCsv,
    /// text/plain
    TextPlain,
    /// application/octet-stream (binary)
    ApplicationOctetStream,
    /// application/x-www-form-urlencoded
    ApplicationFormUrlEncoded,
    /// text/xml / application/xml
    ApplicationXml,
    /// application/openapi+json
    ApplicationOpenApi,
    /// */* (any type)
    Any,
    /// Other/unknown media type
    Other(OtherMediaType),
}

/// Storage for unknown media types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OtherMediaType {
    /// Full media type string
    pub full: String,
    /// Type part (e.g., "application")
    pub type_: String,
    /// Subtype part (e.g., "json")
    pub subtype: String,
}

impl MediaType {
    /// Parse a media type string.
    ///
    /// # Examples
    ///
    /// ```
    /// use pgrest::types::media::MediaType;
    ///
    /// assert_eq!(MediaType::parse("application/json"), MediaType::ApplicationJson);
    /// assert_eq!(MediaType::parse("text/csv"), MediaType::TextCsv);
    /// assert_eq!(MediaType::parse("*/*"), MediaType::Any);
    /// ```
    pub fn parse(input: &str) -> Self {
        // Strip parameters (e.g., "application/json; charset=utf-8")
        let media_type = input
            .split(';')
            .next()
            .unwrap_or(input)
            .trim()
            .to_lowercase();

        match media_type.as_str() {
            "application/json" => MediaType::ApplicationJson,
            "application/vnd.pgrst.object+json" => MediaType::ApplicationVndPgrstObject,
            "application/vnd.pgrst.array+json" => MediaType::ApplicationVndPgrstArray,
            "text/csv" => MediaType::TextCsv,
            "text/plain" => MediaType::TextPlain,
            "application/octet-stream" => MediaType::ApplicationOctetStream,
            "application/x-www-form-urlencoded" => MediaType::ApplicationFormUrlEncoded,
            "text/xml" | "application/xml" => MediaType::ApplicationXml,
            "application/openapi+json" => MediaType::ApplicationOpenApi,
            "*/*" => MediaType::Any,
            other => {
                if let Some((type_, subtype)) = other.split_once('/') {
                    MediaType::Other(OtherMediaType {
                        full: other.to_string(),
                        type_: type_.to_string(),
                        subtype: subtype.to_string(),
                    })
                } else {
                    MediaType::Other(OtherMediaType {
                        full: other.to_string(),
                        type_: other.to_string(),
                        subtype: String::new(),
                    })
                }
            }
        }
    }

    /// Get the MIME type string for this media type.
    pub fn as_str(&self) -> &str {
        match self {
            MediaType::ApplicationJson => "application/json",
            MediaType::ApplicationVndPgrstObject => "application/vnd.pgrst.object+json",
            MediaType::ApplicationVndPgrstArray => "application/vnd.pgrst.array+json",
            MediaType::TextCsv => "text/csv",
            MediaType::TextPlain => "text/plain",
            MediaType::ApplicationOctetStream => "application/octet-stream",
            MediaType::ApplicationFormUrlEncoded => "application/x-www-form-urlencoded",
            MediaType::ApplicationXml => "application/xml",
            MediaType::ApplicationOpenApi => "application/openapi+json",
            MediaType::Any => "*/*",
            MediaType::Other(o) => &o.full,
        }
    }

    /// Check if this is a JSON-based media type.
    pub fn is_json(&self) -> bool {
        matches!(
            self,
            MediaType::ApplicationJson
                | MediaType::ApplicationVndPgrstObject
                | MediaType::ApplicationVndPgrstArray
                | MediaType::ApplicationOpenApi
        )
    }

    /// Check if this media type expects a single object response.
    pub fn is_singular(&self) -> bool {
        matches!(self, MediaType::ApplicationVndPgrstObject)
    }

    /// Check if this is a text-based media type.
    pub fn is_text(&self) -> bool {
        matches!(
            self,
            MediaType::TextCsv | MediaType::TextPlain | MediaType::ApplicationXml
        ) || self.is_json()
    }

    /// Check if this is a binary media type.
    pub fn is_binary(&self) -> bool {
        matches!(self, MediaType::ApplicationOctetStream)
    }

    /// Check if this matches another media type (considering wildcards).
    pub fn matches(&self, other: &MediaType) -> bool {
        if *self == MediaType::Any || *other == MediaType::Any {
            return true;
        }
        self == other
    }
}

impl fmt::Display for MediaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Parsed Accept header with quality values
#[derive(Debug, Clone)]
pub struct AcceptItem {
    /// The media type
    pub media_type: MediaType,
    /// Quality value (0.0 to 1.0), default 1.0
    pub quality: f32,
}

impl AcceptItem {
    /// Parse a single Accept header item (e.g., "application/json;q=0.9").
    pub fn parse(input: &str) -> Self {
        let mut parts = input.split(';');
        let media_type = MediaType::parse(parts.next().unwrap_or("*/*"));

        let mut quality = 1.0f32;
        for param in parts {
            let param = param.trim();
            if let Some(q) = param.strip_prefix("q=") {
                quality = q.parse().unwrap_or(1.0);
            }
        }

        Self { media_type, quality }
    }
}

/// Parse an Accept header value into sorted list of media types.
///
/// Returns media types sorted by quality (highest first).
pub fn parse_accept_header(header: &str) -> Vec<AcceptItem> {
    let mut items: Vec<AcceptItem> = header.split(',').map(AcceptItem::parse).collect();

    // Sort by quality (highest first)
    items.sort_by(|a, b| b.quality.partial_cmp(&a.quality).unwrap_or(std::cmp::Ordering::Equal));

    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_media_type_parse() {
        assert_eq!(MediaType::parse("application/json"), MediaType::ApplicationJson);
        assert_eq!(MediaType::parse("APPLICATION/JSON"), MediaType::ApplicationJson);
        assert_eq!(MediaType::parse("text/csv"), MediaType::TextCsv);
        assert_eq!(MediaType::parse("*/*"), MediaType::Any);
        assert_eq!(
            MediaType::parse("application/json; charset=utf-8"),
            MediaType::ApplicationJson
        );
    }

    #[test]
    fn test_media_type_as_str() {
        assert_eq!(MediaType::ApplicationJson.as_str(), "application/json");
        assert_eq!(MediaType::TextCsv.as_str(), "text/csv");
    }

    #[test]
    fn test_media_type_is_json() {
        assert!(MediaType::ApplicationJson.is_json());
        assert!(MediaType::ApplicationVndPgrstObject.is_json());
        assert!(!MediaType::TextCsv.is_json());
    }

    #[test]
    fn test_media_type_is_singular() {
        assert!(MediaType::ApplicationVndPgrstObject.is_singular());
        assert!(!MediaType::ApplicationJson.is_singular());
        assert!(!MediaType::ApplicationVndPgrstArray.is_singular());
    }

    #[test]
    fn test_media_type_matches() {
        assert!(MediaType::ApplicationJson.matches(&MediaType::ApplicationJson));
        assert!(MediaType::Any.matches(&MediaType::ApplicationJson));
        assert!(MediaType::ApplicationJson.matches(&MediaType::Any));
        assert!(!MediaType::ApplicationJson.matches(&MediaType::TextCsv));
    }

    #[test]
    fn test_accept_item_parse() {
        let item = AcceptItem::parse("application/json");
        assert_eq!(item.media_type, MediaType::ApplicationJson);
        assert_eq!(item.quality, 1.0);

        let item = AcceptItem::parse("text/csv;q=0.5");
        assert_eq!(item.media_type, MediaType::TextCsv);
        assert_eq!(item.quality, 0.5);
    }

    #[test]
    fn test_parse_accept_header() {
        let items = parse_accept_header("text/csv;q=0.5, application/json, */*;q=0.1");

        assert_eq!(items.len(), 3);
        // Sorted by quality
        assert_eq!(items[0].media_type, MediaType::ApplicationJson);
        assert_eq!(items[0].quality, 1.0);
        assert_eq!(items[1].media_type, MediaType::TextCsv);
        assert_eq!(items[1].quality, 0.5);
        assert_eq!(items[2].media_type, MediaType::Any);
        assert_eq!(items[2].quality, 0.1);
    }

    #[test]
    fn test_media_type_default() {
        assert_eq!(MediaType::default(), MediaType::ApplicationJson);
    }
}
