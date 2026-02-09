//! HTTP Prefer header parsing
//!
//! Parses HTTP `Prefer` headers into structured preference objects.
//! Parses RFC 7240 Prefer headers into structured preference types.

use compact_str::CompactString;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// ==========================================================================
// Preference enums
// ==========================================================================

/// How to handle duplicate values during upsert.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferResolution {
    MergeDuplicates,
    IgnoreDuplicates,
}

impl PreferResolution {
    fn header_value(&self) -> &'static str {
        match self {
            PreferResolution::MergeDuplicates => "resolution=merge-duplicates",
            PreferResolution::IgnoreDuplicates => "resolution=ignore-duplicates",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "resolution=merge-duplicates" => Some(PreferResolution::MergeDuplicates),
            "resolution=ignore-duplicates" => Some(PreferResolution::IgnoreDuplicates),
            _ => None,
        }
    }
}

/// How to return mutated data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferRepresentation {
    /// Return the body
    Full,
    /// Return the Location header (for POST)
    HeadersOnly,
    /// Return nothing
    None,
}

impl PreferRepresentation {
    fn header_value(&self) -> &'static str {
        match self {
            PreferRepresentation::Full => "return=representation",
            PreferRepresentation::HeadersOnly => "return=headers-only",
            PreferRepresentation::None => "return=minimal",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "return=representation" => Some(PreferRepresentation::Full),
            "return=headers-only" => Some(PreferRepresentation::HeadersOnly),
            "return=minimal" => Some(PreferRepresentation::None),
            _ => None,
        }
    }
}

/// How to count results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferCount {
    /// Exact count (slower)
    Exact,
    /// Query planner estimate
    Planned,
    /// Use planner if count exceeds max-rows, otherwise exact
    Estimated,
}

impl PreferCount {
    fn header_value(&self) -> &'static str {
        match self {
            PreferCount::Exact => "count=exact",
            PreferCount::Planned => "count=planned",
            PreferCount::Estimated => "count=estimated",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "count=exact" => Some(PreferCount::Exact),
            "count=planned" => Some(PreferCount::Planned),
            "count=estimated" => Some(PreferCount::Estimated),
            _ => None,
        }
    }
}

/// Whether to commit or rollback the transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferTransaction {
    Commit,
    Rollback,
}

impl PreferTransaction {
    fn header_value(&self) -> &'static str {
        match self {
            PreferTransaction::Commit => "tx=commit",
            PreferTransaction::Rollback => "tx=rollback",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "tx=commit" => Some(PreferTransaction::Commit),
            "tx=rollback" => Some(PreferTransaction::Rollback),
            _ => None,
        }
    }
}

/// How to handle missing columns in insert/update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferMissing {
    /// Use column defaults
    ApplyDefaults,
    /// Use null
    ApplyNulls,
}

impl PreferMissing {
    fn header_value(&self) -> &'static str {
        match self {
            PreferMissing::ApplyDefaults => "missing=default",
            PreferMissing::ApplyNulls => "missing=null",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "missing=default" => Some(PreferMissing::ApplyDefaults),
            "missing=null" => Some(PreferMissing::ApplyNulls),
            _ => None,
        }
    }
}

/// How to handle unrecognized preferences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferHandling {
    Strict,
    Lenient,
}

impl PreferHandling {
    fn header_value(&self) -> &'static str {
        match self {
            PreferHandling::Strict => "handling=strict",
            PreferHandling::Lenient => "handling=lenient",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "handling=strict" => Some(PreferHandling::Strict),
            "handling=lenient" => Some(PreferHandling::Lenient),
            _ => None,
        }
    }
}

/// Response plurality preference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreferPlurality {
    /// Return array (default)
    Plural,
    /// Return single object (error if != 1 row)
    Singular,
}

impl PreferPlurality {
    fn header_value(&self) -> &'static str {
        match self {
            PreferPlurality::Plural => "plurality=plural",
            PreferPlurality::Singular => "plurality=singular",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "plurality=plural" => Some(PreferPlurality::Plural),
            "plurality=singular" => Some(PreferPlurality::Singular),
            _ => None,
        }
    }
}

// ==========================================================================
// Preferences struct
// ==========================================================================

/// All recognized preferences from Prefer headers.
///
/// Parsed HTTP `Prefer` header values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Preferences {
    pub resolution: Option<PreferResolution>,
    pub representation: Option<PreferRepresentation>,
    pub count: Option<PreferCount>,
    pub transaction: Option<PreferTransaction>,
    pub missing: Option<PreferMissing>,
    pub handling: Option<PreferHandling>,
    pub plurality: Option<PreferPlurality>,
    pub timezone: Option<CompactString>,
    pub max_affected: Option<i64>,
    /// Preference strings that were not recognized
    pub invalid_prefs: Vec<CompactString>,
}

impl Preferences {
    /// Parse preferences from HTTP headers.
    ///
    /// Parse preferences from HTTP headers.
    ///
    /// - `allow_tx_override`: whether to allow `tx=commit`/`tx=rollback`
    /// - `valid_timezones`: set of accepted timezone names
    /// - `headers`: HTTP headers (name, value) pairs
    pub fn from_headers(
        allow_tx_override: bool,
        valid_timezones: &HashSet<String>,
        headers: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Self {
        // Collect all Prefer header values, split by comma
        let prefs: Vec<String> = headers
            .iter()
            .filter(|(name, _)| name.as_ref().eq_ignore_ascii_case("prefer"))
            .flat_map(|(_, value)| {
                value
                    .as_ref()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .collect::<Vec<_>>()
            })
            .filter(|s| !s.is_empty())
            .collect();

        // Parse each preference category (first match wins)
        let resolution = prefs.iter().find_map(|p| PreferResolution::parse(p));
        let representation = prefs.iter().find_map(|p| PreferRepresentation::parse(p));
        let count = prefs.iter().find_map(|p| PreferCount::parse(p));
        let transaction = if allow_tx_override {
            prefs.iter().find_map(|p| PreferTransaction::parse(p))
        } else {
            None
        };
        let missing = prefs.iter().find_map(|p| PreferMissing::parse(p));
        let handling = prefs.iter().find_map(|p| PreferHandling::parse(p));
        let plurality = prefs.iter().find_map(|p| PreferPlurality::parse(p));

        // Parse timezone preference
        let timezone_pref = prefs
            .iter()
            .find_map(|p| p.strip_prefix("timezone=").map(|s| s.to_string()));
        let timezone = timezone_pref.as_ref().and_then(|tz| {
            if valid_timezones.contains(tz) {
                Some(CompactString::from(tz.as_str()))
            } else {
                None
            }
        });
        let timezone_accepted = timezone.is_some();

        // Parse max-affected preference
        let max_affected = prefs
            .iter()
            .find_map(|p| p.strip_prefix("max-affected=").and_then(|s| s.parse().ok()));

        // Build set of all accepted preference strings
        let accepted: HashSet<&str> = [
            PreferResolution::MergeDuplicates.header_value(),
            PreferResolution::IgnoreDuplicates.header_value(),
            PreferRepresentation::Full.header_value(),
            PreferRepresentation::HeadersOnly.header_value(),
            PreferRepresentation::None.header_value(),
            PreferCount::Exact.header_value(),
            PreferCount::Planned.header_value(),
            PreferCount::Estimated.header_value(),
            PreferTransaction::Commit.header_value(),
            PreferTransaction::Rollback.header_value(),
            PreferMissing::ApplyDefaults.header_value(),
            PreferMissing::ApplyNulls.header_value(),
            PreferHandling::Strict.header_value(),
            PreferHandling::Lenient.header_value(),
            PreferPlurality::Plural.header_value(),
            PreferPlurality::Singular.header_value(),
        ]
        .into_iter()
        .collect();

        // Find invalid preferences
        let invalid_prefs: Vec<CompactString> = prefs
            .iter()
            .filter(|p| {
                let p_str = p.as_str();
                !(accepted.contains(p_str)
                    || p_str.starts_with("max-affected=")
                    || (p_str.starts_with("timezone=") && timezone_accepted))
            })
            .map(|p| CompactString::from(p.as_str()))
            .collect();

        Preferences {
            resolution,
            representation,
            count,
            transaction,
            missing,
            handling,
            plurality,
            timezone,
            max_affected,
            invalid_prefs,
        }
    }

    /// Check if we should execute a count query.
    pub fn should_count(&self) -> bool {
        self.count == Some(PreferCount::Exact) || self.count == Some(PreferCount::Estimated)
    }

    /// Check if we should use EXPLAIN for count.
    pub fn should_explain_count(&self) -> bool {
        self.count == Some(PreferCount::Planned) || self.count == Some(PreferCount::Estimated)
    }

    /// Build the Preference-Applied response header value.
    pub fn applied_header(&self) -> Option<String> {
        let mut parts: Vec<&str> = Vec::new();

        if let Some(r) = &self.resolution {
            parts.push(r.header_value());
        }
        if let Some(m) = &self.missing {
            parts.push(m.header_value());
        }
        if let Some(r) = &self.representation {
            parts.push(r.header_value());
        }
        if let Some(c) = &self.count {
            parts.push(c.header_value());
        }
        if let Some(t) = &self.transaction {
            parts.push(t.header_value());
        }
        if let Some(h) = &self.handling {
            parts.push(h.header_value());
        }
        if let Some(p) = &self.plurality {
            parts.push(p.header_value());
        }

        // timezone and max_affected in applied header only if handling=strict
        // (first occurrence wins per spec)

        if parts.is_empty() {
            None
        } else {
            Some(parts.join(", "))
        }
    }

    /// Check if handling is strict.
    pub fn is_strict(&self) -> bool {
        self.handling == Some(PreferHandling::Strict)
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_tz() -> HashSet<String> {
        HashSet::new()
    }

    fn sample_tz() -> HashSet<String> {
        let mut tz = HashSet::new();
        tz.insert("America/Los_Angeles".to_string());
        tz.insert("UTC".to_string());
        tz
    }

    #[test]
    fn test_single_preference() {
        let headers = vec![("Prefer", "return=representation")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.representation, Some(PreferRepresentation::Full));
        assert!(prefs.resolution.is_none());
    }

    #[test]
    fn test_multiple_prefs_comma_separated() {
        let headers = vec![(
            "Prefer",
            "resolution=ignore-duplicates, count=exact, return=representation",
        )];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.resolution, Some(PreferResolution::IgnoreDuplicates));
        assert_eq!(prefs.count, Some(PreferCount::Exact));
        assert_eq!(prefs.representation, Some(PreferRepresentation::Full));
    }

    #[test]
    fn test_multiple_prefer_headers() {
        let headers = vec![
            ("Prefer", "resolution=ignore-duplicates"),
            ("Prefer", "count=exact"),
            ("Prefer", "missing=null"),
            ("Prefer", "handling=lenient"),
        ];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.resolution, Some(PreferResolution::IgnoreDuplicates));
        assert_eq!(prefs.count, Some(PreferCount::Exact));
        assert_eq!(prefs.missing, Some(PreferMissing::ApplyNulls));
        assert_eq!(prefs.handling, Some(PreferHandling::Lenient));
    }

    #[test]
    fn test_first_preference_wins() {
        // Per spec, first occurrence wins
        let headers = vec![("Prefer", "tx=commit, tx=rollback")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.transaction, Some(PreferTransaction::Commit));
    }

    #[test]
    fn test_first_preference_wins_across_headers() {
        let headers = vec![
            ("Prefer", "resolution=ignore-duplicates"),
            ("Prefer", "resolution=merge-duplicates"),
        ];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.resolution, Some(PreferResolution::IgnoreDuplicates));
    }

    #[test]
    fn test_tx_override_disabled() {
        let headers = vec![("Prefer", "tx=commit")];
        let prefs = Preferences::from_headers(false, &empty_tz(), &headers);
        assert!(prefs.transaction.is_none());
    }

    #[test]
    fn test_invalid_preferences() {
        let headers = vec![("Prefer", "invalid, handling=strict")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.handling, Some(PreferHandling::Strict));
        assert_eq!(prefs.invalid_prefs.len(), 1);
        assert_eq!(prefs.invalid_prefs[0].as_str(), "invalid");
    }

    #[test]
    fn test_timezone_preference() {
        let headers = vec![("Prefer", "timezone=America/Los_Angeles")];
        let prefs = Preferences::from_headers(true, &sample_tz(), &headers);
        assert_eq!(prefs.timezone.as_deref(), Some("America/Los_Angeles"));
    }

    #[test]
    fn test_timezone_invalid() {
        let headers = vec![("Prefer", "timezone=Invalid/Zone")];
        let prefs = Preferences::from_headers(true, &sample_tz(), &headers);
        assert!(prefs.timezone.is_none());
        assert_eq!(prefs.invalid_prefs.len(), 1);
    }

    #[test]
    fn test_max_affected() {
        let headers = vec![("Prefer", "max-affected=100")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.max_affected, Some(100));
    }

    #[test]
    fn test_max_affected_not_invalid() {
        let headers = vec![("Prefer", "max-affected=5999")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.max_affected, Some(5999));
        assert!(prefs.invalid_prefs.is_empty());
    }

    #[test]
    fn test_case_insensitive_header_name() {
        let headers = vec![("prefer", "count=exact")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.count, Some(PreferCount::Exact));
    }

    #[test]
    fn test_whitespace_handling() {
        let headers = vec![(
            "Prefer",
            "count=exact,    tx=commit   ,return=representation , missing=default, handling=strict",
        )];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.count, Some(PreferCount::Exact));
        assert_eq!(prefs.transaction, Some(PreferTransaction::Commit));
        assert_eq!(prefs.representation, Some(PreferRepresentation::Full));
        assert_eq!(prefs.missing, Some(PreferMissing::ApplyDefaults));
        assert_eq!(prefs.handling, Some(PreferHandling::Strict));
    }

    #[test]
    fn test_should_count() {
        let mut p = Preferences::default();
        assert!(!p.should_count());

        p.count = Some(PreferCount::Exact);
        assert!(p.should_count());

        p.count = Some(PreferCount::Estimated);
        assert!(p.should_count());

        p.count = Some(PreferCount::Planned);
        assert!(!p.should_count());
    }

    #[test]
    fn test_should_explain_count() {
        let mut p = Preferences::default();
        assert!(!p.should_explain_count());

        p.count = Some(PreferCount::Planned);
        assert!(p.should_explain_count());

        p.count = Some(PreferCount::Estimated);
        assert!(p.should_explain_count());

        p.count = Some(PreferCount::Exact);
        assert!(!p.should_explain_count());
    }

    #[test]
    fn test_applied_header() {
        let mut p = Preferences::default();
        assert!(p.applied_header().is_none());

        p.resolution = Some(PreferResolution::IgnoreDuplicates);
        p.count = Some(PreferCount::Exact);
        let h = p.applied_header().unwrap();
        assert!(h.contains("resolution=ignore-duplicates"));
        assert!(h.contains("count=exact"));
    }

    #[test]
    fn test_is_strict() {
        let mut p = Preferences::default();
        assert!(!p.is_strict());

        p.handling = Some(PreferHandling::Strict);
        assert!(p.is_strict());
    }

    #[test]
    fn test_empty_headers() {
        let headers: Vec<(&str, &str)> = vec![];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs, Preferences::default());
    }

    #[test]
    fn test_comprehensive_parse() {
        let headers = vec![(
            "Prefer",
            "resolution=ignore-duplicates, count=exact, timezone=America/Los_Angeles, max-affected=100",
        )];
        let prefs = Preferences::from_headers(true, &sample_tz(), &headers);
        assert_eq!(prefs.resolution, Some(PreferResolution::IgnoreDuplicates));
        assert_eq!(prefs.count, Some(PreferCount::Exact));
        assert_eq!(prefs.timezone.as_deref(), Some("America/Los_Angeles"));
        assert_eq!(prefs.max_affected, Some(100));
        assert!(prefs.invalid_prefs.is_empty());
    }

    #[test]
    fn test_all_return_values() {
        let headers = vec![("Prefer", "return=minimal")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.representation, Some(PreferRepresentation::None));

        let headers = vec![("Prefer", "return=headers-only")];
        let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
        assert_eq!(prefs.representation, Some(PreferRepresentation::HeadersOnly));
    }

    #[test]
    fn test_all_count_values() {
        for (val, expected) in [
            ("count=exact", PreferCount::Exact),
            ("count=planned", PreferCount::Planned),
            ("count=estimated", PreferCount::Estimated),
        ] {
            let headers = vec![("Prefer", val)];
            let prefs = Preferences::from_headers(true, &empty_tz(), &headers);
            assert_eq!(prefs.count, Some(expected));
        }
    }
}
