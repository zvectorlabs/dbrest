//! Range parsing for limit/offset and HTTP Range header
//!
//! Range types for handling pagination
//! through Range headers and limit/offset query parameters.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A non-negative range representing rows to return.
///
/// A non-negative range for pagination. Uses `Option<i64>` for boundaries
/// where `None` means unbounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    /// Lower bound (0-based, inclusive). Always present.
    pub offset: i64,
    /// Upper bound (inclusive). None means unbounded (all remaining rows).
    pub limit_to: Option<i64>,
}

impl Default for Range {
    /// The default range: all rows starting from 0.
    fn default() -> Self {
        Self::all()
    }
}

impl Range {
    /// Create a range representing all rows (offset=0, unbounded).
    pub fn all() -> Self {
        Self {
            offset: 0,
            limit_to: None,
        }
    }

    /// Create a range from offset to upper bound (inclusive).
    pub fn new(offset: i64, limit_to: i64) -> Self {
        Self {
            offset,
            limit_to: Some(limit_to),
        }
    }

    /// Create a range starting at the given offset with no upper bound.
    pub fn from_offset(offset: i64) -> Self {
        Self {
            offset,
            limit_to: None,
        }
    }

    /// The special limit-zero range (0 <= x <= -1).
    /// Used to allow `limit=0` queries per the API spec.
    pub fn limit_zero() -> Self {
        Self {
            offset: 0,
            limit_to: Some(-1),
        }
    }

    /// Check if this is the limit-zero range.
    pub fn has_limit_zero(&self) -> bool {
        self.limit_to == Some(-1)
    }

    /// Get the number of rows this range covers, if bounded.
    pub fn limit(&self) -> Option<i64> {
        self.limit_to.map(|upper| 1 + upper - self.offset)
    }

    /// Get the offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Check if this range is unbounded (no upper limit).
    pub fn is_all(&self) -> bool {
        self.offset == 0 && self.limit_to.is_none()
    }

    /// Check if this range is empty (lower > upper).
    pub fn is_empty_range(&self) -> bool {
        match self.limit_to {
            Some(upper) => self.offset > upper && !self.has_limit_zero(),
            None => false,
        }
    }

    /// Restrict this range by applying a limit.
    ///
    /// If `max_rows` is Some, ensures the range covers at most that many rows.
    pub fn restrict(&self, max_rows: Option<i64>) -> Self {
        match max_rows {
            None => *self,
            Some(limit) => {
                let new_upper = self.offset + limit - 1;
                match self.limit_to {
                    Some(upper) => Self {
                        offset: self.offset,
                        limit_to: Some(upper.min(new_upper)),
                    },
                    None => Self {
                        offset: self.offset,
                        limit_to: Some(new_upper),
                    },
                }
            }
        }
    }

    /// Apply a limit (number of rows).
    pub fn with_limit(&self, limit: i64) -> Self {
        Self {
            offset: self.offset,
            limit_to: Some(self.offset + limit - 1),
        }
    }

    /// Apply an offset.
    pub fn with_offset(&self, offset: i64) -> Self {
        Self {
            offset,
            limit_to: self.limit_to,
        }
    }

    /// Intersect this range with another.
    pub fn intersect(&self, other: &Range) -> Self {
        let new_offset = self.offset.max(other.offset);
        let new_upper = match (self.limit_to, other.limit_to) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        Self {
            offset: new_offset,
            limit_to: new_upper,
        }
    }

    /// Convert to limit-zero range if it has limit=0, else use fallback.
    pub fn convert_to_limit_zero(&self, fallback: &Range) -> Self {
        if self.has_limit_zero() {
            Self::limit_zero()
        } else {
            *fallback
        }
    }
}

impl fmt::Display for Range {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.limit_to {
            Some(upper) => write!(f, "{}-{}", self.offset, upper),
            None => write!(f, "{}-", self.offset),
        }
    }
}

/// Parse the HTTP `Range` header value.
///
/// Expects format: `items=<start>-<end>` where end is optional.
///
/// # Examples
///
/// ```
/// use pgrest::api_request::range::parse_range_header;
///
/// let r = parse_range_header("items=0-24").unwrap();
/// assert_eq!(r.offset, 0);
/// assert_eq!(r.limit_to, Some(24));
///
/// let r = parse_range_header("items=10-").unwrap();
/// assert_eq!(r.offset, 10);
/// assert_eq!(r.limit_to, None);
/// ```
pub fn parse_range_header(header: &str) -> Option<Range> {
    // Strip "items=" prefix (case-insensitive)
    let range_str = header
        .strip_prefix("items=")
        .or_else(|| header.strip_prefix("Items="))?;

    let (start_str, end_str) = range_str.split_once('-')?;

    let start: i64 = start_str.parse().ok()?;

    if end_str.is_empty() {
        Some(Range::from_offset(start))
    } else {
        let end: i64 = end_str.parse().ok()?;
        Some(Range::new(start, end))
    }
}

/// Build Content-Range header value.
///
/// Format: `<lower>-<upper>/<total>` or `*/<total>` or `<lower>-<upper>/*`
pub fn content_range_header(lower: i64, upper: i64, total: Option<i64>) -> String {
    let total_str = match total {
        Some(t) => t.to_string(),
        None => "*".to_string(),
    };

    let total_not_zero = total != Some(0);
    let from_in_range = lower <= upper;

    if total_not_zero && from_in_range {
        format!("{}-{}/{}", lower, upper, total_str)
    } else {
        format!("*/{}", total_str)
    }
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_all() {
        let r = Range::all();
        assert_eq!(r.offset, 0);
        assert_eq!(r.limit_to, None);
        assert!(r.is_all());
        assert!(!r.is_empty_range());
    }

    #[test]
    fn test_range_new() {
        let r = Range::new(0, 24);
        assert_eq!(r.offset, 0);
        assert_eq!(r.limit_to, Some(24));
        assert!(!r.is_all());
        assert_eq!(r.limit(), Some(25));
    }

    #[test]
    fn test_range_from_offset() {
        let r = Range::from_offset(10);
        assert_eq!(r.offset, 10);
        assert_eq!(r.limit_to, None);
        assert!(!r.is_all());
    }

    #[test]
    fn test_range_limit_zero() {
        let r = Range::limit_zero();
        assert!(r.has_limit_zero());
        assert_eq!(r.limit(), Some(0));
    }

    #[test]
    fn test_range_empty() {
        let r = Range::new(10, 5); // lower > upper
        assert!(r.is_empty_range());
    }

    #[test]
    fn test_range_restrict() {
        let r = Range::all();
        let restricted = r.restrict(Some(25));
        assert_eq!(restricted.offset, 0);
        assert_eq!(restricted.limit_to, Some(24));

        // Restrict with None does nothing
        let same = r.restrict(None);
        assert_eq!(same, r);
    }

    #[test]
    fn test_range_restrict_existing() {
        let r = Range::new(0, 100);
        let restricted = r.restrict(Some(25));
        assert_eq!(restricted.limit_to, Some(24));

        // Restrict larger than current doesn't expand
        let larger = r.restrict(Some(200));
        assert_eq!(larger.limit_to, Some(100));
    }

    #[test]
    fn test_range_with_limit() {
        let r = Range::from_offset(5);
        let limited = r.with_limit(10);
        assert_eq!(limited.offset, 5);
        assert_eq!(limited.limit_to, Some(14));
        assert_eq!(limited.limit(), Some(10));
    }

    #[test]
    fn test_range_with_offset() {
        let r = Range::new(0, 24);
        let offset = r.with_offset(10);
        assert_eq!(offset.offset, 10);
        assert_eq!(offset.limit_to, Some(24));
    }

    #[test]
    fn test_range_intersect() {
        let a = Range::new(0, 100);
        let b = Range::new(10, 50);
        let c = a.intersect(&b);
        assert_eq!(c.offset, 10);
        assert_eq!(c.limit_to, Some(50));

        // Intersect with unbounded
        let d = Range::all();
        let e = a.intersect(&d);
        assert_eq!(e, a);
    }

    #[test]
    fn test_range_display() {
        assert_eq!(Range::new(0, 24).to_string(), "0-24");
        assert_eq!(Range::from_offset(10).to_string(), "10-");
    }

    #[test]
    fn test_parse_range_header() {
        let r = parse_range_header("items=0-24").unwrap();
        assert_eq!(r.offset, 0);
        assert_eq!(r.limit_to, Some(24));

        let r = parse_range_header("items=10-").unwrap();
        assert_eq!(r.offset, 10);
        assert_eq!(r.limit_to, None);

        let r = parse_range_header("Items=5-10").unwrap();
        assert_eq!(r.offset, 5);
        assert_eq!(r.limit_to, Some(10));

        // Invalid
        assert!(parse_range_header("bytes=0-24").is_none());
        assert!(parse_range_header("items=abc-def").is_none());
        assert!(parse_range_header("garbage").is_none());
    }

    #[test]
    fn test_content_range_header() {
        assert_eq!(content_range_header(0, 24, Some(100)), "0-24/100");
        assert_eq!(content_range_header(0, 24, None), "0-24/*");
        assert_eq!(content_range_header(10, 5, Some(100)), "*/100"); // lower > upper
        assert_eq!(content_range_header(0, 0, Some(0)), "*/0"); // total is zero
    }

    #[test]
    fn test_range_default() {
        let r = Range::default();
        assert!(r.is_all());
    }

    #[test]
    fn test_convert_to_limit_zero() {
        let limit_range = Range::limit_zero();
        let fallback = Range::new(0, 24);
        let result = limit_range.convert_to_limit_zero(&fallback);
        assert!(result.has_limit_zero());

        let normal = Range::new(0, 10);
        let result2 = normal.convert_to_limit_zero(&fallback);
        assert_eq!(result2, fallback);
    }
}
