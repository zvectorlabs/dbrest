//! Data representation types for dbrest
//!
//! Data representations define type mapping functions that convert between
//! database types (e.g., converting a bytea to a custom image format).

use compact_str::CompactString;
use std::collections::HashMap;

/// A data representation mapping function
///
/// Maps from one database type to another via a PostgreSQL function.
/// Used for custom type casting in content negotiation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataRepresentation {
    /// Source PostgreSQL type name
    pub source_type: CompactString,
    /// Target PostgreSQL type name
    pub target_type: CompactString,
    /// PostgreSQL function that performs the conversion
    pub function: CompactString,
}

/// Map from (source_type, target_type) pair to the representation
pub type RepresentationsMap = HashMap<(CompactString, CompactString), DataRepresentation>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_representation_creation() {
        let repr = DataRepresentation {
            source_type: "bytea".into(),
            target_type: "image/png".into(),
            function: "bytea_to_png".into(),
        };
        assert_eq!(repr.source_type.as_str(), "bytea");
        assert_eq!(repr.target_type.as_str(), "image/png");
        assert_eq!(repr.function.as_str(), "bytea_to_png");
    }

    #[test]
    fn test_representations_map() {
        let mut map: RepresentationsMap = HashMap::new();
        let repr = DataRepresentation {
            source_type: "bytea".into(),
            target_type: "text".into(),
            function: "encode".into(),
        };
        map.insert(("bytea".into(), "text".into()), repr.clone());

        assert_eq!(map.get(&("bytea".into(), "text".into())), Some(&repr));
        assert!(!map.contains_key(&("text".into(), "bytea".into())));
    }

    #[test]
    fn test_data_representation_equality() {
        let a = DataRepresentation {
            source_type: "int".into(),
            target_type: "text".into(),
            function: "int_to_text".into(),
        };
        let b = a.clone();
        assert_eq!(a, b);
    }
}
