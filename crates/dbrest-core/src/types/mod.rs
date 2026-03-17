//! Core types for PgREST
//!
//! This module provides fundamental types used throughout the crate:
//!
//! - [`identifiers`] - Database identifiers (schema.table, etc.)
//! - [`media`] - Media types for content negotiation

pub mod identifiers;
pub mod media;

// Re-export commonly used types
pub use identifiers::{QualifiedIdentifier, RelIdentifier};
pub use media::MediaType;
