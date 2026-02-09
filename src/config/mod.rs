//! Configuration module
//!
//! This module handles loading and validating PgREST configuration from
//! files and environment variables.
//!
//! # Configuration Sources
//!
//! Configuration is loaded in the following order of precedence (highest first):
//!
//! 1. Environment variables (`PGRST_*`)
//! 2. Configuration file values
//! 3. Default values
//!
//! # Example
//!
//! ```ignore
//! use pgrest::config::load_config;
//! use std::collections::HashMap;
//! use std::path::Path;
//!
//! let config = load_config(
//!     Some(Path::new("/etc/pgrest/config")),
//!     HashMap::new(),
//! ).await?;
//!
//! println!("Server port: {}", config.server_port);
//! ```
//!
//! # Environment Variables
//!
//! All configuration options can be set via environment variables with the `PGRST_` prefix:
//!
//! - `PGRST_DB_URI` → `db-uri`
//! - `PGRST_SERVER_PORT` → `server-port`
//! - `PGRST_JWT_SECRET` → `jwt-secret`
//!
//! Underscores in variable names are converted to hyphens.

pub mod error;
pub mod jwt;
pub mod parser;
pub mod types;

// Re-export main types
pub use error::ConfigError;
pub use jwt::{extract_from_json, parse_js_path, JsPathExp};
pub use parser::{apply_config_value, load_config, parse_bool, validate_config};
pub use types::{AppConfig, IsolationLevel, LogLevel, OpenApiMode};
