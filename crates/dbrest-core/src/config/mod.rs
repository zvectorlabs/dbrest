//! Configuration module
//!
//! This module handles loading and validating dbrest configuration from
//! files and environment variables.
//!
//! # Configuration Sources
//!
//! Configuration is loaded in the following order of precedence (highest first):
//!
//! 1. Environment variables (`DBRST_*`)
//! 2. Configuration file values
//! 3. Default values
//!
//! # Example
//!
//! ```ignore
//! use dbrest::config::load_config;
//! use std::collections::HashMap;
//! use std::path::Path;
//!
//! let config = load_config(
//!     Some(Path::new("/etc/dbrest/config")),
//!     HashMap::new(),
//! ).await?;
//!
//! println!("Server port: {}", config.server_port);
//! ```
//!
//! # Environment Variables
//!
//! All configuration options can be set via environment variables with the `DBRST_` prefix:
//!
//! - `DBRST_DB_URI` → `db-uri`
//! - `DBRST_SERVER_PORT` → `server-port`
//! - `DBRST_JWT_SECRET` → `jwt-secret`
//!
//! Underscores in variable names are converted to hyphens.

pub mod error;
pub mod jwt;
pub mod parser;
pub mod types;

// Re-export main types
pub use error::ConfigError;
pub use jwt::{JsPathExp, extract_from_json, parse_js_path};
pub use parser::{apply_config_value, load_config, parse_bool, validate_config};
pub use types::{AppConfig, IsolationLevel, LogLevel, OpenApiMode};
