//! Configuration error types

use std::fmt;
use std::num::ParseIntError;

/// Errors that can occur during configuration loading and parsing
#[derive(Debug)]
pub enum ConfigError {
    /// I/O error reading config file
    Io(std::io::Error),

    /// Parse error (invalid syntax)
    Parse {
        line: Option<usize>,
        message: String,
    },

    /// Invalid value for a config key
    InvalidValue {
        key: String,
        value: String,
        expected: Option<String>,
    },

    /// Invalid boolean value
    InvalidBool(String),

    /// Invalid integer value
    InvalidInt { key: String, error: ParseIntError },

    /// Invalid JSPath expression for JWT role claim
    InvalidJsPath(String),

    /// Base64 decoding error
    Base64(base64::DecodeError),

    /// UTF-8 decoding error
    Utf8(std::string::FromUtf8Error),

    /// Validation error
    Validation(String),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::Io(err) => write!(f, "Config file I/O error: {}", err),
            ConfigError::Parse { line, message } => {
                if let Some(line_num) = line {
                    write!(f, "Config parse error at line {}: {}", line_num, message)
                } else {
                    write!(f, "Config parse error: {}", message)
                }
            }
            ConfigError::InvalidValue {
                key,
                value,
                expected,
            } => {
                if let Some(exp) = expected {
                    write!(
                        f,
                        "Invalid value '{}' for config key '{}', expected: {}",
                        value, key, exp
                    )
                } else {
                    write!(f, "Invalid value '{}' for config key '{}'", value, key)
                }
            }
            ConfigError::InvalidBool(value) => {
                write!(
                    f,
                    "Invalid boolean value '{}', expected: true/false/yes/no/on/off/1/0",
                    value
                )
            }
            ConfigError::InvalidInt { key, error } => {
                write!(f, "Invalid integer for '{}': {}", key, error)
            }
            ConfigError::InvalidJsPath(msg) => {
                write!(f, "Invalid JWT role claim path: {}", msg)
            }
            ConfigError::Base64(err) => write!(f, "Base64 decode error: {}", err),
            ConfigError::Utf8(err) => write!(f, "UTF-8 decode error: {}", err),
            ConfigError::Validation(msg) => write!(f, "Config validation error: {}", msg),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ConfigError::Io(err) => Some(err),
            ConfigError::InvalidInt { error, .. } => Some(error),
            ConfigError::Base64(err) => Some(err),
            ConfigError::Utf8(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        ConfigError::Io(err)
    }
}

impl From<ParseIntError> for ConfigError {
    fn from(err: ParseIntError) -> Self {
        ConfigError::InvalidInt {
            key: "unknown".to_string(),
            error: err,
        }
    }
}

impl From<base64::DecodeError> for ConfigError {
    fn from(err: base64::DecodeError) -> Self {
        ConfigError::Base64(err)
    }
}

impl From<std::string::FromUtf8Error> for ConfigError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        ConfigError::Utf8(err)
    }
}

/// Convert ConfigError to the main Error type
impl From<ConfigError> for crate::error::Error {
    fn from(err: ConfigError) -> Self {
        crate::error::Error::InvalidConfig {
            message: err.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_error_display() {
        let err = ConfigError::InvalidBool("maybe".to_string());
        assert!(err.to_string().contains("maybe"));

        let err = ConfigError::Validation("test error".to_string());
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_config_error_parse_with_line() {
        let err = ConfigError::Parse {
            line: Some(42),
            message: "unexpected token".to_string(),
        };
        let msg = err.to_string();
        assert!(msg.contains("42"));
        assert!(msg.contains("unexpected token"));
    }
}
