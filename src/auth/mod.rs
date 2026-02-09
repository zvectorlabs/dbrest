//! JWT authentication module
//!
//! Handles the full JWT authentication lifecycle for every HTTP request:
//!
//! 1. **Token extraction** — parses the `Authorization: Bearer <token>` header.
//! 2. **Validation** — verifies the signature and standard claims (`exp`,
//!    `nbf`, `iat`, `aud`) with a 30-second clock-skew tolerance.
//! 3. **Role resolution** — extracts the database role from the JWT claims
//!    using the configured JSPath, falling back to the anonymous role.
//! 4. **Caching** — stores validated results in a lock-free Moka cache
//!    keyed by the raw token string. Cache size is bounded and entries
//!    expire based on the token's `exp` claim (capped at 1 hour).
//!
//! # Supported Algorithms
//!
//! HS256, HS384, HS512, RS256, RS384, RS512, ES256, ES384.
//!
//! # Secret Formats
//!
//! - Plain UTF-8 string
//! - Base64-encoded string (`jwt_secret_is_base64 = true`)
//! - JWKS (JSON Web Key Set) — automatically detected when the secret
//!   starts with `{`.
//!
//! # Error Codes
//!
//! | Code | Meaning |
//! |------|---------|
//! | PGRST300 | Server lacks JWT secret |
//! | PGRST301 | Token decode / signature error |
//! | PGRST302 | Token required (no anonymous role) |
//! | PGRST303 | Claims validation failed |

pub mod cache;
pub mod error;
pub mod jwt;
pub mod middleware;
pub mod types;

// Re-exports for convenience
pub use cache::JwtCache;
pub use error::JwtError;
pub use middleware::{auth_middleware, AuthState};
pub use types::AuthResult;
