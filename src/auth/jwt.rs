//! JWT token parsing, validation, and role extraction
//!
//! This module handles the full lifecycle of JWT authentication:
//!
//! 1. **Secret resolution** — determine the `DecodingKey` from the
//!    configured secret (plain, base64, or JWKS).
//! 2. **Token parsing** — decode the JWT header to find `alg`/`kid`,
//!    then validate the token signature and structure.
//! 3. **Claims validation** — check `exp`, `nbf`, `iat` (with 30-second
//!    skew) and `aud` against the configured audience.
//! 4. **Role extraction** — walk the claims JSON using the configured
//!    JSPath to find the role, falling back to the anonymous role.
//!
//! # Example
//!
//! ```ignore
//! use dbrest::auth::jwt::parse_and_validate;
//! use dbrest::config::AppConfig;
//!
//! let config = AppConfig { jwt_secret: Some("my-secret".into()), ..Default::default() };
//! let result = parse_and_validate("eyJ...", &config)?;
//! println!("Role: {}", result.role);
//! ```

use compact_str::CompactString;
use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation};

use crate::config::AppConfig;
use crate::config::jwt::extract_from_json;

use super::error::{JwtClaimsError, JwtDecodeError, JwtError};
use super::types::AuthResult;

/// The clock-skew tolerance in seconds for `exp`, `nbf`, and `iat` claims.
const ALLOWED_SKEW_SECONDS: u64 = 30;

/// Parse, validate, and extract role from a JWT token.
///
/// Returns an [`AuthResult`] on success. The `claims` map contains all
/// JWT claims (standard and custom), with the `"role"` key always present.
///
/// # Errors
///
/// Returns [`JwtError`] if:
/// - No secret is configured (`SecretMissing`)
/// - The token is structurally invalid (`Decode`)
/// - A claims check fails (`Claims`)
pub fn parse_and_validate(token: &str, config: &AppConfig) -> Result<AuthResult, JwtError> {
    // Empty token check
    if token.is_empty() {
        return Err(JwtDecodeError::EmptyAuthHeader.into());
    }

    // Validate 3-part structure
    let parts = token.split('.').count();
    if parts != 3 {
        return Err(JwtDecodeError::UnexpectedParts(parts).into());
    }

    // Resolve decoding key and validation settings
    let (key, validation) = create_validation_context(token, config)?;

    // Decode and validate signature + standard claims
    let token_data: TokenData<serde_json::Value> =
        jsonwebtoken::decode(token, &key, &validation).map_err(map_decode_error)?;

    // Additional custom validations (iat)
    validate_iat(&token_data.claims)?;

    // Extract claims as a JSON map
    let claims_map = match token_data.claims {
        serde_json::Value::Object(map) => map,
        _ => return Err(JwtClaimsError::ParsingFailed.into()),
    };

    // Extract role from claims via configured path
    let role = extract_role(&claims_map, config)?;

    // Build final claims map with role inserted
    let mut final_claims = claims_map;
    final_claims.insert(
        "role".to_string(),
        serde_json::Value::String(role.to_string()),
    );

    Ok(AuthResult {
        role,
        claims: final_claims,
    })
}

/// Build the `DecodingKey` and `Validation` from config.
fn create_validation_context(
    token: &str,
    config: &AppConfig,
) -> Result<(DecodingKey, Validation), JwtError> {
    let secret_str = config.jwt_secret.as_ref().ok_or(JwtError::SecretMissing)?;

    // Try parsing as JWKS JSON first
    if secret_str.trim_start().starts_with('{')
        && let Ok(jwks) = serde_json::from_str::<jsonwebtoken::jwk::JwkSet>(secret_str)
    {
        return create_jwks_context(token, &jwks, config);
    }

    // Plain secret (or base64-encoded)
    let key = if config.jwt_secret_is_base64 {
        DecodingKey::from_base64_secret(secret_str)
            .map_err(|e| JwtDecodeError::KeyError(e.to_string()))?
    } else {
        DecodingKey::from_secret(secret_str.as_bytes())
    };

    // Decode header to find algorithm
    let header = jsonwebtoken::decode_header(token).map_err(map_decode_error)?;

    let mut validation = Validation::new(header.alg);
    configure_validation(&mut validation, config);

    Ok((key, validation))
}

/// Build context for JWKS-based validation.
fn create_jwks_context(
    token: &str,
    jwks: &jsonwebtoken::jwk::JwkSet,
    config: &AppConfig,
) -> Result<(DecodingKey, Validation), JwtError> {
    let header = jsonwebtoken::decode_header(token).map_err(map_decode_error)?;

    // Find the matching JWK by kid
    let jwk = if let Some(kid) = &header.kid {
        jwks.find(kid)
            .ok_or_else(|| JwtDecodeError::KeyError(format!("No JWK found with kid '{kid}'")))?
    } else {
        // No kid — use the first key
        jwks.keys
            .first()
            .ok_or_else(|| JwtDecodeError::KeyError("JWKS contains no keys".to_string()))?
    };

    let key = DecodingKey::from_jwk(jwk).map_err(|e| JwtDecodeError::KeyError(e.to_string()))?;

    let alg = jwk
        .common
        .key_algorithm
        .and_then(algorithm_from_key_alg)
        .unwrap_or(header.alg);

    let mut validation = Validation::new(alg);
    configure_validation(&mut validation, config);

    Ok((key, validation))
}

/// Configure shared validation settings (audience, leeway).
fn configure_validation(validation: &mut Validation, config: &AppConfig) {
    validation.leeway = ALLOWED_SKEW_SECONDS;

    // Validate exp and nbf when present, but don't require them.
    validation.required_spec_claims.clear();
    validation.validate_exp = true;
    validation.validate_nbf = true;

    // Audience
    if let Some(ref aud) = config.jwt_aud {
        validation.set_audience(&[aud]);
    } else {
        validation.validate_aud = false;
    }
}

/// Map `jsonwebtoken` errors to our error types.
fn map_decode_error(e: jsonwebtoken::errors::Error) -> JwtError {
    use jsonwebtoken::errors::ErrorKind;
    match e.kind() {
        ErrorKind::ExpiredSignature => JwtClaimsError::Expired.into(),
        ErrorKind::ImmatureSignature => JwtClaimsError::NotYetValid.into(),
        ErrorKind::InvalidAudience => JwtClaimsError::NotInAudience.into(),
        ErrorKind::InvalidSignature => JwtDecodeError::BadCrypto.into(),
        ErrorKind::InvalidAlgorithm => JwtDecodeError::BadAlgorithm(e.to_string()).into(),
        ErrorKind::InvalidKeyFormat => JwtDecodeError::KeyError(e.to_string()).into(),
        ErrorKind::InvalidToken => {
            // Could be bad base64, wrong format, etc.
            JwtDecodeError::BadCrypto.into()
        }
        ErrorKind::Base64(_) => JwtDecodeError::BadCrypto.into(),
        ErrorKind::Json(_) => JwtClaimsError::ParsingFailed.into(),
        ErrorKind::Crypto(_) => JwtDecodeError::BadCrypto.into(),
        _ => JwtDecodeError::KeyError(e.to_string()).into(),
    }
}

/// Convert JWK key algorithm to `jsonwebtoken::Algorithm`.
fn algorithm_from_key_alg(alg: jsonwebtoken::jwk::KeyAlgorithm) -> Option<Algorithm> {
    use jsonwebtoken::jwk::KeyAlgorithm;
    match alg {
        KeyAlgorithm::HS256 => Some(Algorithm::HS256),
        KeyAlgorithm::HS384 => Some(Algorithm::HS384),
        KeyAlgorithm::HS512 => Some(Algorithm::HS512),
        KeyAlgorithm::RS256 => Some(Algorithm::RS256),
        KeyAlgorithm::RS384 => Some(Algorithm::RS384),
        KeyAlgorithm::RS512 => Some(Algorithm::RS512),
        KeyAlgorithm::ES256 => Some(Algorithm::ES256),
        KeyAlgorithm::ES384 => Some(Algorithm::ES384),
        _ => None,
    }
}

/// Validate the `iat` claim (issued-at must not be in the future).
///
/// The `jsonwebtoken` crate does not validate `iat` by default. We check
/// it manually with the same 30-second skew tolerance.
fn validate_iat(claims: &serde_json::Value) -> Result<(), JwtError> {
    if let Some(iat) = claims.get("iat").and_then(|v| v.as_i64()) {
        let now = chrono::Utc::now().timestamp();
        if iat > now + ALLOWED_SKEW_SECONDS as i64 {
            return Err(JwtClaimsError::IssuedAtFuture.into());
        }
    }
    Ok(())
}

/// Extract the role from the claims map using the configured JSPath.
///
/// Falls back to `config.db_anon_role` if no role is found in the token.
fn extract_role(
    claims: &serde_json::Map<String, serde_json::Value>,
    config: &AppConfig,
) -> Result<CompactString, JwtError> {
    let claims_value = serde_json::Value::Object(claims.clone());

    // Walk the configured role claim path
    if let Some(value) = extract_from_json(&claims_value, &config.jwt_role_claim_key) {
        let role_str = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        if !role_str.is_empty() {
            return Ok(CompactString::from(role_str));
        }
    }

    // Fall back to anonymous role
    if let Some(ref anon_role) = config.db_anon_role {
        return Ok(CompactString::from(anon_role.as_str()));
    }

    Err(JwtError::TokenRequired)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::jwt::JsPathExp;
    use jsonwebtoken::{EncodingKey, Header as JwtHeader};

    fn test_config(secret: &str) -> AppConfig {
        let mut config = AppConfig::default();
        config.jwt_secret = Some(secret.to_string());
        config.db_anon_role = Some("web_anon".to_string());
        config
    }

    fn encode_token(claims: &serde_json::Value, secret: &str) -> String {
        jsonwebtoken::encode(
            &JwtHeader::default(),
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn encode_token_with_alg(claims: &serde_json::Value, secret: &str, alg: Algorithm) -> String {
        let header = JwtHeader::new(alg);
        jsonwebtoken::encode(
            &header,
            claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    #[test]
    fn test_parse_valid_hs256() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "test_author");
        assert!(result.claims.contains_key("role"));
    }

    #[test]
    fn test_parse_valid_hs384() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token_with_alg(&claims, secret, Algorithm::HS384);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "test_author");
    }

    #[test]
    fn test_parse_expired() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);
        // Expired 60 seconds ago (beyond 30s skew)
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() - 60
        });
        let token = encode_token(&claims, secret);

        let err = parse_and_validate(&token, &config).unwrap_err();
        assert!(matches!(err, JwtError::Claims(JwtClaimsError::Expired)));
        assert_eq!(err.code(), "DBRST303");
    }

    #[test]
    fn test_parse_not_yet_valid() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);
        // nbf 60 seconds in the future (beyond 30s skew)
        let claims = serde_json::json!({
            "role": "test_author",
            "nbf": chrono::Utc::now().timestamp() + 60,
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let err = parse_and_validate(&token, &config).unwrap_err();
        assert!(matches!(err, JwtError::Claims(JwtClaimsError::NotYetValid)));
    }

    #[test]
    fn test_parse_iat_future() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);
        // iat 60 seconds in the future
        let claims = serde_json::json!({
            "role": "test_author",
            "iat": chrono::Utc::now().timestamp() + 60,
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let err = parse_and_validate(&token, &config).unwrap_err();
        assert!(matches!(
            err,
            JwtError::Claims(JwtClaimsError::IssuedAtFuture)
        ));
    }

    #[test]
    fn test_parse_bad_audience() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let mut config = test_config(secret);
        config.jwt_aud = Some("expected_audience".to_string());

        let claims = serde_json::json!({
            "role": "test_author",
            "aud": "wrong_audience",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let err = parse_and_validate(&token, &config).unwrap_err();
        assert!(matches!(
            err,
            JwtError::Claims(JwtClaimsError::NotInAudience)
        ));
    }

    #[test]
    fn test_parse_audience_array_match() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let mut config = test_config(secret);
        config.jwt_aud = Some("my_app".to_string());

        let claims = serde_json::json!({
            "role": "test_author",
            "aud": ["other_app", "my_app"],
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "test_author");
    }

    #[test]
    fn test_parse_audience_array_no_match() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let mut config = test_config(secret);
        config.jwt_aud = Some("my_app".to_string());

        let claims = serde_json::json!({
            "role": "test_author",
            "aud": ["other1", "other2"],
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let err = parse_and_validate(&token, &config).unwrap_err();
        assert!(matches!(
            err,
            JwtError::Claims(JwtClaimsError::NotInAudience)
        ));
    }

    #[test]
    fn test_parse_empty_token() {
        let config = test_config("secret");
        let err = parse_and_validate("", &config).unwrap_err();
        assert!(matches!(
            err,
            JwtError::Decode(JwtDecodeError::EmptyAuthHeader)
        ));
    }

    #[test]
    fn test_parse_two_parts() {
        let config = test_config("secret");
        let err = parse_and_validate("abc.def", &config).unwrap_err();
        assert!(matches!(
            err,
            JwtError::Decode(JwtDecodeError::UnexpectedParts(2))
        ));
    }

    #[test]
    fn test_parse_wrong_secret() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let wrong_secret = "completely_different_secret_value!";
        let config = test_config(wrong_secret);

        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let err = parse_and_validate(&token, &config).unwrap_err();
        // Should be a decode-level error (bad signature)
        assert!(matches!(err, JwtError::Decode(_)));
    }

    #[test]
    fn test_parse_no_secret() {
        let mut config = AppConfig::default();
        config.jwt_secret = None;

        let err = parse_and_validate("a.b.c", &config).unwrap_err();
        assert!(matches!(err, JwtError::SecretMissing));
    }

    #[test]
    fn test_parse_base64_secret() {
        use base64::Engine;
        let raw_secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let b64_secret = base64::engine::general_purpose::STANDARD.encode(raw_secret.as_bytes());

        let mut config = AppConfig::default();
        config.jwt_secret = Some(b64_secret);
        config.jwt_secret_is_base64 = true;
        config.db_anon_role = Some("web_anon".to_string());

        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() + 3600
        });

        // Encode with raw secret bytes
        let token = jsonwebtoken::encode(
            &JwtHeader::default(),
            &claims,
            &EncodingKey::from_secret(raw_secret.as_bytes()),
        )
        .unwrap();

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "test_author");
    }

    #[test]
    fn test_extract_role_simple() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);

        let claims = serde_json::json!({
            "role": "admin",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "admin");
    }

    #[test]
    fn test_extract_role_nested() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let mut config = test_config(secret);
        config.jwt_role_claim_key =
            vec![JsPathExp::Key("user".into()), JsPathExp::Key("role".into())];

        let claims = serde_json::json!({
            "user": { "role": "nested_admin" },
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "nested_admin");
    }

    #[test]
    fn test_extract_role_array_index() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let mut config = test_config(secret);
        config.jwt_role_claim_key = vec![JsPathExp::Key("roles".into()), JsPathExp::Index(0)];

        let claims = serde_json::json!({
            "roles": ["first_role", "second_role"],
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "first_role");
    }

    #[test]
    fn test_extract_role_default_anon() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);

        // Token with no "role" field
        let claims = serde_json::json!({
            "sub": "user123",
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "web_anon");
    }

    #[test]
    fn test_parse_extra_claims_preserved() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);

        let claims = serde_json::json!({
            "role": "test_author",
            "sub": "user123",
            "custom_key": "custom_value",
            "nested": { "a": 1 },
            "exp": chrono::Utc::now().timestamp() + 3600
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(
            result.claims.get("custom_key").unwrap(),
            &serde_json::json!("custom_value")
        );
        assert_eq!(
            result.claims.get("nested").unwrap(),
            &serde_json::json!({"a": 1})
        );
        assert_eq!(
            result.claims.get("sub").unwrap(),
            &serde_json::json!("user123")
        );
    }

    #[test]
    fn test_claims_30s_skew_within() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);

        // Expired 25 seconds ago — within 30s skew
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() - 25
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_claims_30s_skew_beyond() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);

        // Expired 35 seconds ago — beyond 30s skew
        let claims = serde_json::json!({
            "role": "test_author",
            "exp": chrono::Utc::now().timestamp() - 35
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_no_exp_claim_succeeds() {
        let secret = "a]gq@2Yr4wLvA#_6!qnMb*X^tbP$I@av";
        let config = test_config(secret);

        // No exp claim — should still succeed because we cleared
        // required_spec_claims. Matches reference behaviour where exp
        // is optional (only validated if present).
        let claims = serde_json::json!({
            "role": "test_author"
        });
        let token = encode_token(&claims, secret);

        let result = parse_and_validate(&token, &config).unwrap();
        assert_eq!(result.role.as_str(), "test_author");
    }
}
