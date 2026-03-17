//! CallPlan types for dbrest
//!
//! Defines the plan types for RPC function calls.
//! Matches the Haskell `CallPlan` (`FunctionCall`) data type.

use compact_str::CompactString;
use std::collections::HashMap;

use crate::api_request::types::FieldName;
use crate::schema_cache::routine::{Routine, RoutineParam};
use crate::types::identifiers::QualifiedIdentifier;

use super::types::CoercibleSelectField;

// ==========================================================================
// CallPlan
// ==========================================================================

/// A function call plan
///
/// Represents a resolved plan for invoking a PostgreSQL function.
#[derive(Debug, Clone)]
pub struct CallPlan {
    /// Qualified identifier of the function
    pub qi: QualifiedIdentifier,
    /// Resolved parameters
    pub params: CallParams,
    /// Arguments to pass to the function
    pub args: CallArgs,
    /// Whether the function returns a scalar
    pub scalar: bool,
    /// Whether the function returns SETOF scalar
    pub set_of_scalar: bool,
    /// Fields that have filters (for security-definer checks)
    pub filter_fields: Vec<FieldName>,
    /// RETURNING columns
    pub returning: Vec<CoercibleSelectField>,
}

// ==========================================================================
// CallParams
// ==========================================================================

/// Resolved function parameters
#[derive(Debug, Clone)]
pub enum CallParams {
    /// Named parameters (the common case)
    KeyParams(Vec<RoutineParam>),
    /// Single positional parameter (for functions taking a single JSON/text arg)
    OnePosParam(RoutineParam),
}

impl CallParams {
    /// Get all resolved parameters
    pub fn params(&self) -> &[RoutineParam] {
        match self {
            CallParams::KeyParams(params) => params,
            CallParams::OnePosParam(param) => std::slice::from_ref(param),
        }
    }
}

// ==========================================================================
// CallArgs
// ==========================================================================

/// Arguments to pass to the function
#[derive(Debug, Clone)]
pub enum CallArgs {
    /// Direct key-value arguments (from query params or form data)
    DirectArgs(HashMap<CompactString, RpcParamValue>),
    /// JSON body argument (passed directly to function)
    JsonArgs(Option<bytes::Bytes>),
}

/// Value for an RPC parameter
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcParamValue {
    /// A single fixed value
    Fixed(CompactString),
    /// A variadic list of values
    Variadic(Vec<CompactString>),
}

// ==========================================================================
// toRpcParams
// ==========================================================================

/// Convert query parameters and routine metadata into RPC parameters
///
/// Matches the Haskell `toRpcParams` function.
/// Variadic parameters are split on commas; regular parameters are passed as-is.
pub fn to_rpc_params(
    routine: &Routine,
    params: &[(CompactString, CompactString)],
) -> HashMap<CompactString, RpcParamValue> {
    let mut result = HashMap::new();

    for (key, value) in params {
        if let Some(rp) = routine.get_param(key) {
            if rp.is_variadic {
                let values: Vec<CompactString> = value
                    .split(',')
                    .map(|v| CompactString::from(v.trim()))
                    .collect();
                result.insert(key.clone(), RpcParamValue::Variadic(values));
            } else {
                result.insert(key.clone(), RpcParamValue::Fixed(value.clone()));
            }
        } else {
            // Unknown param — include as fixed value (validated downstream)
            result.insert(key.clone(), RpcParamValue::Fixed(value.clone()));
        }
    }

    result
}

// ==========================================================================
// Tests
// ==========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;

    #[test]
    fn test_to_rpc_params_fixed() {
        let routine = test_routine()
            .param(test_param().name("id").pg_type("integer").build())
            .param(test_param().name("name").pg_type("text").build())
            .build();

        let params = vec![("id".into(), "42".into()), ("name".into(), "alice".into())];

        let rpc_params = to_rpc_params(&routine, &params);
        assert_eq!(
            rpc_params.get("id"),
            Some(&RpcParamValue::Fixed("42".into()))
        );
        assert_eq!(
            rpc_params.get("name"),
            Some(&RpcParamValue::Fixed("alice".into()))
        );
    }

    #[test]
    fn test_to_rpc_params_variadic() {
        let routine = test_routine()
            .param(
                test_param()
                    .name("ids")
                    .pg_type("integer")
                    .is_variadic(true)
                    .build(),
            )
            .is_variadic(true)
            .build();

        let params = vec![("ids".into(), "1,2,3".into())];

        let rpc_params = to_rpc_params(&routine, &params);
        assert_eq!(
            rpc_params.get("ids"),
            Some(&RpcParamValue::Variadic(vec![
                "1".into(),
                "2".into(),
                "3".into()
            ]))
        );
    }

    #[test]
    fn test_to_rpc_params_unknown_param() {
        let routine = test_routine()
            .param(test_param().name("id").build())
            .build();

        let params = vec![("unknown_key".into(), "value".into())];

        let rpc_params = to_rpc_params(&routine, &params);
        assert_eq!(
            rpc_params.get("unknown_key"),
            Some(&RpcParamValue::Fixed("value".into()))
        );
    }

    #[test]
    fn test_call_params_key_params() {
        let p = test_param().name("x").build();
        let cp = CallParams::KeyParams(vec![p]);
        assert_eq!(cp.params().len(), 1);
    }

    #[test]
    fn test_call_params_one_pos() {
        let p = test_param().name("body").pg_type("json").build();
        let cp = CallParams::OnePosParam(p);
        assert_eq!(cp.params().len(), 1);
        assert_eq!(cp.params()[0].name.as_str(), "body");
    }
}
