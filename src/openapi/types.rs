//! OpenAPI 3.0 specification types
//!
//! This module defines the Rust types for representing OpenAPI 3.0 specifications.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OpenAPI 3.0 specification root
#[derive(Debug, Serialize, Deserialize)]
pub struct OpenApiSpec {
    pub openapi: String,
    pub info: Info,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub servers: Vec<Server>,
    pub paths: Paths,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub components: Option<Components>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<Vec<SecurityRequirement>>,
}

/// API information
#[derive(Debug, Serialize, Deserialize)]
pub struct Info {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub version: String,
}

/// Server information
#[derive(Debug, Serialize, Deserialize)]
pub struct Server {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Paths object (flattened HashMap in JSON)
#[derive(Debug, Serialize, Deserialize)]
pub struct Paths {
    #[serde(flatten)]
    pub paths: HashMap<String, PathItem>,
}

/// Path item (operations for a path)
#[derive(Debug, Serialize, Deserialize)]
pub struct PathItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub get: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub post: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub patch: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub put: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Operation>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head: Option<Operation>,
}

/// HTTP operation
#[derive(Debug, Serialize, Deserialize)]
pub struct Operation {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub operation_id: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub parameters: Vec<Parameter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_body: Option<RequestBody>,
    pub responses: Responses,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<Vec<SecurityRequirement>>,
}

/// Parameter location
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum ParameterLocation {
    Query,
    Header,
    Path,
    Cookie,
}

/// Request parameter
#[derive(Debug, Serialize, Deserialize)]
pub struct Parameter {
    pub name: String,
    #[serde(rename = "in")]
    pub location: ParameterLocation,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub style: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explode: Option<bool>,
}

/// Request body
#[derive(Debug, Serialize, Deserialize)]
pub struct RequestBody {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    pub content: HashMap<String, MediaTypeObject>,
}

/// Media type object
#[derive(Debug, Serialize, Deserialize)]
pub struct MediaTypeObject {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema: Option<Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub example: Option<serde_json::Value>,
}

/// Responses object (flattened HashMap in JSON)
#[derive(Debug, Serialize, Deserialize)]
pub struct Responses {
    #[serde(flatten)]
    pub responses: HashMap<String, Response>,
}

/// Response definition
#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<HashMap<String, MediaTypeObject>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<HashMap<String, Header>>,
}

/// Response header
#[derive(Debug, Serialize, Deserialize)]
pub struct Header {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    pub schema: Schema,
}

/// JSON Schema (simplified for OpenAPI)
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(untagged)]
pub enum Schema {
    /// Reference to another schema
    Ref {
        #[serde(rename = "$ref")]
        ref_: String,
    },
    /// Schema object
    Object {
        #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
        type_: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        format: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        properties: Option<HashMap<String, Schema>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        required: Option<Vec<String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        items: Option<Box<Schema>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        nullable: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        example: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        enum_: Option<Vec<serde_json::Value>>,
        #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
        enum_values: Option<Vec<serde_json::Value>>,
    },
}

impl Schema {
    /// Create a reference schema
    pub fn ref_(ref_path: &str) -> Self {
        Schema::Ref {
            ref_: ref_path.to_string(),
        }
    }

    /// Create a string schema
    pub fn string() -> Self {
        Schema::Object {
            type_: Some("string".to_string()),
            format: None,
            description: None,
            properties: None,
            required: None,
            items: None,
            nullable: None,
            example: None,
            enum_: None,
            enum_values: None,
        }
    }

    /// Create an integer schema
    pub fn integer() -> Self {
        Schema::Object {
            type_: Some("integer".to_string()),
            format: None,
            description: None,
            properties: None,
            required: None,
            items: None,
            nullable: None,
            example: None,
            enum_: None,
            enum_values: None,
        }
    }

    /// Create a number schema
    pub fn number() -> Self {
        Schema::Object {
            type_: Some("number".to_string()),
            format: None,
            description: None,
            properties: None,
            required: None,
            items: None,
            nullable: None,
            example: None,
            enum_: None,
            enum_values: None,
        }
    }

    /// Create a boolean schema
    pub fn boolean() -> Self {
        Schema::Object {
            type_: Some("boolean".to_string()),
            format: None,
            description: None,
            properties: None,
            required: None,
            items: None,
            nullable: None,
            example: None,
            enum_: None,
            enum_values: None,
        }
    }

    /// Create an array schema
    pub fn array(items: Schema) -> Self {
        Schema::Object {
            type_: Some("array".to_string()),
            format: None,
            description: None,
            properties: None,
            required: None,
            items: Some(Box::new(items)),
            nullable: None,
            example: None,
            enum_: None,
            enum_values: None,
        }
    }

    /// Create an object schema
    pub fn object(properties: HashMap<String, Schema>, required: Vec<String>) -> Self {
        Schema::Object {
            type_: Some("object".to_string()),
            format: None,
            description: None,
            properties: Some(properties),
            required: if required.is_empty() {
                None
            } else {
                Some(required)
            },
            items: None,
            nullable: None,
            example: None,
            enum_: None,
            enum_values: None,
        }
    }

    /// Set nullable
    pub fn nullable(mut self) -> Self {
        if let Schema::Object { nullable, .. } = &mut self {
            *nullable = Some(true);
        }
        self
    }

    /// Set description
    pub fn with_description(mut self, description: String) -> Self {
        if let Schema::Object { description: desc, .. } = &mut self {
            *desc = Some(description);
        }
        self
    }

    /// Set format
    pub fn with_format(mut self, format: String) -> Self {
        if let Schema::Object { format: fmt, .. } = &mut self {
            *fmt = Some(format);
        }
        self
    }
}

/// Components object
#[derive(Debug, Serialize, Deserialize)]
pub struct Components {
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub schemas: HashMap<String, Schema>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security_schemes: Option<HashMap<String, SecurityScheme>>,
}

/// Security scheme
#[derive(Debug, Serialize, Deserialize)]
pub struct SecurityScheme {
    #[serde(rename = "type")]
    pub type_: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bearer_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Security requirement
#[derive(Debug, Serialize, Deserialize)]
pub struct SecurityRequirement {
    #[serde(flatten)]
    pub requirements: HashMap<String, Vec<String>>,
}
