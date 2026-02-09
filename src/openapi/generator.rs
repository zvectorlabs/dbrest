//! OpenAPI 3.0 specification generator
//!
//! Generates OpenAPI 3.0 specifications from the schema cache.

use std::collections::HashMap;
use std::sync::Arc;

use crate::auth::AuthResult;
use crate::config::{AppConfig, OpenApiMode};
use crate::error::Error;
use crate::schema_cache::{Column, Routine, SchemaCache, Table};

use super::types::*;

/// OpenAPI specification generator
pub struct OpenApiGenerator {
    config: Arc<AppConfig>,
    cache: Arc<SchemaCache>,
    #[allow(dead_code)] // reserved for future role-scoped OpenAPI
    auth: Option<AuthResult>,
}

impl OpenApiGenerator {
    /// Create a new OpenAPI generator
    pub fn new(config: Arc<AppConfig>, cache: Arc<SchemaCache>, auth: Option<AuthResult>) -> Self {
        Self {
            config,
            cache,
            auth,
        }
    }

    /// Generate the full OpenAPI 3.0 specification
    pub fn generate(&self) -> Result<OpenApiSpec, Error> {
        // Check if OpenAPI is disabled
        if self.config.openapi_mode == OpenApiMode::Disabled {
            return Err(Error::OpenApiDisabled);
        }

        let spec = OpenApiSpec {
            openapi: "3.0.0".to_string(),
            info: self.generate_info(),
            servers: self.generate_servers(),
            paths: self.generate_paths()?,
            components: Some(self.generate_components()?),
            security: self.generate_security(),
        };

        Ok(spec)
    }

    fn generate_info(&self) -> Info {
        Info {
            title: "PostgREST API".to_string(),
            description: Some("REST API for PostgreSQL database".to_string()),
            version: "1.0.0".to_string(),
        }
    }

    fn generate_servers(&self) -> Vec<Server> {
        if let Some(ref proxy_uri) = self.config.openapi_server_proxy_uri {
            vec![Server {
                url: proxy_uri.clone(),
                description: Some("Proxy server".to_string()),
            }]
        } else {
            vec![Server {
                url: "/".to_string(),
                description: None,
            }]
        }
    }

    fn generate_paths(&self) -> Result<Paths, Error> {
        let mut paths = HashMap::new();

        // Generate paths for each table
        for schema in &self.config.db_schemas {
            for table in self.cache.tables_in_schema(schema) {
                // Check privileges if mode is FollowPrivileges
                if self.config.openapi_mode == OpenApiMode::FollowPrivileges
                    && !self.can_read_table(table)?
                {
                    continue;
                }

                let path = format!("/{}.{}", table.schema, table.name);
                let path_item = self.generate_table_path_item(table)?;
                paths.insert(path, path_item);
            }
        }

        // Generate paths for RPC functions
        for schema in &self.config.db_schemas {
            if let Some(routines) = self.cache.get_routines_by_name(schema, "") {
                // Get all routines in schema
                for routine in routines {
                    if self.config.openapi_mode == OpenApiMode::FollowPrivileges
                        && !self.can_execute_routine(routine)?
                    {
                        continue;
                    }

                    let path = format!("/rpc/{}", routine.name);
                    let path_item = self.generate_rpc_path_item(routine)?;
                    paths.insert(path, path_item);
                }
            }
        }

        // Also iterate through all routines in the cache
        for (qi, routines) in self.cache.routines.iter() {
            if !self.config.db_schemas.contains(&qi.schema.to_string()) {
                continue;
            }

            for routine in routines {
                if self.config.openapi_mode == OpenApiMode::FollowPrivileges
                    && !self.can_execute_routine(routine)?
                {
                    continue;
                }

                let path = format!("/rpc/{}", routine.name);
                let path_item = self.generate_rpc_path_item(routine)?;
                paths.insert(path, path_item);
            }
        }

        Ok(Paths { paths })
    }

    fn generate_table_path_item(&self, table: &Table) -> Result<PathItem, Error> {
        // GET operation
        let get_op = self.generate_get_operation(table)?;

        // POST operation (if insertable)
        let post_op = if table.insertable {
            Some(self.generate_post_operation(table)?)
        } else {
            None
        };

        // PATCH operation (if updatable)
        let patch_op = if table.updatable {
            Some(self.generate_patch_operation(table)?)
        } else {
            None
        };

        // PUT operation (if insertable and updatable)
        let put_op = if table.insertable && table.updatable {
            Some(self.generate_put_operation(table)?)
        } else {
            None
        };

        // DELETE operation (if deletable)
        let delete_op = if table.deletable {
            Some(self.generate_delete_operation(table)?)
        } else {
            None
        };

        // OPTIONS operation
        let options_op = Some(self.generate_options_operation());

        // HEAD operation (same as GET but no body)
        let head_op = Some(self.generate_head_operation(table)?);

        Ok(PathItem {
            get: Some(get_op),
            post: post_op,
            patch: patch_op,
            put: put_op,
            delete: delete_op,
            options: options_op,
            head: head_op,
        })
    }

    fn generate_get_operation(&self, table: &Table) -> Result<Operation, Error> {
        let mut parameters = vec![
            // select parameter
            Parameter {
                name: "select".to_string(),
                location: ParameterLocation::Query,
                description: Some("Columns to select (comma-separated)".to_string()),
                required: Some(false),
                schema: Some(Schema::string()),
                style: None,
                explode: None,
            },
            // order parameter
            Parameter {
                name: "order".to_string(),
                location: ParameterLocation::Query,
                description: Some("Order by column(s)".to_string()),
                required: Some(false),
                schema: Some(Schema::string()),
                style: None,
                explode: None,
            },
            // limit parameter
            Parameter {
                name: "limit".to_string(),
                location: ParameterLocation::Query,
                description: Some("Limit number of results".to_string()),
                required: Some(false),
                schema: Some(Schema::integer().with_format("int64".to_string())),
                style: None,
                explode: None,
            },
            // offset parameter
            Parameter {
                name: "offset".to_string(),
                location: ParameterLocation::Query,
                description: Some("Skip number of results".to_string()),
                required: Some(false),
                schema: Some(Schema::integer().with_format("int64".to_string())),
                style: None,
                explode: None,
            },
        ];

        // Add filter parameters for each column
        for col in table.columns_list() {
            parameters.push(Parameter {
                name: col.name.to_string(),
                location: ParameterLocation::Query,
                description: col.description.clone(),
                required: Some(false),
                schema: Some(self.column_to_schema(col)?),
                style: None,
                explode: None,
            });
        }

        let responses = self.generate_read_responses(table)?;

        Ok(Operation {
            summary: Some(format!("List {} records", table.name)),
            description: table.description.clone(),
            operation_id: format!("get_{}_{}", table.schema, table.name),
            tags: vec![table.schema.to_string()],
            parameters,
            request_body: None,
            responses,
            security: self.generate_operation_security(),
        })
    }

    fn generate_post_operation(&self, table: &Table) -> Result<Operation, Error> {
        let schema_name = format!("{}_{}", table.schema, table.name);
        let schema_ref = format!("#/components/schemas/{}", schema_name);

        let mut content = HashMap::new();
        content.insert(
            "application/json".to_string(),
            MediaTypeObject {
                schema: Some(Schema::ref_(&schema_ref)),
                example: None,
            },
        );

        let request_body = RequestBody {
            description: Some(format!("{} record to insert", table.name)),
            required: Some(true),
            content,
        };

        let mut responses = HashMap::new();
        responses.insert(
            "201".to_string(),
            Response {
                description: "Created".to_string(),
                content: Some({
                    let mut c = HashMap::new();
                    c.insert(
                        "application/json".to_string(),
                        MediaTypeObject {
                            schema: Some(Schema::ref_(&schema_ref)),
                            example: None,
                        },
                    );
                    c
                }),
                headers: Some({
                    let mut h = HashMap::new();
                    h.insert(
                        "Location".to_string(),
                        Header {
                            description: Some("URL of created resource".to_string()),
                            required: Some(false),
                            schema: Schema::string(),
                        },
                    );
                    h
                }),
            },
        );
        responses.insert(
            "400".to_string(),
            Response {
                description: "Bad Request".to_string(),
                content: None,
                headers: None,
            },
        );

        Ok(Operation {
            summary: Some(format!("Create {} record", table.name)),
            description: table.description.clone(),
            operation_id: format!("post_{}_{}", table.schema, table.name),
            tags: vec![table.schema.to_string()],
            parameters: vec![],
            request_body: Some(request_body),
            responses: Responses { responses },
            security: self.generate_operation_security(),
        })
    }

    fn generate_patch_operation(&self, table: &Table) -> Result<Operation, Error> {
        let schema_name = format!("{}_{}", table.schema, table.name);
        let schema_ref = format!("#/components/schemas/{}", schema_name);

        let mut content = HashMap::new();
        content.insert(
            "application/json".to_string(),
            MediaTypeObject {
                schema: Some(Schema::ref_(&schema_ref)),
                example: None,
            },
        );

        let request_body = RequestBody {
            description: Some(format!("{} record to update", table.name)),
            required: Some(true),
            content,
        };

        let mut responses = HashMap::new();
        responses.insert(
            "200".to_string(),
            Response {
                description: "OK".to_string(),
                content: Some({
                    let mut c = HashMap::new();
                    c.insert(
                        "application/json".to_string(),
                        MediaTypeObject {
                            schema: Some(Schema::array(Schema::ref_(&schema_ref))),
                            example: None,
                        },
                    );
                    c
                }),
                headers: None,
            },
        );

        Ok(Operation {
            summary: Some(format!("Update {} records", table.name)),
            description: table.description.clone(),
            operation_id: format!("patch_{}_{}", table.schema, table.name),
            tags: vec![table.schema.to_string()],
            parameters: vec![],
            request_body: Some(request_body),
            responses: Responses { responses },
            security: self.generate_operation_security(),
        })
    }

    fn generate_put_operation(&self, table: &Table) -> Result<Operation, Error> {
        // PUT is similar to POST but for upsert
        self.generate_post_operation(table)
    }

    fn generate_delete_operation(&self, table: &Table) -> Result<Operation, Error> {
        let mut responses = HashMap::new();
        responses.insert(
            "204".to_string(),
            Response {
                description: "No Content".to_string(),
                content: None,
                headers: None,
            },
        );
        responses.insert(
            "200".to_string(),
            Response {
                description: "OK".to_string(),
                content: None,
                headers: None,
            },
        );

        Ok(Operation {
            summary: Some(format!("Delete {} records", table.name)),
            description: table.description.clone(),
            operation_id: format!("delete_{}_{}", table.schema, table.name),
            tags: vec![table.schema.to_string()],
            parameters: vec![],
            request_body: None,
            responses: Responses { responses },
            security: self.generate_operation_security(),
        })
    }

    fn generate_options_operation(&self) -> Operation {
        let mut responses = HashMap::new();
        responses.insert(
            "200".to_string(),
            Response {
                description: "OK".to_string(),
                content: None,
                headers: None,
            },
        );

        Operation {
            summary: None,
            description: None,
            operation_id: "options".to_string(),
            tags: vec![],
            parameters: vec![],
            request_body: None,
            responses: Responses { responses },
            security: None,
        }
    }

    fn generate_head_operation(&self, table: &Table) -> Result<Operation, Error> {
        // HEAD is same as GET but no body
        let mut op = self.generate_get_operation(table)?;
        op.operation_id = format!("head_{}_{}", table.schema, table.name);
        // Remove content from responses
        for response in op.responses.responses.values_mut() {
            response.content = None;
        }
        Ok(op)
    }

    fn generate_read_responses(&self, table: &Table) -> Result<Responses, Error> {
        let schema_name = format!("{}_{}", table.schema, table.name);
        let schema_ref = format!("#/components/schemas/{}", schema_name);

        let mut responses = HashMap::new();
        responses.insert(
            "200".to_string(),
            Response {
                description: "OK".to_string(),
                content: Some({
                    let mut c = HashMap::new();
                    c.insert(
                        "application/json".to_string(),
                        MediaTypeObject {
                            schema: Some(Schema::array(Schema::ref_(&schema_ref))),
                            example: None,
                        },
                    );
                    c
                }),
                headers: Some({
                    let mut h = HashMap::new();
                    h.insert(
                        "Content-Range".to_string(),
                        Header {
                            description: Some("Range of results".to_string()),
                            required: Some(false),
                            schema: Schema::string(),
                        },
                    );
                    h
                }),
            },
        );
        responses.insert(
            "406".to_string(),
            Response {
                description: "Not Acceptable".to_string(),
                content: None,
                headers: None,
            },
        );

        Ok(Responses { responses })
    }

    fn column_to_schema(&self, col: &Column) -> Result<Schema, Error> {
        let mut schema = if col.is_array_type() {
            // For arrays, get the base type
            let base_type = col.data_type.trim_end_matches("[]");
            let item_schema = self.pg_type_to_schema(base_type)?;
            Schema::array(item_schema)
        } else {
            self.pg_type_to_schema(&col.data_type)?
        };

        // Add nullable if column allows null
        if col.nullable {
            schema = schema.nullable();
        }

        // Add description
        if let Some(ref desc) = col.description {
            schema = schema.with_description(desc.clone());
        }

        // Add enum values if it's an enum
        if col.is_enum()
            && let Schema::Object { enum_values, .. } = &mut schema
        {
            *enum_values = Some(
                col.enum_values
                    .iter()
                    .map(|v| serde_json::Value::String(v.clone()))
                    .collect(),
            );
        }

        Ok(schema)
    }

    fn pg_type_to_schema(&self, pg_type: &str) -> Result<Schema, Error> {
        let schema = match pg_type {
            // Integer types
            "integer" | "int" | "int4" => Schema::integer().with_format("int32".to_string()),
            "bigint" | "int8" => Schema::integer().with_format("int64".to_string()),
            "smallint" | "int2" => Schema::integer().with_format("int32".to_string()),
            "serial" | "serial4" => Schema::integer().with_format("int32".to_string()),
            "bigserial" | "serial8" => Schema::integer().with_format("int64".to_string()),

            // Numeric types
            "numeric" | "decimal" => Schema::number().with_format("double".to_string()),
            "real" | "float4" => Schema::number().with_format("float".to_string()),
            "double precision" | "float8" => Schema::number().with_format("double".to_string()),

            // String types
            "text" | "character varying" | "varchar" | "character" | "char" | "name" => {
                Schema::string()
            }

            // Boolean
            "boolean" | "bool" => Schema::boolean(),

            // Date/time types
            "date" => Schema::string().with_format("date".to_string()),
            "time without time zone" | "time" => Schema::string().with_format("time".to_string()),
            "time with time zone" | "timetz" => Schema::string().with_format("time".to_string()),
            "timestamp without time zone" | "timestamp" => {
                Schema::string().with_format("date-time".to_string())
            }
            "timestamp with time zone" | "timestamptz" => {
                Schema::string().with_format("date-time".to_string())
            }
            "interval" => Schema::string(),

            // UUID
            "uuid" => Schema::string().with_format("uuid".to_string()),

            // JSON types
            "json" | "jsonb" => Schema::object(HashMap::new(), vec![]),

            // Binary
            "bytea" => Schema::string().with_format("byte".to_string()),

            // Default: treat as string
            _ => Schema::string(),
        };

        Ok(schema)
    }

    fn generate_components(&self) -> Result<Components, Error> {
        let mut schemas = HashMap::new();

        // Generate schemas for each table
        for schema in &self.config.db_schemas {
            for table in self.cache.tables_in_schema(schema) {
                if self.config.openapi_mode == OpenApiMode::FollowPrivileges
                    && !self.can_read_table(table)?
                {
                    continue;
                }

                let schema_name = format!("{}_{}", table.schema, table.name);
                let table_schema = self.table_to_schema(table)?;
                schemas.insert(schema_name, table_schema);
            }
        }

        let security_schemes = if self.config.openapi_security_active {
            Some(self.generate_security_schemes())
        } else {
            None
        };

        Ok(Components {
            schemas,
            security_schemes,
        })
    }

    fn table_to_schema(&self, table: &Table) -> Result<Schema, Error> {
        let mut properties = HashMap::new();
        let mut required = Vec::new();

        for col in table.columns_list() {
            let col_schema = self.column_to_schema(col)?;
            properties.insert(col.name.to_string(), col_schema);

            if !col.nullable && !col.has_default() && !col.is_generated() {
                required.push(col.name.to_string());
            }
        }

        let mut schema = Schema::object(properties, required);

        // Add description
        if let Some(ref desc) = table.description
            && let Schema::Object { description, .. } = &mut schema
        {
            *description = Some(desc.clone());
        }

        Ok(schema)
    }

    fn generate_security_schemes(&self) -> HashMap<String, SecurityScheme> {
        let mut schemes = HashMap::new();
        schemes.insert(
            "bearer".to_string(),
            SecurityScheme {
                type_: "http".to_string(),
                scheme: Some("bearer".to_string()),
                bearer_format: Some("JWT".to_string()),
                description: Some("JWT authentication".to_string()),
            },
        );
        schemes
    }

    fn generate_security(&self) -> Option<Vec<SecurityRequirement>> {
        if self.config.openapi_security_active {
            let mut req = HashMap::new();
            req.insert("bearer".to_string(), vec![]);
            Some(vec![SecurityRequirement { requirements: req }])
        } else {
            None
        }
    }

    fn generate_operation_security(&self) -> Option<Vec<SecurityRequirement>> {
        self.generate_security()
    }

    fn generate_rpc_path_item(&self, routine: &Routine) -> Result<PathItem, Error> {
        let get_op = self.generate_rpc_get_operation(routine)?;
        let post_op = self.generate_rpc_post_operation(routine)?;

        Ok(PathItem {
            get: Some(get_op),
            post: Some(post_op),
            patch: None,
            put: None,
            delete: None,
            options: Some(self.generate_options_operation()),
            head: None,
        })
    }

    fn generate_rpc_get_operation(&self, routine: &Routine) -> Result<Operation, Error> {
        let mut parameters = vec![];

        // Add parameters for each function parameter
        for param in &routine.params {
            parameters.push(Parameter {
                name: param.name.to_string(),
                location: ParameterLocation::Query,
                description: None, // RoutineParam doesn't have description field
                required: Some(param.required),
                schema: Some(self.routine_param_to_schema(param)?),
                style: None,
                explode: None,
            });
        }

        let responses = self.generate_rpc_responses(routine)?;

        Ok(Operation {
            summary: Some(format!("Call {} function", routine.name)),
            description: routine.description.clone(),
            operation_id: format!("rpc_get_{}", routine.name),
            tags: vec![routine.schema.to_string()],
            parameters,
            request_body: None,
            responses,
            security: self.generate_operation_security(),
        })
    }

    fn generate_rpc_post_operation(&self, routine: &Routine) -> Result<Operation, Error> {
        // For POST, parameters go in the request body
        let mut properties = HashMap::new();
        let mut required = Vec::new();

        for param in &routine.params {
            let param_schema = self.routine_param_to_schema(param)?;
            properties.insert(param.name.to_string(), param_schema);
            if param.required {
                required.push(param.name.to_string());
            }
        }

        let request_body = if properties.is_empty() {
            None
        } else {
            let mut content = HashMap::new();
            content.insert(
                "application/json".to_string(),
                MediaTypeObject {
                    schema: Some(Schema::object(properties, required)),
                    example: None,
                },
            );

            Some(RequestBody {
                description: routine.description.clone(),
                required: Some(true),
                content,
            })
        };

        let responses = self.generate_rpc_responses(routine)?;

        Ok(Operation {
            summary: Some(format!("Call {} function", routine.name)),
            description: routine.description.clone(),
            operation_id: format!("rpc_post_{}", routine.name),
            tags: vec![routine.schema.to_string()],
            parameters: vec![],
            request_body,
            responses,
            security: self.generate_operation_security(),
        })
    }

    fn generate_rpc_responses(&self, routine: &Routine) -> Result<Responses, Error> {
        let mut responses = HashMap::new();

        // Determine response schema based on return type
        let response_schema = self.routine_return_type_to_schema(routine)?;

        responses.insert(
            "200".to_string(),
            Response {
                description: "OK".to_string(),
                content: Some({
                    let mut c = HashMap::new();
                    c.insert(
                        "application/json".to_string(),
                        MediaTypeObject {
                            schema: Some(response_schema),
                            example: None,
                        },
                    );
                    c
                }),
                headers: None,
            },
        );

        Ok(Responses { responses })
    }

    fn routine_param_to_schema(
        &self,
        param: &crate::schema_cache::RoutineParam,
    ) -> Result<Schema, Error> {
        // Use type_max_length which includes length info, or fall back to pg_type
        let type_str = if param.type_max_length != param.pg_type {
            &param.type_max_length
        } else {
            &param.pg_type
        };
        self.pg_type_to_schema(type_str.as_str())
    }

    fn routine_return_type_to_schema(&self, routine: &Routine) -> Result<Schema, Error> {
        use crate::schema_cache::{PgType, ReturnType};

        match &routine.return_type {
            ReturnType::Single(PgType::Scalar(_)) => {
                // Scalar return type - return as object with value field
                let mut props = HashMap::new();
                props.insert("value".to_string(), Schema::string());
                Ok(Schema::object(props, vec![]))
            }
            ReturnType::SetOf(PgType::Scalar(_)) => {
                // Array of scalars
                Ok(Schema::array(Schema::string()))
            }
            ReturnType::Single(PgType::Composite(qi, _)) => {
                // Single composite (table row)
                let schema_name = format!("{}_{}", qi.schema, qi.name);
                Ok(Schema::ref_(&format!(
                    "#/components/schemas/{}",
                    schema_name
                )))
            }
            ReturnType::SetOf(PgType::Composite(qi, _)) => {
                // Array of composites
                let schema_name = format!("{}_{}", qi.schema, qi.name);
                Ok(Schema::array(Schema::ref_(&format!(
                    "#/components/schemas/{}",
                    schema_name
                ))))
            }
        }
    }

    fn can_read_table(&self, table: &Table) -> Result<bool, Error> {
        // Check if OpenAPI mode ignores privileges
        if self.config.openapi_mode == OpenApiMode::IgnorePrivileges {
            return Ok(true);
        }

        // Check actual PostgreSQL SELECT privilege
        Ok(table.readable)
    }

    fn can_execute_routine(&self, routine: &Routine) -> Result<bool, Error> {
        // Check if OpenAPI mode ignores privileges
        if self.config.openapi_mode == OpenApiMode::IgnorePrivileges {
            return Ok(true);
        }

        // Check actual PostgreSQL EXECUTE privilege
        Ok(routine.executable)
    }
}
