//! Load test scenario definitions
//!
//! Defines different load test scenarios aligned with PostgREST's approach.

use serde_json::json;

use crate::load_tester::{LoadTestScenario, RequestType};

/// Mixed workload scenario (80% reads, 20% writes)
/// Aligned with PostgREST's "mixed" scenario
pub fn mixed_scenario() -> LoadTestScenario {
    LoadTestScenario {
        name: "mixed".to_string(),
        requests: vec![
            // 40% - Simple GET
            (
                0.4,
                RequestType::Get {
                    path: "/users?limit=20".to_string(),
                },
            ),
            // 30% - Embedded GET
            (
                0.3,
                RequestType::Get {
                    path: "/users?select=id,name,posts(*)&limit=10".to_string(),
                },
            ),
            // 10% - RPC GET
            (
                0.1,
                RequestType::Get {
                    path: "/rpc/get_active_users".to_string(),
                },
            ),
            // 10% - POST (single)
            (
                0.1,
                RequestType::Post {
                    path: "/posts".to_string(),
                    body: json!({
                        "title": "Test Post",
                        "body": "Test body",
                        "user_id": 1
                    }),
                },
            ),
            // 5% - PATCH
            (
                0.05,
                RequestType::Patch {
                    path: "/users?id=eq.1".to_string(),
                    body: json!({"name": "Updated"}),
                },
            ),
            // 5% - DELETE
            (
                0.05,
                RequestType::Delete {
                    path: "/temp_table?id=gt.1000".to_string(),
                },
            ),
        ],
    }
}

/// Error scenarios (misspelled paths, permission denied)
/// Aligned with PostgREST's "errors" scenario
pub fn errors_scenario() -> LoadTestScenario {
    LoadTestScenario {
        name: "errors".to_string(),
        requests: vec![
            // Misspelled table
            (
                0.33,
                RequestType::Get {
                    path: "/userz".to_string(),
                },
            ),
            // Misspelled embed
            (
                0.33,
                RequestType::Get {
                    path: "/users?select=*,postx(*)".to_string(),
                },
            ),
            // Non-existent RPC
            (
                0.34,
                RequestType::Get {
                    path: "/rpc/nonexistent".to_string(),
                },
            ),
        ],
    }
}

/// Streaming large datasets scenario
/// PgREST-specific feature
pub fn streaming_scenario() -> LoadTestScenario {
    LoadTestScenario {
        name: "streaming".to_string(),
        requests: vec![
            // Large responses that should trigger streaming
            (
                1.0,
                RequestType::Get {
                    path: "/users?limit=1000".to_string(),
                },
            ),
        ],
    }
}
