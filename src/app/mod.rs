//! Application module — HTTP server, routing, and request handling.
//!
//! Ties together all lower-level modules (api_request, plan, query, auth,
//! schema_cache) into a running HTTP server powered by axum.
//!
//! # Architecture
//!
//! ```text
//! reqwest/browser
//!       │
//!       ▼
//! axum::Router  (CORS, compression, auth middleware)
//!       │
//!       ├─ GET  /:resource      → handlers::read_handler
//!       ├─ HEAD /:resource      → handlers::read_handler (headers_only)
//!       ├─ POST /:resource      → handlers::create_handler
//!       ├─ PATCH /:resource     → handlers::update_handler
//!       ├─ DELETE /:resource    → handlers::delete_handler
//!       ├─ POST /rpc/:fn       → handlers::rpc_post_handler
//!       ├─ GET  /rpc/:fn       → handlers::rpc_get_handler
//!       └─ OPTIONS /:resource  → handlers::options_handler
//! ```

pub mod admin;
pub mod handlers;
pub mod listener;
pub mod router;
pub mod server;
pub mod state;
pub mod streaming;

pub use router::create_router;
pub use server::start_server;
pub use state::AppState;
