//! Application module — HTTP server, routing, and request handling.
//!
//! Ties together all lower-level modules (api_request, plan, query, auth,
//! schema_cache) into a running HTTP server powered by axum.

pub mod admin;
pub mod builder;
pub mod handlers;
pub mod router;
pub mod server;
pub mod state;
pub mod streaming;

pub use builder::{Datasource, DbrestApp, DbrestRouters};
pub use router::create_router;
pub use server::start_server;
pub use state::AppState;
