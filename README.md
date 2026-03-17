# dbrest

A high-performance REST API server for PostgreSQL, written in Rust.

dbrest serves a fully RESTful API from any existing PostgreSQL database. It provides a cleaner, more standards-compliant, faster API than you are likely to write from scratch.

## Features

- **High Performance** — Built with Rust, Tokio, and Axum for maximum throughput and minimal latency
- **Zero Boilerplate** — Automatically generates REST endpoints from your database schema
- **JWT Authentication** — Secure your API with JSON Web Tokens and PostgreSQL role-based access control
- **OpenAPI Documentation** — Auto-generated API documentation from your database schema
- **Filtering & Pagination** — Rich query syntax with range headers for pagination
- **JSON Responses** — Serializes responses directly from PostgreSQL for speed
- **Connection Pooling** — Efficient database access via connection pooling
- **Horizontal Scaling** — Stateless design allows easy horizontal scaling

## Quick Start

### Prerequisites

- Rust 1.91+
- PostgreSQL

### Usage

```bash
dbrest --help
```

### Docker

```bash
docker build -t dbrest .
docker run -p 3000:3000 dbrest
```

## Architecture

dbrest is organized as a Cargo workspace:

| Crate | Description |
|---|---|
| `dbrest-core` | Shared traits, types, and interfaces |
| `dbrest-postgres` | PostgreSQL backend implementation |
| `dbrest-sqlite` | SQLite backend implementation |

## Performance

dbrest is designed for speed. Key design decisions that contribute to performance:

- Compiled, async Rust with lightweight Tokio tasks
- Delegates work to PostgreSQL (JSON serialization, validation, authorization)
- Efficient binary protocol via SQLx
- Stateless architecture for horizontal scaling
- Connection pooling with configurable limits

## Security

dbrest handles authentication via JSON Web Tokens and delegates authorization to PostgreSQL's role-based access control. This ensures a single declarative source of truth for security. During a request, the server assumes the identity of the authenticated user and cannot exceed their permissions.

## Versioning

dbrest supports API versioning through database schemas, allowing you to expose tables and views without coupling your API consumers to underlying table structures.

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
