# dbrest

A high-performance REST API server for PostgreSQL and SQLite, written in Rust.

dbrest serves a fully RESTful API from any existing database. Point it at a PostgreSQL or SQLite database and it automatically generates CRUD endpoints, handles authentication, and returns JSON — no application code required.

## Features

- **Multi-Database** — First-class support for both PostgreSQL and SQLite through a pluggable backend architecture
- **High Performance** — Built with Rust, Tokio, and Axum for maximum throughput and minimal latency
- **Zero Boilerplate** — Automatically generates REST endpoints from your database schema
- **JWT Authentication** — Secure your API with JSON Web Tokens and role-based access control
- **OpenAPI Documentation** — Auto-generated API documentation from your database schema
- **Filtering & Pagination** — Rich query syntax with range headers for pagination
- **Resource Embedding** — Follow foreign key relationships in a single request
- **Connection Pooling** — Efficient database access via configurable connection pools
- **Horizontal Scaling** — Stateless design allows easy horizontal scaling
- **Streaming Responses** — Large result sets streamed automatically above a configurable threshold

## Quick Start

### Prerequisites

- Rust 1.91+
- PostgreSQL 14+ or SQLite 3.35+

### Usage

```bash
# PostgreSQL
dbrest --db-uri postgres://user:pass@localhost/mydb

# SQLite
dbrest --db-uri sqlite:path/to/db.sqlite

# SQLite in-memory
dbrest --db-uri sqlite::memory:

# From config file
dbrest --config config.toml
```

### Environment Variables

| Variable | Description |
|---|---|
| `DBREST_CONFIG` | Path to configuration file |
| `DBREST_DB_URI` | Database connection URI |
| `DBREST_SERVER_PORT` | Server bind port |

### Docker

```bash
docker build -t dbrest .
docker run -p 3000:3000 \
  -e DBREST_DB_URI=postgres://user:pass@host/db \
  dbrest
```

## Architecture

dbrest is organized as a Cargo workspace with a pluggable backend design:

| Crate | Description |
|---|---|
| `dbrest-core` | Database-agnostic core: traits, query builder, planner, HTTP layer |
| `dbrest-postgres` | PostgreSQL backend: executor, dialect, schema introspection |
| `dbrest-sqlite` | SQLite backend: executor, dialect, schema introspection |

### Request Pipeline

```
HTTP Request
  -> Auth (JWT validation)
  -> API Request parsing (filters, ordering, pagination)
  -> Action Plan (read / mutate / call)
  -> SQL Generation (dialect-specific)
  -> Execution (within transaction)
  -> JSON Response
```

The core generates a dialect-neutral query plan, then the backend-specific `SqlDialect` implementation produces the actual SQL. This keeps database differences isolated to the dialect and executor layers.

## Backend Capabilities

### Feature Matrix

| Feature | PostgreSQL | SQLite |
|---|---|---|
| Basic CRUD (GET, POST, PATCH, DELETE) | Yes | Yes |
| Filtering (eq, gt, lt, like, in, etc.) | Yes | Yes |
| Ordering and Pagination | Yes | Yes |
| Resource Embedding (foreign keys) | Yes | Yes |
| Views | Yes | Yes |
| JSON Aggregation | `json_agg()` | `json_group_array()` + `json_object()` |
| Type Casting | `::type` syntax | `CAST(expr AS type)` |
| Transactions | Native | WAL mode |
| DML in CTEs | Yes | No (temp table pattern) |
| Stored Functions (RPC) | Yes | Not supported |
| Multiple Schemas | Yes | Single ("main") |
| Role-Based Access Control | Native (`SET ROLE`) | Application-level |
| Row-Level Security | Via PostgreSQL RLS | Not available |
| Session Variables | `set_config()` | Temp table `_dbrest_vars` |
| Schema Change Notifications | `LISTEN`/`NOTIFY` | Not available |
| Full-Text Search | Yes | Not yet (FTS5 planned) |
| LATERAL Joins | Yes | Correlated subqueries |
| Computed Fields | Yes | Not supported |
| Array Types | Yes | Not supported |

### PostgreSQL Backend

The PostgreSQL backend delegates as much work as possible to the database:

- **JSON serialization** happens in PostgreSQL via `json_agg()` and `row_to_json()`, minimizing data transfer
- **Authorization** uses PostgreSQL's native role system — dbrest sets the role with `SET ROLE` per transaction so the database enforces permissions
- **Session variables** are set via `set_config()` and available to RLS policies, triggers, and functions through `current_setting()`
- **Schema cache** is automatically reloaded when the database sends a `NOTIFY dbrst` event (e.g., after DDL changes)
- **Stored functions** are exposed as RPC endpoints under `/rpc/function_name`
- **CTE-wrapped queries** allow mutations with RETURNING clauses inside a single query

### SQLite Backend

The SQLite backend provides the same REST API with implementation differences:

- **WAL mode** is enabled automatically for better read concurrency
- **Foreign keys** are enforced via `PRAGMA foreign_keys = ON`
- **JSON aggregation** uses SQLite's `json_group_array()` with explicit `json_object()` construction (SQLite has no `row_to_json()` equivalent)
- **Mutations** use a split execution pattern since SQLite does not support DML in CTEs — the mutation runs first with RETURNING, results go into a temp table (`_dbrst_mut`), then the aggregation query reads from it
- **Session variables** are stored in a temp table `_dbrest_vars(key, val)` using `INSERT OR REPLACE`, retrieved via subqueries
- **Schema introspection** queries `sqlite_master` and `PRAGMA table_info()` / `PRAGMA foreign_key_list()` to discover tables, columns, and relationships
- **Type normalization** maps SQLite's flexible type system to standard affinities (INTEGER, REAL, TEXT, BLOB) for consistent API behavior
- **Minimum version**: SQLite 3.35+ (required for RETURNING clause support)

## API Examples

```bash
# List all users
curl http://localhost:3000/users

# Filter with query params
curl "http://localhost:3000/users?status=eq.active&order=name.asc"

# Select specific columns
curl "http://localhost:3000/users?select=id,name,email"

# Pagination
curl "http://localhost:3000/users?limit=10&offset=20"

# Embed related resources
curl "http://localhost:3000/posts?select=*,comments(*),users(name)"

# Insert
curl -X POST http://localhost:3000/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'

# Update
curl -X PATCH "http://localhost:3000/users?id=eq.1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Smith"}'

# Delete
curl -X DELETE "http://localhost:3000/users?id=eq.1"

# Call a stored function (PostgreSQL only)
curl -X POST http://localhost:3000/rpc/my_function \
  -H "Content-Type: application/json" \
  -d '{"arg1": "value"}'
```

## Security

dbrest handles authentication via JSON Web Tokens. On PostgreSQL, it delegates authorization to the database's native role-based access control — during a request, the server assumes the identity of the authenticated user and cannot exceed their permissions. On SQLite, authentication is handled at the application level since SQLite has no role system.

Configure JWT with:
- `jwt_secret` — HMAC secret or RSA/ECDSA public key
- `db_anon_role` — role used for unauthenticated requests (PostgreSQL)

## Performance

Key design decisions for speed:

- Compiled, async Rust with lightweight Tokio tasks
- Delegates JSON serialization to the database where possible
- Efficient binary protocol via SQLx
- Configurable connection pooling with acquire timeout, max lifetime, and idle timeout
- Large responses streamed automatically above a configurable threshold
- Stateless architecture for horizontal scaling

## Configuration

dbrest reads configuration from a TOML file, environment variables (`DBRST_*` prefix), or CLI flags. Key settings:

| Setting | Default | Description |
|---|---|---|
| `db_uri` | — | Database connection string |
| `db_schemas` | `["public"]` | Schemas to expose (PostgreSQL) |
| `db_anon_role` | — | Role for unauthenticated requests |
| `db_pool_size` | `10` | Connection pool size |
| `db_max_rows` | — | Maximum rows per response |
| `db_channel` | `dbrst` | NOTIFY channel for schema reload |
| `server_host` | `0.0.0.0` | Bind address |
| `server_port` | `3000` | Bind port |
| `jwt_secret` | — | JWT signing secret |
| `openapi_mode` | `follow-privileges` | OpenAPI spec generation mode |

## Versioning

dbrest supports API versioning through database schemas (PostgreSQL), allowing you to expose tables and views without coupling API consumers to underlying table structures. Use the `Accept-Profile` and `Content-Profile` headers to target specific schemas.

## License

Apache License 2.0 — see [LICENSE](LICENSE) for details.
