# Getting Started

PgREST is a standalone web server that turns your PostgreSQL database into a REST API. It automatically generates REST endpoints for your database tables, views, and functions, allowing you to interact with your data using standard HTTP methods.

## Prerequisites

- **PostgreSQL 17+** - A running PostgreSQL database server
- **Rust 1.91+** (if building from source) - For compiling PgREST
- **Release binary** (if using pre-built) - Download from releases

## Installation

### From Source

Build PgREST from source using Cargo:

```bash
# Clone the repository
git clone <repository-url>
cd pgrest

# Build the release binary
cargo build --release

# The binary will be at target/release/pgrest
```

Or install directly:

```bash
cargo install --path .
```

### From Releases

Download the pre-built binary for your platform from the releases page and place it in your PATH.

### Docker

PgREST includes a Dockerfile for containerized deployments:

```bash
docker build -t pgrest .
docker run -p 3000:3000 pgrest
```

See the [Deployment](deployment.md) section for more Docker configuration options.

## Quick Start

### 1. Prepare Your Database

Create a simple table in PostgreSQL:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL
);

INSERT INTO users (name, email) VALUES
    ('Alice', 'alice@example.com'),
    ('Bob', 'bob@example.com');
```

### 2. Configure PgREST

Create a configuration file `config.toml`:

```ini
db-uri = "postgresql://user:password@localhost:5432/mydb"
db-schemas = "public"
db-anon-role = "anon"
server-port = 3000
```

Or use environment variables:

```bash
export PGRST_DB_URI="postgresql://user:password@localhost:5432/mydb"
export PGRST_DB_SCHEMAS="public"
export PGRST_DB_ANON_ROLE="anon"
export PGRST_SERVER_PORT=3000
```

**Note:** Make sure the `anon` role exists in your database and has appropriate permissions:

```sql
CREATE ROLE anon;
GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON users TO anon;
```

### 3. Start the Server

```bash
# Using config file
./pgrest --config config.toml

# Or using environment variables
./pgrest

# Or with CLI overrides
./pgrest --db-uri "postgresql://..." --port 3000
```

### 4. Make Your First Request

Query the users table:

```bash
curl http://localhost:3000/users
```

You should receive a JSON response:

```json
[
  {"id": 1, "name": "Alice", "email": "alice@example.com"},
  {"id": 2, "name": "Bob", "email": "bob@example.com"}
]
```

### 5. Explore the API

Get the OpenAPI specification:

```bash
curl -H "Accept: application/openapi+json" http://localhost:3000/
```

This returns a complete OpenAPI 3.0 specification describing all available endpoints. You can use this with tools like Swagger UI or Redoc to explore and test the API interactively.

## Next Steps

- Read the [Configuration](configuration.md) guide to learn about all available settings
- Explore the [API Reference](api-reference.md) to understand how to query and modify data
- Check out the [How-tos](how-tos/authentication.md) for common tasks like authentication and CORS setup
- Review [Deployment](deployment.md) options for production environments
