# Configuration

PgREST can be configured using a configuration file, environment variables, or command-line arguments. Configuration values follow a precedence order where later sources override earlier ones.

## Configuration Precedence

1. **Command-line arguments** (highest priority)
2. **Environment variables** (`PGRST_*` prefix)
3. **Configuration file** (key=value format)
4. **Default values** (lowest priority)

## Configuration File Format

Configuration files use a simple `key=value` format with support for comments:

```ini
# This is a comment
-- This is also a comment

# Database connection
db-uri = "postgresql://user:password@localhost:5432/mydb"
db-schemas = "public,api"
db-anon-role = "anon"

# Server settings
server-port = 3000
server-host = "0.0.0.0"

# JWT authentication
jwt-secret = "your-secret-key-here"
```

Values can be quoted with single or double quotes, or left unquoted. Comments start with `#` or `--`.

## Environment Variables

Environment variables use the `PGRST_` prefix. The prefix is stripped and underscores are converted to hyphens:

```bash
export PGRST_DB_URI="postgresql://user:password@localhost:5432/mydb"
export PGRST_DB_SCHEMAS="public,api"
export PGRST_SERVER_PORT=3000
```

## Command-Line Arguments

```bash
pgrest --config config.toml --db-uri "postgresql://..." --port 3000
```

Available CLI flags:
- `--config`, `-c` - Path to configuration file (env: `PGREST_CONFIG`)
- `--db-uri` - Database connection URI (env: `PGREST_DB_URI`)
- `--port`, `-p` - Server bind port (env: `PGREST_SERVER_PORT`)

## Configuration Reference

### Database Settings

| Config Key | Environment Variable | Type | Default | Description |
|------------|---------------------|------|---------|-------------|
| `db-uri` | `PGRST_DB_URI` | String | `postgresql://` | PostgreSQL connection URI |
| `db-schemas` | `PGRST_DB_SCHEMAS` | Comma-separated list | `public` | Schemas to expose (first is default) |
| `db-anon-role` | `PGRST_DB_ANON_ROLE` | String | `None` | Anonymous role for unauthenticated requests |
| `db-pool` | `PGRST_DB_POOL` | Integer | `10` | Connection pool size |
| `db-pool-acquisition-timeout` | `PGRST_DB_POOL_ACQUISITION_TIMEOUT` | Integer (seconds) | `10` | Pool acquisition timeout |
| `db-pool-max-lifetime` | `PGRST_DB_POOL_MAX_LIFETIME` | Integer (seconds) | `1800` | Maximum connection lifetime |
| `db-pool-max-idletime` | `PGRST_DB_POOL_MAX_IDLETIME` | Integer (seconds) | `30` | Maximum idle time before closing connection |
| `db-pool-automatic-recovery` | `PGRST_DB_POOL_AUTOMATIC_RECOVERY` | Boolean | `true` | Enable automatic pool recovery |
| `db-prepared-statements` | `PGRST_DB_PREPARED_STATEMENTS` | Boolean | `true` | Use prepared statements |
| `db-pre-request` | `PGRST_DB_PRE_REQUEST` | Qualified identifier | `None` | Pre-request function to call (schema.name) |
| `db-root-spec` | `PGRST_DB_ROOT_SPEC` | Qualified identifier | `None` | Root spec function for `/` endpoint |
| `db-extra-search-path` | `PGRST_DB_EXTRA_SEARCH_PATH` | Comma-separated list | `public` | Extra schemas for search_path |
| `db-hoisted-tx-settings` | `PGRST_DB_HOISTED_TX_SETTINGS` | Comma-separated list | `statement_timeout,plan_filter.statement_cost_limit,default_transaction_isolation` | Transaction settings to hoist |
| `db-max-rows` | `PGRST_DB_MAX_ROWS` | Integer | `None` | Maximum rows to return (no limit if unset) |
| `db-plan-enabled` | `PGRST_DB_PLAN_ENABLED` | Boolean | `false` | Enable EXPLAIN output |
| `db-tx-end` | `PGRST_DB_TX_END` | String | `commit` | Transaction end behavior: `commit`, `commit-allow-override`, `rollback`, `rollback-allow-override` |
| `db-tx-read-isolation` | `PGRST_DB_TX_READ_ISOLATION` | String | `read-committed` | Isolation level for read transactions: `read-committed`, `repeatable-read`, `serializable` |
| `db-tx-write-isolation` | `PGRST_DB_TX_WRITE_ISOLATION` | String | `read-committed` | Isolation level for write transactions: `read-committed`, `repeatable-read`, `serializable` |
| `db-aggregates-enabled` | `PGRST_DB_AGGREGATES_ENABLED` | Boolean | `false` | Enable aggregate functions |
| `db-config` | `PGRST_DB_CONFIG` | Boolean | `true` | Load config from database |
| `db-pre-config` | `PGRST_DB_PRE_CONFIG` | Qualified identifier | `None` | Pre-config function to call |
| `db-channel` | `PGRST_DB_CHANNEL` | String | `pgrst` | NOTIFY channel name |
| `db-channel-enabled` | `PGRST_DB_CHANNEL_ENABLED` | Boolean | `true` | Enable NOTIFY listener |

### Server Settings

| Config Key | Environment Variable | Type | Default | Description |
|------------|---------------------|------|---------|-------------|
| `server-host` | `PGRST_SERVER_HOST` | String | `!4` | Server bind host (`!4` = IPv4, `!6` = IPv6) |
| `server-port` | `PGRST_SERVER_PORT` | Integer | `3000` | Server bind port |
| `server-unix-socket` | `PGRST_SERVER_UNIX_SOCKET` | Path | `None` | Unix socket path (if set, overrides host/port) |
| `server-unix-socket-mode` | `PGRST_SERVER_UNIX_SOCKET_MODE` | Octal | `660` | Unix socket file mode |
| `server-cors-allowed-origins` | `PGRST_SERVER_CORS_ALLOWED_ORIGINS` | Comma-separated list | `None` | CORS allowed origins (allows all if unset) |
| `server-trace-header` | `PGRST_SERVER_TRACE_HEADER` | String | `None` | Trace header name |
| `server-timing-enabled` | `PGRST_SERVER_TIMING_ENABLED` | Boolean | `false` | Enable Server-Timing header |

### Admin Server Settings

| Config Key | Environment Variable | Type | Default | Description |
|------------|---------------------|------|---------|-------------|
| `admin-server-host` | `PGRST_ADMIN_SERVER_HOST` | String | `!4` | Admin server bind host |
| `admin-server-port` | `PGRST_ADMIN_SERVER_PORT` | Integer | `None` | Admin server bind port (disabled if unset) |

### JWT Settings

| Config Key | Environment Variable | Type | Default | Description |
|------------|---------------------|------|---------|-------------|
| `jwt-secret` | `PGRST_JWT_SECRET` | String | `None` | JWT secret (or JWKS JSON). Must be at least 32 characters |
| `jwt-secret-is-base64` | `PGRST_JWT_SECRET_IS_BASE64` | Boolean | `false` | JWT secret is base64 encoded |
| `jwt-aud` | `PGRST_JWT_AUD` | String | `None` | Expected JWT audience |
| `jwt-role-claim-key` | `PGRST_JWT_ROLE_CLAIM_KEY` | JSON path | `role` | Path to role claim in JWT (e.g., `role`, `user.role`) |
| `jwt-cache-max-entries` | `PGRST_JWT_CACHE_MAX_ENTRIES` | Integer | `1000` | JWT cache maximum entries |

### Logging Settings

| Config Key | Environment Variable | Type | Default | Description |
|------------|---------------------|------|---------|-------------|
| `log-level` | `PGRST_LOG_LEVEL` | String | `error` | Log level: `crit`, `error`, `warn`, `info`, `debug` |
| `log-query` | `PGRST_LOG_QUERY` | Boolean | `false` | Log SQL queries |

### OpenAPI Settings

| Config Key | Environment Variable | Type | Default | Description |
|------------|---------------------|------|---------|-------------|
| `openapi-mode` | `PGRST_OPENAPI_MODE` | String | `follow-privileges` | OpenAPI generation mode: `follow-privileges`, `ignore-privileges`, `disabled` |
| `openapi-security-active` | `PGRST_OPENAPI_SECURITY_ACTIVE` | Boolean | `false` | Include security definitions in OpenAPI |
| `openapi-server-proxy-uri` | `PGRST_OPENAPI_SERVER_PROXY_URI` | String | `None` | OpenAPI server proxy URI |

### Streaming Settings

| Config Key | Environment Variable | Type | Default | Description |
|------------|---------------------|------|---------|-------------|
| `server-streaming-enabled` | `PGRST_SERVER_STREAMING_ENABLED` | Boolean | `true` | Enable streaming responses for large result sets |
| `server-streaming-threshold` | `PGRST_SERVER_STREAMING_THRESHOLD` | Integer (bytes) | `10485760` (10MB) | Threshold in bytes for streaming |

### App Settings

Custom application settings can be set using the `app.settings.*` prefix:

```ini
app.settings.custom-key = "custom-value"
app.settings.another-setting = "value"
```

These are stored in `app_settings` and can be accessed by application code.

## Configuration Examples

### Minimal Configuration

```ini
db-uri = "postgresql://user:password@localhost:5432/mydb"
db-schemas = "public"
db-anon-role = "anon"
```

### Production Configuration

```ini
# Database
db-uri = "postgresql://user:password@localhost:5432/mydb"
db-schemas = "api,public"
db-anon-role = "anon"
db-pool = 20
db-pool-max-lifetime = 3600

# Server
server-host = "0.0.0.0"
server-port = 3000
server-cors-allowed-origins = "https://example.com,https://app.example.com"

# JWT
jwt-secret = "your-very-long-secret-key-at-least-32-characters"
jwt-aud = "api.example.com"

# Logging
log-level = "info"
log-query = false

# OpenAPI
openapi-mode = "follow-privileges"
openapi-security-active = true

# Transaction isolation
db-tx-read-isolation = "repeatable-read"
db-tx-write-isolation = "serializable"
```

### Using Environment Variables

```bash
export PGRST_DB_URI="postgresql://user:password@localhost:5432/mydb"
export PGRST_DB_SCHEMAS="api,public"
export PGRST_DB_ANON_ROLE="anon"
export PGRST_SERVER_PORT=3000
export PGRST_JWT_SECRET="your-secret-key"
export PGRST_LOG_LEVEL="info"

./pgrest
```

## Validation

PgREST validates configuration on startup and will exit with an error if:

- `db-schemas` is empty
- `db-schemas` includes system schemas (`pg_catalog`, `information_schema`)
- `admin-server-port` equals `server-port`
- `jwt-secret` is less than 32 characters (unless it's JWKS JSON)
- `db-pool` is zero

## Qualified Identifiers

Some settings accept qualified identifiers in the format `schema.name` or just `name` (defaults to the first schema):

- `db-pre-request`: `api.auth_hook` or `auth_hook`
- `db-root-spec`: `api.root_spec` or `root_spec`
- `db-pre-config`: `api.pre_config` or `pre_config`

## Boolean Values

Boolean values accept multiple formats:
- `true`, `yes`, `on`, `1` → true
- `false`, `no`, `off`, `0` → false

## Comma-Separated Lists

List values can be comma-separated with optional spaces:
- `db-schemas = "api,public"`
- `db-schemas = "api, public"`
- `server-cors-allowed-origins = "https://example.com,https://app.example.com"`
