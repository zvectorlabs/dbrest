# Advanced Topics

This section covers advanced features and configurations for PgREST.

## Schema Isolation

PgREST supports exposing multiple schemas while maintaining isolation between them.

### Multiple Schemas

Configure multiple schemas:

```ini
db-schemas = "api,public,analytics"
```

The first schema (`api`) becomes the default schema. Resources in other schemas are accessed with schema qualification:

```bash
# Default schema (api)
curl http://localhost:3000/users

# Other schemas
curl http://localhost:3000/public.users
curl http://localhost:3000/analytics.reports
```

### Schema Permissions

Each schema should have appropriate permissions:

```sql
-- Grant access to api schema
GRANT USAGE ON SCHEMA api TO anon;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO anon;

-- Grant access to public schema
GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO anon;
```

## Pre-Request Functions

Pre-request functions allow you to execute custom logic before each request.

### Creating a Pre-Request Function

```sql
CREATE OR REPLACE FUNCTION api.pre_request()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Set request-specific variables
  PERFORM set_config('request.jwt.role', current_setting('request.jwt.role', true), true);
  
  -- Log request
  INSERT INTO api.request_log (path, method, role)
  VALUES (
    current_setting('request.path', true),
    current_setting('request.method', true),
    current_setting('request.jwt.role', true)
  );
END;
$$;
```

### Configuring Pre-Request Function

```ini
db-pre-request = "api.pre_request"
```

### Available Request Variables

Pre-request functions can access:

- `request.path` - Request path
- `request.method` - HTTP method
- `request.jwt.role` - JWT role claim
- `request.jwt.claim.*` - Other JWT claims
- `request.headers.*` - Request headers

### Use Cases

- Request logging
- Setting session variables
- Custom authorization logic
- Request validation
- Rate limiting

## Computed Fields

Computed fields are virtual columns calculated from other columns or functions.

### Creating Computed Fields

```sql
-- Add computed field to table
ALTER TABLE users ADD COLUMN full_name TEXT 
  GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED;

-- Or use a function
CREATE FUNCTION user_full_name(u users) 
RETURNS TEXT AS $$
  SELECT u.first_name || ' ' || u.last_name;
$$ LANGUAGE sql STABLE;

ALTER TABLE users ADD COLUMN full_name TEXT;
COMMENT ON COLUMN users.full_name IS 
  'Computed field using user_full_name(users) function';
```

### Using Computed Fields

Computed fields appear in API responses:

```bash
curl "http://localhost:3000/users?select=id,full_name"
```

Response:

```json
[
  {"id": 1, "full_name": "Alice Smith"},
  {"id": 2, "full_name": "Bob Jones"}
]
```

### Function-Based Computed Fields

Functions can be used for more complex computations:

```sql
CREATE FUNCTION user_age(u users) 
RETURNS INTEGER AS $$
  SELECT EXTRACT(YEAR FROM AGE(u.birth_date))::INTEGER;
$$ LANGUAGE sql STABLE;

COMMENT ON COLUMN users.age IS 
  'Computed field using user_age(users) function';
```

## Aggregate Functions

Enable aggregate functions for statistical queries:

```ini
db-aggregates-enabled = true
```

### Using Aggregates

```bash
# Count
curl "http://localhost:3000/users?select=count"

# Average
curl "http://localhost:3000/users?select=avg(age)"

# Sum
curl "http://localhost:3000/orders?select=sum(total)"

# Multiple aggregates
curl "http://localhost:3000/users?select=count,avg(age),max(age),min(age)"
```

### Grouped Aggregates

```bash
# Group by column
curl "http://localhost:3000/users?select=role,count&group=role"
```

## NOTIFY Listener

PgREST can listen to PostgreSQL NOTIFY events for schema cache invalidation.

### Configuration

```ini
db-channel = "pgrst"
db-channel-enabled = true
```

### Triggering Schema Reload

Send a NOTIFY from PostgreSQL:

```sql
NOTIFY pgrst, 'reload schema';
```

This causes PgREST to reload its schema cache without restarting.

### Custom Channel

Use a custom channel name:

```ini
db-channel = "myapp_schema_reload"
```

Then notify:

```sql
NOTIFY myapp_schema_reload, 'reload schema';
```

### Use Cases

- Schema changes without restart
- Dynamic schema updates
- Multi-instance coordination

## Root Spec Function

Customize the root endpoint (`/`) response:

```sql
CREATE OR REPLACE FUNCTION api.root_spec()
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'name', 'My API',
    'version', '1.0.0',
    'description', 'Custom API description'
  );
$$;
```

Configure:

```ini
db-root-spec = "api.root_spec"
```

## Extra Search Path

Add schemas to the search path for function resolution:

```ini
db-extra-search-path = "extensions,utils"
```

This allows functions in these schemas to be called without schema qualification.

## Hoisted Transaction Settings

Settings that are "hoisted" to the transaction level:

```ini
db-hoisted-tx-settings = "statement_timeout,plan_filter.statement_cost_limit"
```

These settings are set at the transaction level and can be overridden per-request.

## Pre-Config Function

Execute a function before loading configuration:

```sql
CREATE OR REPLACE FUNCTION api.pre_config()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Custom configuration logic
  PERFORM set_config('app.settings.custom', 'value', false);
END;
$$;
```

Configure:

```ini
db-pre-config = "api.pre_config"
```

## Database Configuration

Load configuration from the database:

```ini
db-config = true
```

This allows storing some configuration in the database itself, useful for dynamic configuration.

## Unix Socket

Use Unix sockets instead of TCP:

```ini
server-unix-socket = "/var/run/pgrest.sock"
server-unix-socket-mode = 660
```

Benefits:
- Lower latency
- Better security (file permissions)
- No network overhead

## Connection Pool Tuning

### Pool Size

```ini
db-pool = 20
```

Adjust based on:
- Concurrent request load
- Database connection limits
- Server resources

### Pool Timeouts

```ini
db-pool-acquisition-timeout = 10
db-pool-max-lifetime = 1800
db-pool-max-idletime = 30
```

### Automatic Recovery

```ini
db-pool-automatic-recovery = true
```

Automatically recover from connection failures.

## Prepared Statements

Enable prepared statements for better performance:

```ini
db-prepared-statements = true
```

Prepared statements:
- Reduce parsing overhead
- Improve performance for repeated queries
- May use more memory

## Max Rows Limit

Set a global limit on returned rows:

```ini
db-max-rows = 1000
```

Prevents accidentally returning huge result sets.

## Plan Explanation

Enable EXPLAIN output for debugging:

```ini
db-plan-enabled = true
```

Returns query execution plans in responses (development only).

## Transaction End Behavior

Control transaction commit/rollback behavior:

```ini
db-tx-end = "commit"
```

Options:
- `commit` - Commit transactions (default)
- `commit-allow-override` - Commit but allow Prefer header override
- `rollback` - Rollback all transactions (testing)
- `rollback-allow-override` - Rollback but allow override

## Best Practices

1. **Use pre-request functions** for cross-cutting concerns
2. **Enable aggregates** only when needed
3. **Tune connection pool** based on load
4. **Use Unix sockets** for local connections
5. **Monitor schema cache** reloads via NOTIFY
6. **Set max rows** to prevent accidents
7. **Use computed fields** for derived data
8. **Configure hoisted settings** for performance
