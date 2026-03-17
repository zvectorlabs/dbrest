# Transaction Isolation Levels

dbrest allows you to configure transaction isolation levels separately for read and write operations, providing control over data consistency and concurrency.

## Isolation Levels

PostgreSQL supports three isolation levels:

- **Read Committed** (default) - Each query sees only data committed before it began
- **Repeatable Read** - All queries in a transaction see the same snapshot
- **Serializable** - Highest isolation, prevents all anomalies

## Configuration

### Read Operations

Set isolation level for GET/HEAD requests:

```ini
db-tx-read-isolation = "repeatable-read"
```

Options:
- `read-committed` (default)
- `repeatable-read`
- `serializable`

### Write Operations

Set isolation level for POST/PATCH/PUT/DELETE requests:

```ini
db-tx-write-isolation = "serializable"
```

Options:
- `read-committed` (default)
- `repeatable-read`
- `serializable`

### Environment Variables

```bash
export DBREST_DB_TX_READ_ISOLATION="repeatable-read"
export DBREST_DB_TX_WRITE_ISOLATION="serializable"
```

## Use Cases

### Read Committed (Default)

Best for:
- High concurrency applications
- When stale reads are acceptable
- Most web applications

```ini
db-tx-read-isolation = "read-committed"
db-tx-write-isolation = "read-committed"
```

### Repeatable Read

Best for:
- Reports that need consistent snapshots
- Analytics queries
- When you need to prevent non-repeatable reads

```ini
db-tx-read-isolation = "repeatable-read"
db-tx-write-isolation = "read-committed"
```

### Serializable

Best for:
- Financial transactions
- Critical data integrity requirements
- When you need to prevent all concurrency anomalies

```ini
db-tx-read-isolation = "repeatable-read"
db-tx-write-isolation = "serializable"
```

## Examples

### Configuration File

```ini
# Production configuration
db-tx-read-isolation = "repeatable-read"
db-tx-write-isolation = "serializable"

# Development configuration (more permissive)
db-tx-read-isolation = "read-committed"
db-tx-write-isolation = "read-committed"
```

### Different Levels for Reads and Writes

Common pattern: stricter isolation for writes, more permissive for reads:

```ini
# Allow concurrent reads, ensure write consistency
db-tx-read-isolation = "read-committed"
db-tx-write-isolation = "serializable"
```

## How It Works

### Read Operations

When a GET request is made:

1. Transaction starts with `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ`
2. Query executes
3. Transaction commits

### Write Operations

When a POST/PATCH/PUT/DELETE request is made:

1. Transaction starts with `SET TRANSACTION ISOLATION LEVEL SERIALIZABLE`
2. Pre-request function executes (if configured)
3. Main query executes
4. Transaction commits (or rolls back on error)

## Performance Impact

### Read Committed

- **Performance**: Best (lowest overhead)
- **Concurrency**: Highest
- **Consistency**: Lowest

### Repeatable Read

- **Performance**: Good
- **Concurrency**: Moderate
- **Consistency**: Good

### Serializable

- **Performance**: Lower (more locking)
- **Concurrency**: Lower (more conflicts)
- **Consistency**: Highest

## Transaction Conflicts

### Serialization Failures

With `serializable` isolation, you may encounter serialization failures:

```json
{
  "code": "40001",
  "message": "Serialization failure",
  "details": "Could not serialize access due to concurrent update"
}
```

**Solution**: Retry the request (idempotent operations recommended).

### Deadlocks

Higher isolation levels can lead to deadlocks:

```json
{
  "code": "40P01",
  "message": "Deadlock detected",
  "details": "Process waits for lock"
}
```

**Solution**: Retry with exponential backoff.

## Best Practices

1. **Start with defaults**: Use `read-committed` unless you have specific requirements
2. **Profile first**: Measure performance impact before changing isolation levels
3. **Use appropriate levels**: Don't use `serializable` unless necessary
4. **Handle conflicts**: Implement retry logic for serialization failures
5. **Monitor deadlocks**: Watch for deadlock errors in production

## Testing Isolation Levels

### Test Repeatable Reads

```bash
# Transaction 1: Start read
curl "http://localhost:3000/users?id=eq.1" &

# Transaction 2: Update (in another terminal)
curl -X PATCH "http://localhost:3000/users?id=eq.1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Updated"}'

# Transaction 1: Read again (should see same data with repeatable-read)
curl "http://localhost:3000/users?id=eq.1"
```

### Test Serializable Writes

```bash
# Concurrent updates (may cause serialization failure)
curl -X PATCH "http://localhost:3000/users?id=eq.1" \
  -H "Content-Type: application/json" \
  -d '{"balance": 100}' &

curl -X PATCH "http://localhost:3000/users?id=eq.1" \
  -H "Content-Type: application/json" \
  -d '{"balance": 200}'
```

## Configuration Examples

### E-Commerce Application

```ini
# Read: Allow concurrent browsing
db-tx-read-isolation = "read-committed"

# Write: Ensure order consistency
db-tx-write-isolation = "serializable"
```

### Reporting Application

```ini
# Read: Consistent reports
db-tx-read-isolation = "repeatable-read"

# Write: Standard consistency
db-tx-write-isolation = "read-committed"
```

### High-Concurrency API

```ini
# Both: Maximum concurrency
db-tx-read-isolation = "read-committed"
db-tx-write-isolation = "read-committed"
```
