# Using Prefer Headers

The `Prefer` header allows you to customize request behavior and response format.

## Plurality

### Singular Response

Return a single object instead of an array when filtering to one result:

```bash
curl -H "Prefer: plurality=singular" "http://localhost:3000/users?id=eq.1"
```

**Response** (singular):
```json
{"id": 1, "name": "Alice", "email": "alice@example.com"}
```

**Without Prefer** (plural):
```json
[{"id": 1, "name": "Alice", "email": "alice@example.com"}]
```

### Use Cases

- When you know exactly one result will be returned
- Simplifies client code (no array indexing)
- Returns `404 Not Found` if no results match

## Count Strategies

Control how row counts are calculated and included in the `Content-Range` header.

### Exact Count

Count all matching rows (slower but accurate):

```bash
curl -H "Prefer: count=exact" http://localhost:3000/users
```

**Response Header**:
```
Content-Range: 0-9/100
```

### Planned Count

Use PostgreSQL's query planner estimate (faster):

```bash
curl -H "Prefer: count=planned" http://localhost:3000/users
```

**Response Header**:
```
Content-Range: 0-9/95
```

### Estimated Count

Use a quick estimate (fastest):

```bash
curl -H "Prefer: count=estimated" http://localhost:3000/users
```

**Response Header**:
```
Content-Range: 0-9/~
```

The `~` indicates an estimate.

### No Count

Omit count entirely:

```bash
curl -H "Prefer: count=none" http://localhost:3000/users
```

**Response Header**:
```
Content-Range: 0-9/*
```

## Max Affected Rows

Validate that write operations don't affect more rows than expected:

### Limit Updates

```bash
curl -X PATCH \
  -H "Prefer: max-affected=10" \
  "http://localhost:3000/users?active=is.false" \
  -H "Content-Type: application/json" \
  -d '{"active": true}'
```

If more than 10 rows would be affected, the request fails with `416 Range Not Satisfiable`.

### Limit Deletes

```bash
curl -X DELETE \
  -H "Prefer: max-affected=5" \
  "http://localhost:3000/users?created_at=lt.2020-01-01"
```

### Use Cases

- Prevent accidental bulk updates
- Ensure data integrity
- Implement safety checks in applications

## Combining Preferences

You can combine multiple preferences:

```bash
curl -H "Prefer: plurality=singular,count=exact" \
  "http://localhost:3000/users?id=eq.1"
```

## Preference-Applied Header

The server echoes back applied preferences in the `Preference-Applied` header:

```http
HTTP/1.1 200 OK
Preference-Applied: count=exact
Content-Range: 0-9/100
```

## Examples

### Get Single User with Exact Count

```bash
curl -H "Prefer: plurality=singular,count=exact" \
  "http://localhost:3000/users?email=eq.alice@example.com"
```

### Safe Bulk Update

```bash
curl -X PATCH \
  -H "Prefer: max-affected=100" \
  "http://localhost:3000/users?role=eq.guest" \
  -H "Content-Type: application/json" \
  -d '{"role": "user"}'
```

### Fast Pagination with Estimated Count

```bash
curl -H "Prefer: count=estimated" \
  "http://localhost:3000/users?limit=20&offset=0"
```

## Error Handling

### Max Affected Exceeded

If `max-affected` is exceeded:

```http
HTTP/1.1 416 Range Not Satisfiable
Content-Range: */100

{
  "code": "DBRST116",
  "message": "Maximum affected rows exceeded",
  "details": "Request would affect 100 rows, but max-affected=10"
}
```

### Invalid Preference

Invalid preference values are ignored (no error, just not applied).

## Best Practices

1. **Use `plurality=singular`** when you expect exactly one result
2. **Use `count=exact`** when accuracy is important
3. **Use `count=estimated`** for better performance on large datasets
4. **Always use `max-affected`** for write operations to prevent accidents
5. **Check `Preference-Applied`** header to verify preferences were applied

## Performance Considerations

- **`count=exact`**: Requires full query execution (slowest)
- **`count=planned`**: Uses query planner estimate (faster)
- **`count=estimated`**: Quick estimate (fastest)
- **`plurality=singular`**: No performance impact
- **`max-affected`**: Adds validation overhead (minimal)
