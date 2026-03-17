# API Reference

dbrest automatically generates REST endpoints for your PostgreSQL database tables, views, and functions. The API follows RESTful conventions and uses standard HTTP methods.

## Endpoints

### Root Endpoint

**GET /** - Returns OpenAPI specification or schema information

- **Without Accept header**: Returns JSON listing available tables/views
- **With `Accept: application/openapi+json`**: Returns full OpenAPI 3.0 specification

```bash
# Get OpenAPI spec
curl -H "Accept: application/openapi+json" http://localhost:3000/

# Get schema info
curl http://localhost:3000/
```

**OPTIONS /** - CORS preflight for root endpoint

### Resource Endpoints

Resources are exposed as one-level deep routes based on table/view names.

**GET /:resource** - Read rows from a table or view

```bash
curl http://localhost:3000/users
```

**HEAD /:resource** - Same as GET but returns headers only (no body)

**POST /:resource** - Create new rows

```bash
curl -X POST http://localhost:3000/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'
```

**PATCH /:resource** - Update existing rows (requires filters)

```bash
curl -X PATCH http://localhost:3000/users?id=eq.1 \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Updated"}'
```

**PUT /:resource** - Upsert rows (insert or update)

```bash
curl -X PUT http://localhost:3000/users?id=eq.1 \
  -H "Content-Type: application/json" \
  -d '{"id": 1, "name": "Alice", "email": "alice@example.com"}'
```

**DELETE /:resource** - Delete rows (requires filters)

```bash
curl -X DELETE http://localhost:3000/users?id=eq.1
```

**OPTIONS /:resource** - CORS preflight for resource endpoint

### RPC Endpoints

Functions (stored procedures) are exposed under the `/rpc` prefix.

**GET /rpc/:function** - Call a function via GET

```bash
curl "http://localhost:3000/rpc/get_user?id=1"
```

**POST /rpc/:function** - Call a function via POST

```bash
curl -X POST http://localhost:3000/rpc/create_user \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'
```

## Query Parameters

### Filtering (Horizontal Filtering)

Filter rows by adding query parameters with column names and operators:

```bash
# Equal
curl "http://localhost:3000/users?id=eq.1"

# Greater than
curl "http://localhost:3000/users?age=gt.18"

# Multiple conditions (AND)
curl "http://localhost:3000/users?age=gte.18&active=is.true"

# IN list
curl "http://localhost:3000/users?id=in.(1,2,3)"

# Pattern matching
curl "http://localhost:3000/users?name=like.*Alice*"
```

### Operators

#### Comparison Operators

| Operator | PostgreSQL | Description | Example |
|----------|-----------|-------------|---------|
| `eq` | `=` | equals | `?id=eq.1` |
| `gt` | `>` | greater than | `?age=gt.18` |
| `gte` | `>=` | greater than or equal | `?age=gte.18` |
| `lt` | `<` | less than | `?age=lt.65` |
| `lte` | `<=` | less than or equal | `?age=lte.65` |
| `neq` | `<>` or `!=` | not equal | `?status=neq.inactive` |
| `like` | `LIKE` | pattern match (use `*` for `%`) | `?name=like.*Alice*` |
| `ilike` | `ILIKE` | case-insensitive pattern match | `?name=ilike.*alice*` |
| `match` | `~` | regular expression match | `?email=match.*@example\.com` |
| `imatch` | `~*` | case-insensitive regex match | `?email=imatch.*@example\.com` |
| `in` | `IN` | one of a list | `?id=in.(1,2,3)` |
| `is` | `IS` | exact equality check | `?deleted=is.null` |
| `isdistinct` | `IS DISTINCT FROM` | not equal (NULL-aware) | `?value=isdistinct.0` |

#### Array/JSON Operators

| Operator | PostgreSQL | Description | Example |
|----------|-----------|-------------|---------|
| `cs` | `@>` | contains | `?tags=cs.{example,new}` |
| `cd` | `<@` | contained in | `?values=cd.{1,2,3}` |
| `ov` | `&&` | overlap | `?period=ov.[2024-01-01,2024-12-31]` |
| `sl` | `<<` | strictly left | `?range=sl.[2024-01-01,2024-06-30]` |
| `sr` | `>>` | strictly right | `?range=sr.[2024-07-01,2024-12-31]` |
| `nxr` | `&<` | does not extend right | `?range=nxr.[2024-01-01,2024-06-30]` |
| `nxl` | `&>` | does not extend left | `?range=nxl.[2024-07-01,2024-12-31]` |
| `adj` | `-|-` | adjacent | `?range=adj.[2024-01-01,2024-06-30]` |

#### Full-Text Search Operators

| Operator | PostgreSQL | Description | Example |
|----------|-----------|-------------|---------|
| `fts` | `@@` | full-text search (to_tsquery) | `?content=fts.hello` |
| `plfts` | `@@` | plain full-text search | `?content=plfts.hello world` |
| `phfts` | `@@` | phrase full-text search | `?content=phfts."hello world"` |
| `wfts` | `@@` | websearch full-text search | `?content=wfts.hello OR world` |

Full-text search operators support an optional language parameter:

```bash
curl "http://localhost:3000/articles?content=fts(english).database"
```

#### Quantifiers

Quantifiable operators (`eq`, `gt`, `gte`, `lt`, `lte`, `like`, `ilike`, `match`, `imatch`) support `any` and `all` quantifiers for array/JSON operations:

```bash
# Match any element in array
curl "http://localhost:3000/users?tags=cs(any).{admin,user}"

# Match all elements
curl "http://localhost:3000/users?tags=cs(all).{admin,user}"
```

#### IS Values

The `is` operator supports:
- `null` - IS NULL
- `not_null` - IS NOT NULL
- `true` - IS TRUE
- `false` - IS FALSE
- `unknown` - IS UNKNOWN

### Logical Operators

Combine multiple filters using `and` and `or`:

```bash
# AND (default)
curl "http://localhost:3000/users?age=gte.18&active=is.true"

# OR
curl "http://localhost:3000/users?or=(age.gte.18,age.lt.13)"

# Complex logic
curl "http://localhost:3000/users?and=(active.is.true,or=(age.gte.18,age.lt.13))"
```

### Ordering

Use the `order` parameter to sort results:

```bash
# Ascending (default)
curl "http://localhost:3000/users?order=name"

# Descending
curl "http://localhost:3000/users?order=name.desc"

# Multiple columns
curl "http://localhost:3000/users?order=age.desc,name.asc"

# Nulls first/last
curl "http://localhost:3000/users?order=email.asc.nullsfirst"
```

### Pagination

Use `limit` and `offset` for pagination:

```bash
# First 10 rows
curl "http://localhost:3000/users?limit=10"

# Skip first 20, get next 10
curl "http://localhost:3000/users?limit=10&offset=20"
```

Or use `Range` header:

```bash
curl -H "Range: 0-9" http://localhost:3000/users
```

### Column Selection

Use `select` to specify which columns to return:

```bash
# Specific columns
curl "http://localhost:3000/users?select=id,name"

# All columns (default)
curl "http://localhost:3000/users?select=*"

# Exclude columns
curl "http://localhost:3000/users?select=*!password"
```

### Embedding (Resource Embedding)

Embed related resources using foreign key relationships:

```bash
# Embed one-to-many relationship
curl "http://localhost:3000/users?select=*,posts(*)"

# Embed many-to-one relationship
curl "http://localhost:3000/posts?select=*,author(*)"

# Nested embedding
curl "http://localhost:3000/users?select=*,posts(*,comments(*))"

# Filter embedded resources
curl "http://localhost:3000/users?select=*,posts(*)&posts.published=is.true"
```

## Request Headers

### Authorization

**Authorization: Bearer `<token>`** - JWT authentication token

```bash
curl -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  http://localhost:3000/users
```

### Accept

**Accept** - Response format

- `application/json` (default) - JSON response
- `text/csv` - CSV response
- `application/octet-stream` - Binary response
- `application/openapi+json` - OpenAPI specification (root endpoint only)

```bash
# CSV response
curl -H "Accept: text/csv" http://localhost:3000/users

# Binary response
curl -H "Accept: application/octet-stream" http://localhost:3000/users
```

### Prefer

**Prefer** - Request preferences

- `plurality=singular` - Return single object instead of array
- `count=exact` - Include exact count in `Content-Range` header
- `count=planned` - Include planned count (faster)
- `count=estimated` - Include estimated count (fastest)
- `max-affected=N` - Validate that no more than N rows are affected

```bash
# Return single object
curl -H "Prefer: plurality=singular" "http://localhost:3000/users?id=eq.1"

# Include exact count
curl -H "Prefer: count=exact" http://localhost:3000/users

# Limit affected rows
curl -X PATCH \
  -H "Prefer: max-affected=10" \
  "http://localhost:3000/users?active=is.false" \
  -d '{"active": true}'
```

### Content-Type

**Content-Type: application/json** - Required for POST, PATCH, PUT requests with JSON body

### Range

**Range: `<start>-<end>`** - Pagination using HTTP range requests

```bash
curl -H "Range: 0-9" http://localhost:3000/users
```

## Response Formats

### JSON (Default)

```json
[
  {"id": 1, "name": "Alice", "email": "alice@example.com"},
  {"id": 2, "name": "Bob", "email": "bob@example.com"}
]
```

### CSV

Request with `Accept: text/csv`:

```csv
id,name,email
1,Alice,alice@example.com
2,Bob,bob@example.com
```

### Binary

Request with `Accept: application/octet-stream` for binary data.

## Response Headers

### Content-Range

Indicates the range and total count of results:

```
Content-Range: 0-9/100
```

### Preference-Applied

Echoes back applied preferences:

```
Preference-Applied: count=exact
```

### Server-Timing

When enabled (`server-timing-enabled=true`), includes request timing:

```
Server-Timing: total;dur=123.456
```

## Error Responses

Errors are returned as JSON with appropriate HTTP status codes:

```json
{
  "code": "DBRST123",
  "message": "Column not found",
  "details": "Could not find column 'unknown' in table 'users'",
  "hint": "Available columns: id, name, email"
}
```

Common HTTP status codes:
- `200 OK` - Success
- `201 Created` - Resource created
- `204 No Content` - Success with no body
- `400 Bad Request` - Invalid request
- `401 Unauthorized` - Authentication required
- `403 Forbidden` - Insufficient permissions
- `404 Not Found` - Resource not found
- `406 Not Acceptable` - Unsupported Accept header
- `500 Internal Server Error` - Server error

## OpenAPI Specification

Get the complete OpenAPI 3.0 specification:

```bash
curl -H "Accept: application/openapi+json" http://localhost:3000/
```

The specification includes:
- All available endpoints (tables, views, functions)
- Supported HTTP methods
- Request/response schemas
- Authentication requirements
- Example requests

You can use this specification with tools like:
- Swagger UI
- Redoc
- Postman
- OpenAPI Generator

## Examples

### Complete CRUD Example

```bash
# Create
curl -X POST http://localhost:3000/users \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice", "email": "alice@example.com"}'

# Read (with filters)
curl "http://localhost:3000/users?email=eq.alice@example.com&select=id,name,email"

# Update
curl -X PATCH "http://localhost:3000/users?id=eq.1" \
  -H "Content-Type: application/json" \
  -d '{"name": "Alice Updated"}'

# Delete
curl -X DELETE "http://localhost:3000/users?id=eq.1"
```

### Complex Query Example

```bash
# Get active users over 18, ordered by name, first 10
curl "http://localhost:3000/users?active=is.true&age=gt.18&order=name.asc&limit=10"

# Get users with their posts (embedded)
curl "http://localhost:3000/users?select=id,name,posts(id,title,created_at)&posts.published=is.true"

# Full-text search
curl "http://localhost:3000/articles?content=fts.database&select=title,content"
```
