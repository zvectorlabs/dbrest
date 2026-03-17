# Authentication with JWT

dbrest supports JWT (JSON Web Token) authentication for securing your API endpoints.

## Configuration

### Setting the JWT Secret

Configure the JWT secret in your configuration file:

```ini
jwt-secret = "your-very-long-secret-key-at-least-32-characters-long"
```

Or via environment variable:

```bash
export DBREST_JWT_SECRET="your-very-long-secret-key-at-least-32-characters-long"
```

**Important:** The JWT secret must be at least 32 characters long for security.

### Base64 Encoded Secrets

If your secret is base64 encoded:

```ini
jwt-secret = "dGhpcyBpcyBhIGJhc2U2NCBlbmNvZGVkIHNlY3JldA=="
jwt-secret-is-base64 = true
```

### JWKS (JSON Web Key Set)

You can also use JWKS for public key verification:

```ini
jwt-secret = '{"keys":[{"kty":"RSA","n":"...","e":"AQAB"}]}'
```

## JWT Token Structure

Your JWT tokens should include a role claim that maps to a PostgreSQL role:

```json
{
  "role": "authenticated_user",
  "exp": 1234567890,
  "iat": 1234567890
}
```

### Custom Role Claim Path

By default, dbrest looks for the role in the `role` claim. To use a different path:

```ini
jwt-role-claim-key = "user.role"
```

This would read from:

```json
{
  "user": {
    "role": "authenticated_user"
  }
}
```

### Audience Validation

To validate the JWT audience:

```ini
jwt-aud = "api.example.com"
```

## Using JWT Tokens

### Request Format

Include the JWT token in the `Authorization` header:

```bash
curl -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..." \
  http://localhost:3000/users
```

### Example: Generating a Token

Here's a simple example using Node.js:

```javascript
const jwt = require('jsonwebtoken');

const token = jwt.sign(
  {
    role: 'authenticated_user',
    exp: Math.floor(Date.now() / 1000) + 3600, // 1 hour
  },
  'your-very-long-secret-key-at-least-32-characters-long'
);

console.log(token);
```

### Example: Using Python

```python
import jwt
import time

token = jwt.encode(
    {
        'role': 'authenticated_user',
        'exp': int(time.time()) + 3600,  # 1 hour
    },
    'your-very-long-secret-key-at-least-32-characters-long',
    algorithm='HS256'
)

print(token)
```

## Database Roles

### Creating Roles

Create PostgreSQL roles that match your JWT role claims:

```sql
-- Create an authenticated user role
CREATE ROLE authenticated_user;

-- Grant permissions
GRANT USAGE ON SCHEMA public TO authenticated_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON users TO authenticated_user;
```

### Anonymous Role

For unauthenticated requests, configure an anonymous role:

```ini
db-anon-role = "anon"
```

Create the role:

```sql
CREATE ROLE anon;
GRANT USAGE ON SCHEMA public TO anon;
GRANT SELECT ON users TO anon;  -- Read-only access
```

## Security Best Practices

1. **Use strong secrets**: At least 32 characters, randomly generated
2. **Set expiration**: Always include `exp` claim in tokens
3. **Use HTTPS**: Never send tokens over unencrypted connections
4. **Validate audience**: Use `jwt-aud` to validate token audience
5. **Rotate secrets**: Regularly rotate JWT secrets
6. **Limit permissions**: Grant minimal necessary permissions to roles

## JWT Caching

dbrest caches validated JWT tokens to improve performance:

```ini
jwt-cache-max-entries = 1000
```

Increase this value if you have many unique tokens in use simultaneously.

## Troubleshooting

### "Invalid JWT" Error

- Verify the secret matches between token generation and dbrest config
- Check token expiration (`exp` claim)
- Ensure token is properly formatted (three parts separated by dots)

### "Role not found" Error

- Verify the role exists in PostgreSQL
- Check `jwt-role-claim-key` matches your token structure
- Ensure the role has necessary permissions

### "JWT secret too short" Error

- JWT secret must be at least 32 characters
- Use a longer, randomly generated secret
