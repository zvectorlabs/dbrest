# Enabling CORS

Cross-Origin Resource Sharing (CORS) allows web applications running on different domains to access your dbrest API.

## Configuration

### Allow All Origins (Development)

By default, if `server-cors-allowed-origins` is not set, dbrest allows requests from any origin. This is useful for development but should be restricted in production.

### Restrict to Specific Origins (Production)

Configure allowed origins in your config file:

```ini
server-cors-allowed-origins = "https://example.com,https://app.example.com"
```

Or via environment variable:

```bash
export DBREST_SERVER_CORS_ALLOWED_ORIGINS="https://example.com,https://app.example.com"
```

### Single Origin

For a single origin:

```ini
server-cors-allowed-origins = "https://example.com"
```

## CORS Headers

dbrest automatically handles CORS headers for:

- **Allowed Methods**: GET, POST, PATCH, PUT, DELETE, OPTIONS, HEAD
- **Allowed Headers**: Authorization, Content-Type, Accept, Range, Prefer, Accept-Profile, Content-Profile
- **Exposed Headers**: Content-Range, Preference-Applied, Location

## Preflight Requests

dbrest automatically responds to OPTIONS (preflight) requests with appropriate CORS headers.

### Example Preflight Request

```bash
curl -X OPTIONS http://localhost:3000/users \
  -H "Origin: https://example.com" \
  -H "Access-Control-Request-Method: GET" \
  -v
```

Response includes:

```
Access-Control-Allow-Origin: https://example.com
Access-Control-Allow-Methods: GET, POST, PATCH, PUT, DELETE, OPTIONS, HEAD
Access-Control-Allow-Headers: Authorization, Content-Type, Accept, Range, Prefer
```

## Browser Example

### JavaScript Fetch

```javascript
fetch('http://localhost:3000/users', {
  method: 'GET',
  headers: {
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json',
  },
  credentials: 'include', // Include cookies if needed
})
  .then(response => response.json())
  .then(data => console.log(data));
```

### XMLHttpRequest

```javascript
const xhr = new XMLHttpRequest();
xhr.open('GET', 'http://localhost:3000/users');
xhr.setRequestHeader('Authorization', 'Bearer ' + token);
xhr.setRequestHeader('Content-Type', 'application/json');
xhr.withCredentials = true; // Include cookies if needed
xhr.onload = () => {
  console.log(JSON.parse(xhr.responseText));
};
xhr.send();
```

## Common CORS Issues

### "Access-Control-Allow-Origin" Error

**Problem**: Browser blocks request due to CORS policy.

**Solution**: 
- Add your origin to `server-cors-allowed-origins`
- Ensure the origin matches exactly (including protocol and port)

### Credentials Not Sent

**Problem**: Cookies or authorization headers not sent with CORS requests.

**Solution**: 
- Use `credentials: 'include'` in fetch or `withCredentials: true` in XHR
- Ensure server allows credentials (dbrest does by default)

### Preflight Fails

**Problem**: OPTIONS request fails or returns wrong headers.

**Solution**: 
- dbrest handles OPTIONS automatically
- Verify `server-cors-allowed-origins` includes your origin
- Check browser console for specific error messages

## Production Recommendations

1. **Restrict origins**: Never use wildcard (`*`) in production
2. **Use HTTPS**: Always use HTTPS for CORS-enabled APIs
3. **Validate origins**: Ensure origins match exactly (case-sensitive)
4. **Monitor requests**: Log CORS-related errors to detect issues

## Testing CORS

### Using curl

Test preflight request:

```bash
curl -X OPTIONS http://localhost:3000/users \
  -H "Origin: https://example.com" \
  -H "Access-Control-Request-Method: GET" \
  -H "Access-Control-Request-Headers: Authorization" \
  -v
```

Test actual request:

```bash
curl http://localhost:3000/users \
  -H "Origin: https://example.com" \
  -H "Authorization: Bearer token" \
  -v
```

### Browser Console

Open browser developer tools and check the Network tab for CORS headers in responses.
