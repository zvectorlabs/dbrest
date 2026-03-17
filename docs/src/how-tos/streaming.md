# Streaming Large Responses

dbrest can stream large result sets to reduce memory usage and improve response times for large datasets.

## How Streaming Works

When a response exceeds the configured threshold, dbrest automatically streams the response instead of buffering it in memory.

### Benefits

- **Lower memory usage**: Large results don't consume server memory
- **Faster time-to-first-byte**: Data starts sending immediately
- **Better scalability**: Handle larger datasets without memory issues

## Configuration

### Enable Streaming

Streaming is enabled by default:

```ini
server-streaming-enabled = true
```

Disable if needed:

```ini
server-streaming-enabled = false
```

### Set Threshold

Configure the size threshold (in bytes) for streaming:

```ini
server-streaming-threshold = 10485760  # 10MB (default)
```

For smaller thresholds:

```ini
server-streaming-threshold = 1048576  # 1MB
```

For larger thresholds:

```ini
server-streaming-threshold = 52428800  # 50MB
```

### Environment Variables

```bash
export DBREST_SERVER_STREAMING_ENABLED="true"
export DBREST_SERVER_STREAMING_THRESHOLD="10485760"
```

## When Streaming Occurs

Streaming is triggered when:

1. Response size exceeds `server-streaming-threshold`
2. `server-streaming-enabled` is `true`
3. Response format is JSON (CSV and binary use different handling)

## Using Streaming Responses

### Client-Side Handling

Streaming responses work transparently with most HTTP clients:

```javascript
// Fetch API handles streaming automatically
fetch('http://localhost:3000/users')
  .then(response => response.json())
  .then(data => console.log(data));
```

### Node.js Stream Example

```javascript
const https = require('https');

const req = https.get('http://localhost:3000/users', (res) => {
  let data = '';
  
  res.on('data', (chunk) => {
    data += chunk;
    // Process chunks as they arrive
    console.log('Received chunk:', chunk.length, 'bytes');
  });
  
  res.on('end', () => {
    const json = JSON.parse(data);
    console.log('Complete response:', json);
  });
});
```

### Python Example

```python
import requests
import json

response = requests.get(
    'http://localhost:3000/users',
    stream=True  # Enable streaming
)

# Process chunks as they arrive
for chunk in response.iter_content(chunk_size=8192):
    if chunk:
        print(f'Received chunk: {len(chunk)} bytes')

# Parse complete response
data = response.json()
```

## Monitoring Streaming

### Check Response Headers

Streaming responses include standard headers:

```http
HTTP/1.1 200 OK
Content-Type: application/json
Transfer-Encoding: chunked
```

The `Transfer-Encoding: chunked` header indicates streaming.

### Server Logs

Enable query logging to monitor streaming:

```ini
log-level = "info"
log-query = true
```

## Performance Considerations

### Memory Usage

- **Without streaming**: Entire response buffered in memory
- **With streaming**: Only chunks buffered (much lower memory)

### Network Efficiency

- Streaming starts sending data immediately
- Client can start processing before response completes
- Better for slow network connections

### CPU Usage

- Streaming adds minimal CPU overhead
- Benefits outweigh costs for large responses

## Best Practices

1. **Keep streaming enabled**: Default setting works well for most cases
2. **Adjust threshold**: Tune based on your typical response sizes
3. **Monitor memory**: Watch server memory usage with large queries
4. **Use pagination**: For very large datasets, consider pagination instead
5. **Client handling**: Ensure clients can handle chunked responses

## Limitations

### Not Supported For

- CSV format (uses different streaming mechanism)
- Binary format (handled separately)
- Small responses (below threshold)

### Client Requirements

- Client must support HTTP/1.1 chunked transfer encoding
- Some older clients may not handle streaming correctly

## Troubleshooting

### Streaming Not Working

**Check configuration**:
```bash
# Verify streaming is enabled
curl http://localhost:3000/admin/config | jq '.server_streaming_enabled'
```

**Check response size**: Response must exceed threshold

**Check format**: Only JSON responses are streamed

### Memory Still High

- Lower `server-streaming-threshold`
- Check for other memory leaks
- Monitor connection pool size

### Client Errors

- Ensure client supports chunked encoding
- Check for timeout issues
- Verify network stability

## Example: Large Dataset Export

```bash
# Export large dataset (automatically streamed)
curl -H "Accept: application/json" \
  http://localhost:3000/users > users_export.json

# Monitor streaming
curl -v -H "Accept: application/json" \
  http://localhost:3000/users 2>&1 | grep -i "transfer-encoding"
```

## Configuration Examples

### High-Memory Server

```ini
# Allow larger buffers before streaming
server-streaming-threshold = 52428800  # 50MB
```

### Low-Memory Server

```ini
# Stream earlier to save memory
server-streaming-threshold = 1048576  # 1MB
```

### Disable Streaming

```ini
# Disable if you have specific requirements
server-streaming-enabled = false
```
