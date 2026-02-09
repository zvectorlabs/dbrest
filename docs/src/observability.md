# Observability

PgREST provides several mechanisms for monitoring and observing your API's behavior, including logging, timing headers, and admin endpoints.

## Logging

### Log Levels

Configure the verbosity of logging:

```ini
log-level = "info"
```

Available levels (from least to most verbose):
- `crit` - Critical errors only
- `error` - Errors (default)
- `warn` - Warnings and errors
- `info` - Informational messages, warnings, and errors
- `debug` - All messages including debug information

### Environment Variable

```bash
export PGRST_LOG_LEVEL="info"
```

### Structured Logging

PgREST uses structured logging with the `tracing` crate, providing:
- Timestamps
- Log levels
- Contextual information
- Request tracing

### Log Query

Enable SQL query logging:

```ini
log-query = true
```

This logs all SQL queries executed by PgREST, useful for debugging and performance analysis.

**Warning**: Enabling query logging can be verbose and may expose sensitive data. Use only in development or with caution in production.

### Log Output

Logs are written to stderr by default. In production, redirect to a log file or logging service:

```bash
./pgrest --config config.toml 2>&1 | tee pgrest.log
```

Or with systemd, logs go to journald:

```bash
journalctl -u pgrest -f
```

## Server-Timing Header

The Server-Timing header provides performance metrics for each request.

### Enable Server-Timing

```ini
server-timing-enabled = true
```

### Header Format

```
Server-Timing: total;dur=123.456
```

The `dur` value is in milliseconds and represents the total request processing time.

### Example Response

```http
HTTP/1.1 200 OK
Server-Timing: total;dur=45.123
Content-Type: application/json
```

### Using Server-Timing

#### Browser DevTools

Modern browsers automatically display Server-Timing in the Network tab:

1. Open browser DevTools
2. Go to Network tab
3. Select a request
4. View Timing information

#### Client-Side Monitoring

```javascript
fetch('http://localhost:3000/users')
  .then(response => {
    const timing = response.headers.get('Server-Timing');
    if (timing) {
      const match = timing.match(/total;dur=([\d.]+)/);
      if (match) {
        const duration = parseFloat(match[1]);
        console.log(`Request took ${duration}ms`);
        // Send to monitoring service
        sendMetric('request_duration', duration);
      }
    }
    return response.json();
  });
```

### Performance Monitoring

Use Server-Timing to:
- Monitor API performance
- Identify slow endpoints
- Track performance regressions
- Set up alerts for slow requests

## Admin Server

The admin server provides endpoints for monitoring and managing PgREST.

### Enable Admin Server

```ini
admin-server-host = "127.0.0.1"
admin-server-port = 3001
```

**Security Note**: Only expose the admin server on localhost or a secure network. Never expose it publicly.

### Admin Endpoints

#### Configuration

Get current configuration:

```bash
curl http://localhost:3001/config
```

Response includes all configuration values (sensitive values may be redacted).

#### Health Check

Check server health:

```bash
curl http://localhost:3001/health
```

#### Metrics

Get server metrics (if implemented):

```bash
curl http://localhost:3001/metrics
```

### Use Cases

- Health checks for load balancers
- Configuration verification
- Debugging configuration issues
- Monitoring server state

## Monitoring Best Practices

### Log Aggregation

Use log aggregation tools:
- **ELK Stack** (Elasticsearch, Logstash, Kibana)
- **Loki** (Grafana Labs)
- **CloudWatch** (AWS)
- **Datadog**
- **Splunk**

### Metrics Collection

Collect metrics from:
- Server-Timing headers
- Application metrics
- Database connection pool metrics
- Request/response counts

### Alerting

Set up alerts for:
- High error rates
- Slow requests (using Server-Timing)
- Connection pool exhaustion
- High memory usage

## Example Monitoring Setup

### Prometheus + Grafana

1. Export metrics from Server-Timing headers
2. Scrape with Prometheus
3. Visualize in Grafana

### Logging Pipeline

```bash
# Send logs to centralized logging
./pgrest --config config.toml 2>&1 | \
  fluent-bit -i stdin -o http \
    -p Host=logs.example.com \
    -p Port=443 \
    -p TLS=on
```

### Health Check Script

```bash
#!/bin/bash
# health_check.sh

HEALTH_URL="http://localhost:3001/health"
TIMEOUT=5

response=$(curl -s -o /dev/null -w "%{http_code}" \
  --max-time $TIMEOUT $HEALTH_URL)

if [ "$response" = "200" ]; then
  echo "OK"
  exit 0
else
  echo "FAIL"
  exit 1
fi
```

## Performance Monitoring

### Key Metrics to Track

1. **Request Rate**: Requests per second
2. **Response Time**: P50, P95, P99 latencies (from Server-Timing)
3. **Error Rate**: Percentage of failed requests
4. **Database Pool**: Connection pool utilization
5. **Memory Usage**: Server memory consumption

### Example Dashboard Queries

#### Average Response Time

```promql
avg(server_timing_duration_seconds)
```

#### Error Rate

```promql
rate(http_requests_total{status=~"5.."}[5m]) / 
rate(http_requests_total[5m])
```

#### Request Rate

```promql
rate(http_requests_total[5m])
```

## Troubleshooting

### High Log Volume

- Reduce `log-level` to `warn` or `error`
- Disable `log-query` in production
- Use log sampling for high-traffic endpoints

### Missing Server-Timing

- Verify `server-timing-enabled=true`
- Check that middleware is properly configured
- Ensure client supports custom headers

### Admin Server Not Accessible

- Check firewall rules
- Verify `admin-server-port` is set
- Ensure admin server host is correct
- Check for port conflicts
