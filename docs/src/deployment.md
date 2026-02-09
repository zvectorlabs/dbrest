# Deployment

This guide covers deploying PgREST in production environments using Docker and systemd.

## Docker Deployment

### Building the Image

Build the Docker image from the Dockerfile:

```bash
docker build -t pgrest .
```

### Running with Docker

Basic run command:

```bash
docker run -p 3000:3000 \
  -e PGRST_DB_URI="postgresql://user:password@host:5432/dbname" \
  -e PGRST_DB_SCHEMAS="public" \
  -e PGRST_DB_ANON_ROLE="anon" \
  pgrest
```

### Using Docker Compose

Create a `docker-compose.yml`:

```yaml
version: '3.8'

services:
  pgrest:
    build: .
    ports:
      - "3000:3000"
    environment:
      PGRST_DB_URI: "postgresql://postgres:postgres@postgres:5432/mydb"
      PGRST_DB_SCHEMAS: "public"
      PGRST_DB_ANON_ROLE: "anon"
      PGRST_SERVER_PORT: 3000
      PGRST_JWT_SECRET: "your-secret-key-here"
    depends_on:
      - postgres
    restart: unless-stopped

  postgres:
    image: postgres:17
    environment:
      POSTGRES_DB: mydb
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

volumes:
  postgres_data:
```

Run with:

```bash
docker-compose up -d
```

### Using Config File with Docker

Mount a config file:

```bash
docker run -p 3000:3000 \
  -v $(pwd)/config.toml:/app/config.toml \
  -e PGREST_CONFIG=/app/config.toml \
  pgrest
```

### Docker Health Check

Add a health check to your Dockerfile or docker-compose.yml:

```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://localhost:3000/"]
  interval: 30s
  timeout: 10s
  retries: 3
  start_period: 40s
```

## Systemd Deployment

### Create Systemd Service File

Create `/etc/systemd/system/pgrest.service`:

```ini
[Unit]
Description=PgREST REST API for PostgreSQL
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=pgrest
Group=pgrest
WorkingDirectory=/opt/pgrest
ExecStart=/opt/pgrest/pgrest --config /etc/pgrest/config.toml
Restart=always
RestartSec=10

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/log/pgrest

# Environment
Environment="RUST_LOG=info"

[Install]
WantedBy=multi-user.target
```

### Setup Steps

1. **Create user and directories:**

```bash
sudo useradd -r -s /bin/false pgrest
sudo mkdir -p /opt/pgrest
sudo mkdir -p /etc/pgrest
sudo mkdir -p /var/log/pgrest
```

2. **Install binary:**

```bash
sudo cp pgrest /opt/pgrest/
sudo chown pgrest:pgrest /opt/pgrest/pgrest
sudo chmod +x /opt/pgrest/pgrest
```

3. **Create configuration:**

```bash
sudo nano /etc/pgrest/config.toml
```

4. **Set permissions:**

```bash
sudo chown -R pgrest:pgrest /etc/pgrest
sudo chown -R pgrest:pgrest /var/log/pgrest
```

5. **Enable and start service:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable pgrest
sudo systemctl start pgrest
```

6. **Check status:**

```bash
sudo systemctl status pgrest
sudo journalctl -u pgrest -f
```

## Production Considerations

### Connection Pool Sizing

Configure the connection pool based on your workload:

```ini
# For high-traffic applications
db-pool = 50

# For low-traffic applications
db-pool = 10
```

Monitor connection usage and adjust accordingly. A good starting point is:
- **Low traffic**: 10-20 connections
- **Medium traffic**: 20-50 connections
- **High traffic**: 50-100 connections

### Logging Configuration

Set appropriate log levels:

```ini
# Production: info or warn
log-level = "info"

# Development: debug
log-level = "debug"

# Disable SQL query logging in production
log-query = false
```

### CORS Configuration

Restrict CORS to specific origins in production:

```ini
server-cors-allowed-origins = "https://example.com,https://app.example.com"
```

### Security Best Practices

1. **JWT Secret:**
   - Use a strong, randomly generated secret (at least 32 characters)
   - Store in environment variables or secrets management
   - Never commit secrets to version control

2. **Database Role:**
   - Use least-privilege principle for `db-anon-role`
   - Create separate roles for different access levels
   - Use row-level security (RLS) policies

3. **Network Security:**
   - Use firewall rules to restrict access
   - Consider using a reverse proxy (nginx, Traefik)
   - Enable TLS/SSL termination at the proxy

4. **Resource Limits:**
   - Set `db-max-rows` to prevent large result sets
   - Configure `server-streaming-threshold` for large responses
   - Use `Prefer: max-affected` for write operations

### Reverse Proxy Setup (Nginx)

Example Nginx configuration:

```nginx
upstream pgrest {
    server localhost:3000;
}

server {
    listen 80;
    server_name api.example.com;

    # Redirect to HTTPS
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.example.com;

    ssl_certificate /etc/ssl/certs/api.example.com.crt;
    ssl_certificate_key /etc/ssl/private/api.example.com.key;

    # Security headers
    add_header X-Frame-Options "SAMEORIGIN" always;
    add_header X-Content-Type-Options "nosniff" always;
    add_header X-XSS-Protection "1; mode=block" always;

    # Rate limiting
    limit_req_zone $binary_remote_addr zone=api_limit:10m rate=10r/s;
    limit_req zone=api_limit burst=20 nodelay;

    location / {
        proxy_pass http://pgrest;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }
}
```

### Monitoring

Enable Server-Timing header for performance monitoring:

```ini
server-timing-enabled = true
```

Monitor key metrics:
- Request rate
- Response times
- Error rates
- Database connection pool usage
- Memory usage

### High Availability

For high availability:

1. **Multiple Instances:**
   - Run multiple PgREST instances behind a load balancer
   - Use sticky sessions if needed
   - Ensure all instances use the same database

2. **Database Connection:**
   - Use PostgreSQL connection pooling (PgBouncer)
   - Configure failover/read replicas
   - Monitor database health

3. **Health Checks:**
   - Implement health check endpoints
   - Configure load balancer health checks
   - Set up alerting for failures

### Backup and Recovery

1. **Configuration Backup:**
   - Version control your configuration files
   - Backup configuration regularly

2. **Database Backup:**
   - Use PostgreSQL native backup tools
   - Set up automated backups
   - Test recovery procedures

3. **Disaster Recovery:**
   - Document recovery procedures
   - Test failover scenarios
   - Maintain runbooks

## Troubleshooting

### Service Won't Start

Check logs:

```bash
# Systemd
sudo journalctl -u pgrest -n 50

# Docker
docker logs pgrest
```

Common issues:
- Invalid configuration file
- Database connection failure
- Port already in use
- Permission issues

### High Memory Usage

- Enable streaming for large responses
- Reduce `db-pool-size` if too high
- Check for memory leaks in application code

### Slow Performance

- Check database query performance
- Review connection pool size
- Enable query logging to identify slow queries
- Consider database indexes

### Connection Errors

- Verify database URI is correct
- Check network connectivity
- Verify database user permissions
- Review connection pool settings
