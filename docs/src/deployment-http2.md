# Deployment — TLS + HTTP/2

dbREST's built-in listener speaks **cleartext HTTP/1.1 + h2c** (auto-negotiated
per connection). It does **not** ship with native TLS. For production you
should front dbREST with a TLS-terminating reverse proxy that speaks HTTP/2
to browsers and forwards to dbREST over cleartext H1.1 or h2c.

This page provides working recipes for Caddy, nginx, and Traefik, plus
SSE-specific tuning notes.

## Why HTTP/2 matters

- **SSE scaling.** Browsers cap HTTP/1.1 at ~6 connections per origin. A
  dashboard subscribing to many tables via `EventSource` exhausts this fast.
  HTTP/2 multiplexes unlimited streams over one TCP connection.
- **Header compression (HPACK).** Every request carries a JWT in
  `Authorization: Bearer ...`. HPACK compresses repeated headers, a real
  win for JWT-authenticated clients.
- **Single connection reuse.** Concurrent `db.from(...)` calls from the
  client SDK share one TCP connection with lower latency.

## Topology

```
+------------+    H2 (TLS+ALPN)   +---------+   H1.1 / h2c   +-------+   SQL
|  Browser   |------------------->|         |--------------->|       |------>+-------+
|  (fetch /  |                    |  Proxy  |                | dbREST|       |  DB   |
|  Event     |<-------------------| (TLS)   |<---------------| Axum  |<------| PG/SL |
|  Source)   |    H2 streams      |         |                |  H1+  |       +-------+
+------------+                    +---------+                |  h2c  |
                                                             +-------+

+------------+    H1.1 (default) or H2 (undici allowH2)
|  Node app  |------------------------------------------> (same proxy)
|  @dbrest/* |
+------------+
```

## Caddy

Caddy enables TLS and HTTP/2 (and HTTP/3) automatically. Forward to dbREST
as h2c so the proxy doesn't re-handshake TLS upstream.

```caddy
api.example.com {
    # Auto HTTPS; ALPN negotiates h2 for modern browsers.
    encode gzip zstd

    # SSE endpoint — disable proxy buffering so events are flushed immediately.
    @sse path /listen/*
    reverse_proxy @sse 127.0.0.1:3000 {
        transport http {
            versions h2c 1.1
        }
        flush_interval -1
    }

    # Everything else
    reverse_proxy 127.0.0.1:3000 {
        transport http {
            versions h2c 1.1
        }
    }
}
```

Notes:
- `versions h2c 1.1` lets Caddy speak h2c upstream to dbREST's auto builder.
- `flush_interval -1` disables response buffering for SSE.
- Caddy handles certificate provisioning automatically via Let's Encrypt /
  ZeroSSL.

## nginx

nginx speaks HTTP/2 to clients (`listen 443 ssl http2;`) but only HTTP/1.1
upstream. That's fine — multiplexing on the client side is what matters.

```nginx
server {
    listen 443 ssl http2;
    server_name api.example.com;

    ssl_certificate     /etc/letsencrypt/live/api.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.example.com/privkey.pem;

    # SSE endpoint — buffering must be disabled.
    location /listen/ {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # SSE-critical:
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 24h;
        add_header X-Accel-Buffering no;
    }

    location / {
        proxy_pass http://127.0.0.1:3000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

Notes:
- `listen 443 ssl http2;` is the only knob needed for client-side H2.
- `X-Accel-Buffering: no` prevents downstream CDNs (Cloudflare, Fastly) and
  nginx itself from buffering SSE responses.
- `proxy_read_timeout 24h` keeps long-lived SSE connections alive.
- Upstream is plain HTTP/1.1 — no h2c needed unless you want it.

## Traefik

Traefik v3 auto-enables HTTP/2 on HTTPS entrypoints. Docker-compose example:

```yaml
services:
  traefik:
    image: traefik:v3
    command:
      - --entrypoints.websecure.address=:443
      - --entrypoints.websecure.http.tls=true
      - --providers.docker=true
      - --certificatesresolvers.le.acme.email=ops@example.com
      - --certificatesresolvers.le.acme.storage=/acme.json
      - --certificatesresolvers.le.acme.tlschallenge=true
    ports:
      - "443:443"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - ./acme.json:/acme.json

  dbrest:
    image: your-org/dbrest:latest
    labels:
      - traefik.enable=true
      - traefik.http.routers.dbrest.rule=Host(`api.example.com`)
      - traefik.http.routers.dbrest.entrypoints=websecure
      - traefik.http.routers.dbrest.tls.certresolver=le
      - traefik.http.services.dbrest.loadbalancer.server.port=3000
      # SSE-specific service with relaxed buffering / long timeouts:
      - traefik.http.routers.dbrest-sse.rule=Host(`api.example.com`) && PathPrefix(`/listen`)
      - traefik.http.routers.dbrest-sse.entrypoints=websecure
      - traefik.http.routers.dbrest-sse.tls.certresolver=le
      - traefik.http.routers.dbrest-sse.service=dbrest-sse
      - traefik.http.services.dbrest-sse.loadbalancer.server.port=3000
      - traefik.http.services.dbrest-sse.loadbalancer.responseForwarding.flushInterval=1ms
```

Notes:
- `flushInterval=1ms` on the SSE service forwards events without buffering.
- Avoid sticky sessions on the SSE service — they can trap reconnects on a
  dead backend.

## SSE tuning checklist (all proxies)

- **Disable response buffering** on `/listen/*`:
  - Caddy: `flush_interval -1`
  - nginx: `proxy_buffering off` + `X-Accel-Buffering: no`
  - Traefik: `flushInterval=1ms`
- **Raise read timeouts** to cover long-lived streams (≥ 1h; many hours is
  typical).
- **Avoid `gzip` on SSE** — event frames are already small and compression
  buffers defeat flushing. Keep compression on other routes.
- **No sticky sessions** on the SSE service — reconnects should land on any
  healthy backend.

## Verifying HTTP/2

From a browser devtools Network panel, the "Protocol" column should read
`h2` for requests to your domain. Or from the command line:

```bash
curl -I --http2 https://api.example.com/
# HTTP/2 200
```

To confirm the SDK reuses connections under H2, inspect Network in devtools:
all concurrent `fetch` calls to `api.example.com` should share one
connection ID.

## Node SDK — HTTP/2 behavior

The SDK picks the best transport for your environment automatically. You
can override if needed.

```typescript
import { createClient } from '@dbrest/client'

// Default: http2 omitted === 'auto'.
//   - Browser:     native fetch + ALPN => HTTP/2 automatic.
//   - Node + HTTPS + undici installed => HTTP/2 via undici Agent({ allowH2: true }).
//   - Node otherwise => HTTP/1.1 (native fetch). No throws, no required deps.
const db = createClient<Database>('https://api.example.com')

// Force HTTP/2 in Node (throws if `undici` is not installed).
const db2 = createClient<Database>(url, { http2: true })

// Force HTTP/1.1 (useful for debugging or incompatible proxies).
const db3 = createClient<Database>(url, { http2: false })

// Power-user: bring your own undici Dispatcher (pool sizing, mTLS, etc.).
import { Agent } from 'undici'
const db4 = createClient<Database>(url, {
  dispatcher: new Agent({ allowH2: true, keepAliveTimeout: 60_000 }),
})

// Ultimate override: replace fetch entirely (e.g. for tracing).
const db5 = createClient<Database>(url, { fetch: myInstrumentedFetch })
```

Precedence (highest to lowest): `fetch` → `dispatcher` → `http2: true|false`
→ `http2: 'auto'` (default). In browsers, `http2` and `dispatcher` are
ignored — native `fetch` + ALPN handles HTTP/2.

## Non-goals

- dbREST does not, and is not planned to, terminate TLS natively.
- HTTP/3 / QUIC is out of scope; rely on the proxy if you want it.
- HTTP/2 server push is deprecated and not used.
