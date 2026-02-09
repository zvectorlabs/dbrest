# Multi-stage build for PgREST
# Stage 1: Build
FROM rust:1.91 AS builder

WORKDIR /app

# Copy dependency files and source structure for better caching
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY tests ./tests

# Build the release binary
RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

# Install ca-certificates for HTTPS and curl for healthcheck
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 pgrest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/pgrest /app/pgrest

# Copy config directory (if it exists)
COPY --chown=pgrest:pgrest config /app/config

# Switch to non-root user
USER pgrest

# Expose port
EXPOSE 3000

# Default environment variables
ENV PGREST_CONFIG=/app/config/bench.toml
ENV PGREST_DB_URI=postgresql://postgres:postgres@postgres:5432/postgres
ENV PGREST_SERVER_PORT=3000

# Run PgREST
CMD ["/app/pgrest"]
