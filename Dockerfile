# Multi-stage build for dbrest
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
RUN useradd -m -u 1000 dbrest

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/target/release/dbrest /app/dbrest

# Copy config directory (if it exists)
COPY --chown=dbrest:dbrest config /app/config

# Switch to non-root user
USER dbrest

# Expose port
EXPOSE 3000

# Default environment variables
ENV DBREST_CONFIG=/app/config/bench.toml
ENV DBREST_DB_URI=postgresql://postgres:postgres@postgres:5432/postgres
ENV DBREST_SERVER_PORT=3000

# Run dbrest
CMD ["/app/dbrest"]
