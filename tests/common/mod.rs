//! Common test utilities for integration tests
//!
//! This module provides helpers for setting up test databases using testcontainers
//! (PostgreSQL) and in-memory SQLite.

#![allow(dead_code)]

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;

/// Test database container and pool (PostgreSQL)
pub struct TestDb {
    pub pool: PgPool,
    // Keep container alive for the duration of the test
    #[allow(dead_code)]
    container: ContainerAsync<Postgres>,
}

impl TestDb {
    /// Create a new test database with the test schema
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        // Start PostgreSQL container with latest version (17)
        let postgres_image = Postgres::default();
        let container = postgres_image.with_tag("17").start().await?;

        // Get connection string
        let host = container.get_host().await?;
        let port = container.get_host_port_ipv4(5432).await?;
        let connection_string = format!("postgres://postgres:postgres@{}:{}/postgres", host, port);

        // Create connection pool
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&connection_string)
            .await?;

        // Run fixtures
        let fixtures = include_str!("../fixtures/schema.sql");
        sqlx::raw_sql(fixtures).execute(&pool).await?;

        Ok(Self { pool, container })
    }

    /// Get a reference to the pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

/// Test database for SQLite (in-memory, no Docker needed)
pub struct TestSqliteDb {
    pub pool: sqlx::SqlitePool,
}

impl TestSqliteDb {
    /// Create a new in-memory SQLite database with the test schema.
    pub async fn new() -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        use sqlx::sqlite::SqlitePoolOptions;

        // In-memory database shared across connections
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect("sqlite::memory:")
            .await?;

        // Enable foreign keys and WAL mode
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&pool)
            .await?;

        // Load fixtures
        let fixtures = include_str!("../fixtures/schema_sqlite.sql");
        sqlx::raw_sql(fixtures).execute(&pool).await?;

        // Create the session vars temp table (needed by SQLite dialect)
        sqlx::query("CREATE TABLE IF NOT EXISTS _dbrest_vars(key TEXT PRIMARY KEY, val TEXT)")
            .execute(&pool)
            .await?;

        Ok(Self { pool })
    }

    /// Get a reference to the pool
    pub fn pool(&self) -> &sqlx::SqlitePool {
        &self.pool
    }
}
