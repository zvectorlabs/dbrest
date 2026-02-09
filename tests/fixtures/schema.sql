-- Test schema for pgrest integration tests
-- This schema contains all relationship types and various column types

-- Create test schema
DROP SCHEMA IF EXISTS test_api CASCADE;
CREATE SCHEMA test_api;

-- Create enum type
CREATE TYPE test_api.user_status AS ENUM ('active', 'inactive', 'pending');
CREATE TYPE test_api.priority AS ENUM ('low', 'medium', 'high', 'critical');

-- ============================================================================
-- Core Tables
-- ============================================================================

-- Users table (referenced by many others)
CREATE TABLE test_api.users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    name TEXT NOT NULL,
    status test_api.user_status NOT NULL DEFAULT 'pending',
    bio TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

COMMENT ON TABLE test_api.users IS 'Application users';
COMMENT ON COLUMN test_api.users.email IS 'User email address (unique)';
COMMENT ON COLUMN test_api.users.status IS 'Account status';

-- Profiles table (O2O with users)
CREATE TABLE test_api.profiles (
    user_id INTEGER PRIMARY KEY REFERENCES test_api.users(id) ON DELETE CASCADE,
    avatar_url TEXT,
    website TEXT,
    location TEXT,
    birth_date DATE
);

COMMENT ON TABLE test_api.profiles IS 'User profiles (one-to-one with users)';

-- Organizations table
CREATE TABLE test_api.organizations (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    slug VARCHAR(100) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Roles table (for M2M relationship)
CREATE TABLE test_api.roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    permissions JSONB DEFAULT '[]'::jsonb
);

-- ============================================================================
-- Junction Tables (M2M)
-- ============================================================================

-- User roles (M2M between users and roles)
CREATE TABLE test_api.user_roles (
    user_id INTEGER NOT NULL REFERENCES test_api.users(id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES test_api.roles(id) ON DELETE CASCADE,
    granted_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    granted_by INTEGER REFERENCES test_api.users(id),
    PRIMARY KEY (user_id, role_id)
);

COMMENT ON TABLE test_api.user_roles IS 'Junction table for user-role M2M relationship';

-- Organization members (M2M between users and organizations)
CREATE TABLE test_api.org_members (
    org_id INTEGER NOT NULL REFERENCES test_api.organizations(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES test_api.users(id) ON DELETE CASCADE,
    role VARCHAR(50) DEFAULT 'member',
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    PRIMARY KEY (org_id, user_id)
);

-- ============================================================================
-- Posts and Comments (M2O/O2M relationships)
-- ============================================================================

-- Posts table
CREATE TABLE test_api.posts (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES test_api.users(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    body TEXT,
    published BOOLEAN DEFAULT false,
    view_count INTEGER DEFAULT 0,
    tags TEXT[] DEFAULT '{}',
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

COMMENT ON TABLE test_api.posts IS 'Blog posts';

-- Comments table (references posts and users, self-referencing for replies)
CREATE TABLE test_api.comments (
    id SERIAL PRIMARY KEY,
    post_id INTEGER NOT NULL REFERENCES test_api.posts(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES test_api.users(id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES test_api.comments(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

COMMENT ON TABLE test_api.comments IS 'Post comments (self-referencing for replies)';

-- ============================================================================
-- Tasks (various column types)
-- ============================================================================

CREATE TABLE test_api.tasks (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    priority test_api.priority DEFAULT 'medium',
    due_date DATE,
    due_time TIME,
    estimated_hours NUMERIC(5,2),
    actual_hours NUMERIC(5,2),
    is_completed BOOLEAN DEFAULT false,
    assigned_to INTEGER REFERENCES test_api.users(id),
    tags TEXT[] DEFAULT '{}',
    extra JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- ============================================================================
-- Views
-- ============================================================================

-- Active users view
CREATE VIEW test_api.active_users AS
SELECT id, email, name, created_at
FROM test_api.users
WHERE status = 'active';

-- Published posts view
CREATE VIEW test_api.published_posts AS
SELECT p.id, p.title, p.body, p.created_at, u.name AS author_name
FROM test_api.posts p
JOIN test_api.users u ON p.user_id = u.id
WHERE p.published = true;

-- ============================================================================
-- Functions
-- ============================================================================

-- Simple scalar function
CREATE FUNCTION test_api.add_numbers(a INTEGER, b INTEGER)
RETURNS INTEGER
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT a + b;
$$;

COMMENT ON FUNCTION test_api.add_numbers IS 'Add two numbers';

-- Set-returning function (returns users)
CREATE FUNCTION test_api.get_active_users()
RETURNS SETOF test_api.users
LANGUAGE SQL
STABLE
AS $$
    SELECT * FROM test_api.users WHERE status = 'active';
$$;

-- Function with table row parameter (computed relationship)
CREATE FUNCTION test_api.user_post_count(u test_api.users)
RETURNS BIGINT
LANGUAGE SQL
STABLE
AS $$
    SELECT COUNT(*) FROM test_api.posts WHERE user_id = u.id;
$$;

-- Computed field function: returns full name (scalar)
CREATE FUNCTION test_api.full_name(u test_api.users)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT COALESCE(u.name, '') || ' (' || u.email || ')';
$$;

COMMENT ON FUNCTION test_api.full_name IS 'Computed field: user full name with email';

-- Computed field function: returns initials (scalar)
CREATE FUNCTION test_api.initials(u test_api.users)
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT UPPER(SUBSTRING(u.name, 1, 1) || COALESCE(SUBSTRING(u.name, POSITION(' ' IN u.name) + 1, 1), ''));
$$;

-- Function returning a single row
CREATE FUNCTION test_api.get_user_by_email(email_param TEXT)
RETURNS test_api.users
LANGUAGE SQL
STABLE
AS $$
    SELECT * FROM test_api.users WHERE email = email_param LIMIT 1;
$$;

-- Function with optional parameters
CREATE FUNCTION test_api.search_posts(
    search_term TEXT,
    published_only BOOLEAN DEFAULT true,
    limit_count INTEGER DEFAULT 10
)
RETURNS SETOF test_api.posts
LANGUAGE SQL
STABLE
AS $$
    SELECT * FROM test_api.posts
    WHERE (body ILIKE '%' || search_term || '%' OR title ILIKE '%' || search_term || '%')
      AND (NOT published_only OR published = true)
    LIMIT limit_count;
$$;

-- Variadic function
CREATE FUNCTION test_api.concat_values(VARIADIC vals TEXT[])
RETURNS TEXT
LANGUAGE SQL
IMMUTABLE
AS $$
    SELECT array_to_string(vals, ', ');
$$;

-- ============================================================================
-- Sample Data
-- ============================================================================

-- Insert roles
INSERT INTO test_api.roles (name, permissions) VALUES
    ('admin', '["read", "write", "delete", "admin"]'),
    ('editor', '["read", "write"]'),
    ('viewer', '["read"]');

-- Insert users
INSERT INTO test_api.users (email, name, status) VALUES
    ('alice@example.com', 'Alice Johnson', 'active'),
    ('bob@example.com', 'Bob Smith', 'active'),
    ('charlie@example.com', 'Charlie Brown', 'inactive'),
    ('diana@example.com', 'Diana Prince', 'pending');

-- Insert profiles
INSERT INTO test_api.profiles (user_id, avatar_url, website) VALUES
    (1, 'https://example.com/alice.jpg', 'https://alice.example.com'),
    (2, 'https://example.com/bob.jpg', NULL);

-- Insert organizations
INSERT INTO test_api.organizations (name, slug, description) VALUES
    ('Acme Corp', 'acme', 'A sample organization'),
    ('Tech Inc', 'tech-inc', 'Technology company');

-- Insert user roles
INSERT INTO test_api.user_roles (user_id, role_id) VALUES
    (1, 1), -- Alice is admin
    (2, 2), -- Bob is editor
    (3, 3); -- Charlie is viewer

-- Insert org members
INSERT INTO test_api.org_members (org_id, user_id, role) VALUES
    (1, 1, 'owner'),
    (1, 2, 'member'),
    (2, 1, 'member');

-- Insert posts
INSERT INTO test_api.posts (user_id, title, body, published, tags) VALUES
    (1, 'Hello World', 'This is my first post!', true, '{"intro", "welcome"}'),
    (1, 'Advanced Topics', 'Let''s dive deeper...', true, '{"advanced"}'),
    (2, 'Draft Post', 'Work in progress', false, '{}'),
    (2, 'Tips and Tricks', 'Here are some useful tips', true, '{"tips"}');

-- Insert comments
INSERT INTO test_api.comments (post_id, user_id, body) VALUES
    (1, 2, 'Great first post!'),
    (1, 3, 'Welcome to the platform!');

-- Insert reply comment (self-reference)
INSERT INTO test_api.comments (post_id, user_id, parent_id, body) VALUES
    (1, 1, 1, 'Thanks Bob!');

-- Insert tasks
INSERT INTO test_api.tasks (title, priority, assigned_to, due_date) VALUES
    ('Complete documentation', 'high', 1, CURRENT_DATE + INTERVAL '7 days'),
    ('Review PR', 'medium', 2, CURRENT_DATE + INTERVAL '2 days'),
    ('Fix bug', 'critical', 1, CURRENT_DATE);

-- ============================================================================
-- Products table (for e2e mutation & numeric filter tests)
-- ============================================================================

CREATE TABLE test_api.products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(10,2) NOT NULL DEFAULT 0,
    in_stock BOOLEAN DEFAULT true,
    category TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

COMMENT ON TABLE test_api.products IS 'Product catalog';

CREATE TABLE test_api.order_items (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL REFERENCES test_api.products(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES test_api.users(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

INSERT INTO test_api.products (name, price, in_stock, category) VALUES
    ('Widget', 9.99, true, 'gadgets'),
    ('Gizmo', 24.50, true, 'gadgets'),
    ('Thingamajig', 99.99, false, 'gadgets'),
    ('Doohickey', 4.99, true, 'tools');

INSERT INTO test_api.order_items (product_id, user_id, quantity) VALUES
    (1, 1, 2),
    (2, 1, 1),
    (1, 2, 5);

-- ============================================================================
-- Roles and RLS (for auth-gated e2e tests)
-- ============================================================================

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'web_anon') THEN
        CREATE ROLE web_anon NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin_user') THEN
        CREATE ROLE admin_user NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'test_authenticator') THEN
        CREATE ROLE test_authenticator NOLOGIN;
    END IF;
END
$$;

-- ============================================================================
-- Composite Types and Array Columns (for testing JSON/composite access)
-- ============================================================================

-- Create composite type for coordinates
CREATE TYPE test_api.coordinates AS (
    lat DECIMAL(8,6),
    long DECIMAL(9,6)
);

-- Countries table with composite and array columns
CREATE TABLE test_api.countries (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    location test_api.coordinates,
    languages TEXT[] DEFAULT '{}',
    population INTEGER,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

COMMENT ON TABLE test_api.countries IS 'Countries with composite location and array languages';
COMMENT ON COLUMN test_api.countries.location IS 'Geographic coordinates (composite type)';
COMMENT ON COLUMN test_api.countries.languages IS 'Array of language codes';

-- Insert test data for countries
INSERT INTO test_api.countries (name, location, languages, population) VALUES
    ('United States', ROW(37.7749, -122.4194)::test_api.coordinates, ARRAY['en', 'es'], 331000000),
    ('Canada', ROW(45.5017, -73.5673)::test_api.coordinates, ARRAY['en', 'fr'], 38000000),
    ('Mexico', ROW(19.4326, -99.1332)::test_api.coordinates, ARRAY['es'], 128000000),
    ('France', ROW(48.8566, 2.3522)::test_api.coordinates, ARRAY['fr'], 67000000),
    ('Germany', ROW(52.5200, 13.4050)::test_api.coordinates, ARRAY['de'], 83000000);

-- Grant schema usage
GRANT USAGE ON SCHEMA test_api TO web_anon;
GRANT USAGE ON SCHEMA test_api TO admin_user;
GRANT USAGE ON SCHEMA test_api TO test_authenticator;

-- Grant role switching
GRANT web_anon TO test_authenticator;
GRANT admin_user TO test_authenticator;

-- web_anon: read-only on most tables
GRANT SELECT ON ALL TABLES IN SCHEMA test_api TO web_anon;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA test_api TO web_anon;

-- admin_user: full access
GRANT ALL ON ALL TABLES IN SCHEMA test_api TO admin_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA test_api TO admin_user;

-- RLS on posts: anon sees only published, admin sees all
ALTER TABLE test_api.posts ENABLE ROW LEVEL SECURITY;

CREATE POLICY posts_anon_read ON test_api.posts
    FOR SELECT TO web_anon
    USING (published = true);

CREATE POLICY posts_admin_all ON test_api.posts
    FOR ALL TO admin_user
    USING (true)
    WITH CHECK (true);
