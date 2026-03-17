-- Test schema for dbrest SQLite integration tests
-- SQLite equivalent of schema.sql (no schemas, no enums, no arrays, no RLS)

-- ============================================================================
-- Core Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    bio TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS profiles (
    user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    avatar_url TEXT,
    website TEXT,
    location TEXT,
    birth_date TEXT
);

CREATE TABLE IF NOT EXISTS organizations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    description TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS roles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    permissions TEXT DEFAULT '[]'
);

-- ============================================================================
-- Junction Tables (M2M)
-- ============================================================================

CREATE TABLE IF NOT EXISTS user_roles (
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role_id INTEGER NOT NULL REFERENCES roles(id) ON DELETE CASCADE,
    granted_at TEXT DEFAULT (datetime('now')),
    granted_by INTEGER REFERENCES users(id),
    PRIMARY KEY (user_id, role_id)
);

CREATE TABLE IF NOT EXISTS org_members (
    org_id INTEGER NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT DEFAULT 'member',
    joined_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (org_id, user_id)
);

-- ============================================================================
-- Posts and Comments
-- ============================================================================

CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title TEXT NOT NULL,
    body TEXT,
    published INTEGER DEFAULT 0,
    view_count INTEGER DEFAULT 0,
    metadata TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    post_id INTEGER NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    parent_id INTEGER REFERENCES comments(id) ON DELETE CASCADE,
    body TEXT NOT NULL,
    created_at TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- Tasks (various column types)
-- ============================================================================

CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    priority TEXT DEFAULT 'medium',
    due_date TEXT,
    due_time TEXT,
    estimated_hours REAL,
    actual_hours REAL,
    is_completed INTEGER DEFAULT 0,
    assigned_to INTEGER REFERENCES users(id),
    extra TEXT DEFAULT '{}',
    created_at TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- Products and Order Items
-- ============================================================================

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price REAL NOT NULL DEFAULT 0,
    in_stock INTEGER DEFAULT 1,
    category TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS order_items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id INTEGER NOT NULL REFERENCES products(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- Views
-- ============================================================================

CREATE VIEW IF NOT EXISTS active_users AS
SELECT id, email, name, created_at
FROM users
WHERE status = 'active';

CREATE VIEW IF NOT EXISTS published_posts AS
SELECT p.id, p.title, p.body, p.created_at, u.name AS author_name
FROM posts p
JOIN users u ON p.user_id = u.id
WHERE p.published = 1;

-- ============================================================================
-- Countries (simpler than PG — no composites or arrays)
-- ============================================================================

CREATE TABLE IF NOT EXISTS countries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    lat REAL,
    long REAL,
    population INTEGER,
    created_at TEXT DEFAULT (datetime('now'))
);

-- ============================================================================
-- Sample Data
-- ============================================================================

INSERT INTO roles (name, permissions) VALUES
    ('admin', '["read", "write", "delete", "admin"]'),
    ('editor', '["read", "write"]'),
    ('viewer', '["read"]');

INSERT INTO users (email, name, status) VALUES
    ('alice@example.com', 'Alice Johnson', 'active'),
    ('bob@example.com', 'Bob Smith', 'active'),
    ('charlie@example.com', 'Charlie Brown', 'inactive'),
    ('diana@example.com', 'Diana Prince', 'pending');

INSERT INTO profiles (user_id, avatar_url, website) VALUES
    (1, 'https://example.com/alice.jpg', 'https://alice.example.com'),
    (2, 'https://example.com/bob.jpg', NULL);

INSERT INTO organizations (name, slug, description) VALUES
    ('Acme Corp', 'acme', 'A sample organization'),
    ('Tech Inc', 'tech-inc', 'Technology company');

INSERT INTO user_roles (user_id, role_id) VALUES
    (1, 1),
    (2, 2),
    (3, 3);

INSERT INTO org_members (org_id, user_id, role) VALUES
    (1, 1, 'owner'),
    (1, 2, 'member'),
    (2, 1, 'member');

INSERT INTO posts (user_id, title, body, published) VALUES
    (1, 'Hello World', 'This is my first post!', 1),
    (1, 'Advanced Topics', 'Let''s dive deeper...', 1),
    (2, 'Draft Post', 'Work in progress', 0),
    (2, 'Tips and Tricks', 'Here are some useful tips', 1);

INSERT INTO comments (post_id, user_id, body) VALUES
    (1, 2, 'Great first post!'),
    (1, 3, 'Welcome to the platform!');

INSERT INTO comments (post_id, user_id, parent_id, body) VALUES
    (1, 1, 1, 'Thanks Bob!');

INSERT INTO tasks (title, priority, assigned_to, due_date) VALUES
    ('Complete documentation', 'high', 1, date('now', '+7 days')),
    ('Review PR', 'medium', 2, date('now', '+2 days')),
    ('Fix bug', 'critical', 1, date('now'));

INSERT INTO products (name, price, in_stock, category) VALUES
    ('Widget', 9.99, 1, 'gadgets'),
    ('Gizmo', 24.50, 1, 'gadgets'),
    ('Thingamajig', 99.99, 0, 'gadgets'),
    ('Doohickey', 4.99, 1, 'tools');

INSERT INTO order_items (product_id, user_id, quantity) VALUES
    (1, 1, 2),
    (2, 1, 1),
    (1, 2, 5);

INSERT INTO countries (name, lat, long, population) VALUES
    ('United States', 37.7749, -122.4194, 331000000),
    ('Canada', 45.5017, -73.5673, 38000000),
    ('Mexico', 19.4326, -99.1332, 128000000),
    ('France', 48.8566, 2.3522, 67000000),
    ('Germany', 52.5200, 13.4050, 83000000);
