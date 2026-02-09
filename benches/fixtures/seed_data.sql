-- Benchmark seed data for PgREST performance testing
-- This file generates realistic test data for benchmarking
-- Reuses schema from tests/fixtures/schema.sql

-- Insert 10,000 users
INSERT INTO test_api.users (name, email, bio, status)
SELECT
  'User ' || i,
  'user' || i || '@example.com',
  'Bio for user ' || i || '. This is a longer bio to simulate realistic data.',
  CASE (i % 3)
    WHEN 0 THEN 'active'::test_api.user_status
    WHEN 1 THEN 'inactive'::test_api.user_status
    ELSE 'pending'::test_api.user_status
  END
FROM generate_series(1, 10000) AS i
ON CONFLICT (email) DO NOTHING;

-- Insert 50,000 posts
INSERT INTO test_api.posts (title, body, user_id, published, view_count, tags)
SELECT
  'Post ' || i,
  'Body for post ' || i || '. This is a longer body text to simulate realistic blog post content. ' ||
  'Lorem ipsum dolor sit amet, consectetur adipiscing elit. ' ||
  'Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.',
  (random() * 9999 + 1)::int,
  random() > 0.3, -- 70% published
  (random() * 1000)::int,
  ARRAY['tag' || (random() * 10)::int, 'category' || (random() * 5)::int]
FROM generate_series(1, 50000) AS i;

-- Insert 200,000 comments
INSERT INTO test_api.comments (body, post_id, user_id)
SELECT
  'Comment ' || i || ' on post. This is a sample comment text.',
  (random() * 49999 + 1)::int,
  (random() * 9999 + 1)::int
FROM generate_series(1, 200000) AS i;

-- Insert some profiles (O2O with users)
INSERT INTO test_api.profiles (user_id, avatar_url, website, location, birth_date)
SELECT
  i,
  'https://example.com/avatars/user' || i || '.jpg',
  'https://example.com/user' || i,
  'Location ' || (i % 100),
  CURRENT_DATE - INTERVAL '20 years' - (random() * 365 * 40)::int * INTERVAL '1 day'
FROM generate_series(1, 5000) AS i
ON CONFLICT (user_id) DO NOTHING;

-- Insert organizations
INSERT INTO test_api.organizations (name, slug, description)
SELECT
  'Organization ' || i,
  'org-' || i,
  'Description for organization ' || i
FROM generate_series(1, 100) AS i
ON CONFLICT (slug) DO NOTHING;

-- Insert roles
INSERT INTO test_api.roles (name, permissions)
SELECT
  'role_' || i,
  jsonb_build_array('permission_' || i, 'permission_' || (i + 1))
FROM generate_series(1, 20) AS i
ON CONFLICT (name) DO NOTHING;

-- Insert user roles (M2M)
INSERT INTO test_api.user_roles (user_id, role_id)
SELECT DISTINCT
  (random() * 9999 + 1)::int,
  (random() * 19 + 1)::int
FROM generate_series(1, 10000)
ON CONFLICT (user_id, role_id) DO NOTHING;

-- Insert organization members (M2M)
INSERT INTO test_api.org_members (org_id, user_id, role)
SELECT DISTINCT
  (random() * 99 + 1)::int,
  (random() * 9999 + 1)::int,
  CASE (random() * 3)::int
    WHEN 0 THEN 'admin'
    WHEN 1 THEN 'member'
    ELSE 'viewer'
  END
FROM generate_series(1, 5000)
ON CONFLICT (org_id, user_id) DO NOTHING;

-- Analyze tables for query planner
ANALYZE test_api.users;
ANALYZE test_api.posts;
ANALYZE test_api.comments;
ANALYZE test_api.profiles;
ANALYZE test_api.organizations;
ANALYZE test_api.roles;
ANALYZE test_api.user_roles;
ANALYZE test_api.org_members;
