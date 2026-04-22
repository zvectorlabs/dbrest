# Setting up SSE (Server-Sent Events) for PostgreSQL

dbREST can stream real-time change events via SSE when a PostgreSQL trigger is installed.

## Step 1: Create the notify function

```sql
CREATE OR REPLACE FUNCTION dbrest_notify() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify(
    'dbrest_changes',
    json_build_object(
      'table', TG_TABLE_NAME,
      'schema', TG_TABLE_SCHEMA,
      'event', TG_OP,
      'new', CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW) ELSE NULL END,
      'old', CASE WHEN TG_OP IN ('DELETE', 'UPDATE') THEN row_to_json(OLD) ELSE NULL END
    )::text
  );
  RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;
```

## Step 2: Attach trigger to tables you want to monitor

```sql
CREATE TRIGGER posts_changes
  AFTER INSERT OR UPDATE OR DELETE ON posts
  FOR EACH ROW EXECUTE FUNCTION dbrest_notify();

CREATE TRIGGER users_changes
  AFTER INSERT OR UPDATE OR DELETE ON users
  FOR EACH ROW EXECUTE FUNCTION dbrest_notify();
```

## Step 3: Connect via SSE

```bash
curl -N 'http://localhost:3000/listen/posts?token=<jwt>'
```
