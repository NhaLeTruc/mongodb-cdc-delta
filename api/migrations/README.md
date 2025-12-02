# Database Migrations

This directory contains SQL migration scripts for the CDC Pipeline Management API database schema. All migrations are designed to be idempotent and production-ready.

## Overview

The migration system uses PostgreSQL SQL scripts that can be executed manually or through automation tools. Each migration file is numbered sequentially and includes verification checks to ensure data integrity.

## Migration Files

| File | Description | Status |
|------|-------------|--------|
| `001_initial_schema.sql` | Creates core tables (users, roles, user_roles, audit_logs) | Required |
| `002_seed_roles.sql` | Seeds default roles and creates admin user | Required |

## Prerequisites

Before running migrations, ensure you have:

1. PostgreSQL 12+ installed and running
2. Database created: `cdc_metadata`
3. Database user with appropriate permissions
4. Connection credentials configured in environment variables

### Required PostgreSQL Extensions

The migrations require the following PostgreSQL extensions:
- `uuid-ossp` - UUID generation
- `pgcrypto` - Password hashing functions

These extensions are automatically installed by the migration scripts if not already present.

## Running Migrations

### Method 1: Using psql (Recommended for Manual Execution)

Run migrations in order using the PostgreSQL command-line tool:

```bash
# Set database connection parameters
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=cdc_metadata
export PGUSER=cdc_api
export PGPASSWORD=cdc_password

# Run migrations in order
psql -f 001_initial_schema.sql
psql -f 002_seed_roles.sql
```

### Method 2: Using Docker (For Containerized Environments)

If running PostgreSQL in Docker:

```bash
# Copy migration files to container
docker cp 001_initial_schema.sql postgres:/tmp/
docker cp 002_seed_roles.sql postgres:/tmp/

# Execute migrations
docker exec -i postgres psql -U cdc_api -d cdc_metadata -f /tmp/001_initial_schema.sql
docker exec -i postgres psql -U cdc_api -d cdc_metadata -f /tmp/002_seed_roles.sql
```

### Method 3: Using Docker Compose Init Script

For automated execution during container startup, copy migration files to the PostgreSQL init directory:

```bash
# Assuming your docker-compose.yaml mounts ./postgres-init:/docker-entrypoint-initdb.d
cp 001_initial_schema.sql ../postgres-init/
cp 002_seed_roles.sql ../postgres-init/

# Restart PostgreSQL container
docker-compose down postgres
docker-compose up -d postgres
```

**Note:** Init scripts only run on first container startup when the database is empty.

### Method 4: Using Python Script (For Automated Deployments)

Create a migration runner script:

```python
#!/usr/bin/env python3
import psycopg2
from pathlib import Path

# Database connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="cdc_metadata",
    user="cdc_api",
    password="cdc_password"
)

# Run migrations
migrations_dir = Path(__file__).parent
migrations = sorted(migrations_dir.glob("*.sql"))

for migration in migrations:
    print(f"Running {migration.name}...")
    with open(migration) as f:
        cursor = conn.cursor()
        cursor.execute(f.read())
        conn.commit()
        cursor.close()
    print(f"✓ {migration.name} completed")

conn.close()
print("All migrations completed successfully")
```

## Migration Order

**CRITICAL:** Migrations must be executed in the following order:

1. `001_initial_schema.sql` - Creates all tables, indexes, and constraints
2. `002_seed_roles.sql` - Populates roles and creates default admin user

Running migrations out of order will result in foreign key constraint violations.

## Idempotency

All migration scripts are designed to be idempotent, meaning they can be safely run multiple times without causing errors or data duplication:

- Tables use `CREATE TABLE IF NOT EXISTS`
- Indexes use `CREATE INDEX IF NOT EXISTS`
- Role inserts use `ON CONFLICT DO UPDATE`
- User creation checks for existing users before inserting
- Role assignments use `ON CONFLICT DO NOTHING`

This design allows for:
- Safe re-execution after failures
- Simplified deployment pipelines
- Development environment resets

## Verification

After running migrations, verify the schema was created correctly:

```sql
-- Check tables exist
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
ORDER BY table_name;

-- Expected output:
-- audit_logs
-- roles
-- user_roles
-- users

-- Check roles were seeded
SELECT name, description FROM roles ORDER BY name;

-- Expected output:
-- admin    | Full system access...
-- analyst  | Read-only access...
-- operator | Operational access...
-- viewer   | Limited read-only access...

-- Check admin user exists
SELECT username, email, is_active
FROM users
WHERE username = 'admin';

-- Expected output:
-- admin | admin@cdc-pipeline.local | t

-- Check admin has all roles
SELECT u.username, r.name as role
FROM users u
JOIN user_roles ur ON u.id = ur.user_id
JOIN roles r ON ur.role_id = r.id
WHERE u.username = 'admin'
ORDER BY r.name;

-- Expected output (4 rows):
-- admin | admin
-- admin | analyst
-- admin | operator
-- admin | viewer
```

## Rollback Procedures

### Rolling Back 002_seed_roles.sql

To remove seeded data while preserving schema:

```sql
-- Remove admin user (cascade deletes user_roles entries)
DELETE FROM users WHERE username = 'admin';

-- Remove all roles (cascade deletes user_roles entries)
DELETE FROM roles;

-- Verify
SELECT COUNT(*) FROM users;    -- Should be 0
SELECT COUNT(*) FROM roles;    -- Should be 0
SELECT COUNT(*) FROM user_roles; -- Should be 0
```

### Rolling Back 001_initial_schema.sql

To completely remove all tables and start fresh:

```sql
-- Drop tables in reverse order of dependencies
DROP TABLE IF EXISTS audit_logs CASCADE;
DROP TABLE IF EXISTS user_roles CASCADE;
DROP TABLE IF EXISTS roles CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Drop trigger function
DROP FUNCTION IF EXISTS update_updated_at_column() CASCADE;

-- Verify
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public';
-- Should return no results
```

**WARNING:** This will permanently delete all data. Always backup before rolling back in production.

## Security Considerations

### Default Admin Credentials

The `002_seed_roles.sql` migration creates a default admin user:

- **Username:** `admin`
- **Password:** `Admin123!`

**CRITICAL SECURITY REQUIREMENT:**

These default credentials MUST be changed immediately in any non-development environment:

```bash
# Method 1: Using the API
curl -X PATCH http://localhost:8000/api/v1/users/{user_id} \
  -H "Authorization: Bearer {token}" \
  -H "Content-Type: application/json" \
  -d '{"password": "your-strong-password-here"}'

# Method 2: Directly in database
psql -c "UPDATE users SET password_hash = crypt('your-new-password', gen_salt('bf', 12)) WHERE username = 'admin';" cdc_metadata
```

### Password Hashing

All passwords are hashed using BCrypt with 12 rounds (configurable via `CDC_API_PASSWORD_BCRYPT_ROUNDS`). The default hash in the migration was generated as:

```python
import bcrypt
password = "Admin123!"
hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12))
print(hashed.decode())
```

### Audit Trail

The `audit_logs` table provides a comprehensive audit trail for compliance:
- All user actions are logged
- IP addresses and user agents are captured
- JSONB details column stores request/response context
- Indexed for fast querying and reporting

## Troubleshooting

### Migration Fails with Permission Denied

Ensure your database user has sufficient privileges:

```sql
-- Grant necessary permissions
GRANT ALL PRIVILEGES ON DATABASE cdc_metadata TO cdc_api;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cdc_api;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cdc_api;
```

### Extension Installation Fails

If PostgreSQL extensions cannot be installed:

```sql
-- Install extensions as superuser
\c cdc_metadata postgres
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
```

### Foreign Key Violations

If you encounter foreign key errors:
1. Ensure migrations are run in the correct order
2. Verify no manual data modifications were made
3. Roll back and re-run migrations in sequence

### Migration Already Applied

Migrations are idempotent. If a migration appears to fail but tables exist:

```sql
-- Check what exists
\dt  -- List tables
\di  -- List indexes

-- If schema is correct, the migration already succeeded
-- Re-running will update existing rows and skip existing objects
```

## Database Schema Diagram

```
┌─────────────────────┐
│       users         │
├─────────────────────┤
│ id (PK)            │←──┐
│ username (UNIQUE)  │   │
│ email (UNIQUE)     │   │
│ password_hash      │   │
│ is_active          │   │
│ created_at         │   │
│ updated_at         │   │
└─────────────────────┘   │
         ↑                │
         │                │
         │                │
┌────────┴────────────┐   │
│    user_roles       │   │
├─────────────────────┤   │
│ user_id (PK, FK)   │───┘
│ role_id (PK, FK)   │───┐
│ assigned_at        │   │
│ assigned_by (FK)   │   │
└─────────────────────┘   │
         │                │
         ↓                │
┌─────────────────────┐   │
│       roles         │   │
├─────────────────────┤   │
│ id (PK)            │←──┘
│ name (UNIQUE)      │
│ description        │
│ created_at         │
│ updated_at         │
└─────────────────────┘

┌─────────────────────┐
│    audit_logs       │
├─────────────────────┤
│ id (PK)            │
│ user_id (FK)       │───→ users.id
│ action             │
│ resource_type      │
│ resource_id        │
│ details (JSONB)    │
│ ip_address         │
│ user_agent         │
│ status_code        │
│ created_at         │
└─────────────────────┘
```

## Production Deployment Checklist

- [ ] Backup existing database (if applicable)
- [ ] Review migration scripts for environment-specific changes
- [ ] Test migrations in staging environment first
- [ ] Verify database user has necessary permissions
- [ ] Ensure PostgreSQL extensions are available
- [ ] Run migrations during maintenance window
- [ ] Verify schema creation with verification queries
- [ ] **Change default admin password immediately**
- [ ] Test application connectivity and authentication
- [ ] Monitor audit_logs for any errors
- [ ] Document migration completion in deployment log

## Support

For issues or questions regarding migrations:

1. Check the troubleshooting section above
2. Verify PostgreSQL version compatibility (12+)
3. Review PostgreSQL logs for detailed error messages
4. Consult the API documentation at `/docs` endpoint

## Maintenance

### Audit Log Retention

The `audit_logs` table will grow over time. Implement a retention policy:

```sql
-- Delete audit logs older than 90 days (configurable via CDC_API_AUDIT_RETENTION_DAYS)
DELETE FROM audit_logs
WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '90 days';

-- Or create a scheduled job (cron/pg_cron)
SELECT cron.schedule('cleanup-audit-logs', '0 2 * * *', $$
  DELETE FROM audit_logs WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '90 days'
$$);
```

### Vacuum and Analyze

Regularly maintain database performance:

```sql
-- After large deletions
VACUUM ANALYZE audit_logs;

-- Full vacuum (requires maintenance window)
VACUUM FULL ANALYZE;
```
