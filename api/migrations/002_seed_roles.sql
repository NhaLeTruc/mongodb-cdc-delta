-- ============================================================================
-- Migration: 002_seed_roles.sql
-- Description: Seed default roles and create admin user
-- Author: AI Assistant
-- Date: 2025-12-02
-- ============================================================================

-- This migration populates the roles table with default system roles and
-- creates a default admin user with all roles assigned. This migration is
-- idempotent and can be safely run multiple times.

-- ============================================================================
-- SEED ROLES
-- ============================================================================

-- Insert roles only if they don't already exist
-- Using ON CONFLICT to make this migration idempotent

INSERT INTO roles (name, description)
VALUES
    (
        'admin',
        'Full system access with all permissions including user management, system configuration, and sensitive data access'
    ),
    (
        'operator',
        'Operational access to manage pipelines, trigger syncs, pause/resume operations, and view metrics'
    ),
    (
        'analyst',
        'Read-only access to view mappings, metrics, checkpoints, and audit logs for analysis purposes'
    ),
    (
        'viewer',
        'Limited read-only access to basic metrics and mapping information without access to sensitive data'
    )
ON CONFLICT (name) DO UPDATE
SET
    description = EXCLUDED.description,
    updated_at = CURRENT_TIMESTAMP;

-- ============================================================================
-- CREATE DEFAULT ADMIN USER
-- ============================================================================

-- Create default admin user if it doesn't exist
-- Default password: "Admin123!" (MUST be changed in production)
-- Password hash generated using BCrypt with 12 rounds

DO $$
DECLARE
    v_admin_user_id UUID;
    v_admin_role_id INTEGER;
    v_operator_role_id INTEGER;
    v_analyst_role_id INTEGER;
    v_viewer_role_id INTEGER;
BEGIN
    -- Check if admin user already exists
    SELECT id INTO v_admin_user_id
    FROM users
    WHERE username = 'admin';

    -- Create admin user if not exists
    IF v_admin_user_id IS NULL THEN
        -- Insert admin user
        -- Password: Admin123! (BCrypt hash with 12 rounds)
        INSERT INTO users (username, email, password_hash, is_active)
        VALUES (
            'admin',
            'admin@cdc-pipeline.local',
            '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/L3MjP2fJ5kZ4j0YEy', -- Admin123!
            TRUE
        )
        RETURNING id INTO v_admin_user_id;

        RAISE NOTICE 'Created default admin user with ID: %', v_admin_user_id;
    ELSE
        RAISE NOTICE 'Admin user already exists with ID: %', v_admin_user_id;
    END IF;

    -- Get role IDs
    SELECT id INTO v_admin_role_id FROM roles WHERE name = 'admin';
    SELECT id INTO v_operator_role_id FROM roles WHERE name = 'operator';
    SELECT id INTO v_analyst_role_id FROM roles WHERE name = 'analyst';
    SELECT id INTO v_viewer_role_id FROM roles WHERE name = 'viewer';

    -- Assign all roles to admin user (idempotent)
    INSERT INTO user_roles (user_id, role_id, assigned_by)
    VALUES
        (v_admin_user_id, v_admin_role_id, NULL),
        (v_admin_user_id, v_operator_role_id, NULL),
        (v_admin_user_id, v_analyst_role_id, NULL),
        (v_admin_user_id, v_viewer_role_id, NULL)
    ON CONFLICT (user_id, role_id) DO NOTHING;

    RAISE NOTICE 'Assigned all roles to admin user';
END $$;

-- ============================================================================
-- VERIFICATION
-- ============================================================================

-- Verify roles were created
DO $$
DECLARE
    role_count INTEGER;
    admin_user_exists BOOLEAN;
    admin_role_count INTEGER;
BEGIN
    -- Check roles
    SELECT COUNT(*) INTO role_count FROM roles;

    IF role_count < 4 THEN
        RAISE EXCEPTION 'Migration failed: Expected at least 4 roles, found %', role_count;
    END IF;

    RAISE NOTICE 'Verified % roles exist in the system', role_count;

    -- Check admin user exists
    SELECT EXISTS(SELECT 1 FROM users WHERE username = 'admin')
    INTO admin_user_exists;

    IF NOT admin_user_exists THEN
        RAISE EXCEPTION 'Migration failed: Admin user not created';
    END IF;

    RAISE NOTICE 'Verified admin user exists';

    -- Check admin has all roles
    SELECT COUNT(*) INTO admin_role_count
    FROM user_roles ur
    JOIN users u ON ur.user_id = u.id
    WHERE u.username = 'admin';

    IF admin_role_count < 4 THEN
        RAISE WARNING 'Admin user has only % roles assigned, expected at least 4', admin_role_count;
    ELSE
        RAISE NOTICE 'Verified admin user has % roles assigned', admin_role_count;
    END IF;

    RAISE NOTICE 'Migration 002_seed_roles.sql completed successfully';
END $$;

-- ============================================================================
-- SECURITY REMINDER
-- ============================================================================

-- Display security reminder
DO $$
BEGIN
    RAISE WARNING '╔════════════════════════════════════════════════════════════╗';
    RAISE WARNING '║          SECURITY NOTICE - ACTION REQUIRED                 ║';
    RAISE WARNING '╠════════════════════════════════════════════════════════════╣';
    RAISE WARNING '║ Default admin credentials have been created:               ║';
    RAISE WARNING '║   Username: admin                                          ║';
    RAISE WARNING '║   Password: Admin123!                                      ║';
    RAISE WARNING '║                                                            ║';
    RAISE WARNING '║ ⚠️  CHANGE THIS PASSWORD IMMEDIATELY IN PRODUCTION!  ⚠️   ║';
    RAISE WARNING '║                                                            ║';
    RAISE WARNING '║ To change the password, use the API endpoint:              ║';
    RAISE WARNING '║   PATCH /api/v1/users/{user_id}                            ║';
    RAISE WARNING '║                                                            ║';
    RAISE WARNING '║ Or update directly in the database:                        ║';
    RAISE WARNING '║   UPDATE users                                             ║';
    RAISE WARNING '║   SET password_hash = crypt(''your-new-password'', gen_salt(''bf'', 12)) ║';
    RAISE WARNING '║   WHERE username = ''admin'';                                ║';
    RAISE WARNING '╚════════════════════════════════════════════════════════════╝';
END $$;
