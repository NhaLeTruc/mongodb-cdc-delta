-- ============================================================================
-- Migration: 001_initial_schema.sql
-- Description: Initial database schema for CDC Pipeline Management API
-- Author: AI Assistant
-- Date: 2025-12-02
-- ============================================================================

-- This migration creates the core authentication and audit logging tables
-- for the CDC Pipeline Management API. It includes users, roles, user_roles
-- junction table, and audit_logs table with appropriate indexes and constraints.

-- ============================================================================
-- EXTENSIONS
-- ============================================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Enable pgcrypto for additional cryptographic functions
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- TABLES
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Users Table
-- ----------------------------------------------------------------------------
-- Stores user accounts with authentication credentials and status

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username VARCHAR(50) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT users_username_length CHECK (char_length(username) >= 3),
    CONSTRAINT users_email_format CHECK (email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
);

-- Add table comment
COMMENT ON TABLE users IS 'User accounts for authentication and authorization';
COMMENT ON COLUMN users.id IS 'Unique user identifier (UUID)';
COMMENT ON COLUMN users.username IS 'Unique username (3-50 characters)';
COMMENT ON COLUMN users.email IS 'Unique email address';
COMMENT ON COLUMN users.password_hash IS 'BCrypt hashed password';
COMMENT ON COLUMN users.is_active IS 'Account active status (soft delete flag)';
COMMENT ON COLUMN users.created_at IS 'Account creation timestamp (UTC)';
COMMENT ON COLUMN users.updated_at IS 'Last account update timestamp (UTC)';

-- ----------------------------------------------------------------------------
-- Roles Table
-- ----------------------------------------------------------------------------
-- Stores available roles in the system with descriptions

CREATE TABLE IF NOT EXISTS roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE,

    -- Constraints
    CONSTRAINT roles_name_lowercase CHECK (name = lower(name)),
    CONSTRAINT roles_name_length CHECK (char_length(name) >= 2)
);

-- Add table comment
COMMENT ON TABLE roles IS 'Available roles for role-based access control (RBAC)';
COMMENT ON COLUMN roles.id IS 'Unique role identifier (auto-increment)';
COMMENT ON COLUMN roles.name IS 'Unique role name (lowercase, 2-50 characters)';
COMMENT ON COLUMN roles.description IS 'Human-readable role description';
COMMENT ON COLUMN roles.created_at IS 'Role creation timestamp (UTC)';
COMMENT ON COLUMN roles.updated_at IS 'Last role update timestamp (UTC)';

-- ----------------------------------------------------------------------------
-- User Roles Junction Table
-- ----------------------------------------------------------------------------
-- Many-to-many relationship between users and roles

CREATE TABLE IF NOT EXISTS user_roles (
    user_id UUID NOT NULL,
    role_id INTEGER NOT NULL,
    assigned_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    assigned_by UUID,

    -- Primary key (composite)
    PRIMARY KEY (user_id, role_id),

    -- Foreign keys
    CONSTRAINT fk_user_roles_user_id
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_user_roles_role_id
        FOREIGN KEY (role_id)
        REFERENCES roles(id)
        ON DELETE CASCADE,

    CONSTRAINT fk_user_roles_assigned_by
        FOREIGN KEY (assigned_by)
        REFERENCES users(id)
        ON DELETE SET NULL
);

-- Add table comment
COMMENT ON TABLE user_roles IS 'Junction table mapping users to their assigned roles';
COMMENT ON COLUMN user_roles.user_id IS 'Reference to users table';
COMMENT ON COLUMN user_roles.role_id IS 'Reference to roles table';
COMMENT ON COLUMN user_roles.assigned_at IS 'Timestamp when role was assigned (UTC)';
COMMENT ON COLUMN user_roles.assigned_by IS 'User who assigned the role (nullable)';

-- ----------------------------------------------------------------------------
-- Audit Logs Table
-- ----------------------------------------------------------------------------
-- Comprehensive audit trail for all system activities

CREATE TABLE IF NOT EXISTS audit_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID,
    action VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50),
    resource_id VARCHAR(255),
    details JSONB,
    ip_address INET,
    user_agent TEXT,
    status_code INTEGER,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Foreign key (nullable for anonymous actions)
    CONSTRAINT fk_audit_logs_user_id
        FOREIGN KEY (user_id)
        REFERENCES users(id)
        ON DELETE SET NULL,

    -- Constraints
    CONSTRAINT audit_logs_action_length CHECK (char_length(action) >= 2),
    CONSTRAINT audit_logs_status_code_range CHECK (status_code IS NULL OR (status_code >= 100 AND status_code <= 599))
);

-- Add table comment
COMMENT ON TABLE audit_logs IS 'Comprehensive audit trail for security and compliance';
COMMENT ON COLUMN audit_logs.id IS 'Unique audit log identifier (UUID)';
COMMENT ON COLUMN audit_logs.user_id IS 'Reference to user who performed action (nullable for anonymous)';
COMMENT ON COLUMN audit_logs.action IS 'Action performed (e.g., login_success, user_create)';
COMMENT ON COLUMN audit_logs.resource_type IS 'Type of resource affected (e.g., user, mapping)';
COMMENT ON COLUMN audit_logs.resource_id IS 'Identifier of affected resource';
COMMENT ON COLUMN audit_logs.details IS 'Additional context as JSON (request/response data)';
COMMENT ON COLUMN audit_logs.ip_address IS 'Client IP address (IPv4 or IPv6)';
COMMENT ON COLUMN audit_logs.user_agent IS 'Client user agent string';
COMMENT ON COLUMN audit_logs.status_code IS 'HTTP status code of the operation';
COMMENT ON COLUMN audit_logs.created_at IS 'Timestamp of action (UTC)';

-- ============================================================================
-- INDEXES
-- ============================================================================

-- Users table indexes
CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON users(created_at DESC);

-- User roles table indexes
CREATE INDEX IF NOT EXISTS idx_user_roles_user_id ON user_roles(user_id);
CREATE INDEX IF NOT EXISTS idx_user_roles_role_id ON user_roles(role_id);

-- Audit logs table indexes (critical for query performance)
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource_type ON audit_logs(resource_type);
CREATE INDEX IF NOT EXISTS idx_audit_logs_resource_id ON audit_logs(resource_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_created_at ON audit_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_logs_ip_address ON audit_logs(ip_address);

-- Composite index for common query patterns
CREATE INDEX IF NOT EXISTS idx_audit_logs_user_action_date
    ON audit_logs(user_id, action, created_at DESC);

-- GIN index for JSONB details column (enables efficient JSON queries)
CREATE INDEX IF NOT EXISTS idx_audit_logs_details_gin ON audit_logs USING GIN(details);

-- ============================================================================
-- TRIGGERS
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Updated At Trigger Function
-- ----------------------------------------------------------------------------
-- Automatically updates the updated_at timestamp on row modifications

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to users table
DROP TRIGGER IF EXISTS trigger_users_updated_at ON users;
CREATE TRIGGER trigger_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Apply trigger to roles table
DROP TRIGGER IF EXISTS trigger_roles_updated_at ON roles;
CREATE TRIGGER trigger_roles_updated_at
    BEFORE UPDATE ON roles
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- IDEMPOTENCY CHECK
-- ============================================================================

-- Verify all tables were created successfully
DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_name IN ('users', 'roles', 'user_roles', 'audit_logs');

    IF table_count != 4 THEN
        RAISE EXCEPTION 'Migration failed: Expected 4 tables, found %', table_count;
    END IF;

    RAISE NOTICE 'Migration 001_initial_schema.sql completed successfully';
    RAISE NOTICE 'Created tables: users, roles, user_roles, audit_logs';
END $$;
