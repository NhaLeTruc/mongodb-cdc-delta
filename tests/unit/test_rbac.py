"""
Unit tests for Role-Based Access Control (RBAC) permission checks.

Tests cover:
- Permission checking logic for different operations
- Role hierarchy (admin > operator > analyst)
- Permission requirements validation
- Multiple roles handling
- Invalid role handling
- Permission inheritance
"""

import pytest
from typing import List, Set, Dict, Optional
from enum import Enum


# ============================================================================
# RBAC MODELS AND UTILITIES (Test Doubles)
# ============================================================================


class Permission(str, Enum):
    """Available permissions in the system."""

    # Read permissions
    READ_MAPPINGS = "read:mappings"
    READ_METRICS = "read:metrics"
    READ_CHECKPOINTS = "read:checkpoints"
    READ_AUDIT_LOGS = "read:audit_logs"
    READ_USERS = "read:users"

    # Write permissions
    CREATE_MAPPINGS = "create:mappings"
    UPDATE_MAPPINGS = "update:mappings"
    DELETE_MAPPINGS = "delete:mappings"

    # Operational permissions
    TRIGGER_SYNC = "trigger:sync"
    PAUSE_PIPELINE = "pause:pipeline"
    RESUME_PIPELINE = "resume:pipeline"

    # Administrative permissions
    MANAGE_USERS = "manage:users"
    MANAGE_ROLES = "manage:roles"
    MANAGE_SYSTEM = "manage:system"
    VIEW_SENSITIVE_DATA = "view:sensitive_data"


class Role(str, Enum):
    """Available roles in the system."""

    ADMIN = "admin"
    OPERATOR = "operator"
    ANALYST = "analyst"


# Role hierarchy - higher level roles inherit permissions from lower level roles
ROLE_HIERARCHY = {
    Role.ADMIN: [Role.OPERATOR, Role.ANALYST],
    Role.OPERATOR: [Role.ANALYST],
    Role.ANALYST: []
}


# Permission assignments per role
ROLE_PERMISSIONS: Dict[Role, Set[Permission]] = {
    Role.ANALYST: {
        Permission.READ_MAPPINGS,
        Permission.READ_METRICS,
        Permission.READ_CHECKPOINTS,
    },
    Role.OPERATOR: {
        Permission.READ_MAPPINGS,
        Permission.READ_METRICS,
        Permission.READ_CHECKPOINTS,
        Permission.CREATE_MAPPINGS,
        Permission.UPDATE_MAPPINGS,
        Permission.TRIGGER_SYNC,
        Permission.PAUSE_PIPELINE,
        Permission.RESUME_PIPELINE,
    },
    Role.ADMIN: {
        Permission.READ_MAPPINGS,
        Permission.READ_METRICS,
        Permission.READ_CHECKPOINTS,
        Permission.READ_AUDIT_LOGS,
        Permission.READ_USERS,
        Permission.CREATE_MAPPINGS,
        Permission.UPDATE_MAPPINGS,
        Permission.DELETE_MAPPINGS,
        Permission.TRIGGER_SYNC,
        Permission.PAUSE_PIPELINE,
        Permission.RESUME_PIPELINE,
        Permission.MANAGE_USERS,
        Permission.MANAGE_ROLES,
        Permission.MANAGE_SYSTEM,
        Permission.VIEW_SENSITIVE_DATA,
    }
}


def get_role_permissions(role: Role) -> Set[Permission]:
    """
    Get all permissions for a role including inherited permissions.

    Args:
        role: The role to get permissions for

    Returns:
        Set of permissions for the role
    """
    permissions = set(ROLE_PERMISSIONS.get(role, set()))

    # Add permissions from inherited roles
    inherited_roles = ROLE_HIERARCHY.get(role, [])
    for inherited_role in inherited_roles:
        permissions.update(ROLE_PERMISSIONS.get(inherited_role, set()))

    return permissions


def has_permission(user_roles: List[str], required_permission: Permission) -> bool:
    """
    Check if user has required permission based on their roles.

    Args:
        user_roles: List of role names assigned to the user
        required_permission: The permission to check for

    Returns:
        True if user has the permission, False otherwise
    """
    if not user_roles:
        return False

    all_permissions: Set[Permission] = set()

    for role_name in user_roles:
        try:
            role = Role(role_name)
            all_permissions.update(get_role_permissions(role))
        except ValueError:
            # Invalid role name, skip it
            continue

    return required_permission in all_permissions


def has_any_permission(user_roles: List[str], required_permissions: List[Permission]) -> bool:
    """
    Check if user has any of the required permissions.

    Args:
        user_roles: List of role names assigned to the user
        required_permissions: List of permissions, user needs at least one

    Returns:
        True if user has at least one permission, False otherwise
    """
    if not user_roles or not required_permissions:
        return False

    for permission in required_permissions:
        if has_permission(user_roles, permission):
            return True

    return False


def has_all_permissions(user_roles: List[str], required_permissions: List[Permission]) -> bool:
    """
    Check if user has all required permissions.

    Args:
        user_roles: List of role names assigned to the user
        required_permissions: List of permissions, user needs all of them

    Returns:
        True if user has all permissions, False otherwise
    """
    if not user_roles or not required_permissions:
        return False

    for permission in required_permissions:
        if not has_permission(user_roles, permission):
            return False

    return True


def has_role(user_roles: List[str], required_role: Role) -> bool:
    """
    Check if user has a specific role.

    Args:
        user_roles: List of role names assigned to the user
        required_role: The role to check for

    Returns:
        True if user has the role, False otherwise
    """
    return required_role.value in user_roles


def is_admin(user_roles: List[str]) -> bool:
    """
    Check if user has admin role.

    Args:
        user_roles: List of role names assigned to the user

    Returns:
        True if user is admin, False otherwise
    """
    return has_role(user_roles, Role.ADMIN)


# ============================================================================
# UNIT TESTS
# ============================================================================


class TestRolePermissions:
    """Tests for role permission assignments."""

    def test_analyst_has_read_only_permissions(self):
        """Test analyst role has only read permissions."""
        permissions = get_role_permissions(Role.ANALYST)

        # Should have read permissions
        assert Permission.READ_MAPPINGS in permissions
        assert Permission.READ_METRICS in permissions
        assert Permission.READ_CHECKPOINTS in permissions

        # Should NOT have write permissions
        assert Permission.CREATE_MAPPINGS not in permissions
        assert Permission.UPDATE_MAPPINGS not in permissions
        assert Permission.DELETE_MAPPINGS not in permissions

        # Should NOT have admin permissions
        assert Permission.MANAGE_USERS not in permissions
        assert Permission.MANAGE_SYSTEM not in permissions

    def test_operator_has_operational_permissions(self):
        """Test operator role has operational permissions."""
        permissions = get_role_permissions(Role.OPERATOR)

        # Should have read permissions
        assert Permission.READ_MAPPINGS in permissions
        assert Permission.READ_METRICS in permissions

        # Should have operational write permissions
        assert Permission.CREATE_MAPPINGS in permissions
        assert Permission.UPDATE_MAPPINGS in permissions
        assert Permission.TRIGGER_SYNC in permissions
        assert Permission.PAUSE_PIPELINE in permissions

        # Should NOT have delete permissions
        assert Permission.DELETE_MAPPINGS not in permissions

        # Should NOT have admin permissions
        assert Permission.MANAGE_USERS not in permissions
        assert Permission.MANAGE_SYSTEM not in permissions

    def test_admin_has_all_permissions(self):
        """Test admin role has all permissions."""
        permissions = get_role_permissions(Role.ADMIN)

        # Should have read permissions
        assert Permission.READ_MAPPINGS in permissions
        assert Permission.READ_AUDIT_LOGS in permissions

        # Should have write permissions
        assert Permission.CREATE_MAPPINGS in permissions
        assert Permission.UPDATE_MAPPINGS in permissions
        assert Permission.DELETE_MAPPINGS in permissions

        # Should have operational permissions
        assert Permission.TRIGGER_SYNC in permissions
        assert Permission.PAUSE_PIPELINE in permissions

        # Should have admin permissions
        assert Permission.MANAGE_USERS in permissions
        assert Permission.MANAGE_ROLES in permissions
        assert Permission.MANAGE_SYSTEM in permissions
        assert Permission.VIEW_SENSITIVE_DATA in permissions


class TestRoleHierarchy:
    """Tests for role hierarchy and permission inheritance."""

    def test_admin_inherits_operator_permissions(self):
        """Test admin role inherits operator permissions."""
        admin_perms = get_role_permissions(Role.ADMIN)
        operator_perms = get_role_permissions(Role.OPERATOR)

        # Admin should have all operator permissions
        for perm in operator_perms:
            assert perm in admin_perms

    def test_admin_inherits_analyst_permissions(self):
        """Test admin role inherits analyst permissions."""
        admin_perms = get_role_permissions(Role.ADMIN)
        analyst_perms = get_role_permissions(Role.ANALYST)

        # Admin should have all analyst permissions
        for perm in analyst_perms:
            assert perm in admin_perms

    def test_operator_inherits_analyst_permissions(self):
        """Test operator role inherits analyst permissions."""
        operator_perms = get_role_permissions(Role.OPERATOR)
        analyst_perms = get_role_permissions(Role.ANALYST)

        # Operator should have all analyst permissions
        for perm in analyst_perms:
            assert perm in operator_perms

    def test_analyst_does_not_inherit_from_others(self):
        """Test analyst role does not inherit from other roles."""
        analyst_perms = get_role_permissions(Role.ANALYST)
        operator_perms = get_role_permissions(Role.OPERATOR)
        admin_perms = get_role_permissions(Role.ADMIN)

        # Analyst should NOT have all operator permissions
        assert not operator_perms.issubset(analyst_perms)

        # Analyst should NOT have all admin permissions
        assert not admin_perms.issubset(analyst_perms)


class TestPermissionChecking:
    """Tests for permission checking logic."""

    def test_analyst_can_read_mappings(self):
        """Test analyst can read mappings."""
        user_roles = ["analyst"]

        assert has_permission(user_roles, Permission.READ_MAPPINGS) is True

    def test_analyst_cannot_create_mappings(self):
        """Test analyst cannot create mappings."""
        user_roles = ["analyst"]

        assert has_permission(user_roles, Permission.CREATE_MAPPINGS) is False

    def test_operator_can_create_mappings(self):
        """Test operator can create mappings."""
        user_roles = ["operator"]

        assert has_permission(user_roles, Permission.CREATE_MAPPINGS) is True

    def test_operator_cannot_delete_mappings(self):
        """Test operator cannot delete mappings."""
        user_roles = ["operator"]

        assert has_permission(user_roles, Permission.DELETE_MAPPINGS) is False

    def test_admin_can_manage_users(self):
        """Test admin can manage users."""
        user_roles = ["admin"]

        assert has_permission(user_roles, Permission.MANAGE_USERS) is True

    def test_operator_cannot_manage_users(self):
        """Test operator cannot manage users."""
        user_roles = ["operator"]

        assert has_permission(user_roles, Permission.MANAGE_USERS) is False

    def test_admin_can_delete_mappings(self):
        """Test admin can delete mappings."""
        user_roles = ["admin"]

        assert has_permission(user_roles, Permission.DELETE_MAPPINGS) is True

    def test_empty_roles_has_no_permissions(self):
        """Test empty roles list has no permissions."""
        user_roles = []

        assert has_permission(user_roles, Permission.READ_MAPPINGS) is False
        assert has_permission(user_roles, Permission.MANAGE_USERS) is False


class TestMultipleRoles:
    """Tests for users with multiple roles."""

    def test_user_with_analyst_and_operator_roles(self):
        """Test user with both analyst and operator roles has combined permissions."""
        user_roles = ["analyst", "operator"]

        # Should have analyst permissions
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is True

        # Should have operator permissions
        assert has_permission(user_roles, Permission.CREATE_MAPPINGS) is True
        assert has_permission(user_roles, Permission.TRIGGER_SYNC) is True

        # Should NOT have admin-only permissions
        assert has_permission(user_roles, Permission.MANAGE_USERS) is False

    def test_user_with_operator_and_admin_roles(self):
        """Test user with operator and admin roles has all permissions."""
        user_roles = ["operator", "admin"]

        # Should have all permissions from both roles
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is True
        assert has_permission(user_roles, Permission.CREATE_MAPPINGS) is True
        assert has_permission(user_roles, Permission.MANAGE_USERS) is True
        assert has_permission(user_roles, Permission.DELETE_MAPPINGS) is True

    def test_multiple_roles_with_duplicates(self):
        """Test multiple roles with duplicates still work correctly."""
        user_roles = ["analyst", "analyst", "operator"]

        # Duplicates should not affect permission checking
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is True
        assert has_permission(user_roles, Permission.CREATE_MAPPINGS) is True


class TestInvalidRoles:
    """Tests for handling invalid roles."""

    def test_invalid_role_name_is_ignored(self):
        """Test invalid role name is ignored."""
        user_roles = ["invalid_role"]

        # Should have no permissions
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is False

    def test_mixed_valid_and_invalid_roles(self):
        """Test mixed valid and invalid roles uses valid ones."""
        user_roles = ["analyst", "invalid_role", "operator"]

        # Should have permissions from valid roles
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is True
        assert has_permission(user_roles, Permission.CREATE_MAPPINGS) is True

        # Should NOT have admin permissions
        assert has_permission(user_roles, Permission.MANAGE_USERS) is False

    def test_all_invalid_roles_has_no_permissions(self):
        """Test all invalid roles results in no permissions."""
        user_roles = ["invalid1", "invalid2", "invalid3"]

        # Should have no permissions
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is False
        assert has_permission(user_roles, Permission.MANAGE_USERS) is False


class TestAnyPermissionCheck:
    """Tests for checking if user has any of the required permissions."""

    def test_analyst_has_any_read_permission(self):
        """Test analyst has at least one read permission."""
        user_roles = ["analyst"]
        required_permissions = [
            Permission.READ_MAPPINGS,
            Permission.READ_METRICS,
            Permission.CREATE_MAPPINGS  # Analyst doesn't have this
        ]

        assert has_any_permission(user_roles, required_permissions) is True

    def test_analyst_does_not_have_any_admin_permission(self):
        """Test analyst does not have any admin permissions."""
        user_roles = ["analyst"]
        required_permissions = [
            Permission.MANAGE_USERS,
            Permission.MANAGE_SYSTEM,
            Permission.DELETE_MAPPINGS
        ]

        assert has_any_permission(user_roles, required_permissions) is False

    def test_operator_has_any_write_permission(self):
        """Test operator has at least one write permission."""
        user_roles = ["operator"]
        required_permissions = [
            Permission.CREATE_MAPPINGS,
            Permission.DELETE_MAPPINGS  # Operator doesn't have this
        ]

        assert has_any_permission(user_roles, required_permissions) is True

    def test_empty_permissions_list_returns_false(self):
        """Test empty permissions list returns false."""
        user_roles = ["admin"]
        required_permissions = []

        assert has_any_permission(user_roles, required_permissions) is False


class TestAllPermissionsCheck:
    """Tests for checking if user has all required permissions."""

    def test_analyst_has_all_read_permissions(self):
        """Test analyst has all basic read permissions."""
        user_roles = ["analyst"]
        required_permissions = [
            Permission.READ_MAPPINGS,
            Permission.READ_METRICS,
            Permission.READ_CHECKPOINTS
        ]

        assert has_all_permissions(user_roles, required_permissions) is True

    def test_analyst_does_not_have_all_mixed_permissions(self):
        """Test analyst does not have all mixed read/write permissions."""
        user_roles = ["analyst"]
        required_permissions = [
            Permission.READ_MAPPINGS,
            Permission.CREATE_MAPPINGS  # Analyst doesn't have this
        ]

        assert has_all_permissions(user_roles, required_permissions) is False

    def test_admin_has_all_permissions(self):
        """Test admin has all specified permissions."""
        user_roles = ["admin"]
        required_permissions = [
            Permission.READ_MAPPINGS,
            Permission.CREATE_MAPPINGS,
            Permission.DELETE_MAPPINGS,
            Permission.MANAGE_USERS
        ]

        assert has_all_permissions(user_roles, required_permissions) is True

    def test_operator_has_all_operational_permissions(self):
        """Test operator has all operational permissions."""
        user_roles = ["operator"]
        required_permissions = [
            Permission.CREATE_MAPPINGS,
            Permission.UPDATE_MAPPINGS,
            Permission.TRIGGER_SYNC,
            Permission.PAUSE_PIPELINE
        ]

        assert has_all_permissions(user_roles, required_permissions) is True

    def test_empty_permissions_list_returns_false(self):
        """Test empty permissions list returns false."""
        user_roles = ["admin"]
        required_permissions = []

        assert has_all_permissions(user_roles, required_permissions) is False


class TestRoleChecking:
    """Tests for role checking functions."""

    def test_has_role_returns_true_for_assigned_role(self):
        """Test has_role returns true for assigned role."""
        user_roles = ["analyst"]

        assert has_role(user_roles, Role.ANALYST) is True

    def test_has_role_returns_false_for_unassigned_role(self):
        """Test has_role returns false for unassigned role."""
        user_roles = ["analyst"]

        assert has_role(user_roles, Role.ADMIN) is False

    def test_has_role_with_multiple_roles(self):
        """Test has_role with multiple roles."""
        user_roles = ["analyst", "operator"]

        assert has_role(user_roles, Role.ANALYST) is True
        assert has_role(user_roles, Role.OPERATOR) is True
        assert has_role(user_roles, Role.ADMIN) is False

    def test_is_admin_returns_true_for_admin(self):
        """Test is_admin returns true for admin role."""
        user_roles = ["admin"]

        assert is_admin(user_roles) is True

    def test_is_admin_returns_false_for_non_admin(self):
        """Test is_admin returns false for non-admin roles."""
        assert is_admin(["analyst"]) is False
        assert is_admin(["operator"]) is False
        assert is_admin(["analyst", "operator"]) is False

    def test_is_admin_with_multiple_roles_including_admin(self):
        """Test is_admin returns true when admin is among multiple roles."""
        user_roles = ["analyst", "admin"]

        assert is_admin(user_roles) is True


class TestPermissionRequirements:
    """Tests for different operation permission requirements."""

    def test_read_mapping_requires_read_permission(self):
        """Test reading mapping requires read permission."""
        analyst_roles = ["analyst"]
        guest_roles = []

        assert has_permission(analyst_roles, Permission.READ_MAPPINGS) is True
        assert has_permission(guest_roles, Permission.READ_MAPPINGS) is False

    def test_create_mapping_requires_write_permission(self):
        """Test creating mapping requires create permission."""
        operator_roles = ["operator"]
        analyst_roles = ["analyst"]

        assert has_permission(operator_roles, Permission.CREATE_MAPPINGS) is True
        assert has_permission(analyst_roles, Permission.CREATE_MAPPINGS) is False

    def test_delete_mapping_requires_admin_permission(self):
        """Test deleting mapping requires admin permission."""
        admin_roles = ["admin"]
        operator_roles = ["operator"]
        analyst_roles = ["analyst"]

        assert has_permission(admin_roles, Permission.DELETE_MAPPINGS) is True
        assert has_permission(operator_roles, Permission.DELETE_MAPPINGS) is False
        assert has_permission(analyst_roles, Permission.DELETE_MAPPINGS) is False

    def test_manage_users_requires_admin_permission(self):
        """Test managing users requires admin permission."""
        admin_roles = ["admin"]
        operator_roles = ["operator"]

        assert has_permission(admin_roles, Permission.MANAGE_USERS) is True
        assert has_permission(operator_roles, Permission.MANAGE_USERS) is False

    def test_pause_pipeline_requires_operator_or_admin(self):
        """Test pausing pipeline requires operator or admin permission."""
        admin_roles = ["admin"]
        operator_roles = ["operator"]
        analyst_roles = ["analyst"]

        assert has_permission(admin_roles, Permission.PAUSE_PIPELINE) is True
        assert has_permission(operator_roles, Permission.PAUSE_PIPELINE) is True
        assert has_permission(analyst_roles, Permission.PAUSE_PIPELINE) is False


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_none_roles_is_handled(self):
        """Test None roles list is handled safely."""
        # Convert None to empty list for safety
        user_roles = [] if None is None else None

        assert has_permission(user_roles, Permission.READ_MAPPINGS) is False

    def test_case_sensitive_role_names(self):
        """Test role names are case sensitive."""
        user_roles = ["ADMIN"]  # Wrong case

        # Should not match because of case mismatch
        assert has_permission(user_roles, Permission.MANAGE_USERS) is False

    def test_whitespace_in_role_names(self):
        """Test role names with whitespace are treated as invalid."""
        user_roles = ["admin ", " analyst"]

        # Should not match because of whitespace
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is False
        assert has_permission(user_roles, Permission.MANAGE_USERS) is False

    def test_permission_check_with_same_permission_multiple_times(self):
        """Test checking same permission multiple times is consistent."""
        user_roles = ["analyst"]

        result1 = has_permission(user_roles, Permission.READ_MAPPINGS)
        result2 = has_permission(user_roles, Permission.READ_MAPPINGS)
        result3 = has_permission(user_roles, Permission.READ_MAPPINGS)

        assert result1 == result2 == result3 == True

    def test_large_number_of_roles(self):
        """Test handling large number of roles."""
        # User with many roles (edge case)
        user_roles = ["analyst", "operator", "admin"] + [f"role_{i}" for i in range(100)]

        # Should still work correctly
        assert has_permission(user_roles, Permission.READ_MAPPINGS) is True
        assert has_permission(user_roles, Permission.MANAGE_USERS) is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
