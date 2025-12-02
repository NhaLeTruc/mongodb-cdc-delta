"""
Role-Based Access Control (RBAC) middleware for FastAPI.

Provides:
- Permission checking for fine-grained access control
- Role-based authorization with hierarchy support
- Access control decorators and dependencies
- Permission mapping for different roles
- Custom permission validators
- Audit logging integration for access denials
"""

import structlog
from typing import List, Callable, Set, Optional
from functools import wraps
from fastapi import Request, HTTPException, status, Depends

from api.src.models.auth import CurrentUser, Permission, Role
from api.src.services.auth_service import AuthService
from api.src.config import get_settings

logger = structlog.get_logger(__name__)


# ============================================================================
# ROLE PERMISSION MAPPING
# ============================================================================

# Define comprehensive permission sets for each role
ROLE_PERMISSIONS: dict[Role, Set[Permission]] = {
    Role.VIEWER: {
        Permission.READ_METRICS,
    },
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
        # Admin has all permissions
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


class RBACMiddleware:
    """Middleware for role-based access control."""

    def __init__(self, auth_service: AuthService):
        """
        Initialize RBAC middleware.

        Args:
            auth_service: Authentication service
        """
        self.auth_service = auth_service

    def require_permission(self, permission: Permission):
        """
        Decorator to require specific permission for endpoint.

        Args:
            permission: Required permission

        Returns:
            Decorator function
        """
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

                if not request:
                    request = kwargs.get("request")

                if not request:
                    logger.error("rbac_no_request_object", function=func.__name__)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Internal server error"
                    )

                user = getattr(request.state, "user", None)

                if not user:
                    logger.warning(
                        "rbac_user_not_authenticated",
                        path=request.url.path,
                        permission=permission.value
                    )
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Not authenticated"
                    )

                if not self.auth_service.has_permission(user.roles, permission):
                    logger.warning(
                        "rbac_permission_denied",
                        path=request.url.path,
                        user_id=str(user.id),
                        username=user.username,
                        roles=user.roles,
                        required_permission=permission.value
                    )
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Permission denied: {permission.value} required"
                    )

                logger.debug(
                    "rbac_permission_granted",
                    user_id=str(user.id),
                    username=user.username,
                    permission=permission.value
                )

                return await func(*args, **kwargs)

            return wrapper
        return decorator

    def require_any_permission(self, permissions: List[Permission]):
        """
        Decorator to require any of the specified permissions.

        Args:
            permissions: List of required permissions

        Returns:
            Decorator function
        """
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

                if not request:
                    request = kwargs.get("request")

                if not request:
                    logger.error("rbac_no_request_object", function=func.__name__)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Internal server error"
                    )

                user = getattr(request.state, "user", None)

                if not user:
                    logger.warning(
                        "rbac_user_not_authenticated",
                        path=request.url.path,
                        permissions=[p.value for p in permissions]
                    )
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Not authenticated"
                    )

                if not self.auth_service.has_any_permission(user.roles, permissions):
                    logger.warning(
                        "rbac_permission_denied",
                        path=request.url.path,
                        user_id=str(user.id),
                        username=user.username,
                        roles=user.roles,
                        required_permissions=[p.value for p in permissions]
                    )
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Permission denied: one of {[p.value for p in permissions]} required"
                    )

                logger.debug(
                    "rbac_permission_granted",
                    user_id=str(user.id),
                    username=user.username,
                    permissions=[p.value for p in permissions]
                )

                return await func(*args, **kwargs)

            return wrapper
        return decorator

    def require_role(self, role: Role):
        """
        Decorator to require specific role for endpoint.

        Args:
            role: Required role

        Returns:
            Decorator function
        """
        def decorator(func: Callable):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                request = None
                for arg in args:
                    if isinstance(arg, Request):
                        request = arg
                        break

                if not request:
                    request = kwargs.get("request")

                if not request:
                    logger.error("rbac_no_request_object", function=func.__name__)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail="Internal server error"
                    )

                user = getattr(request.state, "user", None)

                if not user:
                    logger.warning(
                        "rbac_user_not_authenticated",
                        path=request.url.path,
                        role=role.value
                    )
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Not authenticated"
                    )

                if not self.auth_service.has_role(user.roles, role):
                    logger.warning(
                        "rbac_role_denied",
                        path=request.url.path,
                        user_id=str(user.id),
                        username=user.username,
                        roles=user.roles,
                        required_role=role.value
                    )
                    raise HTTPException(
                        status_code=status.HTTP_403_FORBIDDEN,
                        detail=f"Access denied: {role.value} role required"
                    )

                logger.debug(
                    "rbac_role_granted",
                    user_id=str(user.id),
                    username=user.username,
                    role=role.value
                )

                return await func(*args, **kwargs)

            return wrapper
        return decorator

    def require_admin(self):
        """
        Decorator to require admin role for endpoint.

        Returns:
            Decorator function
        """
        return self.require_role(Role.ADMIN)


async def check_permission(request: Request, permission: Permission, auth_service: AuthService):
    """
    Check if current user has required permission.

    Args:
        request: HTTP request
        permission: Required permission
        auth_service: Authentication service

    Raises:
        HTTPException: If user doesn't have permission
    """
    user = getattr(request.state, "user", None)

    if not user:
        logger.warning("permission_check_not_authenticated", permission=permission.value)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )

    if not auth_service.has_permission(user.roles, permission):
        logger.warning(
            "permission_check_denied",
            user_id=str(user.id),
            username=user.username,
            roles=user.roles,
            permission=permission.value
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Permission denied: {permission.value} required"
        )

    logger.debug(
        "permission_check_granted",
        user_id=str(user.id),
        username=user.username,
        permission=permission.value
    )


async def check_role(request: Request, role: Role, auth_service: AuthService):
    """
    Check if current user has required role.

    Args:
        request: HTTP request
        role: Required role
        auth_service: Authentication service

    Raises:
        HTTPException: If user doesn't have role
    """
    user = getattr(request.state, "user", None)

    if not user:
        logger.warning("role_check_not_authenticated", role=role.value)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated"
        )

    if not auth_service.has_role(user.roles, role):
        logger.warning(
            "role_check_denied",
            user_id=str(user.id),
            username=user.username,
            roles=user.roles,
            required_role=role.value
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Access denied: {role.value} role required"
        )

    logger.debug(
        "role_check_granted",
        user_id=str(user.id),
        username=user.username,
        role=role.value
    )


async def require_admin_user(request: Request, auth_service: AuthService):
    """
    Check if current user has admin role.

    Args:
        request: HTTP request
        auth_service: Authentication service

    Raises:
        HTTPException: If user is not admin
    """
    await check_role(request, Role.ADMIN, auth_service)


# ============================================================================
# FASTAPI DEPENDENCY INJECTION HELPERS
# ============================================================================


class RoleChecker:
    """
    Dependency class for checking user roles in FastAPI endpoints.

    Example:
        @app.get("/admin")
        async def admin_only(user: CurrentUser = Depends(RoleChecker(Role.ADMIN))):
            return {"message": "Admin access"}
    """

    def __init__(self, required_roles: List[Role]):
        """
        Initialize role checker.

        Args:
            required_roles: List of roles, user must have at least one
        """
        self.required_roles = required_roles if isinstance(required_roles, list) else [required_roles]

    async def __call__(self, request: Request) -> CurrentUser:
        """
        Check if user has required role.

        Args:
            request: HTTP request

        Returns:
            Current user

        Raises:
            HTTPException: If user doesn't have required role
        """
        user = getattr(request.state, "user", None)

        if not user:
            logger.warning(
                "role_checker_not_authenticated",
                required_roles=[r.value for r in self.required_roles]
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated"
            )

        # Check if user has any of the required roles
        user_has_role = any(role.value in user.roles for role in self.required_roles)

        if not user_has_role:
            logger.warning(
                "role_checker_access_denied",
                user_id=str(user.id),
                username=user.username,
                user_roles=user.roles,
                required_roles=[r.value for r in self.required_roles]
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required role(s): {[r.value for r in self.required_roles]}"
            )

        logger.debug(
            "role_checker_access_granted",
            user_id=str(user.id),
            username=user.username,
            matched_role=[r.value for r in self.required_roles if r.value in user.roles]
        )

        return user


class PermissionChecker:
    """
    Dependency class for checking user permissions in FastAPI endpoints.

    Example:
        @app.post("/mappings")
        async def create_mapping(
            user: CurrentUser = Depends(PermissionChecker([Permission.CREATE_MAPPINGS]))
        ):
            return {"message": "Mapping created"}
    """

    def __init__(self, required_permissions: List[Permission], require_all: bool = False):
        """
        Initialize permission checker.

        Args:
            required_permissions: List of required permissions
            require_all: If True, user must have all permissions; if False, any permission
        """
        self.required_permissions = (
            required_permissions if isinstance(required_permissions, list)
            else [required_permissions]
        )
        self.require_all = require_all

    async def __call__(self, request: Request, auth_service: AuthService) -> CurrentUser:
        """
        Check if user has required permissions.

        Args:
            request: HTTP request
            auth_service: Authentication service

        Returns:
            Current user

        Raises:
            HTTPException: If user doesn't have required permissions
        """
        user = getattr(request.state, "user", None)

        if not user:
            logger.warning(
                "permission_checker_not_authenticated",
                required_permissions=[p.value for p in self.required_permissions]
            )
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Not authenticated"
            )

        # Check permissions
        if self.require_all:
            has_access = auth_service.has_all_permissions(user.roles, self.required_permissions)
        else:
            has_access = auth_service.has_any_permission(user.roles, self.required_permissions)

        if not has_access:
            logger.warning(
                "permission_checker_access_denied",
                user_id=str(user.id),
                username=user.username,
                user_roles=user.roles,
                required_permissions=[p.value for p in self.required_permissions],
                require_all=self.require_all
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Access denied. Required permission(s): {[p.value for p in self.required_permissions]}"
            )

        logger.debug(
            "permission_checker_access_granted",
            user_id=str(user.id),
            username=user.username,
            required_permissions=[p.value for p in self.required_permissions]
        )

        return user


# ============================================================================
# CONVENIENCE DEPENDENCIES
# ============================================================================


def require_admin() -> RoleChecker:
    """
    Dependency that requires admin role.

    Returns:
        RoleChecker for admin role

    Example:
        @app.delete("/users/{user_id}")
        async def delete_user(
            user_id: str,
            admin: CurrentUser = Depends(require_admin())
        ):
            return {"deleted": user_id}
    """
    return RoleChecker([Role.ADMIN])


def require_operator() -> RoleChecker:
    """
    Dependency that requires operator or admin role.

    Returns:
        RoleChecker for operator or admin

    Example:
        @app.post("/pipelines/trigger")
        async def trigger_pipeline(
            operator: CurrentUser = Depends(require_operator())
        ):
            return {"triggered": True}
    """
    return RoleChecker([Role.OPERATOR, Role.ADMIN])


def require_analyst() -> RoleChecker:
    """
    Dependency that requires analyst, operator, or admin role.

    Returns:
        RoleChecker for analyst, operator, or admin

    Example:
        @app.get("/metrics")
        async def get_metrics(
            analyst: CurrentUser = Depends(require_analyst())
        ):
            return {"metrics": []}
    """
    return RoleChecker([Role.ANALYST, Role.OPERATOR, Role.ADMIN])


def require_any_role(*roles: Role) -> RoleChecker:
    """
    Dependency that requires any of the specified roles.

    Args:
        *roles: Variable number of roles

    Returns:
        RoleChecker for specified roles

    Example:
        @app.get("/data")
        async def get_data(
            user: CurrentUser = Depends(require_any_role(Role.ANALYST, Role.OPERATOR))
        ):
            return {"data": []}
    """
    return RoleChecker(list(roles))


# ============================================================================
# PERMISSION UTILITIES
# ============================================================================


def get_user_permissions(user_roles: List[str]) -> Set[Permission]:
    """
    Get all permissions for given user roles.

    Args:
        user_roles: List of role names

    Returns:
        Set of all permissions user has
    """
    permissions: Set[Permission] = set()

    for role_name in user_roles:
        try:
            role = Role(role_name)
            role_perms = ROLE_PERMISSIONS.get(role, set())
            permissions.update(role_perms)
        except ValueError:
            logger.warning("invalid_role_name", role=role_name)
            continue

    return permissions


def check_user_permission(user_roles: List[str], required_permission: Permission) -> bool:
    """
    Check if user has specific permission.

    Args:
        user_roles: List of role names
        required_permission: Required permission

    Returns:
        True if user has permission, False otherwise
    """
    user_permissions = get_user_permissions(user_roles)
    return required_permission in user_permissions


def get_missing_permissions(
    user_roles: List[str],
    required_permissions: List[Permission]
) -> List[Permission]:
    """
    Get list of permissions user is missing.

    Args:
        user_roles: List of role names
        required_permissions: List of required permissions

    Returns:
        List of missing permissions
    """
    user_permissions = get_user_permissions(user_roles)
    missing = [perm for perm in required_permissions if perm not in user_permissions]
    return missing


def can_access_resource(
    user_roles: List[str],
    resource_type: str,
    action: str
) -> bool:
    """
    Check if user can perform action on resource type.

    Maps resource type and action to permission.

    Args:
        user_roles: List of role names
        resource_type: Type of resource (mapping, pipeline, user, etc.)
        action: Action to perform (read, create, update, delete, etc.)

    Returns:
        True if user can access, False otherwise
    """
    # Map resource type and action to permission
    permission_map = {
        ("mapping", "read"): Permission.READ_MAPPINGS,
        ("mapping", "create"): Permission.CREATE_MAPPINGS,
        ("mapping", "update"): Permission.UPDATE_MAPPINGS,
        ("mapping", "delete"): Permission.DELETE_MAPPINGS,
        ("pipeline", "trigger"): Permission.TRIGGER_SYNC,
        ("pipeline", "pause"): Permission.PAUSE_PIPELINE,
        ("pipeline", "resume"): Permission.RESUME_PIPELINE,
        ("user", "read"): Permission.READ_USERS,
        ("user", "manage"): Permission.MANAGE_USERS,
        ("metrics", "read"): Permission.READ_METRICS,
        ("checkpoint", "read"): Permission.READ_CHECKPOINTS,
        ("audit", "read"): Permission.READ_AUDIT_LOGS,
    }

    key = (resource_type.lower(), action.lower())
    required_permission = permission_map.get(key)

    if not required_permission:
        logger.warning(
            "unknown_resource_action",
            resource_type=resource_type,
            action=action
        )
        return False

    return check_user_permission(user_roles, required_permission)


# ============================================================================
# AUDIT LOGGING HELPERS
# ============================================================================


async def log_access_denied(
    request: Request,
    user: Optional[CurrentUser],
    required_roles: Optional[List[Role]] = None,
    required_permissions: Optional[List[Permission]] = None,
    reason: str = "Insufficient permissions"
):
    """
    Log access denied event for audit purposes.

    Args:
        request: HTTP request
        user: Current user (may be None)
        required_roles: Required roles that were missing
        required_permissions: Required permissions that were missing
        reason: Reason for denial
    """
    try:
        from api.src.middleware.audit import log_permission_denied
        from api.src.repositories.audit_repo import AuditRepository
        from api.src.dependencies import get_db_pool

        if user:
            pool = get_db_pool()
            audit_repo = AuditRepository(pool)

            required_perm_str = (
                ", ".join([p.value for p in required_permissions])
                if required_permissions
                else ", ".join([r.value for r in required_roles])
                if required_roles
                else "unknown"
            )

            await log_permission_denied(
                audit_repo=audit_repo,
                request=request,
                user_id=user.id,
                required_permission=required_perm_str
            )
    except Exception as e:
        logger.error("log_access_denied_failed", error=str(e))
