"""
Role-Based Access Control (RBAC) middleware for FastAPI.

Provides:
- Permission checking
- Role-based authorization
- Access control decorators
"""

import structlog
from typing import List, Callable
from functools import wraps
from fastapi import Request, HTTPException, status

from api.src.models.auth import CurrentUser, Permission, Role
from api.src.services.auth_service import AuthService

logger = structlog.get_logger(__name__)


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
