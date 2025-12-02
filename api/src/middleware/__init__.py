"""FastAPI middleware components.

This package contains custom middleware for request/response processing,
authentication, logging, metrics, rate limiting, and error handling.
"""

from api.src.middleware.auth import (
    AuthMiddleware,
    get_current_user_from_request,
    get_optional_user_from_request,
)
from api.src.middleware.rbac import (
    RBACMiddleware,
    check_permission,
    check_role,
    require_admin_user,
)
from api.src.middleware.audit import (
    AuditMiddleware,
    log_security_event,
    log_unauthorized_access,
    log_permission_denied,
    log_rate_limit_exceeded,
)

__all__ = [
    # Auth middleware
    "AuthMiddleware",
    "get_current_user_from_request",
    "get_optional_user_from_request",
    # RBAC middleware
    "RBACMiddleware",
    "check_permission",
    "check_role",
    "require_admin_user",
    # Audit middleware
    "AuditMiddleware",
    "log_security_event",
    "log_unauthorized_access",
    "log_permission_denied",
    "log_rate_limit_exceeded",
]
