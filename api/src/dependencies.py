"""
FastAPI dependency injection for database, authentication, and authorization.

Provides injectable dependencies for:
- Database connections (asyncpg pool)
- User authentication (JWT token validation)
- Authorization (role/permission checking)
- Repository instances
- Service instances

All dependencies use FastAPI's dependency injection system and are designed
to be composable and testable.
"""

import asyncpg
import structlog
from typing import AsyncGenerator, Optional
from functools import lru_cache
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from api.src.config import get_settings, Settings
from api.src.models.auth import CurrentUser, Role
from api.src.repositories.user_repo import UserRepository
from api.src.repositories.audit_repo import AuditRepository
from api.src.services.auth_service import AuthService

logger = structlog.get_logger(__name__)

# HTTP Bearer token scheme
security = HTTPBearer(auto_error=False)


# ============================================================================
# DATABASE CONNECTION POOL
# ============================================================================

_pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> asyncpg.Pool:
    """
    Initialize database connection pool.

    Should be called during application startup.

    Returns:
        asyncpg connection pool
    """
    global _pool

    if _pool is not None:
        return _pool

    settings = get_settings()

    try:
        _pool = await asyncpg.create_pool(
            str(settings.database_url),
            min_size=settings.database_pool_size // 2,
            max_size=settings.database_pool_size,
            max_inactive_connection_lifetime=settings.database_pool_timeout,
            command_timeout=60
        )

        logger.info(
            "database_pool_initialized",
            pool_size=settings.database_pool_size,
            database=str(settings.database_url).split("@")[-1]
        )

        return _pool

    except Exception as e:
        logger.error("database_pool_init_failed", error=str(e))
        raise


async def close_db_pool():
    """
    Close database connection pool.

    Should be called during application shutdown.
    """
    global _pool

    if _pool is not None:
        await _pool.close()
        logger.info("database_pool_closed")
        _pool = None


def get_db_pool() -> asyncpg.Pool:
    """
    Get database connection pool.

    Returns:
        asyncpg connection pool

    Raises:
        RuntimeError: If pool is not initialized
    """
    if _pool is None:
        logger.error("database_pool_not_initialized")
        raise RuntimeError(
            "Database pool not initialized. Call init_db_pool() during startup."
        )
    return _pool


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Get database connection from pool.

    FastAPI dependency that provides a database connection.
    Automatically returns connection to pool after use.

    Yields:
        Database connection

    Example:
        @app.get("/users")
        async def get_users(db: asyncpg.Connection = Depends(get_db)):
            rows = await db.fetch("SELECT * FROM users")
            return rows
    """
    pool = get_db_pool()

    async with pool.acquire() as connection:
        try:
            yield connection
        except Exception as e:
            logger.error("database_connection_error", error=str(e))
            raise


# ============================================================================
# AUTHENTICATION DEPENDENCIES
# ============================================================================


async def get_token_from_header(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> str:
    """
    Extract JWT token from Authorization header.

    Args:
        credentials: HTTP bearer credentials

    Returns:
        JWT token string

    Raises:
        HTTPException: If token is missing or invalid format
    """
    if not credentials:
        logger.warning("auth_missing_credentials")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authentication credentials",
            headers={"WWW-Authenticate": "Bearer"}
        )

    if credentials.scheme.lower() != "bearer":
        logger.warning("auth_invalid_scheme", scheme=credentials.scheme)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication scheme. Expected Bearer token",
            headers={"WWW-Authenticate": "Bearer"}
        )

    return credentials.credentials


async def get_current_user(
    token: str = Depends(get_token_from_header),
    auth_service: AuthService = Depends(lambda: get_auth_service())
) -> CurrentUser:
    """
    Get current authenticated user from JWT token.

    Validates token and retrieves user from database.

    Args:
        token: JWT token
        auth_service: Authentication service

    Returns:
        Current authenticated user

    Raises:
        HTTPException: If token is invalid or user not found

    Example:
        @app.get("/profile")
        async def get_profile(user: CurrentUser = Depends(get_current_user)):
            return {"username": user.username, "roles": user.roles}
    """
    try:
        current_user = await auth_service.get_current_user(token)

        if not current_user:
            logger.warning("auth_invalid_token")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid authentication token",
                headers={"WWW-Authenticate": "Bearer"}
            )

        logger.debug(
            "user_authenticated",
            user_id=str(current_user.id),
            username=current_user.username,
            roles=current_user.roles
        )

        return current_user

    except HTTPException:
        raise
    except Exception as e:
        logger.error("auth_error", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authentication failed",
            headers={"WWW-Authenticate": "Bearer"}
        )


async def get_current_active_user(
    current_user: CurrentUser = Depends(get_current_user)
) -> CurrentUser:
    """
    Get current active user.

    Ensures user is active (not disabled).

    Args:
        current_user: Current authenticated user

    Returns:
        Active user

    Raises:
        HTTPException: If user is inactive

    Example:
        @app.get("/data")
        async def get_data(user: CurrentUser = Depends(get_current_active_user)):
            return {"data": "sensitive"}
    """
    if not current_user.is_active:
        logger.warning(
            "user_inactive",
            user_id=str(current_user.id),
            username=current_user.username
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive"
        )

    return current_user


async def get_optional_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    auth_service: AuthService = Depends(lambda: get_auth_service())
) -> Optional[CurrentUser]:
    """
    Get current user if authenticated, None otherwise.

    Does not raise exceptions for missing/invalid tokens.
    Useful for endpoints that work with or without authentication.

    Args:
        credentials: HTTP bearer credentials (optional)
        auth_service: Authentication service

    Returns:
        Current user or None

    Example:
        @app.get("/public")
        async def public_endpoint(user: Optional[CurrentUser] = Depends(get_optional_user)):
            if user:
                return {"message": f"Hello {user.username}"}
            return {"message": "Hello anonymous"}
    """
    if not credentials:
        return None

    try:
        current_user = await auth_service.get_current_user(credentials.credentials)
        return current_user if current_user and current_user.is_active else None
    except Exception as e:
        logger.debug("optional_auth_failed", error=str(e))
        return None


# ============================================================================
# AUTHORIZATION DEPENDENCIES (ROLE-BASED)
# ============================================================================


async def require_admin(
    current_user: CurrentUser = Depends(get_current_active_user)
) -> CurrentUser:
    """
    Require admin role.

    Args:
        current_user: Current active user

    Returns:
        Admin user

    Raises:
        HTTPException: If user is not admin

    Example:
        @app.post("/admin/users")
        async def create_user(admin: CurrentUser = Depends(require_admin)):
            # Only admins can access this
            pass
    """
    if not current_user.has_role(Role.ADMIN):
        logger.warning(
            "access_denied_admin_required",
            user_id=str(current_user.id),
            username=current_user.username,
            roles=current_user.roles
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin role required"
        )

    logger.debug(
        "admin_access_granted",
        user_id=str(current_user.id),
        username=current_user.username
    )

    return current_user


async def require_operator(
    current_user: CurrentUser = Depends(get_current_active_user)
) -> CurrentUser:
    """
    Require operator role (or higher).

    Admins can also access operator-level resources.

    Args:
        current_user: Current active user

    Returns:
        Operator user

    Raises:
        HTTPException: If user is not operator or admin

    Example:
        @app.post("/mappings")
        async def create_mapping(user: CurrentUser = Depends(require_operator)):
            # Operators and admins can access this
            pass
    """
    if not current_user.has_any_role([Role.OPERATOR, Role.ADMIN]):
        logger.warning(
            "access_denied_operator_required",
            user_id=str(current_user.id),
            username=current_user.username,
            roles=current_user.roles
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Operator or admin role required"
        )

    logger.debug(
        "operator_access_granted",
        user_id=str(current_user.id),
        username=current_user.username
    )

    return current_user


async def require_analyst(
    current_user: CurrentUser = Depends(get_current_active_user)
) -> CurrentUser:
    """
    Require analyst role (or higher).

    Analysts, operators, and admins can access analyst-level resources.

    Args:
        current_user: Current active user

    Returns:
        Analyst user

    Raises:
        HTTPException: If user is not analyst, operator, or admin

    Example:
        @app.get("/metrics")
        async def get_metrics(user: CurrentUser = Depends(require_analyst)):
            # Analysts, operators, and admins can access this
            pass
    """
    if not current_user.has_any_role([Role.ANALYST, Role.OPERATOR, Role.ADMIN]):
        logger.warning(
            "access_denied_analyst_required",
            user_id=str(current_user.id),
            username=current_user.username,
            roles=current_user.roles
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Analyst, operator, or admin role required"
        )

    logger.debug(
        "analyst_access_granted",
        user_id=str(current_user.id),
        username=current_user.username
    )

    return current_user


# ============================================================================
# REPOSITORY DEPENDENCIES
# ============================================================================


async def get_user_repository(
    db: asyncpg.Connection = Depends(get_db)
) -> UserRepository:
    """
    Get user repository instance.

    Args:
        db: Database connection

    Returns:
        User repository

    Example:
        @app.get("/users/{user_id}")
        async def get_user(
            user_id: str,
            repo: UserRepository = Depends(get_user_repository)
        ):
            user = await repo.get_user_by_id(UUID(user_id))
            return user
    """
    # Create a temporary pool-like object that uses the connection
    class ConnectionPool:
        def __init__(self, conn):
            self._conn = conn

        async def acquire(self):
            return self._ConnectionContext(self._conn)

        class _ConnectionContext:
            def __init__(self, conn):
                self._conn = conn

            async def __aenter__(self):
                return self._conn

            async def __aexit__(self, *args):
                pass

    pool = ConnectionPool(db)
    return UserRepository(pool)


async def get_audit_repository(
    db: asyncpg.Connection = Depends(get_db)
) -> AuditRepository:
    """
    Get audit repository instance.

    Args:
        db: Database connection

    Returns:
        Audit repository

    Example:
        @app.get("/audit-logs")
        async def get_logs(
            repo: AuditRepository = Depends(get_audit_repository)
        ):
            logs, total = await repo.list_audit_logs(filter)
            return logs
    """
    # Create a temporary pool-like object that uses the connection
    class ConnectionPool:
        def __init__(self, conn):
            self._conn = conn

        async def acquire(self):
            return self._ConnectionContext(self._conn)

        class _ConnectionContext:
            def __init__(self, conn):
                self._conn = conn

            async def __aenter__(self):
                return self._conn

            async def __aexit__(self, *args):
                pass

    pool = ConnectionPool(db)
    return AuditRepository(pool)


# ============================================================================
# SERVICE DEPENDENCIES
# ============================================================================


@lru_cache()
def get_auth_service() -> AuthService:
    """
    Get authentication service instance (cached).

    Returns cached instance to avoid recreating service.
    Note: This service doesn't need database connection for
    basic token operations, but uses repository for user lookups.

    Returns:
        Authentication service

    Example:
        @app.post("/login")
        async def login(
            auth_service: AuthService = Depends(get_auth_service)
        ):
            token = await auth_service.login(credentials)
            return token
    """
    pool = get_db_pool()
    user_repo = UserRepository(pool)
    return AuthService(user_repo)


def get_auth_service_with_db(
    user_repo: UserRepository = Depends(get_user_repository)
) -> AuthService:
    """
    Get authentication service with injected user repository.

    Use this when you need a fresh auth service with the current
    request's database connection.

    Args:
        user_repo: User repository

    Returns:
        Authentication service
    """
    return AuthService(user_repo)


# ============================================================================
# UTILITY DEPENDENCIES
# ============================================================================


def get_settings_dependency() -> Settings:
    """
    Get application settings.

    Returns:
        Settings instance

    Example:
        @app.get("/config")
        async def get_config(settings: Settings = Depends(get_settings_dependency)):
            return {"environment": settings.environment}
    """
    return get_settings()


async def get_client_ip(request: Request) -> str:
    """
    Get client IP address from request.

    Checks X-Forwarded-For header first (for proxies),
    then falls back to client host.

    Args:
        request: HTTP request

    Returns:
        Client IP address

    Example:
        @app.get("/log-access")
        async def log_access(ip: str = Depends(get_client_ip)):
            logger.info("access", ip=ip)
    """
    forwarded = request.headers.get("X-Forwarded-For")
    if forwarded:
        # X-Forwarded-For can contain multiple IPs, get the first one
        return forwarded.split(",")[0].strip()

    if request.client:
        return request.client.host

    return "unknown"


async def get_user_agent(request: Request) -> Optional[str]:
    """
    Get user agent from request.

    Args:
        request: HTTP request

    Returns:
        User agent string or None

    Example:
        @app.get("/track")
        async def track(user_agent: str = Depends(get_user_agent)):
            logger.info("request", user_agent=user_agent)
    """
    return request.headers.get("User-Agent")


async def get_correlation_id(request: Request) -> Optional[str]:
    """
    Get correlation ID from request.

    Checks X-Correlation-ID header.

    Args:
        request: HTTP request

    Returns:
        Correlation ID or None

    Example:
        @app.get("/process")
        async def process(correlation_id: str = Depends(get_correlation_id)):
            logger.info("processing", correlation_id=correlation_id)
    """
    return request.headers.get("X-Correlation-ID")


# ============================================================================
# PAGINATION DEPENDENCIES
# ============================================================================


class PaginationParams:
    """Pagination parameters for list endpoints."""

    def __init__(
        self,
        limit: int = 100,
        offset: int = 0
    ):
        """
        Initialize pagination parameters.

        Args:
            limit: Maximum number of items (1-1000)
            offset: Number of items to skip
        """
        settings = get_settings()

        # Validate limit
        if limit < 1:
            limit = 1
        elif limit > settings.pagination_max_limit:
            limit = settings.pagination_max_limit

        # Validate offset
        if offset < 0:
            offset = 0

        self.limit = limit
        self.offset = offset


async def get_pagination_params(
    limit: int = 100,
    offset: int = 0
) -> PaginationParams:
    """
    Get pagination parameters from query string.

    Args:
        limit: Maximum number of items (default: 100, max: 1000)
        offset: Number of items to skip (default: 0)

    Returns:
        Pagination parameters

    Example:
        @app.get("/items")
        async def list_items(
            pagination: PaginationParams = Depends(get_pagination_params)
        ):
            items = await get_items(pagination.limit, pagination.offset)
            return items
    """
    return PaginationParams(limit=limit, offset=offset)
