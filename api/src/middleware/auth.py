"""
JWT authentication middleware for FastAPI.

Provides:
- JWT token extraction from Authorization header
- Token validation
- User authentication
- Request context enrichment
"""

import structlog
from typing import Optional, Callable
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from api.src.services.auth_service import AuthService
from api.src.models.auth import CurrentUser

logger = structlog.get_logger(__name__)

security = HTTPBearer(auto_error=False)


class AuthMiddleware(BaseHTTPMiddleware):
    """
    Middleware to authenticate requests using JWT tokens.

    Extracts JWT token from Authorization header, validates it,
    and adds current user to request state.
    """

    def __init__(self, app, auth_service: AuthService, exempt_paths: Optional[list] = None):
        """
        Initialize auth middleware.

        Args:
            app: FastAPI application
            auth_service: Authentication service
            exempt_paths: List of paths that don't require authentication
        """
        super().__init__(app)
        self.auth_service = auth_service
        self.exempt_paths = exempt_paths or [
            "/api/v1/auth/login",
            "/api/v1/health",
            "/api/v1/ready",
            "/metrics",
            "/docs",
            "/openapi.json",
            "/redoc"
        ]

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request and authenticate user.

        Args:
            request: HTTP request
            call_next: Next middleware in chain

        Returns:
            HTTP response
        """
        request.state.user = None
        request.state.correlation_id = request.headers.get("X-Correlation-ID")

        if self._is_exempt_path(request.url.path):
            logger.debug("auth_exempt", path=request.url.path)
            return await call_next(request)

        try:
            token = self._extract_token(request)

            if not token:
                logger.warning(
                    "auth_missing_token",
                    path=request.url.path,
                    method=request.method,
                    client=request.client.host if request.client else None
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Missing authentication token",
                    headers={"WWW-Authenticate": "Bearer"}
                )

            current_user = await self.auth_service.get_current_user(token)

            if not current_user:
                logger.warning(
                    "auth_invalid_token",
                    path=request.url.path,
                    method=request.method,
                    client=request.client.host if request.client else None
                )
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid authentication token",
                    headers={"WWW-Authenticate": "Bearer"}
                )

            request.state.user = current_user

            logger.info(
                "request_authenticated",
                path=request.url.path,
                method=request.method,
                user_id=str(current_user.id),
                username=current_user.username,
                roles=current_user.roles
            )

            response = await call_next(request)
            return response

        except HTTPException:
            raise
        except Exception as e:
            logger.error(
                "auth_middleware_error",
                error=str(e),
                path=request.url.path,
                method=request.method
            )
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Authentication error"
            )

    def _is_exempt_path(self, path: str) -> bool:
        """
        Check if path is exempt from authentication.

        Args:
            path: Request path

        Returns:
            True if exempt, False otherwise
        """
        for exempt_path in self.exempt_paths:
            if path.startswith(exempt_path):
                return True
        return False

    def _extract_token(self, request: Request) -> Optional[str]:
        """
        Extract JWT token from Authorization header.

        Args:
            request: HTTP request

        Returns:
            JWT token or None if not found
        """
        authorization = request.headers.get("Authorization")

        if not authorization:
            return None

        parts = authorization.split()

        if len(parts) != 2 or parts[0].lower() != "bearer":
            logger.warning("auth_malformed_header", authorization=authorization)
            return None

        return parts[1]


async def get_current_user_from_request(request: Request) -> CurrentUser:
    """
    Get current authenticated user from request.

    Args:
        request: HTTP request

    Returns:
        Current user

    Raises:
        HTTPException: If user not authenticated
    """
    user = getattr(request.state, "user", None)

    if not user:
        logger.warning("user_not_authenticated", path=request.url.path)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"}
        )

    return user


async def get_optional_user_from_request(request: Request) -> Optional[CurrentUser]:
    """
    Get current authenticated user from request (optional).

    Args:
        request: HTTP request

    Returns:
        Current user or None if not authenticated
    """
    return getattr(request.state, "user", None)
