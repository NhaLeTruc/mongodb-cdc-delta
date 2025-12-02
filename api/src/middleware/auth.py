"""
JWT authentication middleware for FastAPI.

Provides:
- JWT token extraction and validation from Authorization header
- User authentication from bearer tokens
- Request context enrichment with user data
- Authentication exceptions handling
- Optional authentication support
- Token expiry validation
- User active status checking
"""

import structlog
from typing import Optional, Callable
from datetime import datetime
from fastapi import Request, HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from api.src.services.auth_service import AuthService
from api.src.models.auth import CurrentUser, TokenPayload
from api.src.repositories.user_repo import UserRepository
from api.src.config import get_settings

logger = structlog.get_logger(__name__)

# HTTP Bearer token scheme for dependency injection
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


# ============================================================================
# ENHANCED AUTHENTICATION DEPENDENCIES
# ============================================================================


async def get_token_from_header(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> str:
    """
    Extract JWT token from Authorization header.

    FastAPI dependency for extracting bearer token with validation.

    Args:
        credentials: HTTP bearer credentials

    Returns:
        JWT token string

    Raises:
        HTTPException: If token is missing or invalid format

    Example:
        @app.get("/protected")
        async def protected(token: str = Depends(get_token_from_header)):
            return {"token": token[:10] + "..."}
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

    token = credentials.credentials.strip()

    if not token:
        logger.warning("auth_empty_token")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Empty authentication token",
            headers={"WWW-Authenticate": "Bearer"}
        )

    return token


async def validate_token_and_get_user(
    token: str,
    auth_service: AuthService
) -> CurrentUser:
    """
    Validate JWT token and return authenticated user.

    Args:
        token: JWT token string
        auth_service: Authentication service

    Returns:
        Current authenticated user

    Raises:
        HTTPException: If token is invalid or user not found

    Example:
        user = await validate_token_and_get_user(token, auth_service)
    """
    try:
        # Decode and validate token
        current_user = await auth_service.get_current_user(token)

        if not current_user:
            logger.warning("token_validation_failed")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or expired authentication token",
                headers={"WWW-Authenticate": "Bearer"}
            )

        # Check user active status
        if not current_user.is_active:
            logger.warning(
                "token_validation_user_inactive",
                user_id=str(current_user.id),
                username=current_user.username
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="User account is inactive or disabled"
            )

        logger.debug(
            "token_validated_successfully",
            user_id=str(current_user.id),
            username=current_user.username
        )

        return current_user

    except HTTPException:
        raise
    except Exception as e:
        logger.error("token_validation_error", error=str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token validation failed",
            headers={"WWW-Authenticate": "Bearer"}
        )


async def extract_user_from_token(
    token: str = Depends(get_token_from_header),
    user_repo: Optional[UserRepository] = None
) -> CurrentUser:
    """
    Extract and validate user from JWT token (complete flow).

    Complete authentication flow: extract token -> validate -> get user from DB.

    Args:
        token: JWT token from Authorization header
        user_repo: User repository (optional, will create if None)

    Returns:
        Current authenticated user

    Raises:
        HTTPException: If token invalid or user not found

    Example:
        @app.get("/me")
        async def get_me(user: CurrentUser = Depends(extract_user_from_token)):
            return user
    """
    from api.src.dependencies import get_db_pool

    if user_repo is None:
        pool = get_db_pool()
        user_repo = UserRepository(pool)

    auth_service = AuthService(user_repo)

    return await validate_token_and_get_user(token, auth_service)


# ============================================================================
# AUTHENTICATION ERROR HANDLERS
# ============================================================================


def create_auth_error(detail: str = "Authentication failed") -> HTTPException:
    """
    Create standardized authentication error (401).

    Args:
        detail: Error detail message

    Returns:
        HTTPException with 401 status
    """
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail=detail,
        headers={"WWW-Authenticate": "Bearer"}
    )


def create_forbidden_error(detail: str = "Access forbidden") -> HTTPException:
    """
    Create standardized forbidden error (403).

    Args:
        detail: Error detail message

    Returns:
        HTTPException with 403 status
    """
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail=detail
    )


def handle_missing_token_error() -> HTTPException:
    """
    Create error for missing authentication token.

    Returns:
        HTTPException with 401 status
    """
    return create_auth_error("Missing authentication token")


def handle_invalid_token_error() -> HTTPException:
    """
    Create error for invalid token.

    Returns:
        HTTPException with 401 status
    """
    return create_auth_error("Invalid or expired authentication token")


def handle_inactive_user_error(username: str) -> HTTPException:
    """
    Create error for inactive user.

    Args:
        username: Username of inactive user

    Returns:
        HTTPException with 403 status
    """
    logger.warning("inactive_user_access_attempt", username=username)
    return create_forbidden_error("User account is inactive or disabled")


def handle_expired_token_error() -> HTTPException:
    """
    Create error for expired token.

    Returns:
        HTTPException with 401 status
    """
    return create_auth_error("Authentication token has expired")


# ============================================================================
# TOKEN UTILITIES
# ============================================================================


def is_token_expired(exp: int) -> bool:
    """
    Check if token is expired based on exp claim.

    Args:
        exp: Expiration timestamp (Unix epoch)

    Returns:
        True if expired, False otherwise
    """
    if not exp:
        return True

    expiration = datetime.fromtimestamp(exp)
    now = datetime.utcnow()

    return now >= expiration


def get_token_expiry_seconds(exp: int) -> Optional[int]:
    """
    Get seconds until token expiry.

    Args:
        exp: Expiration timestamp (Unix epoch)

    Returns:
        Seconds until expiry, or None if already expired
    """
    if not exp:
        return None

    expiration = datetime.fromtimestamp(exp)
    now = datetime.utcnow()

    if now >= expiration:
        return None

    delta = expiration - now
    return int(delta.total_seconds())


def decode_token_payload(token: str) -> Optional[dict]:
    """
    Decode JWT token without validation (for inspection).

    WARNING: This does not validate the token signature.
    Use only for inspection purposes, not for authentication.

    Args:
        token: JWT token string

    Returns:
        Token payload as dict, or None if decode fails
    """
    try:
        from jose import jwt

        # Decode without verification (inspection only)
        payload = jwt.decode(
            token,
            options={"verify_signature": False, "verify_exp": False}
        )
        return payload
    except Exception as e:
        logger.debug("token_decode_failed", error=str(e))
        return None


def extract_user_id_from_token(token: str) -> Optional[str]:
    """
    Extract user ID from token without full validation.

    Args:
        token: JWT token string

    Returns:
        User ID string or None
    """
    payload = decode_token_payload(token)
    if payload:
        return payload.get("sub")
    return None


def extract_roles_from_token(token: str) -> list:
    """
    Extract roles from token without full validation.

    Args:
        token: JWT token string

    Returns:
        List of role strings (empty if not found)
    """
    payload = decode_token_payload(token)
    if payload:
        return payload.get("roles", [])
    return []
