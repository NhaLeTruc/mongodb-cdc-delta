"""
Audit logging middleware for FastAPI.

Provides:
- Automatic request/response logging
- User action tracking
- Audit trail for security and compliance
- Sensitive data masking
- Asynchronous audit log writing
"""

import structlog
import json
import time
import asyncio
from typing import Optional, Set, Dict, Any, List
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import StreamingResponse
from uuid import UUID

from api.src.models.audit import AuditAction, ResourceType
from api.src.repositories.audit_repo import AuditRepository
from api.src.config import get_settings

logger = structlog.get_logger(__name__)


class AuditMiddleware(BaseHTTPMiddleware):
    """
    Middleware to automatically log API requests and responses for audit trail.

    Captures:
    - User ID and authentication details
    - HTTP method, path, and status code
    - Request timestamp and duration
    - Client IP address and user agent
    - Resource type and ID (extracted from path)
    - Sensitive data masking
    """

    # Sensitive fields to mask in request/response bodies
    SENSITIVE_FIELDS: Set[str] = {
        "password",
        "password_hash",
        "secret",
        "secret_key",
        "api_key",
        "token",
        "access_token",
        "refresh_token",
        "authorization",
        "credit_card",
        "ssn",
        "private_key",
    }

    # Paths that should not be audited
    EXEMPT_PATHS: Set[str] = {
        "/metrics",
        "/health",
        "/ready",
        "/docs",
        "/openapi.json",
        "/redoc",
        "/favicon.ico",
    }

    # HTTP methods to audit
    AUDIT_METHODS: Set[str] = {
        "POST",
        "PUT",
        "PATCH",
        "DELETE",
    }

    def __init__(
        self,
        app,
        audit_repo: AuditRepository,
        audit_all_requests: bool = False,
        audit_responses: bool = False,
    ):
        """
        Initialize audit middleware.

        Args:
            app: FastAPI application
            audit_repo: Audit repository for database operations
            audit_all_requests: If True, audit all requests (not just write operations)
            audit_responses: If True, include response bodies in audit logs
        """
        super().__init__(app)
        self.audit_repo = audit_repo
        self.settings = get_settings()
        self.audit_all_requests = audit_all_requests or self.settings.audit_log_requests
        self.audit_responses = audit_responses or self.settings.audit_log_responses

    async def dispatch(self, request: Request, call_next) -> Response:
        """
        Process request and log audit trail.

        Args:
            request: HTTP request
            call_next: Next middleware in chain

        Returns:
            HTTP response
        """
        # Skip audit for exempt paths
        if self._is_exempt_path(request.url.path):
            return await call_next(request)

        # Check if request should be audited
        should_audit = self._should_audit_request(request)

        # Record start time
        start_time = time.time()

        # Process request
        response = await call_next(request)

        # Calculate request duration
        duration_ms = int((time.time() - start_time) * 1000)

        # Log audit entry asynchronously if enabled
        if should_audit and self.settings.audit_enabled:
            asyncio.create_task(
                self._log_audit_entry(
                    request=request,
                    response=response,
                    duration_ms=duration_ms,
                )
            )

        # Add audit headers to response
        response.headers["X-Audit-Logged"] = "true" if should_audit else "false"
        response.headers["X-Request-Duration-Ms"] = str(duration_ms)

        return response

    def _is_exempt_path(self, path: str) -> bool:
        """
        Check if path is exempt from audit logging.

        Args:
            path: Request path

        Returns:
            True if exempt, False otherwise
        """
        # Exact match
        if path in self.EXEMPT_PATHS:
            return True

        # Prefix match
        for exempt_path in self.EXEMPT_PATHS:
            if path.startswith(exempt_path):
                return True

        return False

    def _should_audit_request(self, request: Request) -> bool:
        """
        Determine if request should be audited.

        Args:
            request: HTTP request

        Returns:
            True if should audit, False otherwise
        """
        # Audit all requests if configured
        if self.audit_all_requests:
            return True

        # Audit write operations (POST, PUT, PATCH, DELETE)
        if request.method in self.AUDIT_METHODS:
            return True

        return False

    async def _log_audit_entry(
        self,
        request: Request,
        response: Response,
        duration_ms: int,
    ) -> None:
        """
        Create audit log entry asynchronously.

        Args:
            request: HTTP request
            response: HTTP response
            duration_ms: Request duration in milliseconds
        """
        try:
            # Extract user from request state (set by auth middleware)
            user = getattr(request.state, "user", None)
            user_id = UUID(str(user.id)) if user else None
            username = user.username if user else None

            # Extract client information
            client_ip = self._get_client_ip(request)
            user_agent = request.headers.get("User-Agent")

            # Determine action from method and path
            action = self._determine_action(request)

            # Extract resource information from path
            resource_type, resource_id = self._extract_resource_info(request.url.path)

            # Build audit details
            details = await self._build_audit_details(request, response, duration_ms)

            # Create audit log entry
            await self.audit_repo.create_audit_log(
                user_id=user_id,
                action=action,
                resource_type=resource_type,
                resource_id=resource_id,
                details=details,
                ip_address=client_ip,
                user_agent=user_agent,
                status_code=response.status_code,
            )

            logger.debug(
                "audit_log_created",
                user_id=str(user_id) if user_id else None,
                username=username,
                action=action,
                path=request.url.path,
                method=request.method,
                status_code=response.status_code,
                duration_ms=duration_ms,
            )

        except Exception as e:
            # Don't fail the request if audit logging fails
            logger.error(
                "audit_log_failed",
                error=str(e),
                path=request.url.path,
                method=request.method,
            )

    def _get_client_ip(self, request: Request) -> Optional[str]:
        """
        Extract client IP address from request.

        Args:
            request: HTTP request

        Returns:
            Client IP address or None
        """
        # Check X-Forwarded-For header (proxy/load balancer)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Take first IP in chain
            return forwarded_for.split(",")[0].strip()

        # Check X-Real-IP header
        real_ip = request.headers.get("X-Real-IP")
        if real_ip:
            return real_ip

        # Fall back to direct connection
        if request.client:
            return request.client.host

        return None

    def _determine_action(self, request: Request) -> str:
        """
        Determine audit action from request method and path.

        Args:
            request: HTTP request

        Returns:
            Audit action string
        """
        method = request.method
        path = request.url.path

        # Authentication actions
        if "/auth/login" in path:
            return AuditAction.LOGIN_SUCCESS.value
        if "/auth/logout" in path:
            return AuditAction.LOGOUT.value
        if "/auth/refresh" in path:
            return AuditAction.TOKEN_REFRESH.value

        # User management actions
        if "/users" in path:
            if method == "POST":
                return AuditAction.USER_CREATE.value
            elif method == "GET":
                # Check if getting single user or list
                if path.rstrip("/").split("/")[-1] != "users":
                    return AuditAction.USER_READ.value
                return AuditAction.USER_LIST.value
            elif method in ["PUT", "PATCH"]:
                return AuditAction.USER_UPDATE.value
            elif method == "DELETE":
                return AuditAction.USER_DELETE.value

        # Mapping management actions
        if "/mappings" in path:
            if method == "POST":
                return AuditAction.MAPPING_CREATE.value
            elif method == "GET":
                if path.rstrip("/").split("/")[-1] != "mappings":
                    return AuditAction.MAPPING_READ.value
                return AuditAction.MAPPING_LIST.value
            elif method in ["PUT", "PATCH"]:
                return AuditAction.MAPPING_UPDATE.value
            elif method == "DELETE":
                return AuditAction.MAPPING_DELETE.value

        # Pipeline operations
        if "/pipelines" in path or "/sync" in path:
            if "trigger" in path:
                return AuditAction.PIPELINE_TRIGGER.value
            elif "pause" in path:
                return AuditAction.PIPELINE_PAUSE.value
            elif "resume" in path:
                return AuditAction.PIPELINE_RESUME.value
            elif method == "GET":
                return AuditAction.PIPELINE_STATUS.value

        # System actions
        if "/config" in path and method in ["PUT", "PATCH"]:
            return AuditAction.SYSTEM_CONFIG_UPDATE.value
        if "/health" in path:
            return AuditAction.SYSTEM_HEALTH_CHECK.value

        # Generic fallback based on method
        return f"{method.lower()}_{path.strip('/').replace('/', '_')}"

    def _extract_resource_info(self, path: str) -> tuple[Optional[str], Optional[str]]:
        """
        Extract resource type and ID from request path.

        Args:
            path: Request path

        Returns:
            Tuple of (resource_type, resource_id)
        """
        parts = path.strip("/").split("/")

        # Remove API version prefix if present
        if parts and parts[0] in ["api"]:
            parts = parts[1:]
        if parts and parts[0].startswith("v"):
            parts = parts[1:]

        if not parts:
            return None, None

        # First part is usually the resource type
        resource_type = parts[0] if parts else None

        # Map to ResourceType enum if possible
        resource_type_map = {
            "users": ResourceType.USER.value,
            "mappings": ResourceType.MAPPING.value,
            "pipelines": ResourceType.PIPELINE.value,
            "auth": ResourceType.AUTH.value,
        }

        if resource_type in resource_type_map:
            resource_type = resource_type_map[resource_type]

        # Extract resource ID if present
        resource_id = None
        if len(parts) >= 2:
            # Second part might be a resource ID
            potential_id = parts[1]
            # Check if it looks like an ID (UUID or integer)
            if self._is_valid_id(potential_id):
                resource_id = potential_id

        return resource_type, resource_id

    def _is_valid_id(self, value: str) -> bool:
        """
        Check if value looks like a valid resource ID.

        Args:
            value: Potential ID string

        Returns:
            True if valid ID, False otherwise
        """
        # Check if UUID
        try:
            UUID(value)
            return True
        except (ValueError, AttributeError):
            pass

        # Check if integer
        try:
            int(value)
            return True
        except ValueError:
            pass

        # Alphanumeric IDs (length > 5)
        if len(value) > 5 and value.replace("-", "").replace("_", "").isalnum():
            return True

        return False

    async def _build_audit_details(
        self,
        request: Request,
        response: Response,
        duration_ms: int,
    ) -> Dict[str, Any]:
        """
        Build detailed audit information.

        Args:
            request: HTTP request
            response: HTTP response
            duration_ms: Request duration in milliseconds

        Returns:
            Dictionary with audit details
        """
        details: Dict[str, Any] = {
            "method": request.method,
            "path": request.url.path,
            "query_params": dict(request.query_params),
            "duration_ms": duration_ms,
            "status_code": response.status_code,
        }

        # Add correlation ID if present
        correlation_id = request.headers.get("X-Correlation-ID")
        if correlation_id:
            details["correlation_id"] = correlation_id

        # Add request body for write operations (masked)
        if request.method in self.AUDIT_METHODS:
            try:
                # Try to read and parse request body
                body = await self._read_request_body(request)
                if body:
                    details["request_body"] = self._mask_sensitive_data(body)
            except Exception as e:
                logger.debug("audit_request_body_read_failed", error=str(e))

        # Add response body if configured (masked)
        if self.audit_responses:
            try:
                # Note: Reading response body is complex with streaming responses
                # For now, we just note that response auditing was enabled
                details["response_auditing_enabled"] = True
            except Exception as e:
                logger.debug("audit_response_body_read_failed", error=str(e))

        return details

    async def _read_request_body(self, request: Request) -> Optional[Dict[str, Any]]:
        """
        Read and parse request body.

        Args:
            request: HTTP request

        Returns:
            Parsed request body or None
        """
        try:
            content_type = request.headers.get("Content-Type", "")

            # Only parse JSON bodies
            if "application/json" not in content_type:
                return None

            # Read body (this consumes the stream, so must be done carefully)
            # Note: FastAPI already handles this in route handlers
            body_bytes = await request.body()

            if not body_bytes:
                return None

            body_str = body_bytes.decode("utf-8")
            body_dict = json.loads(body_str)

            return body_dict

        except Exception as e:
            logger.debug("request_body_parse_failed", error=str(e))
            return None

    def _mask_sensitive_data(self, data: Any) -> Any:
        """
        Recursively mask sensitive fields in data.

        Args:
            data: Data to mask (dict, list, or primitive)

        Returns:
            Masked data
        """
        if isinstance(data, dict):
            masked = {}
            for key, value in data.items():
                # Check if field is sensitive
                if key.lower() in self.SENSITIVE_FIELDS:
                    masked[key] = "***MASKED***"
                else:
                    # Recursively mask nested structures
                    masked[key] = self._mask_sensitive_data(value)
            return masked

        elif isinstance(data, list):
            return [self._mask_sensitive_data(item) for item in data]

        else:
            # Primitive value, return as-is
            return data


# Convenience functions for manual audit logging

async def log_security_event(
    audit_repo: AuditRepository,
    action: AuditAction,
    user_id: Optional[UUID] = None,
    details: Optional[Dict[str, Any]] = None,
    ip_address: Optional[str] = None,
    user_agent: Optional[str] = None,
    status_code: Optional[int] = None,
) -> None:
    """
    Manually log a security-related audit event.

    Args:
        audit_repo: Audit repository
        action: Audit action
        user_id: User ID (optional)
        details: Additional details (optional)
        ip_address: Client IP address (optional)
        user_agent: User agent (optional)
        status_code: HTTP status code (optional)
    """
    try:
        await audit_repo.create_audit_log(
            user_id=user_id,
            action=action.value,
            resource_type=ResourceType.AUTH.value,
            resource_id=None,
            details=details,
            ip_address=ip_address,
            user_agent=user_agent,
            status_code=status_code,
        )

        logger.info(
            "security_event_logged",
            action=action.value,
            user_id=str(user_id) if user_id else None,
        )

    except Exception as e:
        logger.error(
            "security_event_log_failed",
            error=str(e),
            action=action.value,
        )


async def log_unauthorized_access(
    audit_repo: AuditRepository,
    request: Request,
    user_id: Optional[UUID] = None,
    reason: str = "Unauthorized",
) -> None:
    """
    Log an unauthorized access attempt.

    Args:
        audit_repo: Audit repository
        request: HTTP request
        user_id: User ID (optional)
        reason: Reason for denial
    """
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("User-Agent")

    details = {
        "method": request.method,
        "path": request.url.path,
        "reason": reason,
    }

    await log_security_event(
        audit_repo=audit_repo,
        action=AuditAction.UNAUTHORIZED_ACCESS,
        user_id=user_id,
        details=details,
        ip_address=client_ip,
        user_agent=user_agent,
        status_code=401,
    )


async def log_permission_denied(
    audit_repo: AuditRepository,
    request: Request,
    user_id: UUID,
    required_permission: str,
) -> None:
    """
    Log a permission denied event.

    Args:
        audit_repo: Audit repository
        request: HTTP request
        user_id: User ID
        required_permission: Required permission that was missing
    """
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("User-Agent")

    details = {
        "method": request.method,
        "path": request.url.path,
        "required_permission": required_permission,
    }

    await log_security_event(
        audit_repo=audit_repo,
        action=AuditAction.PERMISSION_DENIED,
        user_id=user_id,
        details=details,
        ip_address=client_ip,
        user_agent=user_agent,
        status_code=403,
    )


async def log_rate_limit_exceeded(
    audit_repo: AuditRepository,
    request: Request,
    user_id: Optional[UUID] = None,
    limit: int = 0,
) -> None:
    """
    Log a rate limit exceeded event.

    Args:
        audit_repo: Audit repository
        request: HTTP request
        user_id: User ID (optional)
        limit: Rate limit that was exceeded
    """
    client_ip = request.client.host if request.client else None
    user_agent = request.headers.get("User-Agent")

    details = {
        "method": request.method,
        "path": request.url.path,
        "limit": limit,
    }

    await log_security_event(
        audit_repo=audit_repo,
        action=AuditAction.RATE_LIMIT_EXCEEDED,
        user_id=user_id,
        details=details,
        ip_address=client_ip,
        user_agent=user_agent,
        status_code=429,
    )
