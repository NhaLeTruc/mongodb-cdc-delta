"""
FastAPI application entry point for CDC Pipeline Management API.

This module provides the main FastAPI application with:
- Health and readiness endpoints
- Authentication and authorization
- Request/response logging and audit trails
- Prometheus metrics
- OpenTelemetry distributed tracing
- CORS, security headers, and rate limiting
- Database connection pool management
- Graceful startup and shutdown
"""

import asyncio
import asyncpg
import structlog
import uvicorn
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Dict, Any

from fastapi import FastAPI, Request, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.exceptions import HTTPException as StarletteHTTPException

from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource

from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from api.src.config import get_settings, Settings
from api.src.middleware.auth import AuthMiddleware
from api.src.middleware.rbac import RBACMiddleware
from api.src.services.auth_service import AuthService
from api.src.repositories.user_repo import UserRepository
from api.src.repositories.audit_repo import AuditRepository
from api.src.models.audit import AuditAction

# Initialize logger
logger = structlog.get_logger(__name__)

# Get settings
settings: Settings = get_settings()

# ============================================================================
# Prometheus Metrics
# ============================================================================

http_requests_total = Counter(
    "http_requests_total",
    "Total HTTP requests",
    ["method", "endpoint", "status"]
)

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["method", "endpoint"]
)

http_requests_in_progress = Gauge(
    "http_requests_in_progress",
    "HTTP requests currently in progress",
    ["method", "endpoint"]
)

database_connections_active = Gauge(
    "database_connections_active",
    "Active database connections"
)

database_connections_idle = Gauge(
    "database_connections_idle",
    "Idle database connections in pool"
)

# ============================================================================
# Application State
# ============================================================================

class AppState:
    """Application state container for shared resources."""

    def __init__(self):
        self.db_pool: asyncpg.Pool = None
        self.user_repo: UserRepository = None
        self.audit_repo: AuditRepository = None
        self.auth_service: AuthService = None
        self.rbac_middleware: RBACMiddleware = None
        self.limiter: Limiter = None

app_state = AppState()

# ============================================================================
# Lifespan Management
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan manager for startup and shutdown events.

    Handles:
    - Database connection pool initialization
    - Service and repository initialization
    - OpenTelemetry tracing setup
    - Graceful shutdown and resource cleanup
    """
    logger.info(
        "application_starting",
        app_name=settings.app_name,
        version=settings.app_version,
        environment=settings.environment
    )

    # ========================================================================
    # Startup: Initialize Resources
    # ========================================================================

    try:
        # Initialize OpenTelemetry tracing
        if settings.tracing_enabled:
            logger.info("initializing_tracing", jaeger_host=settings.tracing_jaeger_host)
            resource = Resource.create({"service.name": settings.app_name})
            trace.set_tracer_provider(TracerProvider(resource=resource))

            jaeger_exporter = JaegerExporter(
                agent_host_name=settings.tracing_jaeger_host,
                agent_port=settings.tracing_jaeger_port,
            )

            trace.get_tracer_provider().add_span_processor(
                BatchSpanProcessor(jaeger_exporter)
            )

            logger.info("tracing_initialized")

        # Initialize database connection pool
        logger.info(
            "initializing_database_pool",
            min_size=settings.database_pool_size,
            max_size=settings.database_pool_size + settings.database_max_overflow
        )

        app_state.db_pool = await asyncpg.create_pool(
            str(settings.database_url),
            min_size=settings.database_pool_size,
            max_size=settings.database_pool_size + settings.database_max_overflow,
            command_timeout=settings.database_pool_timeout
        )

        # Test database connection
        async with app_state.db_pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            logger.info("database_connected", postgres_version=version)

        # Initialize repositories
        logger.info("initializing_repositories")
        app_state.user_repo = UserRepository(app_state.db_pool)
        app_state.audit_repo = AuditRepository(app_state.db_pool)

        # Initialize services
        logger.info("initializing_services")
        app_state.auth_service = AuthService(app_state.user_repo)
        app_state.rbac_middleware = RBACMiddleware(app_state.auth_service)

        # Initialize rate limiter
        if settings.rate_limit_enabled:
            logger.info(
                "initializing_rate_limiter",
                requests=settings.rate_limit_requests,
                window=settings.rate_limit_window
            )
            app_state.limiter = Limiter(key_func=get_remote_address)

        # Update metrics
        database_connections_active.set(app_state.db_pool.get_size())
        database_connections_idle.set(app_state.db_pool.get_idle_size())

        logger.info(
            "application_started",
            app_name=settings.app_name,
            version=settings.app_version,
            environment=settings.environment
        )

        yield

    except Exception as e:
        logger.error("application_startup_failed", error=str(e), exc_info=True)
        raise

    # ========================================================================
    # Shutdown: Cleanup Resources
    # ========================================================================

    finally:
        logger.info("application_shutting_down")

        try:
            # Close database pool
            if app_state.db_pool:
                logger.info("closing_database_pool")
                await app_state.db_pool.close()
                logger.info("database_pool_closed")

            # Shutdown tracing
            if settings.tracing_enabled:
                logger.info("shutting_down_tracing")
                trace.get_tracer_provider().shutdown()

            logger.info("application_shutdown_complete")

        except Exception as e:
            logger.error("application_shutdown_failed", error=str(e), exc_info=True)

# ============================================================================
# FastAPI Application
# ============================================================================

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "Management API for MongoDB CDC to Delta Lake Pipeline. "
        "Provides endpoints for configuration, monitoring, and administration."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    lifespan=lifespan,
    debug=settings.debug,
)

# ============================================================================
# Middleware Configuration
# ============================================================================

# CORS Middleware
if settings.cors_enabled:
    logger.info("configuring_cors", origins=settings.cors_origins)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=settings.cors_allow_credentials,
        allow_methods=settings.cors_allow_methods,
        allow_headers=settings.cors_allow_headers,
    )

# GZip Compression Middleware
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Trusted Host Middleware (security)
if settings.security_require_https and settings.is_production:
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"]  # Configure with actual allowed hosts in production
    )

# Request Logging and Metrics Middleware
class RequestLoggingMiddleware(BaseHTTPMiddleware):
    """Middleware for request logging, metrics, and audit trails."""

    async def dispatch(self, request: Request, call_next):
        """Process request and log details."""
        # Generate correlation ID
        correlation_id = request.headers.get("X-Correlation-ID", None)
        if not correlation_id:
            import uuid
            correlation_id = str(uuid.uuid4())

        # Extract request details
        method = request.method
        path = request.url.path
        client_ip = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("User-Agent", "unknown")

        # Track in-progress requests
        http_requests_in_progress.labels(method=method, endpoint=path).inc()

        # Start timer
        import time
        start_time = time.time()

        logger.info(
            "request_started",
            method=method,
            path=path,
            client_ip=client_ip,
            correlation_id=correlation_id
        )

        try:
            # Process request
            response = await call_next(request)

            # Calculate duration
            duration = time.time() - start_time

            # Update metrics
            http_requests_total.labels(
                method=method,
                endpoint=path,
                status=response.status_code
            ).inc()

            http_request_duration_seconds.labels(
                method=method,
                endpoint=path
            ).observe(duration)

            # Log response
            logger.info(
                "request_completed",
                method=method,
                path=path,
                status_code=response.status_code,
                duration=f"{duration:.3f}s",
                correlation_id=correlation_id
            )

            # Add correlation ID header to response
            response.headers["X-Correlation-ID"] = correlation_id

            # Create audit log for non-health endpoints
            if settings.audit_enabled and not path.startswith("/health") and not path.startswith("/metrics"):
                try:
                    user_id = getattr(request.state, "user", None)
                    user_id = user_id.id if user_id else None

                    asyncio.create_task(
                        app_state.audit_repo.create_audit_log(
                            user_id=user_id,
                            action=self._get_audit_action(method, path),
                            resource_type=self._get_resource_type(path),
                            resource_id=self._extract_resource_id(path),
                            details={"path": path, "method": method},
                            ip_address=client_ip,
                            user_agent=user_agent,
                            status_code=response.status_code
                        )
                    )
                except Exception as e:
                    logger.warning("audit_log_creation_failed", error=str(e))

            return response

        except Exception as e:
            duration = time.time() - start_time
            logger.error(
                "request_failed",
                method=method,
                path=path,
                error=str(e),
                duration=f"{duration:.3f}s",
                correlation_id=correlation_id,
                exc_info=True
            )
            raise

        finally:
            # Decrement in-progress counter
            http_requests_in_progress.labels(method=method, endpoint=path).dec()

    def _get_audit_action(self, method: str, path: str) -> str:
        """Determine audit action from request method and path."""
        if "health" in path or "ready" in path:
            return AuditAction.SYSTEM_HEALTH_CHECK.value
        if "login" in path:
            return AuditAction.LOGIN_SUCCESS.value
        if "users" in path:
            if method == "GET":
                return AuditAction.USER_READ.value
            elif method == "POST":
                return AuditAction.USER_CREATE.value
            elif method == "PUT" or method == "PATCH":
                return AuditAction.USER_UPDATE.value
            elif method == "DELETE":
                return AuditAction.USER_DELETE.value
        return "unknown_action"

    def _get_resource_type(self, path: str) -> str:
        """Extract resource type from path."""
        if "users" in path:
            return "user"
        if "mappings" in path:
            return "mapping"
        if "pipeline" in path:
            return "pipeline"
        return "unknown"

    def _extract_resource_id(self, path: str) -> str:
        """Extract resource ID from path."""
        parts = path.split("/")
        if len(parts) >= 4:
            return parts[-1]
        return None

app.add_middleware(RequestLoggingMiddleware)

# Authentication Middleware
auth_middleware = AuthMiddleware(
    app=app,
    auth_service=app_state.auth_service,
    exempt_paths=[
        f"{settings.api_prefix}/auth/login",
        f"{settings.api_prefix}/health",
        f"{settings.api_prefix}/ready",
        "/health",
        "/ready",
        "/metrics",
        "/docs",
        "/redoc",
        "/openapi.json",
    ]
)
app.add_middleware(BaseHTTPMiddleware, dispatch=auth_middleware.dispatch)

# Security Headers Middleware
class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware to add security headers to responses."""

    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)

        if settings.security_headers_enabled:
            response.headers["X-Content-Type-Options"] = "nosniff"
            response.headers["X-Frame-Options"] = "DENY"
            response.headers["X-XSS-Protection"] = "1; mode=block"
            response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"

            if settings.security_require_https:
                response.headers["Strict-Transport-Security"] = (
                    f"max-age={settings.security_hsts_max_age}; includeSubDomains"
                )

        return response

app.add_middleware(SecurityHeadersMiddleware)

# OpenTelemetry Instrumentation
if settings.tracing_enabled:
    FastAPIInstrumentor.instrument_app(app)

# ============================================================================
# Exception Handlers
# ============================================================================

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle request validation errors."""
    logger.warning(
        "validation_error",
        path=request.url.path,
        errors=exc.errors()
    )
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()}
    )

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions."""
    logger.warning(
        "http_exception",
        path=request.url.path,
        status_code=exc.status_code,
        detail=exc.detail
    )
    return JSONResponse(
        status_code=exc.status_code,
        content={"detail": exc.detail}
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(
        "unexpected_exception",
        path=request.url.path,
        error=str(exc),
        exc_info=True
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"}
    )

# Rate limit exception handler
if settings.rate_limit_enabled:
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# ============================================================================
# Health and Readiness Endpoints
# ============================================================================

@app.get("/health", tags=["Health"], response_class=JSONResponse)
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint.

    Returns basic health status without checking dependencies.
    Use for container health checks.

    Returns:
        Health status
    """
    return {
        "status": "healthy",
        "service": settings.app_name,
        "version": settings.app_version,
        "environment": settings.environment
    }

@app.get("/ready", tags=["Health"], response_class=JSONResponse)
async def readiness_check() -> Dict[str, Any]:
    """
    Readiness check endpoint.

    Checks if application is ready to serve requests by verifying:
    - Database connectivity
    - Critical dependencies

    Returns:
        Readiness status with component health
    """
    checks = {
        "database": "unknown",
    }

    # Check database
    try:
        async with app_state.db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
            checks["database"] = "healthy"
    except Exception as e:
        logger.error("database_health_check_failed", error=str(e))
        checks["database"] = "unhealthy"

    # Update connection metrics
    if app_state.db_pool:
        database_connections_active.set(app_state.db_pool.get_size())
        database_connections_idle.set(app_state.db_pool.get_idle_size())

    # Determine overall status
    all_healthy = all(status == "healthy" for status in checks.values())
    overall_status = "ready" if all_healthy else "not_ready"

    status_code = status.HTTP_200_OK if all_healthy else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=status_code,
        content={
            "status": overall_status,
            "service": settings.app_name,
            "version": settings.app_version,
            "checks": checks
        }
    )

# ============================================================================
# Metrics Endpoint
# ============================================================================

@app.get("/metrics", tags=["Monitoring"], response_class=PlainTextResponse)
async def metrics() -> Response:
    """
    Prometheus metrics endpoint.

    Exposes application metrics in Prometheus format for scraping.

    Returns:
        Prometheus metrics
    """
    # Update connection pool metrics
    if app_state.db_pool:
        database_connections_active.set(app_state.db_pool.get_size())
        database_connections_idle.set(app_state.db_pool.get_idle_size())

    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST
    )

# ============================================================================
# API Router Registration
# ============================================================================

# Note: Routers will be added here once implemented
# Example:
# from api.src.routers import admin, auth
# app.include_router(auth.router, prefix=f"{settings.api_prefix}/auth", tags=["Authentication"])
# app.include_router(admin.router, prefix=f"{settings.api_prefix}/admin", tags=["Administration"])

# ============================================================================
# Application Entry Point
# ============================================================================

if __name__ == "__main__":
    """
    Run the application with Uvicorn for development.

    In production, use Gunicorn with Uvicorn workers (see Dockerfile).
    """
    logger.info(
        "starting_uvicorn_server",
        host=settings.host,
        port=settings.port,
        reload=settings.debug
    )

    uvicorn.run(
        "api.src.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level.lower(),
        access_log=True,
        use_colors=True,
    )
