"""
FastAPI application configuration using Pydantic Settings.

Provides centralized configuration for:
- Database connections (PostgreSQL)
- Authentication (JWT settings)
- API settings (CORS, rate limiting)
- Vault integration
- Kafka Connect settings
- Security settings
- Logging and monitoring

All settings support environment variable overrides and .env file loading.
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, PostgresDsn, field_validator, HttpUrl
from typing import List, Optional
from functools import lru_cache
import os


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings can be overridden via environment variables with the
    prefix "CDC_API_" (e.g., CDC_API_DATABASE_URL).

    Environment variables are loaded from:
    1. System environment
    2. .env file in the current directory
    3. Default values defined below
    """

    # =========================================================================
    # API Settings
    # =========================================================================

    app_name: str = Field(
        default="CDC Pipeline Management API",
        description="Application name"
    )
    app_version: str = Field(
        default="1.0.0",
        description="API version"
    )
    api_prefix: str = Field(
        default="/api/v1",
        description="API URL prefix"
    )

    debug: bool = Field(
        default=False,
        description="Debug mode - enables verbose logging and error traces"
    )
    environment: str = Field(
        default="production",
        description="Environment: development|staging|production"
    )

    host: str = Field(
        default="0.0.0.0",
        description="API bind host"
    )
    port: int = Field(
        default=8000,
        description="API bind port",
        gt=0,
        lt=65536
    )

    # =========================================================================
    # Database Settings (PostgreSQL)
    # =========================================================================

    database_url: str = Field(
        default="postgresql+asyncpg://cdc_api:cdc_password@postgres:5432/cdc_metadata",
        description="PostgreSQL connection URL with asyncpg driver"
    )

    database_pool_size: int = Field(
        default=20,
        description="Database connection pool size",
        gt=0,
        le=100
    )
    database_max_overflow: int = Field(
        default=10,
        description="Max connections above pool size",
        ge=0,
        le=50
    )
    database_pool_timeout: int = Field(
        default=30,
        description="Pool connection timeout (seconds)",
        gt=0
    )
    database_pool_recycle: int = Field(
        default=3600,
        description="Connection recycle time (seconds) - prevents stale connections",
        gt=0
    )
    database_echo: bool = Field(
        default=False,
        description="Echo SQL queries to logs (useful for debugging)"
    )
    database_pool_pre_ping: bool = Field(
        default=True,
        description="Test connections before using (prevents stale connections)"
    )

    # =========================================================================
    # JWT Authentication Settings
    # =========================================================================

    jwt_secret_key: str = Field(
        default="change-this-secret-key-in-production-use-vault-or-env-var-minimum-32-chars",
        description="Secret key for JWT token signing (MUST be changed in production)",
        min_length=32
    )
    jwt_algorithm: str = Field(
        default="HS256",
        description="JWT signing algorithm (HS256, HS384, HS512, RS256, etc.)"
    )
    jwt_access_token_expire_minutes: int = Field(
        default=60,
        description="Access token expiration time in minutes",
        gt=0,
        le=1440  # Max 24 hours
    )
    jwt_refresh_token_expire_days: int = Field(
        default=7,
        description="Refresh token expiration time in days",
        gt=0,
        le=90  # Max 90 days
    )
    jwt_issuer: str = Field(
        default="cdc-pipeline-api",
        description="JWT issuer claim"
    )
    jwt_audience: str = Field(
        default="cdc-pipeline-users",
        description="JWT audience claim"
    )

    # =========================================================================
    # Password Hashing Settings
    # =========================================================================

    password_bcrypt_rounds: int = Field(
        default=12,
        description="BCrypt hash rounds (higher = slower but more secure)",
        ge=10,
        le=14  # 14 is max practical for production
    )
    password_min_length: int = Field(
        default=8,
        description="Minimum password length",
        ge=8,
        le=128
    )
    password_require_uppercase: bool = Field(
        default=True,
        description="Require at least one uppercase letter"
    )
    password_require_lowercase: bool = Field(
        default=True,
        description="Require at least one lowercase letter"
    )
    password_require_digit: bool = Field(
        default=True,
        description="Require at least one digit"
    )
    password_require_special: bool = Field(
        default=True,
        description="Require at least one special character"
    )

    # =========================================================================
    # CORS Settings
    # =========================================================================

    cors_enabled: bool = Field(
        default=True,
        description="Enable CORS middleware"
    )
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        description="Allowed CORS origins"
    )
    cors_allow_credentials: bool = Field(
        default=True,
        description="Allow credentials (cookies, authorization headers) in CORS"
    )
    cors_allow_methods: List[str] = Field(
        default=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        description="Allowed HTTP methods"
    )
    cors_allow_headers: List[str] = Field(
        default=["*"],
        description="Allowed HTTP headers"
    )
    cors_max_age: int = Field(
        default=600,
        description="CORS preflight cache duration (seconds)"
    )

    # =========================================================================
    # Rate Limiting Settings
    # =========================================================================

    rate_limit_enabled: bool = Field(
        default=True,
        description="Enable rate limiting"
    )
    rate_limit_requests: int = Field(
        default=100,
        description="Max requests per window",
        gt=0,
        le=10000
    )
    rate_limit_window: int = Field(
        default=60,
        description="Rate limit window (seconds)",
        gt=0,
        le=3600
    )
    rate_limit_storage_url: Optional[str] = Field(
        default=None,
        description="Redis URL for distributed rate limiting (optional)"
    )

    # =========================================================================
    # Security Settings
    # =========================================================================

    security_require_https: bool = Field(
        default=False,
        description="Require HTTPS for all requests (enable in production)"
    )
    security_api_key_header: str = Field(
        default="X-API-Key",
        description="API key header name"
    )
    security_max_request_size: int = Field(
        default=10 * 1024 * 1024,  # 10 MB
        description="Maximum request body size in bytes",
        gt=0
    )
    security_headers_enabled: bool = Field(
        default=True,
        description="Enable security headers (X-Frame-Options, etc.)"
    )
    security_hsts_max_age: int = Field(
        default=31536000,  # 1 year
        description="HSTS max age (seconds)"
    )
    security_csp_enabled: bool = Field(
        default=False,
        description="Enable Content Security Policy header"
    )

    # =========================================================================
    # Audit Logging Settings
    # =========================================================================

    audit_enabled: bool = Field(
        default=True,
        description="Enable audit logging"
    )
    audit_log_requests: bool = Field(
        default=True,
        description="Log all API requests"
    )
    audit_log_responses: bool = Field(
        default=False,
        description="Log API responses (may contain sensitive data)"
    )
    audit_retention_days: int = Field(
        default=90,
        description="Audit log retention period (days)",
        gt=0,
        le=365
    )
    audit_log_anonymous: bool = Field(
        default=True,
        description="Log requests from unauthenticated users"
    )

    # =========================================================================
    # Monitoring and Observability
    # =========================================================================

    metrics_enabled: bool = Field(
        default=True,
        description="Enable Prometheus metrics"
    )
    metrics_endpoint: str = Field(
        default="/metrics",
        description="Metrics endpoint path"
    )

    tracing_enabled: bool = Field(
        default=True,
        description="Enable OpenTelemetry tracing"
    )
    tracing_jaeger_host: str = Field(
        default="jaeger",
        description="Jaeger agent host"
    )
    tracing_jaeger_port: int = Field(
        default=6831,
        description="Jaeger agent port",
        gt=0,
        lt=65536
    )
    tracing_sample_rate: float = Field(
        default=0.1,
        description="Trace sampling rate (0.0-1.0, where 1.0 = 100%)",
        ge=0.0,
        le=1.0
    )

    # =========================================================================
    # Logging Settings
    # =========================================================================

    log_level: str = Field(
        default="INFO",
        description="Log level: DEBUG|INFO|WARNING|ERROR|CRITICAL"
    )
    log_format: str = Field(
        default="json",
        description="Log format: json|text"
    )
    log_include_correlation_id: bool = Field(
        default=True,
        description="Include correlation ID in logs"
    )
    log_include_request_id: bool = Field(
        default=True,
        description="Include request ID in logs"
    )

    # =========================================================================
    # HashiCorp Vault Settings
    # =========================================================================

    vault_enabled: bool = Field(
        default=False,
        description="Enable HashiCorp Vault integration for secrets"
    )
    vault_url: str = Field(
        default="http://vault:8200",
        description="Vault server URL"
    )
    vault_token: Optional[str] = Field(
        default=None,
        description="Vault token (dev only - use AppRole in production)"
    )
    vault_role_id: Optional[str] = Field(
        default=None,
        description="Vault AppRole role_id (production)"
    )
    vault_secret_id: Optional[str] = Field(
        default=None,
        description="Vault AppRole secret_id (production)"
    )
    vault_mount_point: str = Field(
        default="secret",
        description="Vault secrets mount point"
    )
    vault_path: str = Field(
        default="cdc-pipeline",
        description="Vault secrets path"
    )
    vault_namespace: Optional[str] = Field(
        default=None,
        description="Vault namespace (Vault Enterprise)"
    )

    # =========================================================================
    # Kafka Connect Settings
    # =========================================================================

    kafka_connect_url: str = Field(
        default="http://kafka-connect:8083",
        description="Kafka Connect REST API URL"
    )
    kafka_connect_timeout: int = Field(
        default=30,
        description="Kafka Connect request timeout (seconds)",
        gt=0
    )
    kafka_connect_retry_attempts: int = Field(
        default=3,
        description="Number of retry attempts for Kafka Connect operations",
        ge=0,
        le=10
    )
    kafka_connect_retry_delay: int = Field(
        default=5,
        description="Delay between retry attempts (seconds)",
        gt=0
    )

    # =========================================================================
    # External Service Settings
    # =========================================================================

    mongodb_url: str = Field(
        default="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
        description="MongoDB connection URL"
    )

    minio_endpoint: str = Field(
        default="minio:9000",
        description="MinIO endpoint"
    )
    minio_access_key: str = Field(
        default="minio_access_key",
        description="MinIO access key"
    )
    minio_secret_key: str = Field(
        default="minio_secret_key",
        description="MinIO secret key"
    )
    minio_secure: bool = Field(
        default=False,
        description="Use HTTPS for MinIO"
    )
    minio_bucket: str = Field(
        default="delta-lake",
        description="MinIO bucket for Delta Lake"
    )

    # =========================================================================
    # Pagination Settings
    # =========================================================================

    pagination_default_limit: int = Field(
        default=100,
        description="Default page size",
        gt=0,
        le=1000
    )
    pagination_max_limit: int = Field(
        default=1000,
        description="Maximum page size",
        gt=0,
        le=10000
    )

    # =========================================================================
    # Worker Settings (Gunicorn/Uvicorn)
    # =========================================================================

    workers: int = Field(
        default=4,
        description="Number of Gunicorn worker processes",
        gt=0,
        le=32
    )
    worker_timeout: int = Field(
        default=120,
        description="Worker timeout (seconds)",
        gt=0
    )
    worker_class: str = Field(
        default="uvicorn.workers.UvicornWorker",
        description="Worker class"
    )
    worker_max_requests: int = Field(
        default=10000,
        description="Max requests before worker restart (prevents memory leaks)",
        gt=0
    )
    worker_max_requests_jitter: int = Field(
        default=1000,
        description="Random jitter for max requests",
        ge=0
    )
    worker_keepalive: int = Field(
        default=5,
        description="Worker keepalive timeout (seconds)",
        gt=0
    )

    # =========================================================================
    # Validators
    # =========================================================================

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is one of the allowed values."""
        allowed = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in allowed:
            raise ValueError(f"log_level must be one of {allowed}, got: {v}")
        return v_upper

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of the allowed values."""
        allowed = ["development", "staging", "production"]
        v_lower = v.lower()
        if v_lower not in allowed:
            raise ValueError(f"environment must be one of {allowed}, got: {v}")
        return v_lower

    @field_validator("cors_origins")
    @classmethod
    def validate_cors_origins(cls, v: List[str]) -> List[str]:
        """Validate CORS origins are not empty."""
        if not v:
            return ["*"]  # Allow all if not specified (dev only)
        return v

    @field_validator("jwt_algorithm")
    @classmethod
    def validate_jwt_algorithm(cls, v: str) -> str:
        """Validate JWT algorithm is supported."""
        allowed = ["HS256", "HS384", "HS512", "RS256", "RS384", "RS512"]
        if v not in allowed:
            raise ValueError(f"jwt_algorithm must be one of {allowed}, got: {v}")
        return v

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        """Validate log format."""
        allowed = ["json", "text"]
        v_lower = v.lower()
        if v_lower not in allowed:
            raise ValueError(f"log_format must be one of {allowed}, got: {v}")
        return v_lower

    # =========================================================================
    # Computed Properties
    # =========================================================================

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment == "development"

    @property
    def is_staging(self) -> bool:
        """Check if running in staging mode."""
        return self.environment == "staging"

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment == "production"

    @property
    def database_url_async(self) -> str:
        """Get async database URL (with asyncpg driver)."""
        if "postgresql://" in self.database_url and "asyncpg" not in self.database_url:
            return self.database_url.replace("postgresql://", "postgresql+asyncpg://")
        return self.database_url

    @property
    def database_url_sync(self) -> str:
        """Get sync database URL (with psycopg2 driver)."""
        if "postgresql://" in self.database_url:
            return self.database_url.replace("postgresql+asyncpg://", "postgresql://")
        return self.database_url

    # =========================================================================
    # Model Config
    # =========================================================================

    model_config = SettingsConfigDict(
        env_prefix="CDC_API_",  # Environment variable prefix
        env_file=".env",         # Load from .env file
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",          # Ignore extra environment variables
        validate_default=True,   # Validate default values
    )


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses lru_cache to ensure settings are loaded only once and shared
    across the application. Settings are loaded from:
    1. Environment variables with CDC_API_ prefix
    2. .env file in the current directory
    3. Default values

    Returns:
        Settings: Cached settings instance

    Example:
        >>> from api.src.config import get_settings
        >>> settings = get_settings()
        >>> print(settings.database_url)
        postgresql+asyncpg://cdc_api:password@postgres:5432/cdc_metadata
    """
    return Settings()


# Convenience function to clear settings cache (useful for testing)
def clear_settings_cache():
    """
    Clear the settings cache.

    Useful for testing when you need to reload settings with different
    environment variables.

    Example:
        >>> from api.src.config import get_settings, clear_settings_cache
        >>> settings1 = get_settings()
        >>> os.environ['CDC_API_DEBUG'] = 'true'
        >>> clear_settings_cache()
        >>> settings2 = get_settings()  # Will reload with new env vars
    """
    get_settings.cache_clear()


# Convenience access to settings (lazy-loaded)
def get_current_settings() -> Settings:
    """
    Get current settings without caching.

    Use this only when you need fresh settings (e.g., in tests).
    For normal use, prefer get_settings() which is cached.

    Returns:
        Settings: Fresh settings instance
    """
    return Settings()
