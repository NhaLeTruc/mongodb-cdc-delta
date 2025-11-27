"""
FastAPI application configuration using Pydantic Settings.

Provides centralized configuration for:
- Database connections (PostgreSQL)
- Authentication (JWT settings)
- API settings (CORS, rate limiting)
- Security settings
- Logging and monitoring
"""

from pydantic_settings import BaseSettings
from pydantic import Field, PostgresDsn, field_validator
from typing import List, Optional
from functools import lru_cache


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables.

    All settings can be overridden via environment variables with the
    prefix "CDC_API_" (e.g., CDC_API_DATABASE_URL)
    """

    # =========================================================================
    # API Settings
    # =========================================================================

    app_name: str = Field(default="CDC Pipeline Management API", description="Application name")
    app_version: str = Field(default="1.0.0", description="API version")
    api_prefix: str = Field(default="/api/v1", description="API URL prefix")

    debug: bool = Field(default=False, description="Debug mode")
    environment: str = Field(default="production", description="Environment: development|staging|production")

    host: str = Field(default="0.0.0.0", description="API bind host")
    port: int = Field(default=8000, description="API bind port", gt=0, lt=65536)

    # =========================================================================
    # Database Settings (PostgreSQL)
    # =========================================================================

    database_url: PostgresDsn = Field(
        default="postgresql://cdc_api:cdc_password@postgres:5432/cdc_metadata",
        description="PostgreSQL connection URL"
    )

    database_pool_size: int = Field(default=20, description="Database connection pool size", gt=0)
    database_max_overflow: int = Field(default=10, description="Max connections above pool size", ge=0)
    database_pool_timeout: int = Field(default=30, description="Pool connection timeout (seconds)", gt=0)
    database_echo: bool = Field(default=False, description="Echo SQL queries to logs")

    # =========================================================================
    # JWT Authentication Settings
    # =========================================================================

    jwt_secret_key: str = Field(
        default="change-this-secret-key-in-production-use-vault",
        description="Secret key for JWT token signing (use Vault in production)"
    )
    jwt_algorithm: str = Field(default="HS256", description="JWT signing algorithm")
    jwt_access_token_expire_minutes: int = Field(
        default=60,
        description="Access token expiration time in minutes",
        gt=0
    )
    jwt_refresh_token_expire_days: int = Field(
        default=7,
        description="Refresh token expiration time in days",
        gt=0
    )

    # =========================================================================
    # Password Hashing Settings
    # =========================================================================

    password_bcrypt_rounds: int = Field(
        default=12,
        description="BCrypt hash rounds (higher = slower but more secure)",
        ge=4,
        le=31
    )

    # =========================================================================
    # CORS Settings
    # =========================================================================

    cors_enabled: bool = Field(default=True, description="Enable CORS")
    cors_origins: List[str] = Field(
        default=["http://localhost:3000", "http://localhost:8080"],
        description="Allowed CORS origins"
    )
    cors_allow_credentials: bool = Field(default=True, description="Allow credentials in CORS")
    cors_allow_methods: List[str] = Field(default=["*"], description="Allowed HTTP methods")
    cors_allow_headers: List[str] = Field(default=["*"], description="Allowed HTTP headers")

    # =========================================================================
    # Rate Limiting Settings
    # =========================================================================

    rate_limit_enabled: bool = Field(default=True, description="Enable rate limiting")
    rate_limit_requests: int = Field(default=100, description="Requests per window", gt=0)
    rate_limit_window: int = Field(default=60, description="Rate limit window (seconds)", gt=0)

    # =========================================================================
    # Security Settings
    # =========================================================================

    security_require_https: bool = Field(default=False, description="Require HTTPS for all requests")
    security_api_key_header: str = Field(default="X-API-Key", description="API key header name")
    security_max_request_size: int = Field(
        default=10 * 1024 * 1024,  # 10 MB
        description="Maximum request body size in bytes",
        gt=0
    )

    # Security headers
    security_headers_enabled: bool = Field(default=True, description="Enable security headers")
    security_hsts_max_age: int = Field(default=31536000, description="HSTS max age (seconds)")

    # =========================================================================
    # Audit Logging Settings
    # =========================================================================

    audit_enabled: bool = Field(default=True, description="Enable audit logging")
    audit_log_requests: bool = Field(default=True, description="Log all API requests")
    audit_log_responses: bool = Field(default=False, description="Log API responses")
    audit_retention_days: int = Field(default=90, description="Audit log retention (days)", gt=0)

    # =========================================================================
    # Monitoring and Observability
    # =========================================================================

    metrics_enabled: bool = Field(default=True, description="Enable Prometheus metrics")
    metrics_endpoint: str = Field(default="/metrics", description="Metrics endpoint path")

    tracing_enabled: bool = Field(default=True, description="Enable OpenTelemetry tracing")
    tracing_jaeger_host: str = Field(default="jaeger", description="Jaeger agent host")
    tracing_jaeger_port: int = Field(default=6831, description="Jaeger agent port", gt=0, lt=65536)
    tracing_sample_rate: float = Field(
        default=0.1,
        description="Trace sampling rate (0.0-1.0)",
        ge=0.0,
        le=1.0
    )

    # =========================================================================
    # Logging Settings
    # =========================================================================

    log_level: str = Field(default="INFO", description="Log level: DEBUG|INFO|WARNING|ERROR|CRITICAL")
    log_format: str = Field(default="json", description="Log format: json|text")
    log_include_correlation_id: bool = Field(default=True, description="Include correlation ID in logs")

    # =========================================================================
    # HashiCorp Vault Settings
    # =========================================================================

    vault_enabled: bool = Field(default=False, description="Enable HashiCorp Vault integration")
    vault_url: str = Field(default="http://vault:8200", description="Vault server URL")
    vault_token: Optional[str] = Field(default=None, description="Vault token (use AppRole in production)")
    vault_role_id: Optional[str] = Field(default=None, description="Vault AppRole role_id")
    vault_secret_id: Optional[str] = Field(default=None, description="Vault AppRole secret_id")

    # =========================================================================
    # External Service Settings
    # =========================================================================

    kafka_connect_url: str = Field(
        default="http://kafka-connect:8083",
        description="Kafka Connect REST API URL"
    )

    mongodb_url: str = Field(
        default="mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
        description="MongoDB connection URL"
    )

    minio_endpoint: str = Field(default="minio:9000", description="MinIO endpoint")
    minio_access_key: str = Field(default="minio_access_key", description="MinIO access key")
    minio_secret_key: str = Field(default="minio_secret_key", description="MinIO secret key")
    minio_secure: bool = Field(default=False, description="Use HTTPS for MinIO")

    # =========================================================================
    # Pagination Settings
    # =========================================================================

    pagination_default_limit: int = Field(default=100, description="Default page size", gt=0, le=1000)
    pagination_max_limit: int = Field(default=1000, description="Maximum page size", gt=0, le=10000)

    # =========================================================================
    # Worker Settings
    # =========================================================================

    workers: int = Field(default=4, description="Number of Gunicorn worker processes", gt=0)
    worker_timeout: int = Field(default=120, description="Worker timeout (seconds)", gt=0)
    worker_class: str = Field(default="uvicorn.workers.UvicornWorker", description="Worker class")
    worker_max_requests: int = Field(default=10000, description="Max requests before worker restart", gt=0)
    worker_max_requests_jitter: int = Field(default=1000, description="Jitter for max requests", ge=0)

    # =========================================================================
    # Validators
    # =========================================================================

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is one of the allowed values."""
        allowed = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in allowed:
            raise ValueError(f"log_level must be one of {allowed}")
        return v.upper()

    @field_validator("environment")
    @classmethod
    def validate_environment(cls, v: str) -> str:
        """Validate environment is one of the allowed values."""
        allowed = ["development", "staging", "production"]
        if v.lower() not in allowed:
            raise ValueError(f"environment must be one of {allowed}")
        return v.lower()

    @field_validator("cors_origins")
    @classmethod
    def validate_cors_origins(cls, v: List[str]) -> List[str]:
        """Validate CORS origins are not empty in production."""
        if not v:
            return ["*"]  # Allow all in development
        return v

    # =========================================================================
    # Computed Properties
    # =========================================================================

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return self.environment == "development"

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return self.environment == "production"

    @property
    def database_url_str(self) -> str:
        """Get database URL as string."""
        return str(self.database_url)

    # =========================================================================
    # Model Config
    # =========================================================================

    model_config = {
        "env_prefix": "CDC_API_",  # Environment variable prefix
        "env_file": ".env",         # Load from .env file
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",          # Ignore extra fields
    }


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.

    Uses lru_cache to ensure settings are loaded only once.

    Returns:
        Settings instance

    Example:
        >>> from api.src.config import get_settings
        >>> settings = get_settings()
        >>> print(settings.database_url)
    """
    return Settings()


# Convenience access to settings
settings = get_settings()
