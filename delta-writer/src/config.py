"""Configuration management for Delta Writer service.

Uses Pydantic Settings for environment-based configuration.
See IMPLEMENTATION_GUIDE.md for full implementation details.
"""

from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class KafkaConfig(BaseSettings):
    """Kafka connection configuration."""

    bootstrap_servers: str = Field(default="kafka:9092", description="Kafka brokers")
    consumer_group: str = Field(default="delta-writer", description="Consumer group ID")
    topic_pattern: str = Field(default="mongodb.*", description="Topic pattern to subscribe")
    auto_offset_reset: str = Field(default="earliest", description="Offset reset policy")
    enable_auto_commit: bool = Field(default=False, description="Auto-commit offsets")
    max_poll_records: int = Field(default=2000, description="Max records per poll")
    session_timeout_ms: int = Field(default=30000, description="Session timeout")

    model_config = SettingsConfigDict(env_prefix="KAFKA_")


class MinIOConfig(BaseSettings):
    """MinIO/S3 configuration."""

    endpoint: str = Field(default="http://minio:9000", description="MinIO endpoint")
    access_key: str = Field(default="minioadmin", description="Access key")
    secret_key: str = Field(default="minioadmin123", description="Secret key")
    bucket: str = Field(default="lakehouse", description="Bucket name")
    region: str = Field(default="us-east-1", description="AWS region")
    secure: bool = Field(default=False, description="Use HTTPS")

    model_config = SettingsConfigDict(env_prefix="MINIO_")


class DeltaConfig(BaseSettings):
    """Delta Lake configuration."""

    table_base_path: str = Field(
        default="s3://lakehouse/tables", description="Base path for Delta tables"
    )
    schema_cache_ttl: int = Field(default=300, description="Schema cache TTL (seconds)")
    write_mode: str = Field(default="append", description="Write mode (append/overwrite/upsert)")
    partition_by: List[str] = Field(default_factory=list, description="Partition columns")
    batch_size: int = Field(default=1000, description="Batch size for writes")
    batch_timeout_ms: int = Field(default=5000, description="Batch timeout (milliseconds)")

    model_config = SettingsConfigDict(env_prefix="DELTA_")


class Config(BaseSettings):
    """Main service configuration."""

    # Sub-configurations
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    minio: MinIOConfig = Field(default_factory=MinIOConfig)
    delta: DeltaConfig = Field(default_factory=DeltaConfig)

    # Service configuration
    service_name: str = Field(default="delta-writer", description="Service name")
    log_level: str = Field(default="INFO", description="Log level")
    metrics_port: int = Field(default=8001, description="Prometheus metrics port")

    # Vault integration (optional)
    vault_enabled: bool = Field(default=False, description="Enable Vault for secrets")
    vault_addr: str = Field(default="http://vault:8200", description="Vault address")
    vault_token: str = Field(default="", description="Vault token")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


# Global config instance
_config_instance: Config | None = None


def get_config() -> Config:
    """Get or create configuration instance.

    Returns:
        Config instance
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = Config()
    return _config_instance
