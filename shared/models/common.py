"""Common Pydantic models shared across services."""

from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional

from pydantic import BaseModel, Field, validator


class HealthStatus(str, Enum):
    """Health status enum."""

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"


class MongoDBConfig(BaseModel):
    """MongoDB connection configuration."""

    connection_string: str = Field(..., description="MongoDB connection string")
    database: str = Field(..., description="Database name")
    collection: str = Field(..., description="Collection name")
    replica_set: Optional[str] = Field(None, description="Replica set name")
    auth_source: str = Field("admin", description="Authentication database")

    class Config:
        """Pydantic config."""

        frozen = True


class KafkaConfig(BaseModel):
    """Kafka connection configuration."""

    bootstrap_servers: List[str] = Field(..., description="Kafka broker addresses")
    topic_prefix: str = Field("mongodb", description="Topic prefix for CDC events")
    consumer_group: str = Field(
        "delta-writer", description="Consumer group ID"
    )
    auto_offset_reset: str = Field("earliest", description="Offset reset policy")
    enable_auto_commit: bool = Field(False, description="Enable auto-commit")
    max_poll_records: int = Field(2000, description="Max records per poll")

    @validator("bootstrap_servers")
    def validate_bootstrap_servers(cls, v: List[str]) -> List[str]:
        """Validate bootstrap servers list is not empty."""
        if not v:
            raise ValueError("bootstrap_servers cannot be empty")
        return v

    class Config:
        """Pydantic config."""

        frozen = True


class DeltaLakeConfig(BaseModel):
    """Delta Lake storage configuration."""

    storage_endpoint: str = Field(..., description="S3-compatible storage endpoint")
    access_key: str = Field(..., description="Storage access key")
    secret_key: str = Field(..., description="Storage secret key")
    bucket: str = Field("lakehouse", description="Bucket name")
    table_path: str = Field(..., description="Table path within bucket")
    schema_mode: str = Field("merge", description="Schema evolution mode")
    partition_by: Optional[List[str]] = Field(
        None, description="Partition columns"
    )

    class Config:
        """Pydantic config."""

        frozen = True


class PipelineConfig(BaseModel):
    """CDC Pipeline configuration."""

    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    name: str = Field(..., description="Pipeline name")
    description: Optional[str] = Field(None, description="Pipeline description")

    mongodb: MongoDBConfig = Field(..., description="MongoDB configuration")
    kafka: KafkaConfig = Field(..., description="Kafka configuration")
    delta_lake: DeltaLakeConfig = Field(..., description="Delta Lake configuration")

    enabled: bool = Field(True, description="Whether pipeline is enabled")
    batch_size: int = Field(1000, description="Batch size for processing")
    batch_timeout_ms: int = Field(5000, description="Batch timeout in milliseconds")

    retry_max_attempts: int = Field(3, description="Max retry attempts on failure")
    retry_backoff_ms: int = Field(1000, description="Retry backoff in milliseconds")

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    metadata: Dict[str, str] = Field(
        default_factory=dict, description="Additional metadata"
    )

    @validator("batch_size")
    def validate_batch_size(cls, v: int) -> int:
        """Validate batch size is positive."""
        if v <= 0:
            raise ValueError("batch_size must be positive")
        return v

    @validator("batch_timeout_ms")
    def validate_batch_timeout(cls, v: int) -> int:
        """Validate batch timeout is positive."""
        if v <= 0:
            raise ValueError("batch_timeout_ms must be positive")
        return v

    class Config:
        """Pydantic config."""

        json_encoders = {datetime: lambda v: v.isoformat()}


class ServiceInfo(BaseModel):
    """Service information model."""

    service_name: str = Field(..., description="Service name")
    version: str = Field(..., description="Service version")
    status: HealthStatus = Field(..., description="Service health status")
    uptime_seconds: float = Field(..., description="Service uptime in seconds")
    dependencies: Dict[str, HealthStatus] = Field(
        default_factory=dict, description="Dependency health status"
    )

    class Config:
        """Pydantic config."""

        use_enum_values = True
