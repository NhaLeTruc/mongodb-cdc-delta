"""
Audit logging models.

Provides both SQLAlchemy ORM models and Pydantic schemas for:
- Audit log entries
- API request/response tracking
- Security event logging
- User action tracking
- Resource access logging

Uses SQLAlchemy 2.0 declarative syntax with async compatibility.
"""

from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from uuid import UUID, uuid4
from enum import Enum

from sqlalchemy import String, Integer, DateTime, Index, text, Text
from sqlalchemy.dialects.postgresql import UUID as PGUUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column
from pydantic import BaseModel, Field, field_validator

from api.src.models.auth import Base


# ============================================================================
# Enums
# ============================================================================


class AuditAction(str, Enum):
    """
    Audit action types.

    Comprehensive set of actions that can be logged for audit purposes.
    """
    # Authentication actions
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    LOGOUT = "logout"
    TOKEN_REFRESH = "token_refresh"
    TOKEN_REVOKE = "token_revoke"

    # User management actions
    USER_CREATE = "user_create"
    USER_READ = "user_read"
    USER_UPDATE = "user_update"
    USER_DELETE = "user_delete"
    USER_LIST = "user_list"
    USER_PASSWORD_CHANGE = "user_password_change"
    USER_ACTIVATE = "user_activate"
    USER_DEACTIVATE = "user_deactivate"

    # Role management actions
    ROLE_ASSIGN = "role_assign"
    ROLE_REVOKE = "role_revoke"
    ROLE_CREATE = "role_create"
    ROLE_UPDATE = "role_update"
    ROLE_DELETE = "role_delete"

    # Mapping management actions
    MAPPING_CREATE = "mapping_create"
    MAPPING_READ = "mapping_read"
    MAPPING_UPDATE = "mapping_update"
    MAPPING_DELETE = "mapping_delete"
    MAPPING_LIST = "mapping_list"
    MAPPING_VALIDATE = "mapping_validate"

    # Pipeline operations
    PIPELINE_START = "pipeline_start"
    PIPELINE_STOP = "pipeline_stop"
    PIPELINE_PAUSE = "pipeline_pause"
    PIPELINE_RESUME = "pipeline_resume"
    PIPELINE_STATUS = "pipeline_status"
    PIPELINE_TRIGGER = "pipeline_trigger"

    # Checkpoint operations
    CHECKPOINT_CREATE = "checkpoint_create"
    CHECKPOINT_UPDATE = "checkpoint_update"
    CHECKPOINT_DELETE = "checkpoint_delete"
    CHECKPOINT_RESTORE = "checkpoint_restore"

    # Reconciliation operations
    RECONCILIATION_START = "reconciliation_start"
    RECONCILIATION_COMPLETE = "reconciliation_complete"
    RECONCILIATION_FAILED = "reconciliation_failed"

    # System actions
    SYSTEM_CONFIG_UPDATE = "system_config_update"
    SYSTEM_HEALTH_CHECK = "system_health_check"
    SYSTEM_MAINTENANCE_START = "system_maintenance_start"
    SYSTEM_MAINTENANCE_END = "system_maintenance_end"

    # Security actions
    UNAUTHORIZED_ACCESS = "unauthorized_access"
    PERMISSION_DENIED = "permission_denied"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"
    INVALID_TOKEN = "invalid_token"
    SECURITY_VIOLATION = "security_violation"


class ResourceType(str, Enum):
    """
    Resource types that can be audited.

    Defines the types of resources that can have actions logged against them.
    """
    USER = "user"
    ROLE = "role"
    MAPPING = "mapping"
    PIPELINE = "pipeline"
    CHECKPOINT = "checkpoint"
    RECONCILIATION = "reconciliation"
    SYSTEM = "system"
    AUTH = "auth"


class AuditStatus(str, Enum):
    """
    Audit log status.

    Indicates the outcome of the audited action.
    """
    SUCCESS = "success"
    FAILURE = "failure"
    ERROR = "error"
    PENDING = "pending"


# ============================================================================
# SQLAlchemy Models
# ============================================================================


class AuditLog(Base):
    """
    Audit log model for tracking all system actions.

    Stores comprehensive audit information including:
    - Who performed the action (user_id)
    - What action was performed (action)
    - What resource was affected (resource_type, resource_id)
    - When it occurred (timestamp)
    - Where it came from (ip_address, user_agent)
    - Additional context (details JSON)
    - Outcome (status_code, status)
    """
    __tablename__ = "audit_logs"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    user_id: Mapped[Optional[UUID]] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
        index=True,
        comment="User who performed the action (NULL for anonymous/system)"
    )
    action: Mapped[str] = mapped_column(
        String(100),
        nullable=False,
        index=True,
        comment="Action performed (e.g., user_create, login_success)"
    )
    resource_type: Mapped[Optional[str]] = mapped_column(
        String(50),
        nullable=True,
        index=True,
        comment="Type of resource affected (e.g., user, mapping, pipeline)"
    )
    resource_id: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True,
        index=True,
        comment="Identifier of the affected resource"
    )
    details: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB,
        nullable=True,
        comment="Additional context and details in JSON format"
    )
    ip_address: Mapped[Optional[str]] = mapped_column(
        String(45),  # IPv6 max length
        nullable=True,
        index=True,
        comment="Client IP address"
    )
    user_agent: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="Client user agent string"
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP"),
        index=True,
        comment="When the action occurred"
    )
    status_code: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="HTTP status code of the action (if applicable)"
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="success",
        index=True,
        comment="Outcome status: success, failure, error, pending"
    )
    duration_ms: Mapped[Optional[int]] = mapped_column(
        Integer,
        nullable=True,
        comment="Action duration in milliseconds (if applicable)"
    )
    correlation_id: Mapped[Optional[str]] = mapped_column(
        String(100),
        nullable=True,
        index=True,
        comment="Correlation ID for tracing related actions"
    )

    # Indexes for common query patterns
    __table_args__ = (
        Index("idx_audit_logs_user_id", "user_id"),
        Index("idx_audit_logs_action", "action"),
        Index("idx_audit_logs_resource_type", "resource_type"),
        Index("idx_audit_logs_resource_id", "resource_id"),
        Index("idx_audit_logs_timestamp", "timestamp"),
        Index("idx_audit_logs_status", "status"),
        Index("idx_audit_logs_ip_address", "ip_address"),
        Index("idx_audit_logs_correlation_id", "correlation_id"),
        # Composite indexes for common queries
        Index("idx_audit_logs_user_timestamp", "user_id", "timestamp"),
        Index("idx_audit_logs_action_timestamp", "action", "timestamp"),
        Index("idx_audit_logs_resource", "resource_type", "resource_id", "timestamp"),
    )

    def __repr__(self) -> str:
        """String representation."""
        return (
            f"<AuditLog(id={self.id}, action='{self.action}', "
            f"user_id={self.user_id}, timestamp={self.timestamp})>"
        )


# ============================================================================
# Pydantic Request Models
# ============================================================================


class CreateAuditLogRequest(BaseModel):
    """Request model for creating audit logs."""
    user_id: Optional[UUID] = Field(
        None,
        description="User ID (None for anonymous/system actions)"
    )
    action: AuditAction = Field(
        ...,
        description="Action performed"
    )
    resource_type: Optional[ResourceType] = Field(
        None,
        description="Type of resource affected"
    )
    resource_id: Optional[str] = Field(
        None,
        max_length=255,
        description="Resource identifier"
    )
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional details (JSON)"
    )
    ip_address: Optional[str] = Field(
        None,
        max_length=45,
        description="Client IP address"
    )
    user_agent: Optional[str] = Field(
        None,
        description="Client user agent"
    )
    status_code: Optional[int] = Field(
        None,
        ge=100,
        le=599,
        description="HTTP status code"
    )
    status: AuditStatus = Field(
        default=AuditStatus.SUCCESS,
        description="Action outcome status"
    )
    duration_ms: Optional[int] = Field(
        None,
        ge=0,
        description="Action duration in milliseconds"
    )
    correlation_id: Optional[str] = Field(
        None,
        max_length=100,
        description="Correlation ID for tracing"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id": "550e8400-e29b-41d4-a716-446655440000",
                "action": "user_create",
                "resource_type": "user",
                "resource_id": "660e8400-e29b-41d4-a716-446655440001",
                "details": {"username": "newuser", "roles": ["analyst"]},
                "ip_address": "192.168.1.100",
                "user_agent": "Mozilla/5.0",
                "status_code": 201,
                "status": "success",
                "duration_ms": 45
            }
        }
    }


# ============================================================================
# Pydantic Response Models
# ============================================================================


class AuditLogEntry(BaseModel):
    """Response model for a single audit log entry."""
    id: str = Field(
        ...,
        description="Audit log ID (UUID)"
    )
    user_id: Optional[str] = Field(
        None,
        description="User ID (UUID)"
    )
    username: Optional[str] = Field(
        None,
        description="Username (resolved from user_id)"
    )
    action: str = Field(
        ...,
        description="Action performed"
    )
    resource_type: Optional[str] = Field(
        None,
        description="Type of resource"
    )
    resource_id: Optional[str] = Field(
        None,
        description="Resource identifier"
    )
    details: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional details"
    )
    ip_address: Optional[str] = Field(
        None,
        description="Client IP address"
    )
    user_agent: Optional[str] = Field(
        None,
        description="Client user agent"
    )
    timestamp: datetime = Field(
        ...,
        description="Timestamp of action"
    )
    status_code: Optional[int] = Field(
        None,
        description="HTTP status code"
    )
    status: str = Field(
        ...,
        description="Outcome status"
    )
    duration_ms: Optional[int] = Field(
        None,
        description="Duration in milliseconds"
    )
    correlation_id: Optional[str] = Field(
        None,
        description="Correlation ID"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "770e8400-e29b-41d4-a716-446655440000",
                "user_id": "550e8400-e29b-41d4-a716-446655440000",
                "username": "admin",
                "action": "user_create",
                "resource_type": "user",
                "resource_id": "660e8400-e29b-41d4-a716-446655440001",
                "details": {"username": "newuser", "roles": ["analyst"]},
                "ip_address": "192.168.1.100",
                "user_agent": "Mozilla/5.0",
                "timestamp": "2025-01-15T10:30:00Z",
                "status_code": 201,
                "status": "success",
                "duration_ms": 45,
                "correlation_id": "abc123"
            }
        }
    }


class AuditLogListResponse(BaseModel):
    """Response model for list of audit logs with pagination."""
    items: List[AuditLogEntry] = Field(
        ...,
        description="Audit log entries"
    )
    total: int = Field(
        ...,
        ge=0,
        description="Total count of matching records"
    )
    limit: int = Field(
        ...,
        gt=0,
        description="Page size"
    )
    offset: int = Field(
        ...,
        ge=0,
        description="Page offset"
    )
    has_more: bool = Field(
        ...,
        description="Whether there are more records"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "items": [],
                "total": 0,
                "limit": 100,
                "offset": 0,
                "has_more": False
            }
        }
    }


# ============================================================================
# Filter Models
# ============================================================================


class AuditLogFilter(BaseModel):
    """
    Filter parameters for querying audit logs.

    Supports filtering by:
    - User
    - Action type
    - Resource type and ID
    - Date range
    - IP address
    - Status code
    - Correlation ID
    """
    user_id: Optional[UUID] = Field(
        None,
        description="Filter by user ID"
    )
    action: Optional[AuditAction] = Field(
        None,
        description="Filter by action type"
    )
    resource_type: Optional[ResourceType] = Field(
        None,
        description="Filter by resource type"
    )
    resource_id: Optional[str] = Field(
        None,
        description="Filter by resource ID"
    )
    start_date: Optional[datetime] = Field(
        None,
        description="Filter by start date (inclusive)"
    )
    end_date: Optional[datetime] = Field(
        None,
        description="Filter by end date (inclusive)"
    )
    ip_address: Optional[str] = Field(
        None,
        description="Filter by IP address"
    )
    status_code: Optional[int] = Field(
        None,
        ge=100,
        le=599,
        description="Filter by HTTP status code"
    )
    status: Optional[AuditStatus] = Field(
        None,
        description="Filter by outcome status"
    )
    correlation_id: Optional[str] = Field(
        None,
        description="Filter by correlation ID"
    )
    limit: int = Field(
        default=100,
        gt=0,
        le=1000,
        description="Page size (max 1000)"
    )
    offset: int = Field(
        default=0,
        ge=0,
        description="Page offset"
    )

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v: Optional[datetime], info) -> Optional[datetime]:
        """Validate end_date is after start_date."""
        if v is None:
            return v

        start_date = info.data.get("start_date")
        if start_date and v < start_date:
            raise ValueError("end_date must be after or equal to start_date")

        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id": "550e8400-e29b-41d4-a716-446655440000",
                "action": "user_create",
                "resource_type": "user",
                "start_date": "2025-01-01T00:00:00Z",
                "end_date": "2025-01-31T23:59:59Z",
                "status": "success",
                "limit": 100,
                "offset": 0
            }
        }
    }


class AuditStatistics(BaseModel):
    """
    Audit statistics model for analytics.

    Provides aggregated statistics about audit logs.
    """
    total_actions: int = Field(
        ...,
        ge=0,
        description="Total number of actions"
    )
    unique_users: int = Field(
        ...,
        ge=0,
        description="Number of unique users"
    )
    actions_by_type: Dict[str, int] = Field(
        ...,
        description="Count of actions by type"
    )
    actions_by_resource: Dict[str, int] = Field(
        ...,
        description="Count of actions by resource type"
    )
    success_rate: float = Field(
        ...,
        ge=0.0,
        le=100.0,
        description="Percentage of successful actions"
    )
    time_range_start: Optional[datetime] = Field(
        None,
        description="Start of time range"
    )
    time_range_end: Optional[datetime] = Field(
        None,
        description="End of time range"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "total_actions": 1000,
                "unique_users": 10,
                "actions_by_type": {
                    "user_create": 50,
                    "user_update": 100,
                    "login_success": 500
                },
                "actions_by_resource": {
                    "user": 150,
                    "mapping": 200,
                    "pipeline": 100
                },
                "success_rate": 95.5,
                "time_range_start": "2025-01-01T00:00:00Z",
                "time_range_end": "2025-01-31T23:59:59Z"
            }
        }
    }
