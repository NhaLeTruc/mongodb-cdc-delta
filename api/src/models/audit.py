"""
Pydantic models for audit logging.

Provides data validation and serialization for:
- Audit log entries
- API request/response tracking
- Security event logging
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, Dict, Any
from datetime import datetime
from uuid import UUID, uuid4
from enum import Enum
import ipaddress


class AuditAction(str, Enum):
    """Available audit actions."""

    # Authentication actions
    LOGIN_SUCCESS = "login_success"
    LOGIN_FAILURE = "login_failure"
    LOGOUT = "logout"
    TOKEN_REFRESH = "token_refresh"

    # User management actions
    USER_CREATE = "user_create"
    USER_READ = "user_read"
    USER_UPDATE = "user_update"
    USER_DELETE = "user_delete"
    USER_LIST = "user_list"

    # Mapping management actions
    MAPPING_CREATE = "mapping_create"
    MAPPING_READ = "mapping_read"
    MAPPING_UPDATE = "mapping_update"
    MAPPING_DELETE = "mapping_delete"
    MAPPING_LIST = "mapping_list"

    # Pipeline operations
    PIPELINE_TRIGGER = "pipeline_trigger"
    PIPELINE_PAUSE = "pipeline_pause"
    PIPELINE_RESUME = "pipeline_resume"
    PIPELINE_STATUS = "pipeline_status"

    # System actions
    SYSTEM_CONFIG_UPDATE = "system_config_update"
    SYSTEM_HEALTH_CHECK = "system_health_check"

    # Security actions
    UNAUTHORIZED_ACCESS = "unauthorized_access"
    PERMISSION_DENIED = "permission_denied"
    RATE_LIMIT_EXCEEDED = "rate_limit_exceeded"


class ResourceType(str, Enum):
    """Types of resources that can be audited."""
    USER = "user"
    MAPPING = "mapping"
    PIPELINE = "pipeline"
    SYSTEM = "system"
    AUTH = "auth"


# ============================================================================
# REQUEST MODELS
# ============================================================================


class CreateAuditLogRequest(BaseModel):
    """Request model for creating audit logs."""
    user_id: Optional[UUID] = Field(None, description="User ID (None for anonymous)")
    action: AuditAction = Field(..., description="Action performed")
    resource_type: Optional[ResourceType] = Field(None, description="Type of resource")
    resource_id: Optional[str] = Field(None, max_length=255, description="Resource identifier")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details (JSON)")
    ip_address: Optional[str] = Field(None, description="Client IP address")
    user_agent: Optional[str] = Field(None, description="Client user agent")
    status_code: Optional[int] = Field(None, ge=100, le=599, description="HTTP status code")

    @field_validator("ip_address")
    @classmethod
    def validate_ip_address(cls, v: Optional[str]) -> Optional[str]:
        """Validate IP address format."""
        if v is None:
            return v
        try:
            ipaddress.ip_address(v)
            return v
        except ValueError:
            raise ValueError(f"Invalid IP address: {v}")

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
                "status_code": 201
            }
        }
    }


# ============================================================================
# RESPONSE MODELS
# ============================================================================


class AuditLogResponse(BaseModel):
    """Response model for audit log entries."""
    id: str = Field(..., description="Audit log ID")
    user_id: Optional[str] = Field(None, description="User ID")
    username: Optional[str] = Field(None, description="Username")
    action: str = Field(..., description="Action performed")
    resource_type: Optional[str] = Field(None, description="Type of resource")
    resource_id: Optional[str] = Field(None, description="Resource identifier")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details")
    ip_address: Optional[str] = Field(None, description="Client IP address")
    user_agent: Optional[str] = Field(None, description="Client user agent")
    timestamp: datetime = Field(..., description="Timestamp of action")
    status_code: Optional[int] = Field(None, description="HTTP status code")

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
                "status_code": 201
            }
        }
    }


class AuditLogListResponse(BaseModel):
    """Response model for list of audit logs."""
    items: list[AuditLogResponse] = Field(..., description="Audit log entries")
    total: int = Field(..., ge=0, description="Total count")
    limit: int = Field(..., gt=0, description="Page size")
    offset: int = Field(..., ge=0, description="Page offset")

    model_config = {
        "json_schema_extra": {
            "example": {
                "items": [],
                "total": 0,
                "limit": 100,
                "offset": 0
            }
        }
    }


# ============================================================================
# DATABASE MODELS
# ============================================================================


class AuditLogDB(BaseModel):
    """Audit log database model."""
    id: UUID = Field(default_factory=uuid4, description="Audit log ID")
    user_id: Optional[UUID] = Field(None, description="User ID")
    action: str = Field(..., max_length=100, description="Action performed")
    resource_type: Optional[str] = Field(None, max_length=50, description="Type of resource")
    resource_id: Optional[str] = Field(None, max_length=255, description="Resource identifier")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional details (JSON)")
    ip_address: Optional[str] = Field(None, description="Client IP address")
    user_agent: Optional[str] = Field(None, description="Client user agent")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of action")
    status_code: Optional[int] = Field(None, description="HTTP status code")

    model_config = {
        "from_attributes": True
    }

    def to_response(self, username: Optional[str] = None) -> AuditLogResponse:
        """Convert to API response model."""
        return AuditLogResponse(
            id=str(self.id),
            user_id=str(self.user_id) if self.user_id else None,
            username=username,
            action=self.action,
            resource_type=self.resource_type,
            resource_id=self.resource_id,
            details=self.details,
            ip_address=self.ip_address,
            user_agent=self.user_agent,
            timestamp=self.timestamp,
            status_code=self.status_code
        )


# ============================================================================
# FILTER MODELS
# ============================================================================


class AuditLogFilter(BaseModel):
    """Filter parameters for querying audit logs."""
    user_id: Optional[UUID] = Field(None, description="Filter by user ID")
    action: Optional[AuditAction] = Field(None, description="Filter by action")
    resource_type: Optional[ResourceType] = Field(None, description="Filter by resource type")
    resource_id: Optional[str] = Field(None, description="Filter by resource ID")
    start_date: Optional[datetime] = Field(None, description="Filter by start date")
    end_date: Optional[datetime] = Field(None, description="Filter by end date")
    ip_address: Optional[str] = Field(None, description="Filter by IP address")
    status_code: Optional[int] = Field(None, ge=100, le=599, description="Filter by HTTP status code")
    limit: int = Field(default=100, gt=0, le=1000, description="Page size")
    offset: int = Field(default=0, ge=0, description="Page offset")

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v: Optional[datetime], info) -> Optional[datetime]:
        """Validate end_date is after start_date."""
        if v is None:
            return v
        start_date = info.data.get("start_date")
        if start_date and v < start_date:
            raise ValueError("end_date must be after start_date")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id": "550e8400-e29b-41d4-a716-446655440000",
                "action": "user_create",
                "resource_type": "user",
                "start_date": "2025-01-01T00:00:00Z",
                "end_date": "2025-01-31T23:59:59Z",
                "limit": 100,
                "offset": 0
            }
        }
    }
