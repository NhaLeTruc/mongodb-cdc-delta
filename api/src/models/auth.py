"""
Pydantic models for authentication and user management.

Provides data validation and serialization for:
- User entities (database and API)
- Authentication requests and responses
- JWT tokens
- Role management
"""

from pydantic import BaseModel, Field, EmailStr, field_validator
from typing import List, Optional
from datetime import datetime
from uuid import UUID, uuid4
from enum import Enum


class Role(str, Enum):
    """Available roles in the system."""
    ADMIN = "admin"
    OPERATOR = "operator"
    ANALYST = "analyst"
    VIEWER = "viewer"


class Permission(str, Enum):
    """Available permissions in the system."""

    # Read permissions
    READ_MAPPINGS = "read:mappings"
    READ_METRICS = "read:metrics"
    READ_CHECKPOINTS = "read:checkpoints"
    READ_AUDIT_LOGS = "read:audit_logs"
    READ_USERS = "read:users"

    # Write permissions
    CREATE_MAPPINGS = "create:mappings"
    UPDATE_MAPPINGS = "update:mappings"
    DELETE_MAPPINGS = "delete:mappings"

    # Operational permissions
    TRIGGER_SYNC = "trigger:sync"
    PAUSE_PIPELINE = "pause:pipeline"
    RESUME_PIPELINE = "resume:pipeline"

    # Administrative permissions
    MANAGE_USERS = "manage:users"
    MANAGE_ROLES = "manage:roles"
    MANAGE_SYSTEM = "manage:system"
    VIEW_SENSITIVE_DATA = "view:sensitive_data"


# ============================================================================
# REQUEST MODELS
# ============================================================================


class LoginRequest(BaseModel):
    """Login request schema."""
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    password: str = Field(..., min_length=8, description="Password")

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "admin",
                "password": "SecurePassword123!"
            }
        }
    }


class CreateUserRequest(BaseModel):
    """Create user request schema."""
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    email: EmailStr = Field(..., description="Email address")
    password: str = Field(..., min_length=8, description="Password")
    roles: List[str] = Field(default=["analyst"], min_length=1, description="User roles")

    @field_validator("roles")
    @classmethod
    def validate_roles(cls, v: List[str]) -> List[str]:
        """Validate roles are valid."""
        valid_roles = {role.value for role in Role}
        for role in v:
            if role not in valid_roles:
                raise ValueError(f"Invalid role: {role}. Must be one of {valid_roles}")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "analyst1",
                "email": "analyst1@example.com",
                "password": "SecurePassword123!",
                "roles": ["analyst"]
            }
        }
    }


class UpdateUserRequest(BaseModel):
    """Update user request schema."""
    email: Optional[EmailStr] = Field(None, description="Email address")
    password: Optional[str] = Field(None, min_length=8, description="New password")
    roles: Optional[List[str]] = Field(None, min_length=1, description="User roles")
    is_active: Optional[bool] = Field(None, description="Active status")

    @field_validator("roles")
    @classmethod
    def validate_roles(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate roles are valid."""
        if v is None:
            return v
        valid_roles = {role.value for role in Role}
        for role in v:
            if role not in valid_roles:
                raise ValueError(f"Invalid role: {role}. Must be one of {valid_roles}")
        return v

    model_config = {
        "json_schema_extra": {
            "example": {
                "email": "newemail@example.com",
                "roles": ["analyst", "operator"],
                "is_active": True
            }
        }
    }


# ============================================================================
# RESPONSE MODELS
# ============================================================================


class TokenResponse(BaseModel):
    """JWT token response schema."""
    access_token: str = Field(..., min_length=10, description="JWT access token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., gt=0, description="Token expiration time in seconds")

    model_config = {
        "json_schema_extra": {
            "example": {
                "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
                "token_type": "bearer",
                "expires_in": 3600
            }
        }
    }


class UserResponse(BaseModel):
    """User information response schema."""
    id: str = Field(..., min_length=1, description="User ID")
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    email: str = Field(..., description="Email address")
    roles: List[str] = Field(..., min_length=1, description="User roles")
    is_active: bool = Field(..., description="Active status")
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")

    model_config = {
        "json_schema_extra": {
            "example": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "username": "analyst1",
                "email": "analyst1@example.com",
                "roles": ["analyst"],
                "is_active": True,
                "created_at": "2025-01-15T10:30:00Z",
                "updated_at": "2025-01-15T10:30:00Z"
            }
        }
    }


class ErrorResponse(BaseModel):
    """Error response schema."""
    detail: str = Field(..., min_length=1, description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")

    model_config = {
        "json_schema_extra": {
            "example": {
                "detail": "Invalid credentials",
                "error_code": "AUTH_001"
            }
        }
    }


class ValidationErrorResponse(BaseModel):
    """Validation error response schema."""
    detail: List[dict] = Field(..., min_length=1, description="Validation errors")

    model_config = {
        "json_schema_extra": {
            "example": {
                "detail": [
                    {
                        "loc": ["body", "username"],
                        "msg": "field required",
                        "type": "value_error.missing"
                    }
                ]
            }
        }
    }


# ============================================================================
# DATABASE MODELS
# ============================================================================


class UserDB(BaseModel):
    """User database model."""
    id: UUID = Field(default_factory=uuid4, description="User ID")
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    email: str = Field(..., description="Email address")
    password_hash: str = Field(..., description="Hashed password")
    is_active: bool = Field(default=True, description="Active status")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")

    model_config = {
        "from_attributes": True
    }

    def to_response(self, roles: List[str]) -> UserResponse:
        """Convert to API response model."""
        return UserResponse(
            id=str(self.id),
            username=self.username,
            email=self.email,
            roles=roles,
            is_active=self.is_active,
            created_at=self.created_at,
            updated_at=self.updated_at
        )


class TokenPayload(BaseModel):
    """JWT token payload."""
    sub: str = Field(..., description="Subject (user ID)")
    username: str = Field(..., description="Username")
    roles: List[str] = Field(..., description="User roles")
    exp: int = Field(..., description="Expiration timestamp")
    iat: int = Field(..., description="Issued at timestamp")

    model_config = {
        "json_schema_extra": {
            "example": {
                "sub": "550e8400-e29b-41d4-a716-446655440000",
                "username": "admin",
                "roles": ["admin"],
                "exp": 1706270400,
                "iat": 1706266800
            }
        }
    }


class CurrentUser(BaseModel):
    """Current authenticated user model."""
    id: UUID = Field(..., description="User ID")
    username: str = Field(..., description="Username")
    email: str = Field(..., description="Email address")
    roles: List[str] = Field(..., description="User roles")
    is_active: bool = Field(..., description="Active status")

    model_config = {
        "from_attributes": True
    }

    def has_role(self, role: Role) -> bool:
        """Check if user has a specific role."""
        return role.value in self.roles

    def has_any_role(self, roles: List[Role]) -> bool:
        """Check if user has any of the specified roles."""
        return any(role.value in self.roles for role in roles)

    def is_admin(self) -> bool:
        """Check if user has admin role."""
        return self.has_role(Role.ADMIN)
