"""
Authentication and user management models.

Provides both SQLAlchemy ORM models and Pydantic schemas for:
- User entities (database and API)
- Authentication requests and responses
- JWT tokens and payloads
- Role management
- Password validation

Uses SQLAlchemy 2.0 declarative syntax with async compatibility.
"""

from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID, uuid4
from enum import Enum
import re

from sqlalchemy import String, Boolean, DateTime, Table, Column, ForeignKey, Index, text
from sqlalchemy.dialects.postgresql import UUID as PGUUID, ARRAY
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from pydantic import BaseModel, Field, EmailStr, field_validator


# ============================================================================
# SQLAlchemy Base
# ============================================================================


class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy models.

    Provides common functionality for all database models including
    timezone-aware timestamps and UUID primary keys.
    """
    pass


# ============================================================================
# Role Enum
# ============================================================================


class Role(str, Enum):
    """
    User roles with hierarchical permissions.

    - ADMIN: Full system access, user management
    - OPERATOR: Pipeline operations, mapping management
    - ANALYST: Read-only access to metrics and data
    """
    ADMIN = "admin"
    OPERATOR = "operator"
    ANALYST = "analyst"


class Permission(str, Enum):
    """Granular permissions for fine-grained access control."""

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
# SQLAlchemy Models
# ============================================================================


# Association table for many-to-many relationship between users and roles
user_roles = Table(
    "user_roles",
    Base.metadata,
    Column(
        "user_id",
        PGUUID(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False
    ),
    Column(
        "role_id",
        PGUUID(as_uuid=True),
        ForeignKey("roles.id", ondelete="CASCADE"),
        primary_key=True,
        nullable=False
    ),
    Column(
        "assigned_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    ),
    Index("idx_user_roles_user_id", "user_id"),
    Index("idx_user_roles_role_id", "role_id"),
)


class User(Base):
    """
    User account model.

    Stores user authentication and profile information.
    Uses UUID primary keys and bcrypt password hashing.
    """
    __tablename__ = "users"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    username: Mapped[str] = mapped_column(
        String(50),
        unique=True,
        nullable=False,
        index=True
    )
    email: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        index=True
    )
    password_hash: Mapped[str] = mapped_column(
        String(255),
        nullable=False
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        default=True,
        nullable=False,
        index=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )
    updated_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
        onupdate=lambda: datetime.now(timezone.utc)
    )

    # Relationships
    roles: Mapped[List["RoleModel"]] = relationship(
        "RoleModel",
        secondary=user_roles,
        back_populates="users",
        lazy="selectin"
    )

    # Indexes
    __table_args__ = (
        Index("idx_users_username", "username"),
        Index("idx_users_email", "email"),
        Index("idx_users_is_active", "is_active"),
        Index("idx_users_created_at", "created_at"),
    )

    def __repr__(self) -> str:
        """String representation."""
        return f"<User(id={self.id}, username='{self.username}', email='{self.email}')>"


class RoleModel(Base):
    """
    Role model for role-based access control.

    Defines available roles and their descriptions.
    Users can have multiple roles through the user_roles association table.
    """
    __tablename__ = "roles"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
        nullable=False
    )
    name: Mapped[str] = mapped_column(
        String(50),
        unique=True,
        nullable=False,
        index=True
    )
    description: Mapped[Optional[str]] = mapped_column(
        String(255),
        nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text("CURRENT_TIMESTAMP")
    )

    # Relationships
    users: Mapped[List[User]] = relationship(
        "User",
        secondary=user_roles,
        back_populates="roles",
        lazy="selectin"
    )

    # Indexes
    __table_args__ = (
        Index("idx_roles_name", "name"),
    )

    def __repr__(self) -> str:
        """String representation."""
        return f"<RoleModel(id={self.id}, name='{self.name}')>"


# ============================================================================
# Pydantic Request Models
# ============================================================================


class LoginRequest(BaseModel):
    """Login request schema."""
    username: str = Field(
        ...,
        min_length=3,
        max_length=50,
        description="Username"
    )
    password: str = Field(
        ...,
        min_length=8,
        description="Password"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "username": "admin",
                "password": "SecurePassword123!"
            }
        }
    }


class CreateUserRequest(BaseModel):
    """Create user request schema with password validation."""
    username: str = Field(
        ...,
        min_length=3,
        max_length=50,
        description="Username (3-50 characters)"
    )
    email: EmailStr = Field(
        ...,
        description="Email address"
    )
    password: str = Field(
        ...,
        min_length=8,
        max_length=128,
        description="Password (minimum 8 characters)"
    )
    roles: List[str] = Field(
        default=["analyst"],
        min_length=1,
        description="User roles"
    )

    @field_validator("username")
    @classmethod
    def validate_username(cls, v: str) -> str:
        """Validate username format."""
        if not re.match(r"^[a-zA-Z0-9_-]+$", v):
            raise ValueError(
                "Username must contain only letters, numbers, hyphens, and underscores"
            )
        return v

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: str) -> str:
        """
        Validate password complexity.

        Requirements:
        - Minimum 8 characters
        - At least one uppercase letter
        - At least one lowercase letter
        - At least one digit
        - At least one special character
        """
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters long")

        if not re.search(r"[A-Z]", v):
            raise ValueError("Password must contain at least one uppercase letter")

        if not re.search(r"[a-z]", v):
            raise ValueError("Password must contain at least one lowercase letter")

        if not re.search(r"\d", v):
            raise ValueError("Password must contain at least one digit")

        if not re.search(r"[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?]", v):
            raise ValueError("Password must contain at least one special character")

        return v

    @field_validator("roles")
    @classmethod
    def validate_roles(cls, v: List[str]) -> List[str]:
        """Validate roles are valid."""
        valid_roles = {role.value for role in Role}
        for role in v:
            if role not in valid_roles:
                raise ValueError(
                    f"Invalid role: {role}. Must be one of {valid_roles}"
                )
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
    """Update user request schema with optional fields."""
    email: Optional[EmailStr] = Field(
        None,
        description="Email address"
    )
    password: Optional[str] = Field(
        None,
        min_length=8,
        max_length=128,
        description="New password"
    )
    roles: Optional[List[str]] = Field(
        None,
        min_length=1,
        description="User roles"
    )
    is_active: Optional[bool] = Field(
        None,
        description="Active status"
    )

    @field_validator("password")
    @classmethod
    def validate_password(cls, v: Optional[str]) -> Optional[str]:
        """Validate password complexity if provided."""
        if v is None:
            return v

        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters long")

        if not re.search(r"[A-Z]", v):
            raise ValueError("Password must contain at least one uppercase letter")

        if not re.search(r"[a-z]", v):
            raise ValueError("Password must contain at least one lowercase letter")

        if not re.search(r"\d", v):
            raise ValueError("Password must contain at least one digit")

        if not re.search(r"[!@#$%^&*()_+\-=\[\]{};':\"\\|,.<>\/?]", v):
            raise ValueError("Password must contain at least one special character")

        return v

    @field_validator("roles")
    @classmethod
    def validate_roles(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate roles are valid if provided."""
        if v is None:
            return v

        valid_roles = {role.value for role in Role}
        for role in v:
            if role not in valid_roles:
                raise ValueError(
                    f"Invalid role: {role}. Must be one of {valid_roles}"
                )
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
# Pydantic Response Models
# ============================================================================


class TokenResponse(BaseModel):
    """JWT token response schema."""
    access_token: str = Field(
        ...,
        min_length=10,
        description="JWT access token"
    )
    token_type: str = Field(
        default="bearer",
        description="Token type"
    )
    expires_in: int = Field(
        ...,
        gt=0,
        description="Token expiration time in seconds"
    )

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
    id: str = Field(
        ...,
        description="User ID (UUID)"
    )
    username: str = Field(
        ...,
        min_length=3,
        max_length=50,
        description="Username"
    )
    email: str = Field(
        ...,
        description="Email address"
    )
    roles: List[str] = Field(
        ...,
        min_length=1,
        description="User roles"
    )
    is_active: bool = Field(
        ...,
        description="Active status"
    )
    created_at: datetime = Field(
        ...,
        description="Creation timestamp"
    )
    updated_at: Optional[datetime] = Field(
        None,
        description="Last update timestamp"
    )

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
    detail: str = Field(
        ...,
        min_length=1,
        description="Error message"
    )
    error_code: Optional[str] = Field(
        None,
        description="Error code"
    )

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
    detail: List[dict] = Field(
        ...,
        min_length=1,
        description="Validation errors"
    )

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
# Token Models
# ============================================================================


class TokenData(BaseModel):
    """
    JWT token payload/claims.

    Contains user identity and authorization information
    embedded in the JWT token.
    """
    sub: str = Field(
        ...,
        description="Subject (user ID)"
    )
    username: str = Field(
        ...,
        description="Username"
    )
    roles: List[str] = Field(
        ...,
        description="User roles"
    )
    exp: int = Field(
        ...,
        description="Expiration timestamp (Unix epoch)"
    )
    iat: int = Field(
        ...,
        description="Issued at timestamp (Unix epoch)"
    )
    iss: Optional[str] = Field(
        None,
        description="Issuer"
    )
    aud: Optional[str] = Field(
        None,
        description="Audience"
    )

    model_config = {
        "json_schema_extra": {
            "example": {
                "sub": "550e8400-e29b-41d4-a716-446655440000",
                "username": "admin",
                "roles": ["admin"],
                "exp": 1706270400,
                "iat": 1706266800,
                "iss": "cdc-pipeline-api",
                "aud": "cdc-pipeline-users"
            }
        }
    }


class CurrentUser(BaseModel):
    """
    Current authenticated user model.

    Used in request handlers to represent the authenticated user
    making the request. Injected via dependency injection.
    """
    id: UUID = Field(
        ...,
        description="User ID"
    )
    username: str = Field(
        ...,
        description="Username"
    )
    email: str = Field(
        ...,
        description="Email address"
    )
    roles: List[str] = Field(
        ...,
        description="User roles"
    )
    is_active: bool = Field(
        ...,
        description="Active status"
    )

    model_config = {
        "from_attributes": True
    }

    def has_role(self, role: Role) -> bool:
        """
        Check if user has a specific role.

        Args:
            role: Role to check

        Returns:
            True if user has the role, False otherwise
        """
        return role.value in self.roles

    def has_any_role(self, roles: List[Role]) -> bool:
        """
        Check if user has any of the specified roles.

        Args:
            roles: List of roles to check

        Returns:
            True if user has any of the roles, False otherwise
        """
        return any(role.value in self.roles for role in roles)

    def is_admin(self) -> bool:
        """
        Check if user has admin role.

        Returns:
            True if user is admin, False otherwise
        """
        return self.has_role(Role.ADMIN)

    def is_operator(self) -> bool:
        """
        Check if user has operator role.

        Returns:
            True if user is operator, False otherwise
        """
        return self.has_role(Role.OPERATOR)

    def is_analyst(self) -> bool:
        """
        Check if user has analyst role.

        Returns:
            True if user is analyst, False otherwise
        """
        return self.has_role(Role.ANALYST)
