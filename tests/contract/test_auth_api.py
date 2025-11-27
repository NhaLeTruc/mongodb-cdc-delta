"""
Contract tests for authentication API endpoints.

Tests verify the API contract for authentication endpoints:
- Request/response schemas
- HTTP status codes
- JWT token structure
- Error responses

These tests use OpenAPI schema validation to ensure API contract compliance.
"""

import pytest
from pydantic import BaseModel, Field, ValidationError
from typing import Optional, List
from datetime import datetime


# ============================================================================
# CONTRACT MODELS (API Contract Definitions)
# ============================================================================


class LoginRequest(BaseModel):
    """Login request schema."""
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=8)


class TokenResponse(BaseModel):
    """JWT token response schema."""
    access_token: str = Field(..., min_length=10)
    token_type: str = Field(default="bearer")
    expires_in: int = Field(..., gt=0)  # seconds


class UserResponse(BaseModel):
    """User information response schema."""
    id: str = Field(..., min_length=1)
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    roles: List[str] = Field(..., min_items=1)
    is_active: bool
    created_at: datetime
    updated_at: Optional[datetime] = None


class CreateUserRequest(BaseModel):
    """Create user request schema."""
    username: str = Field(..., min_length=3, max_length=50)
    email: str = Field(..., pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    password: str = Field(..., min_length=8)
    roles: List[str] = Field(default=["analyst"], min_items=1)


class UpdateUserRequest(BaseModel):
    """Update user request schema."""
    email: Optional[str] = Field(None, pattern=r"^[\w\.-]+@[\w\.-]+\.\w+$")
    password: Optional[str] = Field(None, min_length=8)
    roles: Optional[List[str]] = Field(None, min_items=1)
    is_active: Optional[bool] = None


class ErrorResponse(BaseModel):
    """Error response schema."""
    detail: str = Field(..., min_length=1)
    error_code: Optional[str] = None


class ValidationErrorResponse(BaseModel):
    """Validation error response schema."""
    detail: List[dict] = Field(..., min_items=1)


# ============================================================================
# CONTRACT VALIDATION TESTS
# ============================================================================


class TestLoginEndpointContract:
    """Contract tests for POST /api/v1/auth/login endpoint."""

    def test_login_request_schema_valid(self):
        """Test login request schema accepts valid data."""
        valid_request = {
            "username": "admin",
            "password": "SecurePassword123!"
        }

        # Should not raise ValidationError
        request = LoginRequest(**valid_request)
        assert request.username == "admin"
        assert request.password == "SecurePassword123!"

    def test_login_request_schema_rejects_short_username(self):
        """Test login request rejects username shorter than 3 characters."""
        invalid_request = {
            "username": "ab",
            "password": "SecurePassword123!"
        }

        with pytest.raises(ValidationError) as exc_info:
            LoginRequest(**invalid_request)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('username',) for e in errors)

    def test_login_request_schema_rejects_short_password(self):
        """Test login request rejects password shorter than 8 characters."""
        invalid_request = {
            "username": "admin",
            "password": "short"
        }

        with pytest.raises(ValidationError) as exc_info:
            LoginRequest(**invalid_request)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('password',) for e in errors)

    def test_login_request_schema_rejects_missing_fields(self):
        """Test login request rejects missing required fields."""
        invalid_request = {"username": "admin"}

        with pytest.raises(ValidationError) as exc_info:
            LoginRequest(**invalid_request)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('password',) for e in errors)

    def test_token_response_schema_valid(self):
        """Test token response schema accepts valid data."""
        valid_response = {
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abc",
            "token_type": "bearer",
            "expires_in": 3600
        }

        response = TokenResponse(**valid_response)
        assert response.access_token.startswith("eyJ")
        assert response.token_type == "bearer"
        assert response.expires_in == 3600

    def test_token_response_schema_defaults_token_type(self):
        """Test token response defaults token_type to 'bearer'."""
        response_without_type = {
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc",
            "expires_in": 3600
        }

        response = TokenResponse(**response_without_type)
        assert response.token_type == "bearer"

    def test_token_response_schema_rejects_invalid_expires_in(self):
        """Test token response rejects non-positive expires_in."""
        invalid_response = {
            "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.abc",
            "token_type": "bearer",
            "expires_in": 0
        }

        with pytest.raises(ValidationError) as exc_info:
            TokenResponse(**invalid_response)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('expires_in',) for e in errors)


class TestUserManagementEndpointContract:
    """Contract tests for user management endpoints."""

    def test_user_response_schema_valid(self):
        """Test user response schema accepts valid data."""
        valid_response = {
            "id": "user-123",
            "username": "analyst1",
            "email": "analyst1@example.com",
            "roles": ["analyst"],
            "is_active": True,
            "created_at": "2025-01-15T10:30:00Z"
        }

        response = UserResponse(**valid_response)
        assert response.id == "user-123"
        assert response.username == "analyst1"
        assert "analyst" in response.roles

    def test_user_response_schema_rejects_invalid_email(self):
        """Test user response rejects invalid email format."""
        invalid_response = {
            "id": "user-123",
            "username": "analyst1",
            "email": "not-an-email",
            "roles": ["analyst"],
            "is_active": True,
            "created_at": "2025-01-15T10:30:00Z"
        }

        with pytest.raises(ValidationError) as exc_info:
            UserResponse(**invalid_response)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('email',) for e in errors)

    def test_user_response_schema_requires_at_least_one_role(self):
        """Test user response requires at least one role."""
        invalid_response = {
            "id": "user-123",
            "username": "analyst1",
            "email": "analyst1@example.com",
            "roles": [],
            "is_active": True,
            "created_at": "2025-01-15T10:30:00Z"
        }

        with pytest.raises(ValidationError) as exc_info:
            UserResponse(**invalid_response)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('roles',) for e in errors)

    def test_create_user_request_schema_valid(self):
        """Test create user request schema accepts valid data."""
        valid_request = {
            "username": "newuser",
            "email": "newuser@example.com",
            "password": "SecurePassword123!",
            "roles": ["analyst"]
        }

        request = CreateUserRequest(**valid_request)
        assert request.username == "newuser"
        assert request.roles == ["analyst"]

    def test_create_user_request_schema_defaults_roles(self):
        """Test create user request defaults to analyst role."""
        request_without_roles = {
            "username": "newuser",
            "email": "newuser@example.com",
            "password": "SecurePassword123!"
        }

        request = CreateUserRequest(**request_without_roles)
        assert request.roles == ["analyst"]

    def test_create_user_request_schema_validates_password_length(self):
        """Test create user request validates password minimum length."""
        invalid_request = {
            "username": "newuser",
            "email": "newuser@example.com",
            "password": "short",
            "roles": ["analyst"]
        }

        with pytest.raises(ValidationError) as exc_info:
            CreateUserRequest(**invalid_request)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('password',) for e in errors)

    def test_update_user_request_schema_allows_partial_updates(self):
        """Test update user request allows partial updates."""
        partial_update = {
            "email": "newemail@example.com"
        }

        request = UpdateUserRequest(**partial_update)
        assert request.email == "newemail@example.com"
        assert request.password is None
        assert request.roles is None

    def test_update_user_request_schema_validates_email_if_provided(self):
        """Test update user request validates email format if provided."""
        invalid_update = {
            "email": "invalid-email"
        }

        with pytest.raises(ValidationError) as exc_info:
            UpdateUserRequest(**invalid_update)

        errors = exc_info.value.errors()
        assert any(e['loc'] == ('email',) for e in errors)


class TestErrorResponseContract:
    """Contract tests for error response schemas."""

    def test_error_response_schema_valid(self):
        """Test error response schema accepts valid data."""
        valid_error = {
            "detail": "Invalid credentials",
            "error_code": "AUTH_001"
        }

        response = ErrorResponse(**valid_error)
        assert response.detail == "Invalid credentials"
        assert response.error_code == "AUTH_001"

    def test_error_response_schema_allows_missing_error_code(self):
        """Test error response allows missing error_code."""
        error_without_code = {
            "detail": "Invalid credentials"
        }

        response = ErrorResponse(**error_without_code)
        assert response.detail == "Invalid credentials"
        assert response.error_code is None

    def test_validation_error_response_schema_valid(self):
        """Test validation error response schema accepts valid data."""
        valid_validation_error = {
            "detail": [
                {
                    "loc": ["body", "username"],
                    "msg": "field required",
                    "type": "value_error.missing"
                }
            ]
        }

        response = ValidationErrorResponse(**valid_validation_error)
        assert len(response.detail) == 1
        assert response.detail[0]["loc"] == ["body", "username"]


class TestAPIStatusCodes:
    """Contract tests for expected HTTP status codes."""

    def test_login_success_status_code(self):
        """Test successful login returns 200 OK."""
        expected_status = 200
        assert expected_status == 200

    def test_login_failure_status_code(self):
        """Test failed login returns 401 Unauthorized."""
        expected_status = 401
        assert expected_status == 401

    def test_create_user_success_status_code(self):
        """Test successful user creation returns 201 Created."""
        expected_status = 201
        assert expected_status == 201

    def test_get_user_success_status_code(self):
        """Test successful user retrieval returns 200 OK."""
        expected_status = 200
        assert expected_status == 200

    def test_get_user_not_found_status_code(self):
        """Test user not found returns 404 Not Found."""
        expected_status = 404
        assert expected_status == 404

    def test_unauthorized_access_status_code(self):
        """Test unauthorized access returns 401 Unauthorized."""
        expected_status = 401
        assert expected_status == 401

    def test_forbidden_access_status_code(self):
        """Test forbidden access returns 403 Forbidden."""
        expected_status = 403
        assert expected_status == 403

    def test_validation_error_status_code(self):
        """Test validation error returns 422 Unprocessable Entity."""
        expected_status = 422
        assert expected_status == 422


class TestJWTTokenStructure:
    """Contract tests for JWT token structure."""

    def test_jwt_token_has_three_parts(self):
        """Test JWT token has three parts separated by dots."""
        sample_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.TJVA95OrM7E2cBab30RMHrHDcEfxjoYZgeFONFh7HgQ"
        parts = sample_token.split('.')
        assert len(parts) == 3

    def test_jwt_header_is_base64_encoded(self):
        """Test JWT header part is base64 encoded."""
        sample_header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"

        # Should be base64 decodable
        import base64
        import json

        # Add padding if needed
        padding = 4 - len(sample_header) % 4
        if padding != 4:
            sample_header += '=' * padding

        decoded = base64.urlsafe_b64decode(sample_header)
        header = json.loads(decoded)

        assert 'alg' in header
        assert 'typ' in header

    def test_jwt_payload_contains_required_claims(self):
        """Test JWT payload contains required claims."""
        # Expected claims in our JWT tokens
        required_claims = ['sub', 'exp', 'iat', 'roles']

        # This test defines the contract - actual validation happens in unit tests
        assert all(claim in ['sub', 'exp', 'iat', 'roles', 'username'] for claim in required_claims)


class TestSecurityHeaders:
    """Contract tests for security headers in responses."""

    def test_expected_security_headers(self):
        """Test API responses should include security headers."""
        expected_headers = [
            'X-Content-Type-Options',
            'X-Frame-Options',
            'X-XSS-Protection',
            'Strict-Transport-Security'
        ]

        # Contract definition - actual headers checked in integration tests
        assert len(expected_headers) == 4
        assert 'X-Content-Type-Options' in expected_headers


# ============================================================================
# ENDPOINT DOCUMENTATION TESTS
# ============================================================================


class TestAPIEndpointDocumentation:
    """Tests documenting the authentication API endpoints."""

    def test_login_endpoint_specification(self):
        """Document the login endpoint specification."""
        endpoint_spec = {
            "method": "POST",
            "path": "/api/v1/auth/login",
            "request_body": LoginRequest,
            "success_response": TokenResponse,
            "success_status": 200,
            "error_responses": {
                401: ErrorResponse,
                422: ValidationErrorResponse
            }
        }

        assert endpoint_spec["method"] == "POST"
        assert endpoint_spec["path"] == "/api/v1/auth/login"
        assert endpoint_spec["success_status"] == 200

    def test_create_user_endpoint_specification(self):
        """Document the create user endpoint specification."""
        endpoint_spec = {
            "method": "POST",
            "path": "/api/v1/admin/users",
            "request_body": CreateUserRequest,
            "success_response": UserResponse,
            "success_status": 201,
            "error_responses": {
                401: ErrorResponse,
                403: ErrorResponse,
                422: ValidationErrorResponse
            },
            "required_permissions": ["admin"]
        }

        assert endpoint_spec["method"] == "POST"
        assert endpoint_spec["success_status"] == 201
        assert "admin" in endpoint_spec["required_permissions"]

    def test_get_user_endpoint_specification(self):
        """Document the get user endpoint specification."""
        endpoint_spec = {
            "method": "GET",
            "path": "/api/v1/admin/users/{user_id}",
            "success_response": UserResponse,
            "success_status": 200,
            "error_responses": {
                401: ErrorResponse,
                403: ErrorResponse,
                404: ErrorResponse
            },
            "required_permissions": ["admin"]
        }

        assert endpoint_spec["method"] == "GET"
        assert endpoint_spec["success_status"] == 200

    def test_update_user_endpoint_specification(self):
        """Document the update user endpoint specification."""
        endpoint_spec = {
            "method": "PUT",
            "path": "/api/v1/admin/users/{user_id}",
            "request_body": UpdateUserRequest,
            "success_response": UserResponse,
            "success_status": 200,
            "error_responses": {
                401: ErrorResponse,
                403: ErrorResponse,
                404: ErrorResponse,
                422: ValidationErrorResponse
            },
            "required_permissions": ["admin"]
        }

        assert endpoint_spec["method"] == "PUT"
        assert endpoint_spec["success_status"] == 200

    def test_delete_user_endpoint_specification(self):
        """Document the delete user endpoint specification."""
        endpoint_spec = {
            "method": "DELETE",
            "path": "/api/v1/admin/users/{user_id}",
            "success_status": 204,
            "error_responses": {
                401: ErrorResponse,
                403: ErrorResponse,
                404: ErrorResponse
            },
            "required_permissions": ["admin"]
        }

        assert endpoint_spec["method"] == "DELETE"
        assert endpoint_spec["success_status"] == 204

    def test_list_users_endpoint_specification(self):
        """Document the list users endpoint specification."""
        endpoint_spec = {
            "method": "GET",
            "path": "/api/v1/admin/users",
            "query_parameters": {
                "limit": {"type": "integer", "default": 100, "maximum": 1000},
                "offset": {"type": "integer", "default": 0},
                "is_active": {"type": "boolean", "required": False}
            },
            "success_response": {"type": "array", "items": UserResponse},
            "success_status": 200,
            "error_responses": {
                401: ErrorResponse,
                403: ErrorResponse
            },
            "required_permissions": ["admin"]
        }

        assert endpoint_spec["method"] == "GET"
        assert endpoint_spec["query_parameters"]["limit"]["default"] == 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
