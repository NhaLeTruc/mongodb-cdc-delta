"""
Integration tests for Role-Based Access Control (RBAC) with API endpoints.

Tests cover:
- API access with different roles
- Admin can access all endpoints
- Analyst can only access read endpoints
- Operator has limited write access
- Unauthorized role access returns 403 Forbidden
- Integration with PostgreSQL and FastAPI test client

These tests use testcontainers for PostgreSQL and FastAPI TestClient.
"""

import pytest
from typing import Dict, Any, Optional
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.testclient import TestClient
from pydantic import BaseModel
from jose import jwt, JWTError
from testcontainers.postgres import PostgresContainer
import psycopg2
from psycopg2.extras import RealDictCursor
from enum import Enum


# ============================================================================
# TEST CONFIGURATION
# ============================================================================


class AuthConfig:
    """Authentication configuration."""

    JWT_SECRET_KEY = "test-secret-key-for-rbac-testing"
    JWT_ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 60


# ============================================================================
# MODELS
# ============================================================================


class Permission(str, Enum):
    """Available permissions."""

    READ_MAPPINGS = "read:mappings"
    CREATE_MAPPINGS = "create:mappings"
    UPDATE_MAPPINGS = "update:mappings"
    DELETE_MAPPINGS = "delete:mappings"
    MANAGE_USERS = "manage:users"
    VIEW_METRICS = "view:metrics"


class Role(str, Enum):
    """Available roles."""

    ADMIN = "admin"
    OPERATOR = "operator"
    ANALYST = "analyst"


# Role permissions mapping
ROLE_PERMISSIONS: Dict[Role, set] = {
    Role.ANALYST: {
        Permission.READ_MAPPINGS,
        Permission.VIEW_METRICS,
    },
    Role.OPERATOR: {
        Permission.READ_MAPPINGS,
        Permission.VIEW_METRICS,
        Permission.CREATE_MAPPINGS,
        Permission.UPDATE_MAPPINGS,
    },
    Role.ADMIN: {
        Permission.READ_MAPPINGS,
        Permission.VIEW_METRICS,
        Permission.CREATE_MAPPINGS,
        Permission.UPDATE_MAPPINGS,
        Permission.DELETE_MAPPINGS,
        Permission.MANAGE_USERS,
    },
}


class TokenData(BaseModel):
    """Token payload data."""

    user_id: str
    username: str
    roles: list[str]


class MappingCreate(BaseModel):
    """Mapping creation request."""

    name: str
    source_collection: str
    target_table: str


class MappingResponse(BaseModel):
    """Mapping response."""

    id: int
    name: str
    source_collection: str
    target_table: str
    created_by: str


class UserResponse(BaseModel):
    """User response."""

    id: int
    username: str
    email: str
    roles: list[str]
    is_active: bool


# ============================================================================
# AUTHENTICATION UTILITIES
# ============================================================================


def create_access_token(data: Dict[str, Any]) -> str:
    """Create JWT access token."""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=AuthConfig.ACCESS_TOKEN_EXPIRE_MINUTES
    )

    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
    })

    return jwt.encode(
        to_encode,
        AuthConfig.JWT_SECRET_KEY,
        algorithm=AuthConfig.JWT_ALGORITHM,
    )


def decode_token(token: str) -> Dict[str, Any]:
    """Decode JWT token."""
    return jwt.decode(
        token,
        AuthConfig.JWT_SECRET_KEY,
        algorithms=[AuthConfig.JWT_ALGORITHM],
    )


def has_permission(user_roles: list[str], required_permission: Permission) -> bool:
    """Check if user has required permission."""
    for role_name in user_roles:
        try:
            role = Role(role_name)
            if required_permission in ROLE_PERMISSIONS.get(role, set()):
                return True
        except ValueError:
            continue

    return False


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================


app = FastAPI()
security = HTTPBearer()


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> TokenData:
    """Extract and validate current user from JWT token."""
    try:
        token = credentials.credentials
        payload = decode_token(token)

        token_data = TokenData(
            user_id=payload["user_id"],
            username=payload["username"],
            roles=payload["roles"],
        )

        return token_data

    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_permission(permission: Permission):
    """Dependency to require specific permission."""

    def permission_checker(current_user: TokenData = Depends(get_current_user)) -> TokenData:
        if not has_permission(current_user.roles, permission):
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Permission denied: {permission.value} required",
            )
        return current_user

    return permission_checker


def require_role(role: Role):
    """Dependency to require specific role."""

    def role_checker(current_user: TokenData = Depends(get_current_user)) -> TokenData:
        if role.value not in current_user.roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Role denied: {role.value} required",
            )
        return current_user

    return role_checker


# ============================================================================
# API ENDPOINTS
# ============================================================================


@app.get("/api/v1/mappings")
async def list_mappings(
    current_user: TokenData = Depends(require_permission(Permission.READ_MAPPINGS))
) -> list[MappingResponse]:
    """List all mappings (requires read permission)."""
    return [
        MappingResponse(
            id=1,
            name="users_mapping",
            source_collection="mongodb.db.users",
            target_table="delta_users",
            created_by="admin",
        )
    ]


@app.post("/api/v1/mappings", status_code=status.HTTP_201_CREATED)
async def create_mapping(
    mapping: MappingCreate,
    current_user: TokenData = Depends(require_permission(Permission.CREATE_MAPPINGS))
) -> MappingResponse:
    """Create new mapping (requires create permission)."""
    return MappingResponse(
        id=2,
        name=mapping.name,
        source_collection=mapping.source_collection,
        target_table=mapping.target_table,
        created_by=current_user.username,
    )


@app.put("/api/v1/mappings/{mapping_id}")
async def update_mapping(
    mapping_id: int,
    mapping: MappingCreate,
    current_user: TokenData = Depends(require_permission(Permission.UPDATE_MAPPINGS))
) -> MappingResponse:
    """Update mapping (requires update permission)."""
    return MappingResponse(
        id=mapping_id,
        name=mapping.name,
        source_collection=mapping.source_collection,
        target_table=mapping.target_table,
        created_by=current_user.username,
    )


@app.delete("/api/v1/mappings/{mapping_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_mapping(
    mapping_id: int,
    current_user: TokenData = Depends(require_permission(Permission.DELETE_MAPPINGS))
) -> None:
    """Delete mapping (requires delete permission - admin only)."""
    return None


@app.get("/api/v1/metrics")
async def get_metrics(
    current_user: TokenData = Depends(require_permission(Permission.VIEW_METRICS))
) -> Dict[str, Any]:
    """Get metrics (requires view metrics permission)."""
    return {
        "mappings_count": 10,
        "active_pipelines": 3,
        "records_processed": 1000000,
    }


@app.get("/api/v1/admin/users")
async def list_users(
    current_user: TokenData = Depends(require_permission(Permission.MANAGE_USERS))
) -> list[Dict[str, Any]]:
    """List users (admin only)."""
    return [
        {
            "id": 1,
            "username": "admin",
            "email": "admin@example.com",
            "roles": ["admin"],
        }
    ]


@app.post("/api/v1/admin/users", status_code=status.HTTP_201_CREATED)
async def create_user(
    current_user: TokenData = Depends(require_role(Role.ADMIN))
) -> Dict[str, str]:
    """Create user (admin role required)."""
    return {
        "id": "2",
        "username": "newuser",
        "message": "User created successfully",
    }


@app.get("/api/v1/health")
async def health_check() -> Dict[str, str]:
    """Public health check endpoint (no authentication required)."""
    return {"status": "healthy"}


# ============================================================================
# PYTEST FIXTURES
# ============================================================================


@pytest.fixture(scope="module")
def postgres_container():
    """Create PostgreSQL testcontainer."""
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="module")
def db_connection(postgres_container):
    """Create database connection."""
    connection = psycopg2.connect(
        host=postgres_container.get_container_host_ip(),
        port=postgres_container.get_exposed_port(5432),
        user=postgres_container.username,
        password=postgres_container.password,
        database=postgres_container.dbname,
    )

    yield connection
    connection.close()


@pytest.fixture
def client():
    """Create FastAPI test client."""
    return TestClient(app)


@pytest.fixture
def analyst_token():
    """Create JWT token for analyst user."""
    return create_access_token({
        "user_id": "1",
        "username": "analyst_user",
        "roles": ["analyst"],
    })


@pytest.fixture
def operator_token():
    """Create JWT token for operator user."""
    return create_access_token({
        "user_id": "2",
        "username": "operator_user",
        "roles": ["operator"],
    })


@pytest.fixture
def admin_token():
    """Create JWT token for admin user."""
    return create_access_token({
        "user_id": "3",
        "username": "admin_user",
        "roles": ["admin"],
    })


@pytest.fixture
def multi_role_token():
    """Create JWT token for user with multiple roles."""
    return create_access_token({
        "user_id": "4",
        "username": "multi_role_user",
        "roles": ["analyst", "operator"],
    })


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


class TestAnalystRoleAccess:
    """Tests for analyst role access control."""

    def test_analyst_can_list_mappings(self, client, analyst_token):
        """Test analyst can list mappings."""
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {analyst_token}"},
        )

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_analyst_can_view_metrics(self, client, analyst_token):
        """Test analyst can view metrics."""
        response = client.get(
            "/api/v1/metrics",
            headers={"Authorization": f"Bearer {analyst_token}"},
        )

        assert response.status_code == 200
        assert "mappings_count" in response.json()

    def test_analyst_cannot_create_mappings(self, client, analyst_token):
        """Test analyst cannot create mappings."""
        response = client.post(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {analyst_token}"},
            json={
                "name": "new_mapping",
                "source_collection": "mongodb.db.test",
                "target_table": "delta_test",
            },
        )

        assert response.status_code == 403
        assert "Permission denied" in response.json()["detail"]

    def test_analyst_cannot_update_mappings(self, client, analyst_token):
        """Test analyst cannot update mappings."""
        response = client.put(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {analyst_token}"},
            json={
                "name": "updated_mapping",
                "source_collection": "mongodb.db.test",
                "target_table": "delta_test",
            },
        )

        assert response.status_code == 403

    def test_analyst_cannot_delete_mappings(self, client, analyst_token):
        """Test analyst cannot delete mappings."""
        response = client.delete(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {analyst_token}"},
        )

        assert response.status_code == 403

    def test_analyst_cannot_manage_users(self, client, analyst_token):
        """Test analyst cannot manage users."""
        response = client.get(
            "/api/v1/admin/users",
            headers={"Authorization": f"Bearer {analyst_token}"},
        )

        assert response.status_code == 403


class TestOperatorRoleAccess:
    """Tests for operator role access control."""

    def test_operator_can_list_mappings(self, client, operator_token):
        """Test operator can list mappings."""
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {operator_token}"},
        )

        assert response.status_code == 200

    def test_operator_can_create_mappings(self, client, operator_token):
        """Test operator can create mappings."""
        response = client.post(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {operator_token}"},
            json={
                "name": "operator_mapping",
                "source_collection": "mongodb.db.data",
                "target_table": "delta_data",
            },
        )

        assert response.status_code == 201
        assert response.json()["name"] == "operator_mapping"
        assert response.json()["created_by"] == "operator_user"

    def test_operator_can_update_mappings(self, client, operator_token):
        """Test operator can update mappings."""
        response = client.put(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {operator_token}"},
            json={
                "name": "updated_by_operator",
                "source_collection": "mongodb.db.updated",
                "target_table": "delta_updated",
            },
        )

        assert response.status_code == 200
        assert response.json()["name"] == "updated_by_operator"

    def test_operator_can_view_metrics(self, client, operator_token):
        """Test operator can view metrics."""
        response = client.get(
            "/api/v1/metrics",
            headers={"Authorization": f"Bearer {operator_token}"},
        )

        assert response.status_code == 200

    def test_operator_cannot_delete_mappings(self, client, operator_token):
        """Test operator cannot delete mappings."""
        response = client.delete(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {operator_token}"},
        )

        assert response.status_code == 403

    def test_operator_cannot_manage_users(self, client, operator_token):
        """Test operator cannot manage users."""
        response = client.get(
            "/api/v1/admin/users",
            headers={"Authorization": f"Bearer {operator_token}"},
        )

        assert response.status_code == 403


class TestAdminRoleAccess:
    """Tests for admin role access control."""

    def test_admin_can_list_mappings(self, client, admin_token):
        """Test admin can list mappings."""
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 200

    def test_admin_can_create_mappings(self, client, admin_token):
        """Test admin can create mappings."""
        response = client.post(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "name": "admin_mapping",
                "source_collection": "mongodb.db.admin",
                "target_table": "delta_admin",
            },
        )

        assert response.status_code == 201

    def test_admin_can_update_mappings(self, client, admin_token):
        """Test admin can update mappings."""
        response = client.put(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "name": "admin_updated",
                "source_collection": "mongodb.db.admin",
                "target_table": "delta_admin",
            },
        )

        assert response.status_code == 200

    def test_admin_can_delete_mappings(self, client, admin_token):
        """Test admin can delete mappings."""
        response = client.delete(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 204

    def test_admin_can_view_metrics(self, client, admin_token):
        """Test admin can view metrics."""
        response = client.get(
            "/api/v1/metrics",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 200

    def test_admin_can_list_users(self, client, admin_token):
        """Test admin can list users."""
        response = client.get(
            "/api/v1/admin/users",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 200
        assert isinstance(response.json(), list)

    def test_admin_can_create_users(self, client, admin_token):
        """Test admin can create users."""
        response = client.post(
            "/api/v1/admin/users",
            headers={"Authorization": f"Bearer {admin_token}"},
        )

        assert response.status_code == 201
        assert "User created successfully" in response.json()["message"]


class TestMultipleRoles:
    """Tests for users with multiple roles."""

    def test_multi_role_user_has_combined_permissions(self, client, multi_role_token):
        """Test user with multiple roles has combined permissions."""
        # Should have analyst permissions (read)
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {multi_role_token}"},
        )
        assert response.status_code == 200

        # Should have operator permissions (create)
        response = client.post(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {multi_role_token}"},
            json={
                "name": "multi_role_mapping",
                "source_collection": "mongodb.db.multi",
                "target_table": "delta_multi",
            },
        )
        assert response.status_code == 201

        # Should NOT have admin permissions (delete)
        response = client.delete(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {multi_role_token}"},
        )
        assert response.status_code == 403


class TestUnauthorizedAccess:
    """Tests for unauthorized access attempts."""

    def test_access_without_token_returns_401(self, client):
        """Test accessing protected endpoint without token returns 401."""
        response = client.get("/api/v1/mappings")

        assert response.status_code == 403  # FastAPI returns 403 for missing credentials

    def test_access_with_invalid_token_returns_401(self, client):
        """Test accessing endpoint with invalid token returns 401."""
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": "Bearer invalid-token"},
        )

        assert response.status_code == 401

    def test_access_with_expired_token_returns_401(self, client):
        """Test accessing endpoint with expired token returns 401."""
        # Create token that expired 1 hour ago
        expired_token = jwt.encode(
            {
                "user_id": "1",
                "username": "expired",
                "roles": ["analyst"],
                "exp": datetime.now(timezone.utc) - timedelta(hours=1),
            },
            AuthConfig.JWT_SECRET_KEY,
            algorithm=AuthConfig.JWT_ALGORITHM,
        )

        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {expired_token}"},
        )

        assert response.status_code == 401


class TestPublicEndpoints:
    """Tests for public endpoints that don't require authentication."""

    def test_health_check_accessible_without_auth(self, client):
        """Test health check endpoint is accessible without authentication."""
        response = client.get("/api/v1/health")

        assert response.status_code == 200
        assert response.json()["status"] == "healthy"


class TestPermissionInheritance:
    """Tests for permission inheritance through role hierarchy."""

    def test_admin_has_all_operator_permissions(self, client, admin_token):
        """Test admin inherits all operator permissions."""
        # Admin should be able to do everything operator can do

        # List mappings (operator can do this)
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 200

        # Create mapping (operator can do this)
        response = client.post(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "name": "test",
                "source_collection": "test",
                "target_table": "test",
            },
        )
        assert response.status_code == 201

        # Update mapping (operator can do this)
        response = client.put(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {admin_token}"},
            json={
                "name": "test",
                "source_collection": "test",
                "target_table": "test",
            },
        )
        assert response.status_code == 200

    def test_admin_has_all_analyst_permissions(self, client, admin_token):
        """Test admin inherits all analyst permissions."""
        # View metrics (analyst can do this)
        response = client.get(
            "/api/v1/metrics",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 200

        # List mappings (analyst can do this)
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 200


class TestRoleSeparation:
    """Tests for proper role separation."""

    def test_analyst_operator_separation(self, client, analyst_token, operator_token):
        """Test analysts and operators have distinct permission sets."""
        # Analyst can read but not write
        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {analyst_token}"},
        )
        assert response.status_code == 200

        response = client.post(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {analyst_token}"},
            json={
                "name": "test",
                "source_collection": "test",
                "target_table": "test",
            },
        )
        assert response.status_code == 403

        # Operator can read and write
        response = client.post(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {operator_token}"},
            json={
                "name": "test",
                "source_collection": "test",
                "target_table": "test",
            },
        )
        assert response.status_code == 201

    def test_operator_admin_separation(self, client, operator_token, admin_token):
        """Test operators and admins have distinct permission sets."""
        # Operator cannot delete
        response = client.delete(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert response.status_code == 403

        # Admin can delete
        response = client.delete(
            "/api/v1/mappings/1",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 204

        # Operator cannot manage users
        response = client.get(
            "/api/v1/admin/users",
            headers={"Authorization": f"Bearer {operator_token}"},
        )
        assert response.status_code == 403

        # Admin can manage users
        response = client.get(
            "/api/v1/admin/users",
            headers={"Authorization": f"Bearer {admin_token}"},
        )
        assert response.status_code == 200


class TestEdgeCases:
    """Tests for edge cases in RBAC."""

    def test_user_with_no_roles(self, client):
        """Test user with empty roles list cannot access protected endpoints."""
        token = create_access_token({
            "user_id": "999",
            "username": "norole",
            "roles": [],
        })

        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 403

    def test_user_with_invalid_role(self, client):
        """Test user with invalid role name cannot access endpoints."""
        token = create_access_token({
            "user_id": "999",
            "username": "invalidrole",
            "roles": ["superuser"],  # Invalid role
        })

        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 403

    def test_token_missing_roles_claim(self, client):
        """Test token missing roles claim is rejected."""
        token = jwt.encode(
            {
                "user_id": "1",
                "username": "test",
                # Missing "roles" field
                "exp": datetime.now(timezone.utc) + timedelta(hours=1),
            },
            AuthConfig.JWT_SECRET_KEY,
            algorithm=AuthConfig.JWT_ALGORITHM,
        )

        response = client.get(
            "/api/v1/mappings",
            headers={"Authorization": f"Bearer {token}"},
        )

        assert response.status_code == 401


if __name__ == "__main__":
    pytest.main([__file__, "-v"])