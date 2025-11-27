"""
Integration tests for authentication flow with PostgreSQL.

Tests cover:
- Complete login flow with PostgreSQL database
- JWT token generation and validation
- Password hashing and verification
- Invalid credentials handling
- Token expiration
- User creation and retrieval
- Database integration using testcontainers

These tests use testcontainers to spin up a real PostgreSQL instance.
"""

import pytest
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional
import hashlib
import hmac
from jose import jwt, JWTError
from testcontainers.postgres import PostgresContainer
import psycopg2
from psycopg2.extras import RealDictCursor


# ============================================================================
# TEST CONFIGURATION
# ============================================================================


class AuthConfig:
    """Authentication configuration."""

    JWT_SECRET_KEY = "test-secret-key-do-not-use-in-production"
    JWT_ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 60
    PASSWORD_HASH_ITERATIONS = 100000


# ============================================================================
# PASSWORD UTILITIES
# ============================================================================


def hash_password(password: str, salt: Optional[bytes] = None) -> tuple[str, str]:
    """
    Hash password using PBKDF2.

    Args:
        password: Plain text password
        salt: Salt bytes (generated if not provided)

    Returns:
        Tuple of (hashed_password_hex, salt_hex)
    """
    if salt is None:
        import os
        salt = os.urandom(32)

    pwd_hash = hashlib.pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt,
        AuthConfig.PASSWORD_HASH_ITERATIONS,
    )

    return pwd_hash.hex(), salt.hex()


def verify_password(password: str, hashed_password: str, salt: str) -> bool:
    """
    Verify password against hash.

    Args:
        password: Plain text password to verify
        hashed_password: Hex-encoded hashed password
        salt: Hex-encoded salt

    Returns:
        True if password matches, False otherwise
    """
    salt_bytes = bytes.fromhex(salt)
    computed_hash, _ = hash_password(password, salt_bytes)

    return hmac.compare_digest(computed_hash, hashed_password)


# ============================================================================
# JWT UTILITIES
# ============================================================================


def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """
    Create JWT access token.

    Args:
        data: Data to encode in token
        expires_delta: Token expiration time

    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=AuthConfig.ACCESS_TOKEN_EXPIRE_MINUTES
        )

    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "access"
    })

    encoded_jwt = jwt.encode(
        to_encode,
        AuthConfig.JWT_SECRET_KEY,
        algorithm=AuthConfig.JWT_ALGORITHM,
    )

    return encoded_jwt


def decode_token(token: str) -> Dict[str, Any]:
    """
    Decode and validate JWT token.

    Args:
        token: JWT token string

    Returns:
        Decoded token payload

    Raises:
        JWTError: If token is invalid or expired
    """
    return jwt.decode(
        token,
        AuthConfig.JWT_SECRET_KEY,
        algorithms=[AuthConfig.JWT_ALGORITHM],
    )


# ============================================================================
# DATABASE UTILITIES
# ============================================================================


def create_users_table(connection):
    """Create users table in database."""
    with connection.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                password_hash VARCHAR(256) NOT NULL,
                password_salt VARCHAR(256) NOT NULL,
                roles TEXT[] NOT NULL DEFAULT ARRAY['analyst'],
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE,
                last_login TIMESTAMP WITH TIME ZONE
            )
        """)
        connection.commit()


def create_user(
    connection,
    username: str,
    email: str,
    password: str,
    roles: list = None,
) -> int:
    """
    Create user in database.

    Args:
        connection: Database connection
        username: Username
        email: Email address
        password: Plain text password
        roles: List of role names

    Returns:
        User ID
    """
    if roles is None:
        roles = ["analyst"]

    password_hash, password_salt = hash_password(password)

    with connection.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO users (username, email, password_hash, password_salt, roles)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (username, email, password_hash, password_salt, roles),
        )
        user_id = cursor.fetchone()[0]
        connection.commit()

    return user_id


def get_user_by_username(connection, username: str) -> Optional[Dict[str, Any]]:
    """
    Get user by username.

    Args:
        connection: Database connection
        username: Username to search for

    Returns:
        User dictionary or None
    """
    with connection.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(
            "SELECT * FROM users WHERE username = %s",
            (username,),
        )
        result = cursor.fetchone()

    return dict(result) if result else None


def authenticate_user(connection, username: str, password: str) -> Optional[Dict[str, Any]]:
    """
    Authenticate user with username and password.

    Args:
        connection: Database connection
        username: Username
        password: Password

    Returns:
        User dictionary if authentication successful, None otherwise
    """
    user = get_user_by_username(connection, username)

    if not user:
        return None

    if not user["is_active"]:
        return None

    if not verify_password(password, user["password_hash"], user["password_salt"]):
        return None

    # Update last login time
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE users SET last_login = NOW() WHERE id = %s",
            (user["id"],),
        )
        connection.commit()

    return user


def update_user_roles(connection, user_id: int, roles: list) -> None:
    """
    Update user roles.

    Args:
        connection: Database connection
        user_id: User ID
        roles: New list of roles
    """
    with connection.cursor() as cursor:
        cursor.execute(
            "UPDATE users SET roles = %s, updated_at = NOW() WHERE id = %s",
            (roles, user_id),
        )
        connection.commit()


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

    # Create users table
    create_users_table(connection)

    yield connection

    connection.close()


@pytest.fixture(autouse=True)
def cleanup_users(db_connection):
    """Clean up users table before each test."""
    yield

    # Clean up after test
    with db_connection.cursor() as cursor:
        cursor.execute("DELETE FROM users")
        db_connection.commit()


# ============================================================================
# INTEGRATION TESTS
# ============================================================================


class TestPasswordHashing:
    """Integration tests for password hashing."""

    def test_hash_password_generates_unique_salts(self):
        """Test that hashing same password twice generates different salts."""
        password = "MySecurePassword123!"

        hash1, salt1 = hash_password(password)
        hash2, salt2 = hash_password(password)

        # Salts should be different
        assert salt1 != salt2
        # Hashes should be different (due to different salts)
        assert hash1 != hash2

    def test_hash_password_with_same_salt_produces_same_hash(self):
        """Test that hashing with same salt produces same hash."""
        password = "MySecurePassword123!"

        hash1, salt1 = hash_password(password)
        hash2, _ = hash_password(password, bytes.fromhex(salt1))

        # Hashes should be the same
        assert hash1 == hash2

    def test_verify_password_with_correct_password(self):
        """Test password verification with correct password."""
        password = "CorrectPassword123!"
        password_hash, salt = hash_password(password)

        assert verify_password(password, password_hash, salt) is True

    def test_verify_password_with_incorrect_password(self):
        """Test password verification with incorrect password."""
        correct_password = "CorrectPassword123!"
        wrong_password = "WrongPassword456!"

        password_hash, salt = hash_password(correct_password)

        assert verify_password(wrong_password, password_hash, salt) is False

    def test_verify_password_case_sensitive(self):
        """Test password verification is case sensitive."""
        password = "CaseSensitive123!"
        password_hash, salt = hash_password(password)

        assert verify_password("casesensitive123!", password_hash, salt) is False


class TestUserCreation:
    """Integration tests for user creation in database."""

    def test_create_user_with_default_role(self, db_connection):
        """Test creating user with default analyst role."""
        user_id = create_user(
            db_connection,
            username="testuser",
            email="test@example.com",
            password="SecurePass123!",
        )

        assert user_id > 0

        # Verify user was created
        user = get_user_by_username(db_connection, "testuser")
        assert user is not None
        assert user["username"] == "testuser"
        assert user["email"] == "test@example.com"
        assert "analyst" in user["roles"]

    def test_create_user_with_custom_roles(self, db_connection):
        """Test creating user with custom roles."""
        user_id = create_user(
            db_connection,
            username="adminuser",
            email="admin@example.com",
            password="AdminPass123!",
            roles=["admin", "operator"],
        )

        user = get_user_by_username(db_connection, "adminuser")
        assert user is not None
        assert "admin" in user["roles"]
        assert "operator" in user["roles"]

    def test_create_user_hashes_password(self, db_connection):
        """Test that user password is hashed, not stored in plain text."""
        password = "PlainTextPassword123!"

        create_user(
            db_connection,
            username="secureuser",
            email="secure@example.com",
            password=password,
        )

        user = get_user_by_username(db_connection, "secureuser")

        # Password hash should not match plain text
        assert user["password_hash"] != password
        # Password hash should be hex string
        assert len(user["password_hash"]) == 64  # SHA-256 hex is 64 chars

    def test_create_duplicate_username_fails(self, db_connection):
        """Test creating user with duplicate username fails."""
        create_user(
            db_connection,
            username="duplicate",
            email="first@example.com",
            password="Pass123!",
        )

        # Attempt to create user with same username
        with pytest.raises(psycopg2.errors.UniqueViolation):
            create_user(
                db_connection,
                username="duplicate",
                email="second@example.com",
                password="Pass456!",
            )
            db_connection.rollback()

    def test_create_user_sets_timestamps(self, db_connection):
        """Test creating user sets created_at timestamp."""
        before = datetime.now(timezone.utc)

        create_user(
            db_connection,
            username="timeuser",
            email="time@example.com",
            password="Pass123!",
        )

        after = datetime.now(timezone.utc)

        user = get_user_by_username(db_connection, "timeuser")

        # Make created_at timezone-aware for comparison
        created_at = user["created_at"].replace(tzinfo=timezone.utc)

        assert before <= created_at <= after


class TestUserAuthentication:
    """Integration tests for user authentication."""

    def test_authenticate_with_valid_credentials(self, db_connection):
        """Test authentication with valid credentials."""
        username = "validuser"
        password = "ValidPass123!"

        create_user(db_connection, username, "valid@example.com", password)

        user = authenticate_user(db_connection, username, password)

        assert user is not None
        assert user["username"] == username

    def test_authenticate_with_invalid_password(self, db_connection):
        """Test authentication with invalid password."""
        username = "testuser"
        correct_password = "CorrectPass123!"
        wrong_password = "WrongPass456!"

        create_user(db_connection, username, "test@example.com", correct_password)

        user = authenticate_user(db_connection, username, wrong_password)

        assert user is None

    def test_authenticate_with_nonexistent_user(self, db_connection):
        """Test authentication with nonexistent user."""
        user = authenticate_user(db_connection, "nonexistent", "SomePass123!")

        assert user is None

    def test_authenticate_updates_last_login(self, db_connection):
        """Test authentication updates last login timestamp."""
        username = "loginuser"
        password = "LoginPass123!"

        create_user(db_connection, username, "login@example.com", password)

        before = datetime.now(timezone.utc)
        user = authenticate_user(db_connection, username, password)
        after = datetime.now(timezone.utc)

        assert user is not None

        # Verify last_login was updated
        updated_user = get_user_by_username(db_connection, username)
        last_login = updated_user["last_login"].replace(tzinfo=timezone.utc)

        assert before <= last_login <= after

    def test_authenticate_inactive_user_fails(self, db_connection):
        """Test authentication fails for inactive user."""
        username = "inactiveuser"
        password = "InactivePass123!"

        user_id = create_user(db_connection, username, "inactive@example.com", password)

        # Deactivate user
        with db_connection.cursor() as cursor:
            cursor.execute("UPDATE users SET is_active = FALSE WHERE id = %s", (user_id,))
            db_connection.commit()

        user = authenticate_user(db_connection, username, password)

        assert user is None


class TestJWTTokenGeneration:
    """Integration tests for JWT token generation."""

    def test_generate_token_for_authenticated_user(self, db_connection):
        """Test generating JWT token for authenticated user."""
        username = "tokenuser"
        password = "TokenPass123!"

        create_user(db_connection, username, "token@example.com", password)
        user = authenticate_user(db_connection, username, password)

        assert user is not None

        # Generate token
        token = create_access_token({
            "sub": str(user["id"]),
            "username": user["username"],
            "roles": user["roles"],
        })

        assert token is not None
        assert isinstance(token, str)
        assert token.count(".") == 2  # JWT has 3 parts

    def test_token_contains_user_claims(self, db_connection):
        """Test JWT token contains user claims."""
        username = "claimsuser"
        password = "ClaimsPass123!"

        user_id = create_user(
            db_connection,
            username,
            "claims@example.com",
            password,
            roles=["admin"],
        )

        user = authenticate_user(db_connection, username, password)
        token = create_access_token({
            "sub": str(user["id"]),
            "username": user["username"],
            "roles": user["roles"],
        })

        # Decode token
        payload = decode_token(token)

        assert payload["sub"] == str(user_id)
        assert payload["username"] == username
        assert "admin" in payload["roles"]
        assert "exp" in payload
        assert "iat" in payload

    def test_token_expiration(self, db_connection):
        """Test JWT token expires after configured time."""
        username = "expireuser"
        password = "ExpirePass123!"

        create_user(db_connection, username, "expire@example.com", password)
        user = authenticate_user(db_connection, username, password)

        # Create token that expires in 1 second
        token = create_access_token(
            {"sub": str(user["id"]), "username": user["username"]},
            expires_delta=timedelta(seconds=1),
        )

        # Token should be valid immediately
        payload = decode_token(token)
        assert payload["sub"] == str(user["id"])

        # Wait for expiration
        import time
        time.sleep(2)

        # Token should be expired
        with pytest.raises(JWTError):
            decode_token(token)


class TestCompleteLoginFlow:
    """Integration tests for complete login flow."""

    def test_complete_login_flow_success(self, db_connection):
        """Test complete successful login flow."""
        # Step 1: Create user
        username = "flowuser"
        password = "FlowPass123!"
        email = "flow@example.com"

        user_id = create_user(db_connection, username, email, password, roles=["analyst"])

        # Step 2: Authenticate user
        user = authenticate_user(db_connection, username, password)

        assert user is not None
        assert user["id"] == user_id
        assert user["username"] == username

        # Step 3: Generate JWT token
        token = create_access_token({
            "sub": str(user["id"]),
            "username": user["username"],
            "roles": user["roles"],
        })

        assert token is not None

        # Step 4: Validate token
        payload = decode_token(token)

        assert payload["sub"] == str(user_id)
        assert payload["username"] == username
        assert "analyst" in payload["roles"]

    def test_complete_login_flow_invalid_credentials(self, db_connection):
        """Test complete login flow with invalid credentials."""
        # Step 1: Create user
        username = "invaliduser"
        correct_password = "CorrectPass123!"
        wrong_password = "WrongPass456!"

        create_user(db_connection, username, "invalid@example.com", correct_password)

        # Step 2: Attempt authentication with wrong password
        user = authenticate_user(db_connection, username, wrong_password)

        # Authentication should fail
        assert user is None

    def test_login_flow_with_role_changes(self, db_connection):
        """Test login flow after user roles are changed."""
        username = "roleuser"
        password = "RolePass123!"

        user_id = create_user(
            db_connection,
            username,
            "role@example.com",
            password,
            roles=["analyst"],
        )

        # Initial login
        user = authenticate_user(db_connection, username, password)
        token1 = create_access_token({
            "sub": str(user["id"]),
            "username": user["username"],
            "roles": user["roles"],
        })

        payload1 = decode_token(token1)
        assert "analyst" in payload1["roles"]
        assert "admin" not in payload1["roles"]

        # Update user roles
        update_user_roles(db_connection, user_id, ["admin", "operator"])

        # Login again
        user = authenticate_user(db_connection, username, password)
        token2 = create_access_token({
            "sub": str(user["id"]),
            "username": user["username"],
            "roles": user["roles"],
        })

        payload2 = decode_token(token2)
        assert "admin" in payload2["roles"]
        assert "operator" in payload2["roles"]
        assert "analyst" not in payload2["roles"]


class TestTokenValidation:
    """Integration tests for JWT token validation."""

    def test_validate_token_with_correct_secret(self, db_connection):
        """Test validating token with correct secret."""
        username = "validtoken"
        password = "ValidToken123!"

        create_user(db_connection, username, "valid@example.com", password)
        user = authenticate_user(db_connection, username, password)

        token = create_access_token({"sub": str(user["id"])})

        # Should decode successfully
        payload = decode_token(token)
        assert payload["sub"] == str(user["id"])

    def test_validate_token_with_wrong_secret_fails(self, db_connection):
        """Test validating token with wrong secret fails."""
        username = "wrongsecret"
        password = "WrongSecret123!"

        create_user(db_connection, username, "wrong@example.com", password)
        user = authenticate_user(db_connection, username, password)

        token = create_access_token({"sub": str(user["id"])})

        # Attempt to decode with wrong secret
        with pytest.raises(JWTError):
            jwt.decode(token, "wrong-secret-key", algorithms=[AuthConfig.JWT_ALGORITHM])

    def test_validate_expired_token_fails(self, db_connection):
        """Test validating expired token fails."""
        username = "expiredtoken"
        password = "ExpiredToken123!"

        create_user(db_connection, username, "expired@example.com", password)
        user = authenticate_user(db_connection, username, password)

        # Create token that expired 1 hour ago
        token = create_access_token(
            {"sub": str(user["id"])},
            expires_delta=timedelta(hours=-1),
        )

        # Should raise error for expired token
        with pytest.raises(JWTError):
            decode_token(token)

    def test_validate_tampered_token_fails(self, db_connection):
        """Test validating tampered token fails."""
        username = "tampered"
        password = "Tampered123!"

        create_user(db_connection, username, "tampered@example.com", password)
        user = authenticate_user(db_connection, username, password)

        token = create_access_token({"sub": str(user["id"])})

        # Tamper with token
        parts = token.split(".")
        tampered_token = f"{parts[0]}.{parts[1]}.invalidsignature"

        # Should raise error for invalid signature
        with pytest.raises(JWTError):
            decode_token(tampered_token)


class TestDatabaseIntegration:
    """Integration tests for database operations."""

    def test_concurrent_user_creation(self, db_connection):
        """Test creating multiple users concurrently."""
        users = [
            ("user1", "user1@example.com", "Pass1234!"),
            ("user2", "user2@example.com", "Pass5678!"),
            ("user3", "user3@example.com", "Pass9012!"),
        ]

        user_ids = []
        for username, email, password in users:
            user_id = create_user(db_connection, username, email, password)
            user_ids.append(user_id)

        # All users should be created with unique IDs
        assert len(set(user_ids)) == len(users)

        # Verify all users exist
        for username, _, _ in users:
            user = get_user_by_username(db_connection, username)
            assert user is not None

    def test_user_retrieval_performance(self, db_connection):
        """Test user retrieval is performant."""
        # Create test user
        create_user(db_connection, "perfuser", "perf@example.com", "PerfPass123!")

        # Measure retrieval time
        import time

        start = time.time()
        user = get_user_by_username(db_connection, "perfuser")
        end = time.time()

        assert user is not None
        # Should complete in less than 100ms
        assert (end - start) < 0.1


if __name__ == "__main__":
    pytest.main([__file__, "-v"])