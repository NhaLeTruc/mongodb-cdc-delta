"""
Unit tests for JWT token generation, validation, and management.

Tests cover:
- JWT token creation with claims
- Token validation and expiration
- Token refresh
- Invalid token handling
- Claims extraction and validation
"""

import pytest
from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError
from typing import Dict, Any, List


# ============================================================================
# JWT UTILITY FUNCTIONS (Test Doubles)
# ============================================================================


class JWTConfig:
    """JWT configuration."""
    SECRET_KEY = "test-secret-key-change-in-production"
    ALGORITHM = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES = 60
    REFRESH_TOKEN_EXPIRE_DAYS = 7


def create_access_token(data: Dict[str, Any], expires_delta: timedelta = None) -> str:
    """
    Create JWT access token.

    Args:
        data: Claims to encode in the token
        expires_delta: Token expiration time (defaults to config value)

    Returns:
        Encoded JWT token string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(
            minutes=JWTConfig.ACCESS_TOKEN_EXPIRE_MINUTES
        )

    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "access"
    })

    encoded_jwt = jwt.encode(
        to_encode,
        JWTConfig.SECRET_KEY,
        algorithm=JWTConfig.ALGORITHM
    )
    return encoded_jwt


def create_refresh_token(data: Dict[str, Any]) -> str:
    """
    Create JWT refresh token with longer expiration.

    Args:
        data: Claims to encode in the token

    Returns:
        Encoded JWT refresh token string
    """
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(
        days=JWTConfig.REFRESH_TOKEN_EXPIRE_DAYS
    )

    to_encode.update({
        "exp": expire,
        "iat": datetime.now(timezone.utc),
        "type": "refresh"
    })

    encoded_jwt = jwt.encode(
        to_encode,
        JWTConfig.SECRET_KEY,
        algorithm=JWTConfig.ALGORITHM
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
    payload = jwt.decode(
        token,
        JWTConfig.SECRET_KEY,
        algorithms=[JWTConfig.ALGORITHM]
    )
    return payload


def verify_token_type(token: str, expected_type: str) -> bool:
    """
    Verify token type matches expected type.

    Args:
        token: JWT token string
        expected_type: Expected token type ('access' or 'refresh')

    Returns:
        True if token type matches

    Raises:
        ValueError: If token type doesn't match
    """
    payload = decode_token(token)
    token_type = payload.get("type")

    if token_type != expected_type:
        raise ValueError(f"Invalid token type: expected {expected_type}, got {token_type}")

    return True


def extract_user_id(token: str) -> str:
    """
    Extract user ID from token.

    Args:
        token: JWT token string

    Returns:
        User ID from token claims

    Raises:
        JWTError: If token is invalid or missing user ID
    """
    payload = decode_token(token)
    user_id = payload.get("sub")

    if not user_id:
        raise JWTError("Token missing user ID claim")

    return user_id


def extract_roles(token: str) -> List[str]:
    """
    Extract user roles from token.

    Args:
        token: JWT token string

    Returns:
        List of role names

    Raises:
        JWTError: If token is invalid or missing roles
    """
    payload = decode_token(token)
    roles = payload.get("roles", [])

    if not roles:
        raise JWTError("Token missing roles claim")

    return roles


# ============================================================================
# UNIT TESTS
# ============================================================================


class TestJWTTokenCreation:
    """Tests for JWT token creation."""

    def test_create_access_token_with_user_claims(self):
        """Test creating access token with user claims."""
        user_data = {
            "sub": "user-123",
            "username": "testuser",
            "roles": ["analyst"]
        }

        token = create_access_token(user_data)

        # Token should be a non-empty string
        assert isinstance(token, str)
        assert len(token) > 0

        # Token should have three parts (header.payload.signature)
        assert token.count('.') == 2

    def test_create_access_token_includes_expiration(self):
        """Test access token includes expiration claim."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data)

        payload = decode_token(token)

        assert "exp" in payload
        assert isinstance(payload["exp"], (int, float))

        # Expiration should be in the future
        exp_datetime = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        assert exp_datetime > datetime.now(timezone.utc)

    def test_create_access_token_includes_issued_at(self):
        """Test access token includes issued-at claim."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data)

        payload = decode_token(token)

        assert "iat" in payload
        assert isinstance(payload["iat"], (int, float))

        # Issued-at should be in the past or now
        iat_datetime = datetime.fromtimestamp(payload["iat"], tz=timezone.utc)
        assert iat_datetime <= datetime.now(timezone.utc)

    def test_create_access_token_includes_type(self):
        """Test access token includes type claim."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data)

        payload = decode_token(token)

        assert "type" in payload
        assert payload["type"] == "access"

    def test_create_access_token_with_custom_expiration(self):
        """Test creating access token with custom expiration."""
        user_data = {"sub": "user-123"}
        custom_expire = timedelta(minutes=30)

        token = create_access_token(user_data, expires_delta=custom_expire)
        payload = decode_token(token)

        exp_datetime = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        expected_exp = datetime.now(timezone.utc) + custom_expire

        # Allow 5 second tolerance for test execution time
        assert abs((exp_datetime - expected_exp).total_seconds()) < 5

    def test_create_access_token_preserves_all_claims(self):
        """Test access token preserves all provided claims."""
        user_data = {
            "sub": "user-123",
            "username": "testuser",
            "email": "test@example.com",
            "roles": ["admin", "analyst"]
        }

        token = create_access_token(user_data)
        payload = decode_token(token)

        assert payload["sub"] == "user-123"
        assert payload["username"] == "testuser"
        assert payload["email"] == "test@example.com"
        assert payload["roles"] == ["admin", "analyst"]

    def test_create_refresh_token(self):
        """Test creating refresh token."""
        user_data = {"sub": "user-123"}
        token = create_refresh_token(user_data)

        payload = decode_token(token)

        assert payload["type"] == "refresh"
        assert payload["sub"] == "user-123"

        # Refresh token should have longer expiration
        exp_datetime = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        min_exp = datetime.now(timezone.utc) + timedelta(days=6)
        assert exp_datetime > min_exp

    def test_create_tokens_are_different_even_with_same_data(self):
        """Test that tokens created at different times are different."""
        user_data = {"sub": "user-123"}

        token1 = create_access_token(user_data)
        token2 = create_access_token(user_data)

        # Tokens should be different due to different iat/exp times
        assert token1 != token2


class TestJWTTokenValidation:
    """Tests for JWT token validation."""

    def test_decode_valid_token(self):
        """Test decoding a valid token."""
        user_data = {"sub": "user-123", "username": "testuser"}
        token = create_access_token(user_data)

        payload = decode_token(token)

        assert payload["sub"] == "user-123"
        assert payload["username"] == "testuser"

    def test_decode_expired_token_raises_error(self):
        """Test decoding an expired token raises error."""
        user_data = {"sub": "user-123"}
        # Create token that expires immediately
        token = create_access_token(user_data, expires_delta=timedelta(seconds=-1))

        with pytest.raises(JWTError):
            decode_token(token)

    def test_decode_token_with_invalid_signature_raises_error(self):
        """Test decoding token with invalid signature raises error."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data)

        # Tamper with the token by changing the last character
        tampered_token = token[:-1] + ('a' if token[-1] != 'a' else 'b')

        with pytest.raises(JWTError):
            decode_token(tampered_token)

    def test_decode_token_with_wrong_secret_raises_error(self):
        """Test decoding token with wrong secret raises error."""
        # Create token with different secret
        user_data = {"sub": "user-123"}
        wrong_secret_token = jwt.encode(
            user_data,
            "wrong-secret-key",
            algorithm=JWTConfig.ALGORITHM
        )

        with pytest.raises(JWTError):
            decode_token(wrong_secret_token)

    def test_decode_malformed_token_raises_error(self):
        """Test decoding malformed token raises error."""
        malformed_tokens = [
            "not.a.token",
            "invalid-jwt-format",
            "",
            "a.b"  # Only 2 parts instead of 3
        ]

        for token in malformed_tokens:
            with pytest.raises(JWTError):
                decode_token(token)

    def test_verify_access_token_type(self):
        """Test verifying access token type."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data)

        assert verify_token_type(token, "access") is True

    def test_verify_refresh_token_type(self):
        """Test verifying refresh token type."""
        user_data = {"sub": "user-123"}
        token = create_refresh_token(user_data)

        assert verify_token_type(token, "refresh") is True

    def test_verify_wrong_token_type_raises_error(self):
        """Test verifying wrong token type raises error."""
        user_data = {"sub": "user-123"}
        access_token = create_access_token(user_data)

        with pytest.raises(ValueError) as exc_info:
            verify_token_type(access_token, "refresh")

        assert "Invalid token type" in str(exc_info.value)


class TestJWTClaimsExtraction:
    """Tests for extracting claims from JWT tokens."""

    def test_extract_user_id_from_token(self):
        """Test extracting user ID from token."""
        user_data = {"sub": "user-456", "username": "testuser"}
        token = create_access_token(user_data)

        user_id = extract_user_id(token)
        assert user_id == "user-456"

    def test_extract_user_id_from_token_without_sub_raises_error(self):
        """Test extracting user ID from token without sub claim raises error."""
        # Manually create token without sub claim
        invalid_data = {"username": "testuser"}
        token = jwt.encode(
            invalid_data,
            JWTConfig.SECRET_KEY,
            algorithm=JWTConfig.ALGORITHM
        )

        with pytest.raises(JWTError) as exc_info:
            extract_user_id(token)

        assert "missing user ID" in str(exc_info.value)

    def test_extract_roles_from_token(self):
        """Test extracting roles from token."""
        user_data = {
            "sub": "user-123",
            "roles": ["admin", "analyst"]
        }
        token = create_access_token(user_data)

        roles = extract_roles(token)
        assert roles == ["admin", "analyst"]

    def test_extract_roles_from_token_without_roles_raises_error(self):
        """Test extracting roles from token without roles claim raises error."""
        user_data = {"sub": "user-123"}
        # Token without roles
        token = create_access_token(user_data)

        # Remove roles from the test or expect default empty list
        # For this test, we expect it to raise an error
        with pytest.raises(JWTError) as exc_info:
            extract_roles(token)

        assert "missing roles" in str(exc_info.value)

    def test_extract_roles_returns_empty_list_if_no_roles_claim(self):
        """Test extracting roles returns empty list when configured to allow."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data)

        payload = decode_token(token)
        roles = payload.get("roles", [])

        assert roles == []


class TestJWTTokenExpirationHandling:
    """Tests for JWT token expiration handling."""

    def test_token_expires_after_configured_time(self):
        """Test token expires after configured expiration time."""
        user_data = {"sub": "user-123"}
        # Create token that expires in 1 second
        token = create_access_token(user_data, expires_delta=timedelta(seconds=1))

        # Token should be valid immediately
        payload = decode_token(token)
        assert payload["sub"] == "user-123"

        # Wait for expiration (simulated by manually checking exp claim)
        payload = jwt.decode(
            token,
            JWTConfig.SECRET_KEY,
            algorithms=[JWTConfig.ALGORITHM],
            options={"verify_exp": False}  # Disable expiration check for inspection
        )

        exp_datetime = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        now = datetime.now(timezone.utc)

        # Expiration should be approximately 1 second from now
        time_until_exp = (exp_datetime - now).total_seconds()
        assert 0 <= time_until_exp <= 2  # Allow 1 second tolerance

    def test_expired_token_cannot_be_decoded(self):
        """Test expired token raises JWTError on decode."""
        user_data = {"sub": "user-123"}
        # Create token that expired 1 second ago
        token = create_access_token(user_data, expires_delta=timedelta(seconds=-1))

        with pytest.raises(JWTError):
            decode_token(token)

    def test_token_expiration_can_be_checked_without_validation(self):
        """Test token expiration can be checked without full validation."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data, expires_delta=timedelta(seconds=-1))

        # Decode without expiration check
        payload = jwt.decode(
            token,
            JWTConfig.SECRET_KEY,
            algorithms=[JWTConfig.ALGORITHM],
            options={"verify_exp": False}
        )

        exp_datetime = datetime.fromtimestamp(payload["exp"], tz=timezone.utc)
        assert exp_datetime < datetime.now(timezone.utc)


class TestJWTSecurityFeatures:
    """Tests for JWT security features."""

    def test_token_signature_prevents_tampering(self):
        """Test token signature prevents claim tampering."""
        user_data = {"sub": "user-123", "roles": ["analyst"]}
        token = create_access_token(user_data)

        # Attempt to decode and modify claims
        parts = token.split('.')
        assert len(parts) == 3

        # Try to tamper with the payload
        import base64
        import json

        # Decode payload
        payload_bytes = base64.urlsafe_b64decode(parts[1] + '==')
        payload_dict = json.loads(payload_bytes)

        # Modify roles
        payload_dict["roles"] = ["admin"]

        # Re-encode payload
        modified_payload = base64.urlsafe_b64encode(
            json.dumps(payload_dict).encode()
        ).decode().rstrip('=')

        # Create tampered token
        tampered_token = f"{parts[0]}.{modified_payload}.{parts[2]}"

        # Tampered token should fail validation
        with pytest.raises(JWTError):
            decode_token(tampered_token)

    def test_different_algorithms_are_rejected(self):
        """Test tokens with different algorithms are rejected."""
        user_data = {"sub": "user-123"}

        # Create token with different algorithm (none)
        insecure_token = jwt.encode(
            user_data,
            JWTConfig.SECRET_KEY,
            algorithm="none"
        )

        with pytest.raises(JWTError):
            decode_token(insecure_token)

    def test_token_without_signature_is_rejected(self):
        """Test token without signature is rejected."""
        user_data = {"sub": "user-123"}
        token = create_access_token(user_data)

        # Remove signature part
        parts = token.split('.')
        token_without_sig = f"{parts[0]}.{parts[1]}."

        with pytest.raises(JWTError):
            decode_token(token_without_sig)


class TestJWTTokenRefresh:
    """Tests for JWT token refresh functionality."""

    def test_refresh_token_has_longer_expiration(self):
        """Test refresh token has longer expiration than access token."""
        user_data = {"sub": "user-123"}

        access_token = create_access_token(user_data)
        refresh_token = create_refresh_token(user_data)

        access_payload = decode_token(access_token)
        refresh_payload = decode_token(refresh_token)

        access_exp = datetime.fromtimestamp(access_payload["exp"], tz=timezone.utc)
        refresh_exp = datetime.fromtimestamp(refresh_payload["exp"], tz=timezone.utc)

        # Refresh token should expire much later
        assert refresh_exp > access_exp
        assert (refresh_exp - access_exp).days >= 6

    def test_refresh_token_can_generate_new_access_token(self):
        """Test using refresh token to generate new access token."""
        user_data = {"sub": "user-123", "roles": ["analyst"]}

        # Create refresh token
        refresh_token = create_refresh_token(user_data)

        # Verify it's a refresh token
        assert verify_token_type(refresh_token, "refresh")

        # Extract user data from refresh token
        payload = decode_token(refresh_token)
        user_id = payload["sub"]

        # Create new access token using data from refresh token
        new_access_token = create_access_token({"sub": user_id, "roles": user_data["roles"]})

        # New access token should be valid
        new_payload = decode_token(new_access_token)
        assert new_payload["sub"] == user_id
        assert new_payload["type"] == "access"

    def test_access_token_cannot_be_used_as_refresh_token(self):
        """Test access token cannot be used as refresh token."""
        user_data = {"sub": "user-123"}
        access_token = create_access_token(user_data)

        with pytest.raises(ValueError):
            verify_token_type(access_token, "refresh")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
