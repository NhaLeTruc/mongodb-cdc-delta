"""
Authentication service for user authentication and JWT token management.

Provides:
- Password hashing and verification (passlib + bcrypt)
- JWT token creation and validation
- User authentication
- Role-based permission checks
"""

import structlog
from typing import Optional, List, Set, Dict
from datetime import datetime, timedelta
from uuid import UUID
from passlib.context import CryptContext
from jose import JWTError, jwt

from api.src.config import get_settings
from api.src.models.auth import (
    UserDB, CurrentUser, TokenPayload, Role, Permission,
    LoginRequest, TokenResponse
)
from api.src.repositories.user_repo import UserRepository

logger = structlog.get_logger(__name__)


class AuthService:
    """Service for authentication and authorization operations."""

    def __init__(self, user_repo: UserRepository):
        """
        Initialize auth service.

        Args:
            user_repo: User repository
        """
        self.user_repo = user_repo
        self.settings = get_settings()

        self.pwd_context = CryptContext(
            schemes=["bcrypt"],
            deprecated="auto",
            bcrypt__rounds=self.settings.password_bcrypt_rounds
        )

        self.role_hierarchy = {
            Role.ADMIN: [Role.OPERATOR, Role.ANALYST],
            Role.OPERATOR: [Role.ANALYST],
            Role.ANALYST: [],
            Role.VIEWER: []
        }

        self.role_permissions: Dict[Role, Set[Permission]] = {
            Role.VIEWER: {
                Permission.READ_METRICS,
            },
            Role.ANALYST: {
                Permission.READ_MAPPINGS,
                Permission.READ_METRICS,
                Permission.READ_CHECKPOINTS,
            },
            Role.OPERATOR: {
                Permission.READ_MAPPINGS,
                Permission.READ_METRICS,
                Permission.READ_CHECKPOINTS,
                Permission.CREATE_MAPPINGS,
                Permission.UPDATE_MAPPINGS,
                Permission.TRIGGER_SYNC,
                Permission.PAUSE_PIPELINE,
                Permission.RESUME_PIPELINE,
            },
            Role.ADMIN: {
                Permission.READ_MAPPINGS,
                Permission.READ_METRICS,
                Permission.READ_CHECKPOINTS,
                Permission.READ_AUDIT_LOGS,
                Permission.READ_USERS,
                Permission.CREATE_MAPPINGS,
                Permission.UPDATE_MAPPINGS,
                Permission.DELETE_MAPPINGS,
                Permission.TRIGGER_SYNC,
                Permission.PAUSE_PIPELINE,
                Permission.RESUME_PIPELINE,
                Permission.MANAGE_USERS,
                Permission.MANAGE_ROLES,
                Permission.MANAGE_SYSTEM,
                Permission.VIEW_SENSITIVE_DATA,
            }
        }

    def hash_password(self, password: str) -> str:
        """
        Hash a password using bcrypt.

        Args:
            password: Plain text password

        Returns:
            Hashed password
        """
        try:
            hashed = self.pwd_context.hash(password)
            logger.debug("password_hashed")
            return hashed
        except Exception as e:
            logger.error("password_hash_failed", error=str(e))
            raise

    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        """
        Verify a password against its hash.

        Args:
            plain_password: Plain text password
            hashed_password: Hashed password

        Returns:
            True if password matches, False otherwise
        """
        try:
            verified = self.pwd_context.verify(plain_password, hashed_password)
            logger.debug("password_verified", verified=verified)
            return verified
        except Exception as e:
            logger.error("password_verify_failed", error=str(e))
            return False

    def create_access_token(
        self,
        user_id: UUID,
        username: str,
        roles: List[str],
        expires_delta: Optional[timedelta] = None
    ) -> str:
        """
        Create JWT access token.

        Args:
            user_id: User ID
            username: Username
            roles: User roles
            expires_delta: Custom expiration time (optional)

        Returns:
            JWT token string
        """
        try:
            if expires_delta is None:
                expires_delta = timedelta(minutes=self.settings.jwt_access_token_expire_minutes)

            now = datetime.utcnow()
            expire = now + expires_delta

            payload = {
                "sub": str(user_id),
                "username": username,
                "roles": roles,
                "exp": int(expire.timestamp()),
                "iat": int(now.timestamp())
            }

            token = jwt.encode(
                payload,
                self.settings.jwt_secret_key,
                algorithm=self.settings.jwt_algorithm
            )

            logger.info(
                "access_token_created",
                user_id=str(user_id),
                username=username,
                expires_in=expires_delta.total_seconds()
            )

            return token

        except Exception as e:
            logger.error("token_create_failed", error=str(e), user_id=str(user_id))
            raise

    def decode_token(self, token: str) -> Optional[TokenPayload]:
        """
        Decode and validate JWT token.

        Args:
            token: JWT token string

        Returns:
            Token payload or None if invalid
        """
        try:
            payload = jwt.decode(
                token,
                self.settings.jwt_secret_key,
                algorithms=[self.settings.jwt_algorithm]
            )

            token_payload = TokenPayload(
                sub=payload.get("sub"),
                username=payload.get("username"),
                roles=payload.get("roles", []),
                exp=payload.get("exp"),
                iat=payload.get("iat")
            )

            logger.debug("token_decoded", user_id=token_payload.sub)
            return token_payload

        except JWTError as e:
            logger.warning("token_decode_failed", error=str(e))
            return None
        except Exception as e:
            logger.error("token_decode_error", error=str(e))
            return None

    async def authenticate_user(self, login_request: LoginRequest) -> Optional[UserDB]:
        """
        Authenticate user with username and password.

        Args:
            login_request: Login credentials

        Returns:
            User if authenticated, None otherwise
        """
        try:
            user = await self.user_repo.get_user_by_username(login_request.username)

            if not user:
                logger.warning("authentication_failed_user_not_found", username=login_request.username)
                return None

            if not user.is_active:
                logger.warning("authentication_failed_user_inactive", username=login_request.username)
                return None

            if not self.verify_password(login_request.password, user.password_hash):
                logger.warning("authentication_failed_invalid_password", username=login_request.username)
                return None

            logger.info("user_authenticated", user_id=str(user.id), username=user.username)
            return user

        except Exception as e:
            logger.error("authentication_error", error=str(e), username=login_request.username)
            return None

    async def login(self, login_request: LoginRequest) -> Optional[TokenResponse]:
        """
        Login user and create access token.

        Args:
            login_request: Login credentials

        Returns:
            Token response or None if authentication failed
        """
        try:
            user = await self.authenticate_user(login_request)

            if not user:
                return None

            roles = await self.user_repo.get_user_roles(user.id)

            access_token = self.create_access_token(
                user_id=user.id,
                username=user.username,
                roles=roles
            )

            expires_in = self.settings.jwt_access_token_expire_minutes * 60

            logger.info("login_success", user_id=str(user.id), username=user.username)

            return TokenResponse(
                access_token=access_token,
                token_type="bearer",
                expires_in=expires_in
            )

        except Exception as e:
            logger.error("login_error", error=str(e), username=login_request.username)
            return None

    async def get_current_user(self, token: str) -> Optional[CurrentUser]:
        """
        Get current user from JWT token.

        Args:
            token: JWT token string

        Returns:
            Current user or None if invalid
        """
        try:
            payload = self.decode_token(token)

            if not payload:
                logger.warning("get_current_user_failed_invalid_token")
                return None

            try:
                user_id = UUID(payload.sub)
            except ValueError:
                logger.warning("get_current_user_failed_invalid_user_id", user_id=payload.sub)
                return None

            user = await self.user_repo.get_user_by_id(user_id)

            if not user:
                logger.warning("get_current_user_failed_user_not_found", user_id=str(user_id))
                return None

            if not user.is_active:
                logger.warning("get_current_user_failed_user_inactive", user_id=str(user_id))
                return None

            roles = await self.user_repo.get_user_roles(user.id)

            current_user = CurrentUser(
                id=user.id,
                username=user.username,
                email=user.email,
                roles=roles,
                is_active=user.is_active
            )

            logger.debug("current_user_retrieved", user_id=str(user.id), username=user.username)
            return current_user

        except Exception as e:
            logger.error("get_current_user_error", error=str(e))
            return None

    def get_role_permissions(self, role: Role) -> Set[Permission]:
        """
        Get all permissions for a role including inherited permissions.

        Args:
            role: Role to get permissions for

        Returns:
            Set of permissions
        """
        permissions = set(self.role_permissions.get(role, set()))

        inherited_roles = self.role_hierarchy.get(role, [])
        for inherited_role in inherited_roles:
            permissions.update(self.role_permissions.get(inherited_role, set()))

        return permissions

    def has_permission(self, user_roles: List[str], required_permission: Permission) -> bool:
        """
        Check if user has required permission based on their roles.

        Args:
            user_roles: List of role names
            required_permission: Required permission

        Returns:
            True if user has permission, False otherwise
        """
        if not user_roles:
            return False

        all_permissions: Set[Permission] = set()

        for role_name in user_roles:
            try:
                role = Role(role_name)
                all_permissions.update(self.get_role_permissions(role))
            except ValueError:
                logger.warning("invalid_role_name", role=role_name)
                continue

        has_perm = required_permission in all_permissions
        logger.debug(
            "permission_check",
            roles=user_roles,
            permission=required_permission.value,
            granted=has_perm
        )
        return has_perm

    def has_any_permission(
        self,
        user_roles: List[str],
        required_permissions: List[Permission]
    ) -> bool:
        """
        Check if user has any of the required permissions.

        Args:
            user_roles: List of role names
            required_permissions: List of required permissions

        Returns:
            True if user has at least one permission, False otherwise
        """
        if not user_roles or not required_permissions:
            return False

        for permission in required_permissions:
            if self.has_permission(user_roles, permission):
                return True

        return False

    def has_all_permissions(
        self,
        user_roles: List[str],
        required_permissions: List[Permission]
    ) -> bool:
        """
        Check if user has all required permissions.

        Args:
            user_roles: List of role names
            required_permissions: List of required permissions

        Returns:
            True if user has all permissions, False otherwise
        """
        if not user_roles or not required_permissions:
            return False

        for permission in required_permissions:
            if not self.has_permission(user_roles, permission):
                return False

        return True

    def has_role(self, user_roles: List[str], required_role: Role) -> bool:
        """
        Check if user has a specific role.

        Args:
            user_roles: List of role names
            required_role: Required role

        Returns:
            True if user has role, False otherwise
        """
        return required_role.value in user_roles

    def is_admin(self, user_roles: List[str]) -> bool:
        """
        Check if user has admin role.

        Args:
            user_roles: List of role names

        Returns:
            True if user is admin, False otherwise
        """
        return self.has_role(user_roles, Role.ADMIN)
