"""
User repository for database operations.

Provides async CRUD operations for users using asyncpg with PostgreSQL.
Includes connection pooling, transaction management, and error handling.
"""

import asyncpg
import structlog
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from contextlib import asynccontextmanager

from api.src.models.auth import UserDB
from api.src.config import get_settings

logger = structlog.get_logger(__name__)


class UserRepository:
    """Repository for user database operations."""

    def __init__(self, pool: asyncpg.Pool):
        """
        Initialize user repository.

        Args:
            pool: asyncpg connection pool
        """
        self.pool = pool
        self.settings = get_settings()

    @asynccontextmanager
    async def transaction(self):
        """
        Context manager for database transactions.

        Yields:
            asyncpg.Connection: Database connection
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                yield conn

    async def create_user(
        self,
        username: str,
        email: str,
        password_hash: str,
        roles: List[str],
        is_active: bool = True
    ) -> UserDB:
        """
        Create a new user with roles.

        Args:
            username: Username
            email: Email address
            password_hash: Hashed password
            roles: List of role names
            is_active: Active status

        Returns:
            Created user

        Raises:
            ValueError: If username or email already exists
            asyncpg.PostgresError: On database error
        """
        try:
            async with self.transaction() as conn:
                try:
                    row = await conn.fetchrow(
                        """
                        INSERT INTO users (username, email, password_hash, is_active, created_at, updated_at)
                        VALUES ($1, $2, $3, $4, NOW(), NOW())
                        RETURNING id, username, email, password_hash, is_active, created_at, updated_at
                        """,
                        username,
                        email,
                        password_hash,
                        is_active
                    )

                    user_id = row["id"]

                    for role in roles:
                        await conn.execute(
                            """
                            INSERT INTO user_roles (user_id, role)
                            VALUES ($1, $2)
                            """,
                            user_id,
                            role
                        )

                    logger.info("user_created", user_id=str(user_id), username=username, roles=roles)

                    return UserDB(
                        id=row["id"],
                        username=row["username"],
                        email=row["email"],
                        password_hash=row["password_hash"],
                        is_active=row["is_active"],
                        created_at=row["created_at"],
                        updated_at=row["updated_at"]
                    )
                except asyncpg.UniqueViolationError as e:
                    if "username" in str(e):
                        logger.warning("username_already_exists", username=username)
                        raise ValueError(f"Username '{username}' already exists")
                    elif "email" in str(e):
                        logger.warning("email_already_exists", email=email)
                        raise ValueError(f"Email '{email}' already exists")
                    else:
                        raise

        except ValueError:
            raise
        except Exception as e:
            logger.error("user_create_failed", error=str(e), username=username)
            raise

    async def get_user_by_id(self, user_id: UUID) -> Optional[UserDB]:
        """
        Get user by ID.

        Args:
            user_id: User ID

        Returns:
            User or None if not found
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, username, email, password_hash, is_active, created_at, updated_at
                    FROM users
                    WHERE id = $1
                    """,
                    user_id
                )

                if not row:
                    logger.debug("user_not_found", user_id=str(user_id))
                    return None

                return UserDB(
                    id=row["id"],
                    username=row["username"],
                    email=row["email"],
                    password_hash=row["password_hash"],
                    is_active=row["is_active"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"]
                )

        except Exception as e:
            logger.error("user_get_by_id_failed", error=str(e), user_id=str(user_id))
            raise

    async def get_user_by_username(self, username: str) -> Optional[UserDB]:
        """
        Get user by username.

        Args:
            username: Username

        Returns:
            User or None if not found
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, username, email, password_hash, is_active, created_at, updated_at
                    FROM users
                    WHERE username = $1
                    """,
                    username
                )

                if not row:
                    logger.debug("user_not_found", username=username)
                    return None

                return UserDB(
                    id=row["id"],
                    username=row["username"],
                    email=row["email"],
                    password_hash=row["password_hash"],
                    is_active=row["is_active"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"]
                )

        except Exception as e:
            logger.error("user_get_by_username_failed", error=str(e), username=username)
            raise

    async def get_user_by_email(self, email: str) -> Optional[UserDB]:
        """
        Get user by email.

        Args:
            email: Email address

        Returns:
            User or None if not found
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, username, email, password_hash, is_active, created_at, updated_at
                    FROM users
                    WHERE email = $1
                    """,
                    email
                )

                if not row:
                    logger.debug("user_not_found", email=email)
                    return None

                return UserDB(
                    id=row["id"],
                    username=row["username"],
                    email=row["email"],
                    password_hash=row["password_hash"],
                    is_active=row["is_active"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"]
                )

        except Exception as e:
            logger.error("user_get_by_email_failed", error=str(e), email=email)
            raise

    async def get_user_roles(self, user_id: UUID) -> List[str]:
        """
        Get user roles.

        Args:
            user_id: User ID

        Returns:
            List of role names
        """
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT role
                    FROM user_roles
                    WHERE user_id = $1
                    ORDER BY role
                    """,
                    user_id
                )

                return [row["role"] for row in rows]

        except Exception as e:
            logger.error("user_get_roles_failed", error=str(e), user_id=str(user_id))
            raise

    async def update_user(
        self,
        user_id: UUID,
        email: Optional[str] = None,
        password_hash: Optional[str] = None,
        is_active: Optional[bool] = None,
        roles: Optional[List[str]] = None
    ) -> Optional[UserDB]:
        """
        Update user information.

        Args:
            user_id: User ID
            email: New email (optional)
            password_hash: New password hash (optional)
            is_active: New active status (optional)
            roles: New roles (optional)

        Returns:
            Updated user or None if not found

        Raises:
            ValueError: If email already exists
        """
        try:
            async with self.transaction() as conn:
                updates = []
                params = []
                param_count = 1

                if email is not None:
                    updates.append(f"email = ${param_count}")
                    params.append(email)
                    param_count += 1

                if password_hash is not None:
                    updates.append(f"password_hash = ${param_count}")
                    params.append(password_hash)
                    param_count += 1

                if is_active is not None:
                    updates.append(f"is_active = ${param_count}")
                    params.append(is_active)
                    param_count += 1

                if updates:
                    updates.append(f"updated_at = NOW()")
                    params.append(user_id)

                    try:
                        row = await conn.fetchrow(
                            f"""
                            UPDATE users
                            SET {', '.join(updates)}
                            WHERE id = ${param_count}
                            RETURNING id, username, email, password_hash, is_active, created_at, updated_at
                            """,
                            *params
                        )
                    except asyncpg.UniqueViolationError:
                        logger.warning("email_already_exists", email=email)
                        raise ValueError(f"Email '{email}' already exists")

                    if not row:
                        logger.debug("user_not_found", user_id=str(user_id))
                        return None
                else:
                    row = await conn.fetchrow(
                        """
                        SELECT id, username, email, password_hash, is_active, created_at, updated_at
                        FROM users
                        WHERE id = $1
                        """,
                        user_id
                    )

                    if not row:
                        logger.debug("user_not_found", user_id=str(user_id))
                        return None

                if roles is not None:
                    await conn.execute(
                        """
                        DELETE FROM user_roles
                        WHERE user_id = $1
                        """,
                        user_id
                    )

                    for role in roles:
                        await conn.execute(
                            """
                            INSERT INTO user_roles (user_id, role)
                            VALUES ($1, $2)
                            """,
                            user_id,
                            role
                        )

                logger.info("user_updated", user_id=str(user_id), email=email, roles=roles)

                return UserDB(
                    id=row["id"],
                    username=row["username"],
                    email=row["email"],
                    password_hash=row["password_hash"],
                    is_active=row["is_active"],
                    created_at=row["created_at"],
                    updated_at=row["updated_at"]
                )

        except ValueError:
            raise
        except Exception as e:
            logger.error("user_update_failed", error=str(e), user_id=str(user_id))
            raise

    async def delete_user(self, user_id: UUID) -> bool:
        """
        Delete user (soft delete by setting is_active=False).

        Args:
            user_id: User ID

        Returns:
            True if deleted, False if not found
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    """
                    UPDATE users
                    SET is_active = FALSE, updated_at = NOW()
                    WHERE id = $1
                    """,
                    user_id
                )

                deleted = result.split()[-1] == "1"

                if deleted:
                    logger.info("user_deleted", user_id=str(user_id))
                else:
                    logger.debug("user_not_found", user_id=str(user_id))

                return deleted

        except Exception as e:
            logger.error("user_delete_failed", error=str(e), user_id=str(user_id))
            raise

    async def list_users(
        self,
        limit: int = 100,
        offset: int = 0,
        is_active: Optional[bool] = None
    ) -> List[UserDB]:
        """
        List users with pagination.

        Args:
            limit: Maximum number of users to return
            offset: Number of users to skip
            is_active: Filter by active status (optional)

        Returns:
            List of users
        """
        try:
            async with self.pool.acquire() as conn:
                if is_active is not None:
                    rows = await conn.fetch(
                        """
                        SELECT id, username, email, password_hash, is_active, created_at, updated_at
                        FROM users
                        WHERE is_active = $1
                        ORDER BY created_at DESC
                        LIMIT $2 OFFSET $3
                        """,
                        is_active,
                        limit,
                        offset
                    )
                else:
                    rows = await conn.fetch(
                        """
                        SELECT id, username, email, password_hash, is_active, created_at, updated_at
                        FROM users
                        ORDER BY created_at DESC
                        LIMIT $1 OFFSET $2
                        """,
                        limit,
                        offset
                    )

                return [
                    UserDB(
                        id=row["id"],
                        username=row["username"],
                        email=row["email"],
                        password_hash=row["password_hash"],
                        is_active=row["is_active"],
                        created_at=row["created_at"],
                        updated_at=row["updated_at"]
                    )
                    for row in rows
                ]

        except Exception as e:
            logger.error("user_list_failed", error=str(e), limit=limit, offset=offset)
            raise

    async def count_users(self, is_active: Optional[bool] = None) -> int:
        """
        Count total users.

        Args:
            is_active: Filter by active status (optional)

        Returns:
            Total user count
        """
        try:
            async with self.pool.acquire() as conn:
                if is_active is not None:
                    row = await conn.fetchrow(
                        """
                        SELECT COUNT(*) as count
                        FROM users
                        WHERE is_active = $1
                        """,
                        is_active
                    )
                else:
                    row = await conn.fetchrow(
                        """
                        SELECT COUNT(*) as count
                        FROM users
                        """
                    )

                return row["count"]

        except Exception as e:
            logger.error("user_count_failed", error=str(e))
            raise
