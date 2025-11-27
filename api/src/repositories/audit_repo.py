"""
Audit log repository for database operations.

Provides async CRUD operations for audit logs using asyncpg with PostgreSQL.
Includes efficient querying, filtering, and retention management.
"""

import asyncpg
import structlog
from typing import List, Optional, Dict, Any, Tuple
from uuid import UUID
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

from api.src.models.audit import AuditLogDB, AuditAction, ResourceType, AuditLogFilter
from api.src.config import get_settings

logger = structlog.get_logger(__name__)


class AuditRepository:
    """Repository for audit log database operations."""

    def __init__(self, pool: asyncpg.Pool):
        """
        Initialize audit repository.

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

    async def create_audit_log(
        self,
        user_id: Optional[UUID],
        action: str,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        status_code: Optional[int] = None
    ) -> AuditLogDB:
        """
        Create a new audit log entry.

        Args:
            user_id: User ID (None for anonymous)
            action: Action performed
            resource_type: Type of resource
            resource_id: Resource identifier
            details: Additional details (JSON)
            ip_address: Client IP address
            user_agent: Client user agent
            status_code: HTTP status code

        Returns:
            Created audit log entry
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    INSERT INTO audit_logs (
                        user_id, action, resource_type, resource_id, details,
                        ip_address, user_agent, status_code, timestamp
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
                    RETURNING id, user_id, action, resource_type, resource_id, details,
                              ip_address, user_agent, status_code, timestamp
                    """,
                    user_id,
                    action,
                    resource_type,
                    resource_id,
                    details,
                    ip_address,
                    user_agent,
                    status_code
                )

                logger.debug(
                    "audit_log_created",
                    audit_id=str(row["id"]),
                    user_id=str(user_id) if user_id else None,
                    action=action,
                    resource_type=resource_type
                )

                return AuditLogDB(
                    id=row["id"],
                    user_id=row["user_id"],
                    action=row["action"],
                    resource_type=row["resource_type"],
                    resource_id=row["resource_id"],
                    details=row["details"],
                    ip_address=row["ip_address"],
                    user_agent=row["user_agent"],
                    status_code=row["status_code"],
                    timestamp=row["timestamp"]
                )

        except Exception as e:
            logger.error(
                "audit_log_create_failed",
                error=str(e),
                user_id=str(user_id) if user_id else None,
                action=action
            )
            raise

    async def get_audit_log_by_id(self, audit_id: UUID) -> Optional[AuditLogDB]:
        """
        Get audit log by ID.

        Args:
            audit_id: Audit log ID

        Returns:
            Audit log or None if not found
        """
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, user_id, action, resource_type, resource_id, details,
                           ip_address, user_agent, status_code, timestamp
                    FROM audit_logs
                    WHERE id = $1
                    """,
                    audit_id
                )

                if not row:
                    logger.debug("audit_log_not_found", audit_id=str(audit_id))
                    return None

                return AuditLogDB(
                    id=row["id"],
                    user_id=row["user_id"],
                    action=row["action"],
                    resource_type=row["resource_type"],
                    resource_id=row["resource_id"],
                    details=row["details"],
                    ip_address=row["ip_address"],
                    user_agent=row["user_agent"],
                    status_code=row["status_code"],
                    timestamp=row["timestamp"]
                )

        except Exception as e:
            logger.error("audit_log_get_failed", error=str(e), audit_id=str(audit_id))
            raise

    async def list_audit_logs(self, filter: AuditLogFilter) -> Tuple[List[AuditLogDB], int]:
        """
        List audit logs with filtering and pagination.

        Args:
            filter: Filter parameters

        Returns:
            Tuple of (list of audit logs, total count)
        """
        try:
            async with self.pool.acquire() as conn:
                where_clauses = []
                params = []
                param_count = 1

                if filter.user_id is not None:
                    where_clauses.append(f"user_id = ${param_count}")
                    params.append(filter.user_id)
                    param_count += 1

                if filter.action is not None:
                    where_clauses.append(f"action = ${param_count}")
                    params.append(filter.action.value)
                    param_count += 1

                if filter.resource_type is not None:
                    where_clauses.append(f"resource_type = ${param_count}")
                    params.append(filter.resource_type.value)
                    param_count += 1

                if filter.resource_id is not None:
                    where_clauses.append(f"resource_id = ${param_count}")
                    params.append(filter.resource_id)
                    param_count += 1

                if filter.start_date is not None:
                    where_clauses.append(f"timestamp >= ${param_count}")
                    params.append(filter.start_date)
                    param_count += 1

                if filter.end_date is not None:
                    where_clauses.append(f"timestamp <= ${param_count}")
                    params.append(filter.end_date)
                    param_count += 1

                if filter.ip_address is not None:
                    where_clauses.append(f"ip_address = ${param_count}")
                    params.append(filter.ip_address)
                    param_count += 1

                if filter.status_code is not None:
                    where_clauses.append(f"status_code = ${param_count}")
                    params.append(filter.status_code)
                    param_count += 1

                where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

                count_row = await conn.fetchrow(
                    f"""
                    SELECT COUNT(*) as count
                    FROM audit_logs
                    {where_sql}
                    """,
                    *params
                )
                total_count = count_row["count"]

                params.append(filter.limit)
                limit_param = f"${param_count}"
                param_count += 1

                params.append(filter.offset)
                offset_param = f"${param_count}"

                rows = await conn.fetch(
                    f"""
                    SELECT id, user_id, action, resource_type, resource_id, details,
                           ip_address, user_agent, status_code, timestamp
                    FROM audit_logs
                    {where_sql}
                    ORDER BY timestamp DESC
                    LIMIT {limit_param} OFFSET {offset_param}
                    """,
                    *params
                )

                logs = [
                    AuditLogDB(
                        id=row["id"],
                        user_id=row["user_id"],
                        action=row["action"],
                        resource_type=row["resource_type"],
                        resource_id=row["resource_id"],
                        details=row["details"],
                        ip_address=row["ip_address"],
                        user_agent=row["user_agent"],
                        status_code=row["status_code"],
                        timestamp=row["timestamp"]
                    )
                    for row in rows
                ]

                return logs, total_count

        except Exception as e:
            logger.error("audit_log_list_failed", error=str(e), filter=filter.model_dump())
            raise

    async def get_user_audit_logs(
        self,
        user_id: UUID,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[AuditLogDB], int]:
        """
        Get audit logs for a specific user.

        Args:
            user_id: User ID
            limit: Maximum number of logs to return
            offset: Number of logs to skip

        Returns:
            Tuple of (list of audit logs, total count)
        """
        try:
            async with self.pool.acquire() as conn:
                count_row = await conn.fetchrow(
                    """
                    SELECT COUNT(*) as count
                    FROM audit_logs
                    WHERE user_id = $1
                    """,
                    user_id
                )
                total_count = count_row["count"]

                rows = await conn.fetch(
                    """
                    SELECT id, user_id, action, resource_type, resource_id, details,
                           ip_address, user_agent, status_code, timestamp
                    FROM audit_logs
                    WHERE user_id = $1
                    ORDER BY timestamp DESC
                    LIMIT $2 OFFSET $3
                    """,
                    user_id,
                    limit,
                    offset
                )

                logs = [
                    AuditLogDB(
                        id=row["id"],
                        user_id=row["user_id"],
                        action=row["action"],
                        resource_type=row["resource_type"],
                        resource_id=row["resource_id"],
                        details=row["details"],
                        ip_address=row["ip_address"],
                        user_agent=row["user_agent"],
                        status_code=row["status_code"],
                        timestamp=row["timestamp"]
                    )
                    for row in rows
                ]

                return logs, total_count

        except Exception as e:
            logger.error("user_audit_logs_get_failed", error=str(e), user_id=str(user_id))
            raise

    async def get_resource_audit_logs(
        self,
        resource_type: str,
        resource_id: str,
        limit: int = 100,
        offset: int = 0
    ) -> Tuple[List[AuditLogDB], int]:
        """
        Get audit logs for a specific resource.

        Args:
            resource_type: Resource type
            resource_id: Resource identifier
            limit: Maximum number of logs to return
            offset: Number of logs to skip

        Returns:
            Tuple of (list of audit logs, total count)
        """
        try:
            async with self.pool.acquire() as conn:
                count_row = await conn.fetchrow(
                    """
                    SELECT COUNT(*) as count
                    FROM audit_logs
                    WHERE resource_type = $1 AND resource_id = $2
                    """,
                    resource_type,
                    resource_id
                )
                total_count = count_row["count"]

                rows = await conn.fetch(
                    """
                    SELECT id, user_id, action, resource_type, resource_id, details,
                           ip_address, user_agent, status_code, timestamp
                    FROM audit_logs
                    WHERE resource_type = $1 AND resource_id = $2
                    ORDER BY timestamp DESC
                    LIMIT $3 OFFSET $4
                    """,
                    resource_type,
                    resource_id,
                    limit,
                    offset
                )

                logs = [
                    AuditLogDB(
                        id=row["id"],
                        user_id=row["user_id"],
                        action=row["action"],
                        resource_type=row["resource_type"],
                        resource_id=row["resource_id"],
                        details=row["details"],
                        ip_address=row["ip_address"],
                        user_agent=row["user_agent"],
                        status_code=row["status_code"],
                        timestamp=row["timestamp"]
                    )
                    for row in rows
                ]

                return logs, total_count

        except Exception as e:
            logger.error(
                "resource_audit_logs_get_failed",
                error=str(e),
                resource_type=resource_type,
                resource_id=resource_id
            )
            raise

    async def delete_old_audit_logs(self, retention_days: int) -> int:
        """
        Delete audit logs older than retention period.

        Args:
            retention_days: Number of days to retain logs

        Returns:
            Number of deleted logs
        """
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)

            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    """
                    DELETE FROM audit_logs
                    WHERE timestamp < $1
                    """,
                    cutoff_date
                )

                deleted_count = int(result.split()[-1])

                logger.info(
                    "old_audit_logs_deleted",
                    deleted_count=deleted_count,
                    retention_days=retention_days,
                    cutoff_date=cutoff_date.isoformat()
                )

                return deleted_count

        except Exception as e:
            logger.error("delete_old_audit_logs_failed", error=str(e), retention_days=retention_days)
            raise

    async def get_audit_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get audit log statistics.

        Args:
            start_date: Start date for statistics (optional)
            end_date: End date for statistics (optional)

        Returns:
            Dictionary with statistics
        """
        try:
            async with self.pool.acquire() as conn:
                where_clauses = []
                params = []
                param_count = 1

                if start_date is not None:
                    where_clauses.append(f"timestamp >= ${param_count}")
                    params.append(start_date)
                    param_count += 1

                if end_date is not None:
                    where_clauses.append(f"timestamp <= ${param_count}")
                    params.append(end_date)
                    param_count += 1

                where_sql = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

                total_row = await conn.fetchrow(
                    f"""
                    SELECT COUNT(*) as count
                    FROM audit_logs
                    {where_sql}
                    """,
                    *params
                )

                action_rows = await conn.fetch(
                    f"""
                    SELECT action, COUNT(*) as count
                    FROM audit_logs
                    {where_sql}
                    GROUP BY action
                    ORDER BY count DESC
                    """,
                    *params
                )

                user_rows = await conn.fetch(
                    f"""
                    SELECT user_id, COUNT(*) as count
                    FROM audit_logs
                    {where_sql}
                    GROUP BY user_id
                    ORDER BY count DESC
                    LIMIT 10
                    """,
                    *params
                )

                statistics = {
                    "total_logs": total_row["count"],
                    "actions": {row["action"]: row["count"] for row in action_rows},
                    "top_users": [
                        {"user_id": str(row["user_id"]) if row["user_id"] else None, "count": row["count"]}
                        for row in user_rows
                    ],
                    "start_date": start_date.isoformat() if start_date else None,
                    "end_date": end_date.isoformat() if end_date else None
                }

                return statistics

        except Exception as e:
            logger.error("audit_statistics_failed", error=str(e))
            raise
