"""
Unit tests for audit logging functionality.

Tests cover:
- Audit log entry creation
- Capturing user actions (create, update, delete)
- Audit log fields (user_id, action, resource, timestamp, details)
- Sensitive data masking in audit logs
- Audit log serialization and storage
"""

import pytest
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List
from enum import Enum
import json


# ============================================================================
# AUDIT LOG MODELS AND UTILITIES (Test Doubles)
# ============================================================================


class AuditAction(str, Enum):
    """Types of actions that can be audited."""

    # Authentication actions
    LOGIN = "login"
    LOGOUT = "logout"
    LOGIN_FAILED = "login_failed"

    # CRUD actions
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"

    # Administrative actions
    USER_CREATED = "user_created"
    USER_UPDATED = "user_updated"
    USER_DELETED = "user_deleted"
    ROLE_ASSIGNED = "role_assigned"
    ROLE_REVOKED = "role_revoked"

    # Operational actions
    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_STOPPED = "pipeline_stopped"
    PIPELINE_PAUSED = "pipeline_paused"
    PIPELINE_RESUMED = "pipeline_resumed"
    SYNC_TRIGGERED = "sync_triggered"


class ResourceType(str, Enum):
    """Types of resources that can be audited."""

    USER = "user"
    MAPPING = "mapping"
    CHECKPOINT = "checkpoint"
    PIPELINE = "pipeline"
    SYSTEM = "system"


class AuditLogEntry:
    """Audit log entry model."""

    def __init__(
        self,
        user_id: str,
        action: AuditAction,
        resource_type: ResourceType,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ):
        """
        Initialize audit log entry.

        Args:
            user_id: ID of user performing the action
            action: Action being performed
            resource_type: Type of resource being acted upon
            resource_id: ID of specific resource (optional)
            details: Additional details about the action
            timestamp: When the action occurred (defaults to now)
            ip_address: IP address of the user (optional)
            user_agent: User agent string (optional)
        """
        self.user_id = user_id
        self.action = action
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.details = details or {}
        self.timestamp = timestamp or datetime.now(timezone.utc)
        self.ip_address = ip_address
        self.user_agent = user_agent

    def to_dict(self) -> Dict[str, Any]:
        """Convert audit log entry to dictionary."""
        return {
            "user_id": self.user_id,
            "action": self.action.value if isinstance(self.action, AuditAction) else self.action,
            "resource_type": self.resource_type.value if isinstance(self.resource_type, ResourceType) else self.resource_type,
            "resource_id": self.resource_id,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
            "ip_address": self.ip_address,
            "user_agent": self.user_agent,
        }

    def to_json(self) -> str:
        """Convert audit log entry to JSON string."""
        return json.dumps(self.to_dict())


# Sensitive fields that should be masked in audit logs
SENSITIVE_FIELDS = {"password", "secret", "token", "api_key", "private_key", "credit_card"}


def mask_sensitive_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mask sensitive data in dictionary.

    Args:
        data: Dictionary that may contain sensitive data

    Returns:
        Dictionary with sensitive fields masked
    """
    masked_data = {}

    for key, value in data.items():
        key_lower = key.lower()

        # Check if key contains sensitive field names
        is_sensitive = any(sensitive in key_lower for sensitive in SENSITIVE_FIELDS)

        if is_sensitive:
            masked_data[key] = "***MASKED***"
        elif isinstance(value, dict):
            # Recursively mask nested dictionaries
            masked_data[key] = mask_sensitive_data(value)
        elif isinstance(value, list):
            # Handle lists of dictionaries
            masked_data[key] = [
                mask_sensitive_data(item) if isinstance(item, dict) else item
                for item in value
            ]
        else:
            masked_data[key] = value

    return masked_data


def create_audit_log(
    user_id: str,
    action: AuditAction,
    resource_type: ResourceType,
    resource_id: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
    mask_sensitive: bool = True,
) -> AuditLogEntry:
    """
    Create audit log entry with automatic sensitive data masking.

    Args:
        user_id: ID of user performing the action
        action: Action being performed
        resource_type: Type of resource being acted upon
        resource_id: ID of specific resource
        details: Additional details about the action
        mask_sensitive: Whether to mask sensitive data

    Returns:
        AuditLogEntry instance
    """
    # Mask sensitive data in details if enabled
    if mask_sensitive and details:
        details = mask_sensitive_data(details)

    return AuditLogEntry(
        user_id=user_id,
        action=action,
        resource_type=resource_type,
        resource_id=resource_id,
        details=details,
    )


# ============================================================================
# UNIT TESTS
# ============================================================================


class TestAuditLogEntryCreation:
    """Tests for creating audit log entries."""

    def test_create_basic_audit_log_entry(self):
        """Test creating basic audit log entry."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.LOGIN,
            resource_type=ResourceType.SYSTEM,
        )

        assert entry.user_id == "user-123"
        assert entry.action == AuditAction.LOGIN
        assert entry.resource_type == ResourceType.SYSTEM
        assert entry.resource_id is None
        assert entry.details == {}
        assert isinstance(entry.timestamp, datetime)

    def test_create_audit_log_with_resource_id(self):
        """Test creating audit log entry with resource ID."""
        entry = AuditLogEntry(
            user_id="user-456",
            action=AuditAction.UPDATE,
            resource_type=ResourceType.MAPPING,
            resource_id="mapping-789",
        )

        assert entry.user_id == "user-456"
        assert entry.action == AuditAction.UPDATE
        assert entry.resource_type == ResourceType.MAPPING
        assert entry.resource_id == "mapping-789"

    def test_create_audit_log_with_details(self):
        """Test creating audit log entry with additional details."""
        details = {
            "field_updated": "name",
            "old_value": "old_name",
            "new_value": "new_name",
        }

        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.UPDATE,
            resource_type=ResourceType.USER,
            resource_id="user-456",
            details=details,
        )

        assert entry.details == details
        assert entry.details["field_updated"] == "name"

    def test_create_audit_log_with_custom_timestamp(self):
        """Test creating audit log entry with custom timestamp."""
        custom_time = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)

        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.CREATE,
            resource_type=ResourceType.MAPPING,
            timestamp=custom_time,
        )

        assert entry.timestamp == custom_time

    def test_create_audit_log_with_ip_and_user_agent(self):
        """Test creating audit log entry with IP address and user agent."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.LOGIN,
            resource_type=ResourceType.SYSTEM,
            ip_address="192.168.1.100",
            user_agent="Mozilla/5.0",
        )

        assert entry.ip_address == "192.168.1.100"
        assert entry.user_agent == "Mozilla/5.0"

    def test_audit_log_timestamp_defaults_to_now(self):
        """Test audit log timestamp defaults to current time."""
        before = datetime.now(timezone.utc)
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.READ,
            resource_type=ResourceType.CHECKPOINT,
        )
        after = datetime.now(timezone.utc)

        assert before <= entry.timestamp <= after


class TestAuditActionTypes:
    """Tests for different audit action types."""

    def test_audit_login_action(self):
        """Test auditing login action."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.LOGIN,
            resource_type=ResourceType.SYSTEM,
            details={"username": "admin", "success": True},
        )

        assert entry.action == AuditAction.LOGIN
        assert entry.details["success"] is True

    def test_audit_failed_login_action(self):
        """Test auditing failed login action."""
        entry = create_audit_log(
            user_id="anonymous",
            action=AuditAction.LOGIN_FAILED,
            resource_type=ResourceType.SYSTEM,
            details={"username": "admin", "reason": "invalid_password"},
        )

        assert entry.action == AuditAction.LOGIN_FAILED
        assert entry.details["reason"] == "invalid_password"

    def test_audit_create_action(self):
        """Test auditing create action."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.CREATE,
            resource_type=ResourceType.MAPPING,
            resource_id="mapping-new",
            details={"name": "new_mapping", "source": "mongodb.db.collection"},
        )

        assert entry.action == AuditAction.CREATE
        assert entry.resource_id == "mapping-new"

    def test_audit_update_action(self):
        """Test auditing update action."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.UPDATE,
            resource_type=ResourceType.USER,
            resource_id="user-456",
            details={"updated_fields": ["email", "roles"]},
        )

        assert entry.action == AuditAction.UPDATE
        assert "email" in entry.details["updated_fields"]

    def test_audit_delete_action(self):
        """Test auditing delete action."""
        entry = create_audit_log(
            user_id="admin-123",
            action=AuditAction.DELETE,
            resource_type=ResourceType.MAPPING,
            resource_id="mapping-old",
            details={"reason": "deprecated"},
        )

        assert entry.action == AuditAction.DELETE
        assert entry.details["reason"] == "deprecated"

    def test_audit_pipeline_actions(self):
        """Test auditing pipeline operations."""
        actions = [
            AuditAction.PIPELINE_STARTED,
            AuditAction.PIPELINE_STOPPED,
            AuditAction.PIPELINE_PAUSED,
            AuditAction.PIPELINE_RESUMED,
        ]

        for action in actions:
            entry = create_audit_log(
                user_id="operator-123",
                action=action,
                resource_type=ResourceType.PIPELINE,
                resource_id="pipeline-main",
            )

            assert entry.action == action
            assert entry.resource_type == ResourceType.PIPELINE


class TestSensitiveDataMasking:
    """Tests for masking sensitive data in audit logs."""

    def test_mask_password_field(self):
        """Test password field is masked."""
        data = {"username": "admin", "password": "secret123"}

        masked = mask_sensitive_data(data)

        assert masked["username"] == "admin"
        assert masked["password"] == "***MASKED***"

    def test_mask_token_field(self):
        """Test token field is masked."""
        data = {"user_id": "123", "access_token": "jwt-token-xyz"}

        masked = mask_sensitive_data(data)

        assert masked["user_id"] == "123"
        assert masked["access_token"] == "***MASKED***"

    def test_mask_api_key_field(self):
        """Test API key field is masked."""
        data = {"service": "external", "api_key": "key-abc-123"}

        masked = mask_sensitive_data(data)

        assert masked["service"] == "external"
        assert masked["api_key"] == "***MASKED***"

    def test_mask_multiple_sensitive_fields(self):
        """Test multiple sensitive fields are masked."""
        data = {
            "username": "admin",
            "password": "pass123",
            "api_key": "key-xyz",
            "email": "admin@example.com",
        }

        masked = mask_sensitive_data(data)

        assert masked["username"] == "admin"
        assert masked["password"] == "***MASKED***"
        assert masked["api_key"] == "***MASKED***"
        assert masked["email"] == "admin@example.com"

    def test_mask_nested_sensitive_fields(self):
        """Test sensitive fields in nested dictionaries are masked."""
        data = {
            "user": {
                "username": "admin",
                "credentials": {"password": "secret", "api_key": "key-123"},
            }
        }

        masked = mask_sensitive_data(data)

        assert masked["user"]["username"] == "admin"
        assert masked["user"]["credentials"]["password"] == "***MASKED***"
        assert masked["user"]["credentials"]["api_key"] == "***MASKED***"

    def test_mask_sensitive_fields_in_list(self):
        """Test sensitive fields in lists are masked."""
        data = {
            "users": [
                {"username": "user1", "password": "pass1"},
                {"username": "user2", "password": "pass2"},
            ]
        }

        masked = mask_sensitive_data(data)

        assert masked["users"][0]["username"] == "user1"
        assert masked["users"][0]["password"] == "***MASKED***"
        assert masked["users"][1]["username"] == "user2"
        assert masked["users"][1]["password"] == "***MASKED***"

    def test_case_insensitive_masking(self):
        """Test masking is case insensitive."""
        data = {"PASSWORD": "secret", "Api_Key": "key-123", "SECRET": "value"}

        masked = mask_sensitive_data(data)

        assert masked["PASSWORD"] == "***MASKED***"
        assert masked["Api_Key"] == "***MASKED***"
        assert masked["SECRET"] == "***MASKED***"

    def test_partial_match_masking(self):
        """Test fields containing sensitive keywords are masked."""
        data = {"user_password": "secret", "temp_token": "xyz", "my_api_key": "key"}

        masked = mask_sensitive_data(data)

        assert masked["user_password"] == "***MASKED***"
        assert masked["temp_token"] == "***MASKED***"
        assert masked["my_api_key"] == "***MASKED***"


class TestAuditLogWithMasking:
    """Tests for creating audit logs with automatic masking."""

    def test_create_audit_log_masks_password(self):
        """Test creating audit log automatically masks password."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.USER_UPDATED,
            resource_type=ResourceType.USER,
            resource_id="user-456",
            details={"password": "newpass123", "email": "new@example.com"},
        )

        assert entry.details["password"] == "***MASKED***"
        assert entry.details["email"] == "new@example.com"

    def test_create_audit_log_without_masking(self):
        """Test creating audit log without masking when disabled."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.USER_UPDATED,
            resource_type=ResourceType.USER,
            resource_id="user-456",
            details={"password": "newpass123", "email": "new@example.com"},
            mask_sensitive=False,
        )

        # Password should NOT be masked
        assert entry.details["password"] == "newpass123"
        assert entry.details["email"] == "new@example.com"

    def test_audit_log_masks_login_credentials(self):
        """Test audit log masks login credentials."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.LOGIN,
            resource_type=ResourceType.SYSTEM,
            details={"username": "admin", "password": "secret123"},
        )

        assert entry.details["username"] == "admin"
        assert entry.details["password"] == "***MASKED***"


class TestAuditLogSerialization:
    """Tests for audit log serialization."""

    def test_to_dict_conversion(self):
        """Test converting audit log entry to dictionary."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.CREATE,
            resource_type=ResourceType.MAPPING,
            resource_id="mapping-456",
        )

        log_dict = entry.to_dict()

        assert log_dict["user_id"] == "user-123"
        assert log_dict["action"] == "create"
        assert log_dict["resource_type"] == "mapping"
        assert log_dict["resource_id"] == "mapping-456"
        assert "timestamp" in log_dict

    def test_to_json_conversion(self):
        """Test converting audit log entry to JSON."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.UPDATE,
            resource_type=ResourceType.USER,
            details={"field": "email"},
        )

        log_json = entry.to_json()

        # Should be valid JSON
        parsed = json.loads(log_json)
        assert parsed["user_id"] == "user-123"
        assert parsed["action"] == "update"
        assert parsed["details"]["field"] == "email"

    def test_timestamp_serialization(self):
        """Test timestamp is properly serialized to ISO format."""
        timestamp = datetime(2025, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.READ,
            resource_type=ResourceType.CHECKPOINT,
            timestamp=timestamp,
        )

        log_dict = entry.to_dict()

        assert log_dict["timestamp"] == "2025-01-15T10:30:00+00:00"

    def test_optional_fields_in_serialization(self):
        """Test optional fields are included in serialization."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.LOGIN,
            resource_type=ResourceType.SYSTEM,
            ip_address="192.168.1.1",
            user_agent="Mozilla/5.0",
        )

        log_dict = entry.to_dict()

        assert log_dict["ip_address"] == "192.168.1.1"
        assert log_dict["user_agent"] == "Mozilla/5.0"


class TestAuditLogFields:
    """Tests for audit log field validation."""

    def test_audit_log_has_required_fields(self):
        """Test audit log entry has all required fields."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.CREATE,
            resource_type=ResourceType.MAPPING,
        )

        assert hasattr(entry, "user_id")
        assert hasattr(entry, "action")
        assert hasattr(entry, "resource_type")
        assert hasattr(entry, "timestamp")

    def test_user_id_field(self):
        """Test user_id field is properly set."""
        entry = AuditLogEntry(
            user_id="admin-456",
            action=AuditAction.DELETE,
            resource_type=ResourceType.USER,
        )

        assert entry.user_id == "admin-456"

    def test_action_field(self):
        """Test action field is properly set."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.SYNC_TRIGGERED,
            resource_type=ResourceType.PIPELINE,
        )

        assert entry.action == AuditAction.SYNC_TRIGGERED

    def test_resource_type_field(self):
        """Test resource_type field is properly set."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.READ,
            resource_type=ResourceType.CHECKPOINT,
        )

        assert entry.resource_type == ResourceType.CHECKPOINT

    def test_details_field_defaults_to_empty_dict(self):
        """Test details field defaults to empty dictionary."""
        entry = AuditLogEntry(
            user_id="user-123",
            action=AuditAction.LOGIN,
            resource_type=ResourceType.SYSTEM,
        )

        assert entry.details == {}
        assert isinstance(entry.details, dict)


class TestResourceTypes:
    """Tests for different resource types in audit logs."""

    def test_user_resource_type(self):
        """Test auditing actions on user resources."""
        entry = create_audit_log(
            user_id="admin-123",
            action=AuditAction.USER_CREATED,
            resource_type=ResourceType.USER,
            resource_id="user-new",
        )

        assert entry.resource_type == ResourceType.USER

    def test_mapping_resource_type(self):
        """Test auditing actions on mapping resources."""
        entry = create_audit_log(
            user_id="operator-123",
            action=AuditAction.UPDATE,
            resource_type=ResourceType.MAPPING,
            resource_id="mapping-123",
        )

        assert entry.resource_type == ResourceType.MAPPING

    def test_pipeline_resource_type(self):
        """Test auditing actions on pipeline resources."""
        entry = create_audit_log(
            user_id="operator-123",
            action=AuditAction.PIPELINE_STARTED,
            resource_type=ResourceType.PIPELINE,
            resource_id="pipeline-main",
        )

        assert entry.resource_type == ResourceType.PIPELINE

    def test_system_resource_type(self):
        """Test auditing system-level actions."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.LOGIN,
            resource_type=ResourceType.SYSTEM,
        )

        assert entry.resource_type == ResourceType.SYSTEM


class TestAuditLogEdgeCases:
    """Tests for edge cases in audit logging."""

    def test_empty_details_dictionary(self):
        """Test audit log with empty details dictionary."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.READ,
            resource_type=ResourceType.MAPPING,
            details={},
        )

        assert entry.details == {}

    def test_none_details(self):
        """Test audit log with None details."""
        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.READ,
            resource_type=ResourceType.MAPPING,
            details=None,
        )

        assert entry.details == {}

    def test_complex_nested_details(self):
        """Test audit log with complex nested details."""
        details = {
            "changes": {
                "user": {
                    "roles": {"added": ["admin"], "removed": ["analyst"]},
                    "profile": {"email": "new@example.com"},
                }
            }
        }

        entry = create_audit_log(
            user_id="admin-123",
            action=AuditAction.USER_UPDATED,
            resource_type=ResourceType.USER,
            resource_id="user-456",
            details=details,
        )

        assert entry.details["changes"]["user"]["roles"]["added"] == ["admin"]

    def test_long_user_id(self):
        """Test audit log with very long user ID."""
        long_user_id = "user-" + "x" * 1000

        entry = create_audit_log(
            user_id=long_user_id,
            action=AuditAction.READ,
            resource_type=ResourceType.MAPPING,
        )

        assert entry.user_id == long_user_id

    def test_special_characters_in_details(self):
        """Test audit log with special characters in details."""
        details = {
            "query": "SELECT * FROM users WHERE name = 'O\\'Brien'",
            "path": "/usr/local/bin",
            "message": "Line 1\nLine 2\tTabbed",
        }

        entry = create_audit_log(
            user_id="user-123",
            action=AuditAction.READ,
            resource_type=ResourceType.SYSTEM,
            details=details,
        )

        # Should handle special characters
        log_json = entry.to_json()
        parsed = json.loads(log_json)

        assert parsed["details"]["query"] == details["query"]
        assert parsed["details"]["message"] == details["message"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])