"""Contract tests for Debezium MongoDB change event schema.

Tests verify that Debezium change events conform to the expected schema
for insert, update, and delete operations.
"""

import json
from datetime import datetime
from typing import Any, Dict

import pytest


class TestDebeziumChangeEventSchema:
    """Test Debezium MongoDB change event schema compliance."""

    @pytest.fixture
    def sample_insert_event(self) -> Dict[str, Any]:
        """Sample Debezium insert event."""
        return {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": True, "field": "_id"},
                    {"type": "string", "optional": False, "field": "op"},
                    {"type": "int64", "optional": False, "field": "ts_ms"},
                ],
                "optional": False,
                "name": "mongodb.testdb.users.Envelope",
            },
            "payload": {
                "_id": "507f1f77bcf86cd799439011",
                "after": {
                    "_id": "507f1f77bcf86cd799439011",
                    "name": "Alice Johnson",
                    "email": "alice@example.com",
                    "age": 30,
                    "created_at": "2024-01-15T10:30:00Z",
                },
                "op": "c",  # create/insert
                "ts_ms": 1705318200000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "mongodb",
                    "name": "mongodb",
                    "ts_ms": 1705318200000,
                    "snapshot": "false",
                    "db": "testdb",
                    "sequence": None,
                    "rs": "rs0",
                    "collection": "users",
                    "ord": 1,
                    "h": None,
                },
            },
        }

    @pytest.fixture
    def sample_update_event(self) -> Dict[str, Any]:
        """Sample Debezium update event."""
        return {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": True, "field": "_id"},
                    {"type": "string", "optional": False, "field": "op"},
                    {"type": "int64", "optional": False, "field": "ts_ms"},
                ],
                "optional": False,
                "name": "mongodb.testdb.users.Envelope",
            },
            "payload": {
                "_id": "507f1f77bcf86cd799439011",
                "after": {
                    "_id": "507f1f77bcf86cd799439011",
                    "name": "Alice Johnson",
                    "email": "alice.updated@example.com",
                    "age": 31,
                    "updated_at": "2024-01-20T14:45:00Z",
                },
                "op": "u",  # update
                "ts_ms": 1705764300000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "mongodb",
                    "name": "mongodb",
                    "ts_ms": 1705764300000,
                    "snapshot": "false",
                    "db": "testdb",
                    "sequence": None,
                    "rs": "rs0",
                    "collection": "users",
                    "ord": 2,
                    "h": None,
                },
            },
        }

    @pytest.fixture
    def sample_delete_event(self) -> Dict[str, Any]:
        """Sample Debezium delete event."""
        return {
            "schema": {
                "type": "struct",
                "fields": [
                    {"type": "string", "optional": True, "field": "_id"},
                    {"type": "string", "optional": False, "field": "op"},
                    {"type": "int64", "optional": False, "field": "ts_ms"},
                ],
                "optional": False,
                "name": "mongodb.testdb.users.Envelope",
            },
            "payload": {
                "_id": "507f1f77bcf86cd799439011",
                "before": {
                    "_id": "507f1f77bcf86cd799439011",
                },
                "op": "d",  # delete
                "ts_ms": 1705850700000,
                "source": {
                    "version": "2.5.0.Final",
                    "connector": "mongodb",
                    "name": "mongodb",
                    "ts_ms": 1705850700000,
                    "snapshot": "false",
                    "db": "testdb",
                    "sequence": None,
                    "rs": "rs0",
                    "collection": "users",
                    "ord": 3,
                    "h": None,
                },
            },
        }

    def test_insert_event_has_required_fields(
        self, sample_insert_event: Dict[str, Any]
    ) -> None:
        """Test that insert event contains all required fields."""
        payload = sample_insert_event["payload"]

        assert "_id" in payload
        assert "after" in payload
        assert "op" in payload
        assert payload["op"] == "c"
        assert "ts_ms" in payload
        assert "source" in payload

    def test_insert_event_after_field_contains_document(
        self, sample_insert_event: Dict[str, Any]
    ) -> None:
        """Test that after field contains the inserted document."""
        after = sample_insert_event["payload"]["after"]

        assert "_id" in after
        assert "name" in after
        assert "email" in after
        assert after["name"] == "Alice Johnson"

    def test_update_event_has_required_fields(
        self, sample_update_event: Dict[str, Any]
    ) -> None:
        """Test that update event contains all required fields."""
        payload = sample_update_event["payload"]

        assert "_id" in payload
        assert "after" in payload
        assert "op" in payload
        assert payload["op"] == "u"
        assert "ts_ms" in payload
        assert "source" in payload

    def test_update_event_after_field_contains_updated_document(
        self, sample_update_event: Dict[str, Any]
    ) -> None:
        """Test that after field contains the updated document."""
        after = sample_update_event["payload"]["after"]

        assert "_id" in after
        assert "email" in after
        assert after["email"] == "alice.updated@example.com"

    def test_delete_event_has_required_fields(
        self, sample_delete_event: Dict[str, Any]
    ) -> None:
        """Test that delete event contains all required fields."""
        payload = sample_delete_event["payload"]

        assert "_id" in payload
        assert "op" in payload
        assert payload["op"] == "d"
        assert "ts_ms" in payload
        assert "source" in payload

    def test_delete_event_before_field_contains_id(
        self, sample_delete_event: Dict[str, Any]
    ) -> None:
        """Test that before field contains at least the document ID."""
        before = sample_delete_event["payload"]["before"]

        assert "_id" in before
        assert before["_id"] == "507f1f77bcf86cd799439011"

    def test_source_metadata_has_required_fields(
        self, sample_insert_event: Dict[str, Any]
    ) -> None:
        """Test that source metadata contains required fields."""
        source = sample_insert_event["payload"]["source"]

        assert "connector" in source
        assert source["connector"] == "mongodb"
        assert "db" in source
        assert "collection" in source
        assert "rs" in source  # replica set name
        assert "ts_ms" in source

    def test_timestamp_is_unix_milliseconds(
        self, sample_insert_event: Dict[str, Any]
    ) -> None:
        """Test that timestamp is in Unix milliseconds format."""
        ts_ms = sample_insert_event["payload"]["ts_ms"]

        assert isinstance(ts_ms, int)
        assert ts_ms > 0
        # Should be reasonable timestamp (after 2020)
        assert ts_ms > 1577836800000  # 2020-01-01

    def test_operation_type_is_valid(
        self, sample_insert_event: Dict[str, Any], sample_update_event: Dict[str, Any],
        sample_delete_event: Dict[str, Any]
    ) -> None:
        """Test that operation types are valid."""
        valid_ops = {"c", "u", "d", "r"}  # create, update, delete, read (snapshot)

        assert sample_insert_event["payload"]["op"] in valid_ops
        assert sample_update_event["payload"]["op"] in valid_ops
        assert sample_delete_event["payload"]["op"] in valid_ops

    def test_event_can_be_serialized_to_json(
        self, sample_insert_event: Dict[str, Any]
    ) -> None:
        """Test that event can be serialized to JSON."""
        json_str = json.dumps(sample_insert_event)
        assert json_str is not None

        # Should be deserializable
        deserialized = json.loads(json_str)
        assert deserialized == sample_insert_event
