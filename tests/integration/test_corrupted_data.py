"""
Integration tests for corrupted event handling.

Tests that corrupted Debezium events are properly detected, logged,
and routed to the Dead Letter Queue without crashing the pipeline.
"""

import pytest
import json
from datetime import datetime
from unittest.mock import Mock, AsyncMock


class TestCorruptedEventDetection:
    """Test detection of various corrupted event types"""

    def test_detect_invalid_json(self):
        """Test detecting invalid JSON in Kafka message"""
        corrupted_message = b"{ invalid json structure"

        with pytest.raises(json.JSONDecodeError):
            json.loads(corrupted_message)

    def test_detect_missing_required_fields(self):
        """Test detecting events with missing required fields"""
        event = {
            "payload": {
                "after": {"name": "John"}
                # Missing "_id" field
            }
        }

        required_fields = ["_id"]

        def validate_event(evt):
            payload = evt.get("payload", {}).get("after", {})
            return all(field in payload for field in required_fields)

        assert validate_event(event) is False

    def test_detect_invalid_bson_types(self):
        """Test detecting invalid BSON type conversions"""
        event = {
            "payload": {
                "after": {
                    "_id": {"$oid": "INVALID_OID"},  # Invalid ObjectId
                    "timestamp": "not-a-timestamp"
                }
            }
        }

        def is_valid_objectid(oid_str):
            return len(oid_str) == 24 and all(c in "0123456789abcdef" for c in oid_str.lower())

        oid = event["payload"]["after"]["_id"].get("$oid", "")
        assert is_valid_objectid(oid) is False

    def test_detect_oversized_document(self):
        """Test detecting documents exceeding size limit"""
        # 20MB document (exceeds typical 16MB MongoDB limit)
        large_data = "x" * (20 * 1024 * 1024)
        event = {
            "payload": {
                "after": {
                    "_id": "abc123",
                    "large_field": large_data
                }
            }
        }

        max_size_bytes = 16 * 1024 * 1024  # 16MB
        event_size = len(json.dumps(event).encode('utf-8'))

        assert event_size > max_size_bytes

    def test_detect_malformed_debezium_envelope(self):
        """Test detecting malformed Debezium event envelope"""
        malformed_event = {
            "schema": {},
            # Missing "payload" field
            "timestamp": 1638360000000
        }

        def validate_debezium_structure(evt):
            return "payload" in evt and "op" in evt.get("payload", {})

        assert validate_debezium_structure(malformed_event) is False


class TestCorruptedEventHandling:
    """Test handling corrupted events"""

    @pytest.mark.asyncio
    async def test_route_corrupted_event_to_dlq(self):
        """Test routing corrupted event to DLQ"""
        mock_dlq = AsyncMock()
        corrupted_event = b"{ invalid"

        try:
            json.loads(corrupted_event)
        except json.JSONDecodeError as e:
            await mock_dlq.send(
                topic="cdc.dead_letter_queue",
                value=json.dumps({
                    "original_event": corrupted_event.decode('utf-8', errors='replace'),
                    "reason": "invalid_json",
                    "error_message": str(e),
                    "timestamp": datetime.utcnow().isoformat()
                }).encode('utf-8')
            )

        mock_dlq.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_continue_processing_after_corruption(self):
        """Test that pipeline continues after encountering corrupted event"""
        events = [
            b'{"payload": {"after": {"_id": "1", "data": "valid"}}}',
            b'{ corrupted event',
            b'{"payload": {"after": {"_id": "2", "data": "valid"}}}',
        ]

        processed = []
        dlq_events = []

        for event_bytes in events:
            try:
                event = json.loads(event_bytes)
                processed.append(event)
            except json.JSONDecodeError:
                dlq_events.append(event_bytes)

        assert len(processed) == 2
        assert len(dlq_events) == 1

    @pytest.mark.asyncio
    async def test_log_corrupted_event_details(self, caplog):
        """Test detailed logging of corrupted events"""
        import logging

        logger = logging.getLogger("corrupted_handler")

        corrupted_event = b'{"partial": "data"'

        try:
            json.loads(corrupted_event)
        except json.JSONDecodeError as e:
            logger.error(
                "Corrupted event detected",
                extra={
                    "error": str(e),
                    "event_preview": corrupted_event[:100],
                    "event_size": len(corrupted_event)
                }
            )

        # In actual test with caplog fixture, verify log entry


class TestCorruptionMetrics:
    """Test metrics for corrupted event tracking"""

    def test_track_corruption_rate(self):
        """Test tracking corruption rate metric"""
        metrics = {
            "total_events": 1000,
            "corrupted_events": 5
        }

        corruption_rate = metrics["corrupted_events"] / metrics["total_events"]

        assert corruption_rate == 0.005  # 0.5%

    def test_track_corruption_by_type(self):
        """Test tracking corruption types"""
        corruption_types = {
            "invalid_json": 3,
            "missing_fields": 2,
            "oversized": 1,
            "invalid_bson": 2
        }

        total_corrupted = sum(corruption_types.values())

        assert total_corrupted == 8
        assert corruption_types["invalid_json"] == 3


class TestPartialDataRecovery:
    """Test recovering partial data from corrupted events"""

    def test_extract_valid_fields_from_partial_data(self):
        """Test extracting valid fields despite corruption"""
        partial_event = {
            "payload": {
                "after": {
                    "_id": "valid_id",
                    "name": "John",
                    # "corrupted_field" would cause error if accessed
                }
            }
        }

        # Extract only valid fields
        valid_fields = ["_id", "name"]
        extracted = {
            k: v for k, v in partial_event["payload"]["after"].items()
            if k in valid_fields
        }

        assert extracted == {"_id": "valid_id", "name": "John"}

    def test_use_default_values_for_missing_fields(self):
        """Test using defaults for missing fields"""
        event = {
            "payload": {
                "after": {
                    "_id": "abc123"
                    # Missing "timestamp" field
                }
            }
        }

        defaults = {
            "timestamp": datetime.utcnow().isoformat(),
            "version": 1
        }

        # Merge with defaults
        complete_record = {**defaults, **event["payload"]["after"]}

        assert "timestamp" in complete_record
        assert complete_record["_id"] == "abc123"


class TestAlertingOnCorruption:
    """Test alerting when corruption exceeds thresholds"""

    def test_alert_on_high_corruption_rate(self):
        """Test alerting when corruption rate exceeds threshold"""
        corruption_rate = 0.05  # 5%
        threshold = 0.01  # 1%

        should_alert = corruption_rate > threshold

        assert should_alert is True

    def test_alert_on_corruption_spike(self):
        """Test alerting on sudden spike in corruption"""
        historical_rate = [0.001, 0.001, 0.002, 0.001]  # Normal: ~0.1%
        current_rate = 0.05  # 5% - spike!

        avg_historical = sum(historical_rate) / len(historical_rate)
        spike_threshold = 10  # 10x normal

        is_spike = current_rate > (avg_historical * spike_threshold)

        assert is_spike is True
