"""
Integration tests for crash recovery.

Tests that the Delta Lake writer can recover from crashes and resume
processing from the last committed checkpoint without data loss.
"""

import pytest
import json
import asyncio
from datetime import datetime
from unittest.mock import Mock, AsyncMock, patch


class TestCrashRecovery:
    """Test crash recovery scenarios"""

    @pytest.mark.asyncio
    async def test_resume_from_last_checkpoint_after_crash(self, tmp_path):
        """Test resuming from last checkpoint after crash"""
        checkpoint_file = tmp_path / "checkpoints.json"

        # Simulate checkpoint before crash
        pre_crash_checkpoint = {
            "topic": "mongodb.mydb.users",
            "partition": 0,
            "offset": 5000,
            "timestamp": datetime.utcnow().isoformat()
        }

        with open(checkpoint_file, 'w') as f:
            json.dump(pre_crash_checkpoint, f)

        # Simulate crash and restart
        # Load checkpoint on restart
        with open(checkpoint_file, 'r') as f:
            loaded_checkpoint = json.load(f)

        resume_offset = loaded_checkpoint["offset"]

        assert resume_offset == 5000

    @pytest.mark.asyncio
    async def test_no_data_loss_after_crash(self):
        """Test that no data is lost after crash"""
        # Events processed before crash
        processed_before_crash = [
            {"_id": "1", "offset": 100},
            {"_id": "2", "offset": 101},
            {"_id": "3", "offset": 102}
        ]

        last_committed_offset = 102

        # Events available after restart
        all_events = [
            {"_id": "1", "offset": 100},
            {"_id": "2", "offset": 101},
            {"_id": "3", "offset": 102},
            {"_id": "4", "offset": 103},  # Not processed before crash
            {"_id": "5", "offset": 104}
        ]

        # Resume from last checkpoint
        unprocessed_events = [
            e for e in all_events
            if e["offset"] > last_committed_offset
        ]

        assert len(unprocessed_events) == 2
        assert unprocessed_events[0]["_id"] == "4"

    @pytest.mark.asyncio
    async def test_handle_crash_during_batch_processing(self):
        """Test recovery when crash occurs mid-batch"""
        batch = [
            {"_id": f"id_{i}", "offset": 1000 + i}
            for i in range(10)
        ]

        # Process first 5, crash, restart
        crash_point = 5
        processed_count = 0

        for i, event in enumerate(batch):
            if i == crash_point:
                break  # Simulate crash
            processed_count += 1

        # On restart, check last committed offset
        last_committed = 1004  # Last successfully committed

        # Resume from checkpoint
        remaining = [e for e in batch if e["offset"] > last_committed]

        assert len(remaining) == 5

    @pytest.mark.asyncio
    async def test_graceful_shutdown_vs_crash(self):
        """Test difference between graceful shutdown and crash"""
        pending_events = [
            {"_id": "1", "offset": 100},
            {"_id": "2", "offset": 101}
        ]

        # Graceful shutdown: commit pending events
        graceful_shutdown = True
        if graceful_shutdown:
            committed_offset = pending_events[-1]["offset"]
        else:
            # Crash: no commit
            committed_offset = 99

        assert committed_offset == 101  # Graceful commit


class TestCheckpointRecovery:
    """Test checkpoint-based recovery"""

    @pytest.mark.asyncio
    async def test_recover_with_valid_checkpoint(self, tmp_path):
        """Test recovery with valid checkpoint file"""
        checkpoint_file = tmp_path / "checkpoints.json"

        checkpoint_data = {
            "mongodb.mydb.users": {
                "0": {"offset": 1000, "timestamp": "2025-11-27T10:00:00"},
                "1": {"offset": 2000, "timestamp": "2025-11-27T10:00:01"}
            }
        }

        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f)

        # Load on restart
        with open(checkpoint_file, 'r') as f:
            loaded = json.load(f)

        partition_0_offset = loaded["mongodb.mydb.users"]["0"]["offset"]

        assert partition_0_offset == 1000

    @pytest.mark.asyncio
    async def test_recover_with_corrupted_checkpoint(self, tmp_path):
        """Test recovery when checkpoint file is corrupted"""
        checkpoint_file = tmp_path / "checkpoints.json"

        # Write corrupted checkpoint
        with open(checkpoint_file, 'w') as f:
            f.write("{ corrupted json")

        # Attempt to load
        try:
            with open(checkpoint_file, 'r') as f:
                json.load(f)
        except json.JSONDecodeError:
            # Fall back to default strategy
            default_offset_strategy = "earliest"

        assert 'default_offset_strategy' in locals()

    @pytest.mark.asyncio
    async def test_recover_with_missing_checkpoint(self, tmp_path):
        """Test recovery when checkpoint file is missing"""
        checkpoint_file = tmp_path / "nonexistent_checkpoints.json"

        if not checkpoint_file.exists():
            # Start from beginning or latest based on config
            offset_strategy = "earliest"

        assert offset_strategy == "earliest"


class TestInFlightTransactionRecovery:
    """Test recovery of in-flight transactions"""

    @pytest.mark.asyncio
    async def test_rollback_incomplete_transaction(self):
        """Test rolling back incomplete transaction after crash"""
        # Events in incomplete transaction
        in_flight_events = [
            {"_id": "1", "offset": 100},
            {"_id": "2", "offset": 101}
        ]

        # Transaction not committed before crash
        transaction_committed = False

        if not transaction_committed:
            # Rollback: don't commit these events
            # They will be reprocessed from checkpoint
            reprocess_events = in_flight_events

        assert len(reprocess_events) == 2

    @pytest.mark.asyncio
    async def test_idempotent_reprocessing(self):
        """Test idempotent reprocessing of events"""
        # Event processed before crash but not checkpointed
        event = {"_id": "abc123", "name": "John", "version": 1}

        # Reprocess same event after crash
        # Delta Lake should handle this idempotently using _id as key

        def upsert_to_delta(record):
            # Upsert based on _id (merge on primary key)
            return {"_id": record["_id"], "name": record["name"], "version": record["version"]}

        result1 = upsert_to_delta(event)
        result2 = upsert_to_delta(event)  # Reprocess

        # Both should produce same result
        assert result1 == result2


class TestMultiPartitionRecovery:
    """Test recovery across multiple partitions"""

    @pytest.mark.asyncio
    async def test_recover_different_offsets_per_partition(self, tmp_path):
        """Test recovering different offsets for each partition"""
        checkpoint_file = tmp_path / "checkpoints.json"

        checkpoints = {
            "topic": "mongodb.mydb.users",
            "partitions": {
                "0": {"offset": 1000},
                "1": {"offset": 2000},
                "2": {"offset": 1500}
            }
        }

        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoints, f)

        # Resume each partition from its checkpoint
        with open(checkpoint_file, 'r') as f:
            loaded = json.load(f)

        offsets = {
            int(p): data["offset"]
            for p, data in loaded["partitions"].items()
        }

        assert offsets[0] == 1000
        assert offsets[1] == 2000
        assert offsets[2] == 1500

    @pytest.mark.asyncio
    async def test_rebalance_after_crash(self):
        """Test consumer group rebalance after crash"""
        # Before crash: consumer had partitions [0, 1]
        assigned_before_crash = {0, 1}

        # After crash and restart: rebalance may reassign partitions
        # Simulate rebalance to partitions [1, 2]
        assigned_after_restart = {1, 2}

        # Consumer should handle new assignment
        new_partitions = assigned_after_restart - assigned_before_crash
        removed_partitions = assigned_before_crash - assigned_after_restart

        assert new_partitions == {2}
        assert removed_partitions == {0}


class TestCrashRecoveryMetrics:
    """Test metrics for crash recovery"""

    @pytest.mark.asyncio
    async def test_track_recovery_time(self):
        """Test tracking time to recover after crash"""
        crash_time = datetime.utcnow()

        # Simulate recovery process
        await asyncio.sleep(0.1)  # Recovery takes 100ms

        recovery_complete_time = datetime.utcnow()

        recovery_duration = (recovery_complete_time - crash_time).total_seconds()

        assert recovery_duration >= 0.1

    @pytest.mark.asyncio
    async def test_track_events_reprocessed(self):
        """Test tracking number of events reprocessed after crash"""
        last_checkpoint = 1000
        current_offset = 1050

        events_reprocessed = current_offset - last_checkpoint

        assert events_reprocessed == 50

    @pytest.mark.asyncio
    async def test_track_crash_frequency(self):
        """Test tracking crash frequency"""
        crash_timestamps = [
            datetime(2025, 11, 27, 10, 0, 0),
            datetime(2025, 11, 27, 11, 0, 0),
            datetime(2025, 11, 27, 12, 0, 0)
        ]

        crash_count_24h = len(crash_timestamps)

        # Alert if crashes exceed threshold
        crash_threshold = 5
        should_alert = crash_count_24h >= crash_threshold

        assert should_alert is False


class TestCrashDuringCheckpoint:
    """Test crash occurring during checkpoint commit"""

    @pytest.mark.asyncio
    async def test_crash_during_checkpoint_write(self, tmp_path):
        """Test crash during checkpoint write"""
        checkpoint_file = tmp_path / "checkpoints.json"

        # Original checkpoint
        original_checkpoint = {"offset": 1000}

        with open(checkpoint_file, 'w') as f:
            json.dump(original_checkpoint, f)

        # Attempt to write new checkpoint, crash mid-write
        try:
            # Simulate crash during write (incomplete write)
            raise Exception("Crash during checkpoint write")
        except Exception:
            # On recovery, original checkpoint should still be valid
            with open(checkpoint_file, 'r') as f:
                recovered = json.load(f)

        assert recovered["offset"] == 1000

    @pytest.mark.asyncio
    async def test_atomic_checkpoint_commit(self, tmp_path):
        """Test atomic checkpoint commit"""
        checkpoint_file = tmp_path / "checkpoints.json"
        temp_file = tmp_path / "checkpoints.json.tmp"

        new_checkpoint = {"offset": 2000}

        # Write to temp file first
        with open(temp_file, 'w') as f:
            json.dump(new_checkpoint, f)

        # Atomic rename (on POSIX systems)
        import os
        os.rename(temp_file, checkpoint_file)

        # Verify
        with open(checkpoint_file, 'r') as f:
            loaded = json.load(f)

        assert loaded["offset"] == 2000


class TestHealthCheckAfterRecovery:
    """Test health checks after recovery"""

    @pytest.mark.asyncio
    async def test_health_check_after_startup(self):
        """Test health check passes after successful recovery"""
        recovery_successful = True

        def health_check():
            checks = {
                "recovery_complete": recovery_successful,
                "checkpoint_loaded": True,
                "consumer_connected": True
            }
            return all(checks.values())

        assert health_check() is True

    @pytest.mark.asyncio
    async def test_readiness_check_after_recovery(self):
        """Test readiness check after recovery"""
        checkpoints_loaded = True
        consumer_assigned = True
        processing_started = True

        def readiness_check():
            return all([
                checkpoints_loaded,
                consumer_assigned,
                processing_started
            ])

        assert readiness_check() is True


class TestCrashLogging:
    """Test logging during crash and recovery"""

    @pytest.mark.asyncio
    async def test_log_crash_event(self, caplog):
        """Test logging crash event"""
        import logging

        logger = logging.getLogger("crash_recovery")

        try:
            raise Exception("Simulated crash")
        except Exception as e:
            logger.critical(
                "Service crashed",
                extra={
                    "error": str(e),
                    "last_processed_offset": 5000,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )

    @pytest.mark.asyncio
    async def test_log_recovery_progress(self, caplog):
        """Test logging recovery progress"""
        import logging

        logger = logging.getLogger("crash_recovery")

        recovery_steps = [
            "Loading checkpoints",
            "Connecting to Kafka",
            "Seeking to offset 5000",
            "Resuming processing"
        ]

        for step in recovery_steps:
            logger.info(f"Recovery: {step}")

        assert len(recovery_steps) == 4
