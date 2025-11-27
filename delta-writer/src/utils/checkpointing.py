"""
Kafka offset checkpointing for exactly-once semantics.

Manages checkpoint storage, recovery, and atomic commit operations
to ensure no data loss during crashes or restarts.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict
from kafka import TopicPartition


logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """Checkpoint for a single topic partition"""
    topic: str
    partition: int
    offset: int
    timestamp: str
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert checkpoint to dictionary"""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Checkpoint':
        """Create checkpoint from dictionary"""
        return cls(**data)


class CheckpointStorage:
    """Abstract base for checkpoint storage backends"""

    async def save(self, checkpoints: Dict[str, Checkpoint]):
        """Save checkpoints"""
        raise NotImplementedError

    async def load(self) -> Dict[str, Checkpoint]:
        """Load checkpoints"""
        raise NotImplementedError

    async def clear(self):
        """Clear all checkpoints"""
        raise NotImplementedError


class FileCheckpointStorage(CheckpointStorage):
    """File-based checkpoint storage with atomic writes"""

    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_file = self.checkpoint_dir / "checkpoints.json"
        self.temp_file = self.checkpoint_dir / "checkpoints.json.tmp"

        # Create directory if it doesn't exist
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    async def save(self, checkpoints: Dict[str, Checkpoint]):
        """Save checkpoints atomically"""
        # Convert checkpoints to serializable format
        data = {
            key: checkpoint.to_dict()
            for key, checkpoint in checkpoints.items()
        }

        try:
            # Write to temp file first
            with open(self.temp_file, 'w') as f:
                json.dump(data, f, indent=2)

            # Atomic rename
            os.replace(self.temp_file, self.checkpoint_file)

            logger.debug(
                f"Saved {len(checkpoints)} checkpoints to {self.checkpoint_file}",
                extra={"checkpoint_count": len(checkpoints)}
            )

        except Exception as e:
            logger.error(f"Failed to save checkpoints: {e}")
            raise

    async def load(self) -> Dict[str, Checkpoint]:
        """Load checkpoints from file"""
        if not self.checkpoint_file.exists():
            logger.info("No checkpoint file found, starting fresh")
            return {}

        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)

            checkpoints = {
                key: Checkpoint.from_dict(value)
                for key, value in data.items()
            }

            logger.info(
                f"Loaded {len(checkpoints)} checkpoints from {self.checkpoint_file}",
                extra={"checkpoint_count": len(checkpoints)}
            )

            return checkpoints

        except json.JSONDecodeError as e:
            logger.error(f"Corrupted checkpoint file: {e}, starting from default strategy")
            return {}
        except Exception as e:
            logger.error(f"Failed to load checkpoints: {e}")
            return {}

    async def clear(self):
        """Clear checkpoint file"""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            logger.info("Cleared checkpoint file")


class InMemoryCheckpointStorage(CheckpointStorage):
    """In-memory checkpoint storage (fallback for testing)"""

    def __init__(self):
        self._checkpoints: Dict[str, Checkpoint] = {}

    async def save(self, checkpoints: Dict[str, Checkpoint]):
        """Save checkpoints to memory"""
        self._checkpoints = checkpoints.copy()
        logger.debug(f"Saved {len(checkpoints)} checkpoints to memory")

    async def load(self) -> Dict[str, Checkpoint]:
        """Load checkpoints from memory"""
        return self._checkpoints.copy()

    async def clear(self):
        """Clear in-memory checkpoints"""
        self._checkpoints.clear()


class CheckpointManager:
    """
    Manages Kafka offset checkpoints for crash recovery.

    Provides atomic checkpoint commits, recovery from crashes,
    and multi-partition offset tracking.
    """

    def __init__(
        self,
        consumer_group: str,
        storage: CheckpointStorage,
        commit_interval_seconds: int = 30,
        enable_auto_commit: bool = False
    ):
        self.consumer_group = consumer_group
        self.storage = storage
        self.commit_interval_seconds = commit_interval_seconds
        self.enable_auto_commit = enable_auto_commit

        # In-memory checkpoint cache
        self._checkpoints: Dict[str, Checkpoint] = {}
        self._last_commit_time = datetime.utcnow()
        self._pending_checkpoints: Dict[str, Checkpoint] = {}

        # Metrics
        self.metrics = {
            "checkpoints_committed": 0,
            "checkpoints_loaded": 0,
            "checkpoint_failures": 0,
            "last_commit_timestamp": None
        }

    async def initialize(self):
        """Initialize checkpoint manager and load existing checkpoints"""
        logger.info(f"Initializing CheckpointManager for consumer group: {self.consumer_group}")

        # Load existing checkpoints
        self._checkpoints = await self.storage.load()
        self.metrics["checkpoints_loaded"] = len(self._checkpoints)

        logger.info(
            f"CheckpointManager initialized with {len(self._checkpoints)} checkpoints",
            extra={
                "consumer_group": self.consumer_group,
                "checkpoint_count": len(self._checkpoints)
            }
        )

    def get_checkpoint(self, topic: str, partition: int) -> Optional[Checkpoint]:
        """Get checkpoint for topic/partition"""
        key = self._make_key(topic, partition)
        return self._checkpoints.get(key)

    def get_offset(self, topic: str, partition: int) -> Optional[int]:
        """Get offset for topic/partition"""
        checkpoint = self.get_checkpoint(topic, partition)
        return checkpoint.offset if checkpoint else None

    def update_checkpoint(
        self,
        topic: str,
        partition: int,
        offset: int,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Update checkpoint for topic/partition"""
        key = self._make_key(topic, partition)

        checkpoint = Checkpoint(
            topic=topic,
            partition=partition,
            offset=offset,
            timestamp=datetime.utcnow().isoformat(),
            metadata=metadata
        )

        self._pending_checkpoints[key] = checkpoint

        logger.debug(
            f"Updated checkpoint for {topic}:{partition} to offset {offset}",
            extra={
                "topic": topic,
                "partition": partition,
                "offset": offset
            }
        )

    async def commit(self, force: bool = False):
        """
        Commit pending checkpoints to storage.

        Args:
            force: Force commit even if interval hasn't elapsed
        """
        # Check if we should commit
        elapsed = (datetime.utcnow() - self._last_commit_time).total_seconds()
        should_commit = force or elapsed >= self.commit_interval_seconds

        if not should_commit or not self._pending_checkpoints:
            return

        try:
            # Merge pending checkpoints into current checkpoints
            self._checkpoints.update(self._pending_checkpoints)

            # Save to storage
            await self.storage.save(self._checkpoints)

            # Update metrics
            self.metrics["checkpoints_committed"] += len(self._pending_checkpoints)
            self.metrics["last_commit_timestamp"] = datetime.utcnow().isoformat()
            self._last_commit_time = datetime.utcnow()

            logger.info(
                f"Committed {len(self._pending_checkpoints)} checkpoints",
                extra={
                    "checkpoint_count": len(self._pending_checkpoints),
                    "total_checkpoints": len(self._checkpoints)
                }
            )

            # Clear pending checkpoints
            self._pending_checkpoints.clear()

        except Exception as e:
            self.metrics["checkpoint_failures"] += 1
            logger.error(f"Failed to commit checkpoints: {e}")
            raise

    async def commit_single(self, topic: str, partition: int, offset: int):
        """Commit a single checkpoint immediately"""
        self.update_checkpoint(topic, partition, offset)
        await self.commit(force=True)

    async def get_start_offsets(self, partitions: list) -> Dict[TopicPartition, int]:
        """
        Get start offsets for partitions from checkpoints.

        Args:
            partitions: List of TopicPartition objects

        Returns:
            Dict mapping TopicPartition to offset
        """
        start_offsets = {}

        for tp in partitions:
            checkpoint = self.get_checkpoint(tp.topic, tp.partition)
            if checkpoint:
                # Resume from checkpoint offset + 1
                start_offsets[tp] = checkpoint.offset + 1
                logger.info(
                    f"Resuming {tp.topic}:{tp.partition} from checkpoint offset {checkpoint.offset}",
                    extra={
                        "topic": tp.topic,
                        "partition": tp.partition,
                        "offset": checkpoint.offset
                    }
                )
            else:
                logger.info(
                    f"No checkpoint found for {tp.topic}:{tp.partition}, using default strategy",
                    extra={"topic": tp.topic, "partition": tp.partition}
                )

        return start_offsets

    async def shutdown(self):
        """Shutdown checkpoint manager and commit pending checkpoints"""
        logger.info("Shutting down CheckpointManager...")

        # Commit any pending checkpoints
        if self._pending_checkpoints:
            await self.commit(force=True)

        logger.info(
            "CheckpointManager shutdown complete",
            extra={
                "final_checkpoint_count": len(self._checkpoints),
                "total_commits": self.metrics["checkpoints_committed"]
            }
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get checkpoint metrics"""
        return {
            **self.metrics,
            "current_checkpoint_count": len(self._checkpoints),
            "pending_checkpoint_count": len(self._pending_checkpoints)
        }

    def _make_key(self, topic: str, partition: int) -> str:
        """Create key for topic/partition"""
        return f"{topic}:{partition}"
