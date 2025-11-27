"""
Dead Letter Queue (DLQ) writer for failed events.

Routes failed, corrupted, or unprocessable events to a dedicated
Kafka topic for later analysis and replay.
"""

import asyncio
import json
import logging
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional
from dataclasses import dataclass, asdict
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError


logger = logging.getLogger(__name__)


class DLQReason(str, Enum):
    """Reasons for routing events to DLQ"""
    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    CORRUPTED_DATA = "corrupted_data"
    SCHEMA_VALIDATION_FAILED = "schema_validation_failed"
    OVERSIZED_DOCUMENT = "oversized_document"
    INVALID_BSON = "invalid_bson"
    TRANSFORMATION_ERROR = "transformation_error"
    MINIO_ERROR = "minio_error"
    DELTA_WRITE_ERROR = "delta_write_error"
    UNHANDLED_EXCEPTION = "unhandled_exception"


@dataclass
class DLQEvent:
    """Dead Letter Queue event structure"""
    original_event: Any
    reason: str
    error_message: str
    timestamp: str
    source_topic: str
    partition: int
    offset: int
    retry_count: int = 0
    metadata: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return asdict(self)

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())


class DLQWriter:
    """
    Dead Letter Queue writer for failed events.

    Sends failed events to a dedicated Kafka topic with detailed
    error information for debugging and replay.
    """

    def __init__(
        self,
        dlq_topic: str,
        bootstrap_servers: list,
        max_batch_size: int = 100,
        flush_interval_seconds: int = 10,
        fallback_file: Optional[Path] = None
    ):
        self.dlq_topic = dlq_topic
        self.bootstrap_servers = bootstrap_servers
        self.max_batch_size = max_batch_size
        self.flush_interval_seconds = flush_interval_seconds
        self.fallback_file = fallback_file

        # Kafka producer (lazy initialization)
        self._producer: Optional[KafkaProducer] = None

        # Metrics
        self.metrics = {
            "total_dlq_events": 0,
            "dlq_events_by_reason": {},
            "dlq_write_failures": 0,
            "fallback_writes": 0,
            "last_write_timestamp": None
        }

        # Rate limiter
        self._rate_limiter = {
            "max_events_per_minute": 10000,
            "current_count": 0,
            "window_start": datetime.utcnow()
        }

    def _initialize_producer(self):
        """Initialize Kafka producer"""
        if self._producer is None:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1,
                    compression_type='snappy'
                )
                logger.info(
                    f"DLQ producer initialized for topic {self.dlq_topic}",
                    extra={"dlq_topic": self.dlq_topic}
                )
            except Exception as e:
                logger.error(f"Failed to initialize DLQ producer: {e}")
                raise

    async def write(
        self,
        original_event: Any,
        reason: DLQReason,
        error_message: str,
        source_topic: str,
        partition: int,
        offset: int,
        retry_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Write a single event to DLQ.

        Args:
            original_event: The original failed event
            reason: Reason for DLQ routing
            error_message: Error message describing the failure
            source_topic: Original Kafka topic
            partition: Original Kafka partition
            offset: Original Kafka offset
            retry_count: Number of retries attempted
            metadata: Additional metadata
        """
        # Check rate limit
        if not self._check_rate_limit():
            logger.warning(
                "DLQ rate limit exceeded, dropping event",
                extra={
                    "source_topic": source_topic,
                    "partition": partition,
                    "offset": offset
                }
            )
            return

        # Create DLQ event
        dlq_event = DLQEvent(
            original_event=original_event,
            reason=reason.value if isinstance(reason, DLQReason) else reason,
            error_message=error_message,
            timestamp=datetime.utcnow().isoformat(),
            source_topic=source_topic,
            partition=partition,
            offset=offset,
            retry_count=retry_count,
            metadata=metadata or {}
        )

        try:
            # Initialize producer if needed
            if self._producer is None:
                self._initialize_producer()

            # Determine key for partitioning
            key = f"{source_topic}:{partition}:{offset}"

            # Send to DLQ topic
            self._producer.send(
                topic=self.dlq_topic,
                value=dlq_event.to_dict(),
                key=key
            )

            # Update metrics
            self._update_metrics(reason)

            logger.warning(
                f"Event sent to DLQ: {reason.value}",
                extra={
                    "dlq_topic": self.dlq_topic,
                    "source_topic": source_topic,
                    "partition": partition,
                    "offset": offset,
                    "reason": reason.value,
                    "retry_count": retry_count
                }
            )

        except Exception as e:
            self.metrics["dlq_write_failures"] += 1
            logger.error(f"Failed to write to DLQ: {e}")

            # Fallback to file if configured
            if self.fallback_file:
                await self._write_to_fallback(dlq_event)

    async def write_batch(self, dlq_events: list):
        """
        Write multiple events to DLQ.

        Args:
            dlq_events: List of DLQEvent objects
        """
        if not dlq_events:
            return

        try:
            # Initialize producer if needed
            if self._producer is None:
                self._initialize_producer()

            # Send all events
            for dlq_event in dlq_events:
                key = f"{dlq_event.source_topic}:{dlq_event.partition}:{dlq_event.offset}"

                self._producer.send(
                    topic=self.dlq_topic,
                    value=dlq_event.to_dict(),
                    key=key
                )

                self._update_metrics(dlq_event.reason)

            logger.info(
                f"Sent {len(dlq_events)} events to DLQ",
                extra={
                    "dlq_topic": self.dlq_topic,
                    "event_count": len(dlq_events)
                }
            )

        except Exception as e:
            self.metrics["dlq_write_failures"] += len(dlq_events)
            logger.error(f"Failed to write batch to DLQ: {e}")

    async def flush(self):
        """Flush pending DLQ events"""
        if self._producer:
            try:
                self._producer.flush()
                logger.debug("DLQ producer flushed")
            except Exception as e:
                logger.error(f"Failed to flush DLQ producer: {e}")

    async def shutdown(self):
        """Shutdown DLQ writer"""
        logger.info("Shutting down DLQ writer...")

        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
                logger.info("DLQ producer closed")
            except Exception as e:
                logger.error(f"Error shutting down DLQ producer: {e}")

        logger.info(
            "DLQ writer shutdown complete",
            extra={
                "total_dlq_events": self.metrics["total_dlq_events"],
                "dlq_write_failures": self.metrics["dlq_write_failures"]
            }
        )

    def _check_rate_limit(self) -> bool:
        """Check if DLQ write is within rate limit"""
        now = datetime.utcnow()

        # Reset counter if new window
        elapsed = (now - self._rate_limiter["window_start"]).total_seconds()
        if elapsed >= 60:
            self._rate_limiter["current_count"] = 0
            self._rate_limiter["window_start"] = now

        # Check limit
        if self._rate_limiter["current_count"] >= self._rate_limiter["max_events_per_minute"]:
            return False

        self._rate_limiter["current_count"] += 1
        return True

    def _update_metrics(self, reason: str):
        """Update DLQ metrics"""
        self.metrics["total_dlq_events"] += 1
        self.metrics["last_write_timestamp"] = datetime.utcnow().isoformat()

        # Track by reason
        if reason not in self.metrics["dlq_events_by_reason"]:
            self.metrics["dlq_events_by_reason"][reason] = 0
        self.metrics["dlq_events_by_reason"][reason] += 1

    async def _write_to_fallback(self, dlq_event: DLQEvent):
        """Write to fallback file when Kafka is unavailable"""
        if not self.fallback_file:
            return

        try:
            # Create fallback directory if needed
            self.fallback_file.parent.mkdir(parents=True, exist_ok=True)

            # Append to fallback file
            with open(self.fallback_file, 'a') as f:
                f.write(dlq_event.to_json() + '\n')

            self.metrics["fallback_writes"] += 1

            logger.info(
                f"Event written to fallback file: {self.fallback_file}",
                extra={
                    "fallback_file": str(self.fallback_file),
                    "source_topic": dlq_event.source_topic,
                    "offset": dlq_event.offset
                }
            )

        except Exception as e:
            logger.error(f"Failed to write to fallback file: {e}")

    def get_metrics(self) -> Dict[str, Any]:
        """Get DLQ metrics"""
        return self.metrics.copy()
