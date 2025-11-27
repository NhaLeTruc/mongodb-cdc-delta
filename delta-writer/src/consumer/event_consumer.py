"""Kafka consumer with exactly-once semantics for CDC events."""

from typing import Dict, Any, Optional, Callable
import signal
import time
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import structlog

from .event_handler import EventHandler
from ..writer.batch_processor import PerCollectionBatchProcessor
from ..writer.delta_writer import DeltaWriter

logger = structlog.get_logger(__name__)


class EventConsumer:
    """Kafka consumer for processing Debezium CDC events."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic_pattern: str,
        consumer_group: str,
        delta_writer: DeltaWriter,
        batch_size: int = 2000,
        batch_timeout_seconds: float = 10.0,
        auto_offset_reset: str = "earliest",
        max_poll_records: int = 2000,
        session_timeout_ms: int = 30000,
    ):
        """
        Initialize Kafka consumer.

        Args:
            bootstrap_servers: Kafka broker addresses
            topic_pattern: Topic pattern to subscribe (e.g., "mongodb.*")
            consumer_group: Consumer group ID
            delta_writer: DeltaWriter instance
            batch_size: Records per batch
            batch_timeout_seconds: Batch timeout
            auto_offset_reset: Offset reset policy
            max_poll_records: Max records per poll
            session_timeout_ms: Session timeout
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic_pattern = topic_pattern
        self.consumer_group = consumer_group
        self.delta_writer = delta_writer
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds

        self.event_handler = EventHandler()
        self.batch_processor: Optional[PerCollectionBatchProcessor] = None
        self.consumer: Optional[KafkaConsumer] = None
        self.running = False

        self.consumer_config = {
            "bootstrap_servers": bootstrap_servers.split(','),
            "group_id": consumer_group,
            "auto_offset_reset": auto_offset_reset,
            "enable_auto_commit": False,
            "max_poll_records": max_poll_records,
            "session_timeout_ms": session_timeout_ms,
            "isolation_level": "read_committed",
            "value_deserializer": lambda m: m,
        }

        self._setup_signal_handlers()

    def _setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)

    def _signal_handler(self, signum, frame) -> None:
        """Handle shutdown signals."""
        logger.info("shutdown_signal_received", signal=signum)
        self.shutdown()

    def start(self) -> None:
        """Start the consumer and begin processing events."""
        logger.info(
            "consumer_starting",
            bootstrap_servers=self.bootstrap_servers,
            topic_pattern=self.topic_pattern,
            consumer_group=self.consumer_group
        )

        try:
            self.consumer = KafkaConsumer(**self.consumer_config)
            self.consumer.subscribe(pattern=self.topic_pattern)

            def flush_batch(collection: str, records: list) -> None:
                self._write_batch_to_delta(collection, records)

            self.batch_processor = PerCollectionBatchProcessor(
                batch_size=self.batch_size,
                batch_timeout_seconds=self.batch_timeout_seconds,
                flush_callback=flush_batch
            )

            self.running = True
            logger.info("consumer_started")

            self._consume_loop()

        except Exception as e:
            logger.error("consumer_start_failed", error=str(e))
            raise

    def _consume_loop(self) -> None:
        """Main consume loop."""
        consecutive_errors = 0
        max_consecutive_errors = 10

        while self.running:
            try:
                messages = self.consumer.poll(timeout_ms=1000, max_records=self.batch_size)

                if not messages:
                    continue

                for topic_partition, records in messages.items():
                    for message in records:
                        self._process_message(message)

                self.consumer.commit()
                consecutive_errors = 0

            except KafkaError as e:
                consecutive_errors += 1
                logger.error(
                    "kafka_error",
                    error=str(e),
                    consecutive_errors=consecutive_errors
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.error("max_consecutive_errors_reached", stopping=True)
                    self.shutdown()
                    break

                time.sleep(min(consecutive_errors * 2, 30))

            except Exception as e:
                consecutive_errors += 1
                logger.error(
                    "consume_loop_error",
                    error=str(e),
                    consecutive_errors=consecutive_errors
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.error("max_consecutive_errors_reached", stopping=True)
                    self.shutdown()
                    break

                time.sleep(min(consecutive_errors * 2, 30))

    def _process_message(self, message: Any) -> None:
        """
        Process a single Kafka message.

        Args:
            message: Kafka message
        """
        try:
            record = self.event_handler.process_event(
                kafka_message=message,
                kafka_offset=message.offset,
                kafka_partition=message.partition,
                kafka_topic=message.topic
            )

            if not record:
                return

            value = message.value
            if isinstance(value, bytes):
                import json
                event = json.loads(value.decode('utf-8'))
            elif isinstance(value, str):
                import json
                event = json.loads(value)
            else:
                event = value

            collection = self.event_handler.get_collection_name(event)
            if not collection:
                logger.warning("no_collection_name", offset=message.offset)
                return

            self.batch_processor.add_record(collection, record)

        except Exception as e:
            logger.error(
                "message_processing_failed",
                error=str(e),
                topic=message.topic,
                partition=message.partition,
                offset=message.offset
            )
            raise

    def _write_batch_to_delta(self, collection: str, records: list) -> None:
        """
        Write a batch of records to Delta Lake.

        Args:
            collection: Collection name
            records: List of records
        """
        try:
            table_uri = self._get_table_uri(collection)
            stats = self.delta_writer.write_batch(table_uri, records)
            logger.info(
                "batch_written",
                collection=collection,
                table_uri=table_uri,
                **stats
            )
        except Exception as e:
            logger.error(
                "batch_write_failed",
                collection=collection,
                num_records=len(records),
                error=str(e)
            )
            raise

    def _get_table_uri(self, collection: str) -> str:
        """
        Get Delta table URI for a collection.

        Args:
            collection: Collection name (e.g., "mydb.users")

        Returns:
            Delta table URI
        """
        table_name = collection.replace('.', '_')
        base_path = self.delta_writer.storage_options.get(
            'table_base_path',
            's3://lakehouse/tables'
        )
        return f"{base_path}/{table_name}"

    def shutdown(self) -> None:
        """Shutdown consumer gracefully."""
        if not self.running:
            return

        logger.info("consumer_shutting_down")
        self.running = False

        if self.batch_processor:
            logger.info("flushing_remaining_batches")
            self.batch_processor.flush_all()
            self.batch_processor.stop_all()

        if self.consumer:
            logger.info("committing_final_offsets")
            try:
                self.consumer.commit()
            except Exception as e:
                logger.error("final_commit_failed", error=str(e))

            logger.info("closing_kafka_consumer")
            self.consumer.close()

        logger.info("consumer_shutdown_complete")

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get consumer metrics.

        Returns:
            Dictionary with metrics
        """
        metrics = {
            "event_handler": self.event_handler.get_metrics(),
        }

        if self.batch_processor:
            metrics["batch_processors"] = self.batch_processor.get_all_metrics()

        return metrics
