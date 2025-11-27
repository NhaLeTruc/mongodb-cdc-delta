"""Main entry point for Delta Writer service."""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "shared"))

import structlog
from shared.logging.structured_logger import configure_logging

from config import get_config
from consumer.event_consumer import EventConsumer
from writer.delta_writer import DeltaWriter

logger = structlog.get_logger(__name__)


def build_storage_options(config) -> dict:
    """
    Build storage options for Delta Lake from config.

    Args:
        config: Configuration object

    Returns:
        Dictionary with storage options
    """
    endpoint_url = config.minio.endpoint
    if not endpoint_url.startswith('http'):
        endpoint_url = f"http://{endpoint_url}"

    storage_options = {
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_ACCESS_KEY_ID": config.minio.access_key,
        "AWS_SECRET_ACCESS_KEY": config.minio.secret_key,
        "AWS_REGION": config.minio.region,
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        "AWS_STORAGE_ALLOW_HTTP": "true" if not config.minio.secure else "false",
        "table_base_path": config.delta.table_base_path,
    }

    return storage_options


def main():
    """Main entry point."""
    config = get_config()

    configure_logging(
        log_level=config.log_level,
        json_logs=True,
        service_name=config.service_name
    )

    logger.info(
        "delta_writer_starting",
        service_name=config.service_name,
        kafka_brokers=config.kafka.bootstrap_servers,
        minio_endpoint=config.minio.endpoint,
        delta_base_path=config.delta.table_base_path
    )

    try:
        storage_options = build_storage_options(config)

        delta_writer = DeltaWriter(
            storage_options=storage_options,
            partition_by=config.delta.partition_by or ["_ingestion_date"],
            schema_cache_ttl=config.delta.schema_cache_ttl
        )

        consumer = EventConsumer(
            bootstrap_servers=config.kafka.bootstrap_servers,
            topic_pattern=config.kafka.topic_pattern,
            consumer_group=config.kafka.consumer_group,
            delta_writer=delta_writer,
            batch_size=config.delta.batch_size,
            batch_timeout_seconds=config.delta.batch_timeout_ms / 1000.0,
            auto_offset_reset=config.kafka.auto_offset_reset,
            max_poll_records=config.kafka.max_poll_records,
            session_timeout_ms=config.kafka.session_timeout_ms,
        )

        logger.info("starting_consumer")
        consumer.start()

    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_received")
    except Exception as e:
        logger.error("fatal_error", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
