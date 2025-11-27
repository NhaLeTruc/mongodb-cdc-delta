"""Process Debezium change events and transform them for Delta Lake."""

from typing import Dict, Any, Optional
from datetime import datetime
import json
import structlog

from ..transformers.bson_to_delta import BSONToDeltaConverter

logger = structlog.get_logger(__name__)


class EventHandler:
    """Handles processing of Debezium CDC events."""

    def __init__(self):
        """Initialize event handler."""
        self.converter = BSONToDeltaConverter()
        self._metrics = {
            "events_processed": 0,
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
        }

    def process_event(
        self,
        kafka_message: Any,
        kafka_offset: int,
        kafka_partition: int,
        kafka_topic: str
    ) -> Optional[Dict[str, Any]]:
        """
        Process a Debezium change event.

        Args:
            kafka_message: Kafka message (with value attribute)
            kafka_offset: Kafka offset
            kafka_partition: Kafka partition
            kafka_topic: Kafka topic

        Returns:
            Transformed record for Delta Lake or None if event should be skipped
        """
        try:
            value = kafka_message.value
            if not value:
                logger.debug("empty_kafka_message_skipped", offset=kafka_offset)
                return None

            if isinstance(value, bytes):
                event = json.loads(value.decode('utf-8'))
            elif isinstance(value, str):
                event = json.loads(value)
            elif isinstance(value, dict):
                event = value
            else:
                logger.error("unsupported_message_type", type=type(value).__name__)
                return None

            operation = self._get_operation(event)
            if not operation:
                logger.debug("unknown_operation_skipped", event=event)
                return None

            record = self._extract_document(event, operation)
            if not record:
                return None

            record = self._add_cdc_metadata(
                record,
                event,
                operation,
                kafka_offset,
                kafka_partition,
                kafka_topic
            )

            self._metrics["events_processed"] += 1
            self._metrics[f"{operation}s"] = self._metrics.get(f"{operation}s", 0) + 1

            logger.debug(
                "event_processed",
                operation=operation,
                offset=kafka_offset,
                partition=kafka_partition
            )

            return record

        except Exception as e:
            self._metrics["errors"] += 1
            logger.error(
                "event_processing_failed",
                error=str(e),
                offset=kafka_offset,
                partition=kafka_partition
            )
            raise

    def _get_operation(self, event: Dict[str, Any]) -> Optional[str]:
        """
        Extract operation type from Debezium event.

        Args:
            event: Debezium change event

        Returns:
            Operation type: 'insert', 'update', or 'delete'
        """
        op = event.get('op')
        if not op:
            payload = event.get('payload', {})
            op = payload.get('op')

        if op == 'c' or op == 'r':
            return 'insert'
        elif op == 'u':
            return 'update'
        elif op == 'd':
            return 'delete'
        else:
            return None

    def _extract_document(
        self,
        event: Dict[str, Any],
        operation: str
    ) -> Optional[Dict[str, Any]]:
        """
        Extract MongoDB document from Debezium event.

        Args:
            event: Debezium change event
            operation: Operation type

        Returns:
            Extracted and converted document
        """
        payload = event.get('payload', {})

        if operation == 'delete':
            before = payload.get('before')
            if before:
                doc = before
            else:
                key = payload.get('key', event.get('key', {}))
                if key:
                    doc = key
                else:
                    logger.warning("delete_event_without_document")
                    return None
        else:
            after = payload.get('after')
            if after:
                doc = after
            else:
                doc = event.get('after', event)

        if not doc:
            logger.warning("no_document_in_event", operation=operation)
            return None

        converted_doc = self.converter.convert_document(doc)
        return converted_doc

    def _add_cdc_metadata(
        self,
        record: Dict[str, Any],
        event: Dict[str, Any],
        operation: str,
        kafka_offset: int,
        kafka_partition: int,
        kafka_topic: str
    ) -> Dict[str, Any]:
        """
        Add CDC metadata fields to the record.

        Args:
            record: Converted document
            event: Original Debezium event
            operation: Operation type
            kafka_offset: Kafka offset
            kafka_partition: Kafka partition
            kafka_topic: Kafka topic

        Returns:
            Record with metadata added
        """
        payload = event.get('payload', {})
        source = payload.get('source', event.get('source', {}))

        cdc_timestamp = payload.get('ts_ms', source.get('ts_ms'))
        if cdc_timestamp:
            cdc_timestamp = datetime.fromtimestamp(cdc_timestamp / 1000.0)
        else:
            cdc_timestamp = datetime.now()

        record['_cdc_timestamp'] = cdc_timestamp
        record['_cdc_operation'] = operation
        record['_ingestion_timestamp'] = datetime.now()
        record['_kafka_offset'] = kafka_offset
        record['_kafka_partition'] = kafka_partition
        record['_kafka_topic'] = kafka_topic

        ingestion_date = datetime.now().strftime('%Y-%m-%d')
        record['_ingestion_date'] = ingestion_date

        if source:
            record['_source_database'] = source.get('db', '')
            record['_source_collection'] = source.get('collection', '')

        return record

    def get_collection_name(self, event: Dict[str, Any]) -> Optional[str]:
        """
        Extract collection name from Debezium event.

        Args:
            event: Debezium change event

        Returns:
            Collection name or None
        """
        payload = event.get('payload', {})
        source = payload.get('source', event.get('source', {}))

        database = source.get('db', '')
        collection = source.get('collection', '')

        if database and collection:
            return f"{database}.{collection}"
        elif collection:
            return collection
        else:
            return None

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get event handler metrics.

        Returns:
            Dictionary with metrics
        """
        return self._metrics.copy()
