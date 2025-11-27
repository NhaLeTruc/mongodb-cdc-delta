"""Batch processing for Kafka records before writing to Delta Lake."""

from typing import List, Dict, Any, Callable, Optional
import threading
import time
from datetime import datetime
import structlog

logger = structlog.get_logger(__name__)


class BatchProcessor:
    """Batches Kafka records before writing to Delta Lake."""

    def __init__(
        self,
        batch_size: int = 2000,
        batch_timeout_seconds: float = 10.0,
        flush_callback: Optional[Callable[[List[Dict[str, Any]]], None]] = None
    ):
        """
        Initialize batch processor.

        Args:
            batch_size: Maximum records per batch (default: 2000)
            batch_timeout_seconds: Maximum time to wait before flushing (default: 10s)
            flush_callback: Callback function to process batches
        """
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.flush_callback = flush_callback

        self._batch: List[Dict[str, Any]] = []
        self._lock = threading.Lock()
        self._last_flush_time = time.time()
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None

        self._metrics = {
            "batches_flushed": 0,
            "records_processed": 0,
            "size_flushes": 0,
            "time_flushes": 0,
        }

    def start(self) -> None:
        """Start the background flush thread."""
        if self._running:
            logger.warning("batch_processor_already_running")
            return

        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self._flush_thread.start()
        logger.info("batch_processor_started", batch_size=self.batch_size, timeout=self.batch_timeout_seconds)

    def stop(self) -> None:
        """Stop the batch processor and flush remaining records."""
        if not self._running:
            return

        logger.info("batch_processor_stopping")
        self._running = False

        if self._flush_thread:
            self._flush_thread.join(timeout=5.0)

        self.flush(force=True)
        logger.info("batch_processor_stopped", metrics=self._metrics)

    def add_record(self, record: Dict[str, Any]) -> None:
        """
        Add a record to the current batch.

        If batch size is reached, flush immediately.

        Args:
            record: Record to add to batch
        """
        with self._lock:
            self._batch.append(record)
            self._metrics["records_processed"] += 1

            if len(self._batch) >= self.batch_size:
                logger.debug("batch_size_reached", size=len(self._batch))
                self._flush_unlocked(reason="size")
                self._metrics["size_flushes"] += 1

    def add_records(self, records: List[Dict[str, Any]]) -> None:
        """
        Add multiple records to the current batch.

        Args:
            records: List of records to add
        """
        for record in records:
            self.add_record(record)

    def flush(self, force: bool = False) -> int:
        """
        Flush the current batch.

        Args:
            force: Force flush even if batch is empty

        Returns:
            Number of records flushed
        """
        with self._lock:
            return self._flush_unlocked(reason="manual" if force else "time")

    def _flush_unlocked(self, reason: str = "unknown") -> int:
        """
        Flush the current batch without acquiring lock.

        Must be called with lock held.

        Args:
            reason: Reason for flush (for metrics)

        Returns:
            Number of records flushed
        """
        if not self._batch:
            return 0

        batch_copy = self._batch.copy()
        self._batch.clear()
        self._last_flush_time = time.time()

        batch_size = len(batch_copy)
        self._metrics["batches_flushed"] += 1

        logger.info(
            "batch_flushing",
            size=batch_size,
            reason=reason,
            total_batches=self._metrics["batches_flushed"]
        )

        if self.flush_callback:
            try:
                self.flush_callback(batch_copy)
                logger.debug("batch_flushed_successfully", size=batch_size)
            except Exception as e:
                logger.error("batch_flush_failed", size=batch_size, error=str(e))
                raise

        return batch_size

    def _flush_loop(self) -> None:
        """Background thread to flush batches based on timeout."""
        while self._running:
            time.sleep(1.0)

            with self._lock:
                time_since_last_flush = time.time() - self._last_flush_time

                if self._batch and time_since_last_flush >= self.batch_timeout_seconds:
                    logger.debug(
                        "batch_timeout_reached",
                        size=len(self._batch),
                        time_since_flush=time_since_last_flush
                    )
                    self._flush_unlocked(reason="timeout")
                    self._metrics["time_flushes"] += 1

    def get_current_batch_size(self) -> int:
        """Get the current batch size."""
        with self._lock:
            return len(self._batch)

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get batch processor metrics.

        Returns:
            Dictionary with metrics
        """
        with self._lock:
            return {
                **self._metrics,
                "current_batch_size": len(self._batch),
                "time_since_last_flush": time.time() - self._last_flush_time,
            }


class PerCollectionBatchProcessor:
    """Manages separate batch processors for each MongoDB collection."""

    def __init__(
        self,
        batch_size: int = 2000,
        batch_timeout_seconds: float = 10.0,
        flush_callback: Optional[Callable[[str, List[Dict[str, Any]]], None]] = None
    ):
        """
        Initialize per-collection batch processor.

        Args:
            batch_size: Maximum records per batch
            batch_timeout_seconds: Maximum time to wait before flushing
            flush_callback: Callback function with signature (collection_name, records)
        """
        self.batch_size = batch_size
        self.batch_timeout_seconds = batch_timeout_seconds
        self.flush_callback = flush_callback

        self._processors: Dict[str, BatchProcessor] = {}
        self._lock = threading.Lock()

    def get_processor(self, collection: str) -> BatchProcessor:
        """
        Get or create batch processor for a collection.

        Args:
            collection: Collection name

        Returns:
            BatchProcessor for the collection
        """
        with self._lock:
            if collection not in self._processors:
                def collection_flush_callback(records: List[Dict[str, Any]]) -> None:
                    if self.flush_callback:
                        self.flush_callback(collection, records)

                processor = BatchProcessor(
                    batch_size=self.batch_size,
                    batch_timeout_seconds=self.batch_timeout_seconds,
                    flush_callback=collection_flush_callback
                )
                processor.start()
                self._processors[collection] = processor
                logger.info("collection_batch_processor_created", collection=collection)

            return self._processors[collection]

    def add_record(self, collection: str, record: Dict[str, Any]) -> None:
        """
        Add a record to the appropriate collection batch.

        Args:
            collection: Collection name
            record: Record to add
        """
        processor = self.get_processor(collection)
        processor.add_record(record)

    def flush_all(self) -> Dict[str, int]:
        """
        Flush all collection batches.

        Returns:
            Dictionary mapping collection name to records flushed
        """
        results = {}
        with self._lock:
            for collection, processor in self._processors.items():
                flushed = processor.flush(force=True)
                results[collection] = flushed
        return results

    def stop_all(self) -> None:
        """Stop all batch processors."""
        with self._lock:
            for collection, processor in self._processors.items():
                logger.info("stopping_collection_processor", collection=collection)
                processor.stop()
            self._processors.clear()

    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Get metrics for all collection processors.

        Returns:
            Dictionary mapping collection name to metrics
        """
        with self._lock:
            return {
                collection: processor.get_metrics()
                for collection, processor in self._processors.items()
            }
