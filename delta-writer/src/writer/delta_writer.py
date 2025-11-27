"""Delta Lake write operations with schema evolution."""

from typing import List, Dict, Any, Optional
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable
import structlog

from .schema_manager import SchemaManager
from ..transformers.bson_to_delta import BSONToDeltaConverter
from ..transformers.schema_inferrer import SchemaInferrer

logger = structlog.get_logger(__name__)


class DeltaWriter:
    """Handles writing data to Delta Lake tables."""

    def __init__(
        self,
        storage_options: Dict[str, str],
        partition_by: Optional[List[str]] = None,
        schema_cache_ttl: int = 300
    ):
        """
        Initialize Delta writer.

        Args:
            storage_options: S3 storage options for MinIO/Delta Lake
            partition_by: Default partition columns (e.g., ["_ingestion_date"])
            schema_cache_ttl: Schema cache TTL in seconds
        """
        self.storage_options = storage_options
        self.partition_by = partition_by or ["_ingestion_date"]
        self.schema_manager = SchemaManager(storage_options, schema_cache_ttl)

    def write_batch(
        self,
        table_uri: str,
        records: List[Dict[str, Any]],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Write a batch of records to Delta Lake.

        Args:
            table_uri: Delta table URI (s3://bucket/table)
            records: List of converted MongoDB documents
            metadata: Optional write metadata

        Returns:
            Write statistics (records written, bytes, duration, etc.)
        """
        if not records:
            logger.warning("empty_batch_skipped", table_uri=table_uri)
            return {"records_written": 0}

        try:
            import time
            start_time = time.time()

            inferred_schema = SchemaInferrer.infer_schema_from_documents(records)
            inferred_schema = SchemaInferrer.add_metadata_fields(inferred_schema)

            final_schema = self.schema_manager.ensure_schema_compatible(
                table_uri,
                inferred_schema
            )

            arrow_table = self._records_to_arrow(records, final_schema)

            write_deltalake(
                table_uri,
                arrow_table,
                mode="append",
                schema_mode="merge",
                partition_by=self.partition_by,
                storage_options=self.storage_options,
                engine="rust"
            )

            duration = time.time() - start_time

            stats = {
                "records_written": len(records),
                "bytes_written": arrow_table.nbytes,
                "duration_seconds": duration,
                "records_per_second": len(records) / duration if duration > 0 else 0,
                "table_uri": table_uri,
            }

            logger.info("batch_written_to_delta", **stats)
            return stats

        except Exception as e:
            logger.error(
                "batch_write_failed",
                table_uri=table_uri,
                num_records=len(records),
                error=str(e)
            )
            raise

    def _records_to_arrow(
        self,
        records: List[Dict[str, Any]],
        schema: pa.Schema
    ) -> pa.Table:
        """
        Convert records to PyArrow table with given schema.

        Args:
            records: List of converted documents
            schema: Target PyArrow schema

        Returns:
            PyArrow Table
        """
        arrays = {field.name: [] for field in schema}

        for record in records:
            for field in schema:
                value = record.get(field.name)
                arrays[field.name].append(value)

        arrow_arrays = {}
        for field in schema:
            arrow_arrays[field.name] = pa.array(arrays[field.name], type=field.type)

        return pa.Table.from_arrays(
            list(arrow_arrays.values()),
            schema=schema
        )

    def compact_table(self, table_uri: str) -> Dict[str, Any]:
        """
        Run OPTIMIZE operation on Delta table.

        This compacts small files into larger ones for better query performance.

        Args:
            table_uri: Delta table URI

        Returns:
            Compaction statistics
        """
        try:
            table = DeltaTable(table_uri, storage_options=self.storage_options)

            files_before = len(table.file_uris())

            table.optimize.compact()

            files_after = len(table.file_uris())

            stats = {
                "files_before": files_before,
                "files_after": files_after,
                "files_removed": files_before - files_after,
            }

            logger.info("table_compacted", table_uri=table_uri, **stats)
            return stats

        except Exception as e:
            logger.error("table_compaction_failed", table_uri=table_uri, error=str(e))
            raise

    def vacuum_table(self, table_uri: str, retention_hours: int = 168) -> None:
        """
        Run VACUUM operation to remove old files.

        Args:
            table_uri: Delta table URI
            retention_hours: Retention period in hours (default: 7 days)
        """
        try:
            table = DeltaTable(table_uri, storage_options=self.storage_options)
            table.vacuum(retention_hours=retention_hours)
            logger.info("table_vacuumed", table_uri=table_uri, retention_hours=retention_hours)
        except Exception as e:
            logger.error("table_vacuum_failed", table_uri=table_uri, error=str(e))
            raise
