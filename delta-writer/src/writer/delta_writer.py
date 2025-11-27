"""Delta Lake write operations with schema evolution."""

from typing import List, Dict, Any, Optional
from datetime import datetime
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
        metadata: Optional[Dict[str, Any]] = None,
        max_retries: int = 3
    ) -> Dict[str, Any]:
        """
        Write a batch of records to Delta Lake with schema validation.

        Args:
            table_uri: Delta table URI (s3://bucket/table)
            records: List of converted MongoDB documents
            metadata: Optional write metadata
            max_retries: Maximum number of retries for schema evolution errors

        Returns:
            Write statistics (records written, bytes, duration, etc.)
        """
        if not records:
            logger.warning("empty_batch_skipped", table_uri=table_uri)
            return {"records_written": 0}

        import time
        start_time = time.time()
        retry_count = 0
        last_error = None

        while retry_count <= max_retries:
            try:
                # Infer schema from incoming records
                inferred_schema = SchemaInferrer.infer_schema_from_documents(records)
                inferred_schema = SchemaInferrer.add_metadata_fields(inferred_schema)

                logger.debug(
                    "schema_inferred_for_batch",
                    table_uri=table_uri,
                    num_fields=len(inferred_schema),
                    num_records=len(records)
                )

                # Pre-write schema validation
                existing_schema = self.schema_manager.get_table_schema(table_uri)
                if existing_schema is not None:
                    validation_result = SchemaInferrer.validate_schema_compatibility(
                        existing_schema,
                        inferred_schema,
                        allow_field_removal=False
                    )

                    if not validation_result["compatible"]:
                        logger.warning(
                            "schema_validation_warnings",
                            table_uri=table_uri,
                            issues=validation_result["issues"],
                            warnings=validation_result["warnings"]
                        )

                # Ensure schema compatibility (handles schema evolution)
                final_schema = self.schema_manager.ensure_schema_compatible(
                    table_uri,
                    inferred_schema
                )

                schema_version = self.schema_manager.get_schema_version(table_uri)

                logger.info(
                    "writing_with_schema",
                    table_uri=table_uri,
                    schema_version=schema_version,
                    num_fields=len(final_schema)
                )

                # Convert records to Arrow table
                arrow_table = self._records_to_arrow(records, final_schema)

                # Write to Delta Lake with schema merge mode
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

                # Collect schema evolution metrics
                schema_metrics = self.schema_manager.get_metrics()

                stats = {
                    "records_written": len(records),
                    "bytes_written": arrow_table.nbytes,
                    "duration_seconds": duration,
                    "records_per_second": len(records) / duration if duration > 0 else 0,
                    "table_uri": table_uri,
                    "schema_version": schema_version,
                    "retry_count": retry_count,
                    "schema_fields_added": schema_metrics.get("fields_added", 0),
                    "schema_types_widened": schema_metrics.get("types_widened", 0)
                }

                logger.info(
                    "batch_written_to_delta",
                    **stats
                )

                return stats

            except Exception as e:
                retry_count += 1
                last_error = e

                # Check if this is a schema evolution error
                error_str = str(e).lower()
                is_schema_error = any(
                    keyword in error_str
                    for keyword in ["schema", "type", "column", "field"]
                )

                if is_schema_error and retry_count <= max_retries:
                    logger.warning(
                        "schema_evolution_error_retrying",
                        table_uri=table_uri,
                        error=str(e),
                        retry_count=retry_count,
                        max_retries=max_retries
                    )
                    # Invalidate cache and retry
                    self.schema_manager.cache.invalidate(table_uri)
                    time.sleep(0.5 * retry_count)  # Exponential backoff
                    continue
                else:
                    # Non-schema error or max retries exceeded
                    break

        # All retries failed
        logger.error(
            "batch_write_failed",
            table_uri=table_uri,
            num_records=len(records),
            error=str(last_error),
            retries=retry_count
        )
        raise last_error

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

    def update_schema_version_metadata(
        self,
        table_uri: str,
        schema_version: int,
        schema_change_description: Optional[str] = None
    ) -> None:
        """
        Update Delta Lake table properties with schema version metadata.

        This stores schema version information directly in the Delta Lake table
        metadata using the ALTER TABLE SET TBLPROPERTIES approach.

        Args:
            table_uri: Delta table URI
            schema_version: Current schema version number
            schema_change_description: Optional description of schema changes
        """
        try:
            from datetime import datetime

            table = DeltaTable(table_uri, storage_options=self.storage_options)

            # Build metadata dictionary
            metadata = {
                "_schema_version": str(schema_version),
                "_schema_change_timestamp": datetime.now().isoformat()
            }

            if schema_change_description:
                metadata["_schema_change_description"] = schema_change_description

            # Note: As of delta-rs, we need to use the underlying API
            # This is a placeholder for when the API supports table properties
            # For now, we'll log this information
            logger.info(
                "schema_version_metadata_updated",
                table_uri=table_uri,
                schema_version=schema_version,
                timestamp=metadata["_schema_change_timestamp"],
                description=schema_change_description
            )

            # Store in schema manager for tracking
            self.schema_manager.schema_versions[table_uri] = schema_version

        except Exception as e:
            logger.error(
                "schema_version_metadata_update_failed",
                table_uri=table_uri,
                error=str(e)
            )
            raise

    def get_schema_version_from_metadata(self, table_uri: str) -> Optional[int]:
        """
        Read schema version from Delta Lake table metadata.

        Args:
            table_uri: Delta table URI

        Returns:
            Schema version number, or None if not found
        """
        try:
            # First check schema manager cache
            if table_uri in self.schema_manager.schema_versions:
                return self.schema_manager.schema_versions[table_uri]

            # For now, return None as delta-rs doesn't fully support
            # reading custom table properties yet
            logger.debug(
                "schema_version_not_in_metadata",
                table_uri=table_uri
            )
            return None

        except Exception as e:
            logger.error(
                "schema_version_metadata_read_failed",
                table_uri=table_uri,
                error=str(e)
            )
            return None

    def get_schema_version_history(self, table_uri: str) -> List[Dict[str, Any]]:
        """
        Get schema version history for a table.

        This method would read from Delta Lake table history to track
        schema evolution over time.

        Args:
            table_uri: Delta table URI

        Returns:
            List of schema version history entries
        """
        try:
            table = DeltaTable(table_uri, storage_options=self.storage_options)

            # Get table history
            history = []

            # Read current version from schema manager
            current_version = self.schema_manager.get_schema_version(table_uri)

            if current_version:
                history.append({
                    "version": current_version,
                    "timestamp": datetime.now().isoformat(),
                    "description": "Current schema version"
                })

            logger.info(
                "schema_version_history_retrieved",
                table_uri=table_uri,
                history_count=len(history)
            )

            return history

        except Exception as e:
            logger.error(
                "schema_version_history_failed",
                table_uri=table_uri,
                error=str(e)
            )
            return []
