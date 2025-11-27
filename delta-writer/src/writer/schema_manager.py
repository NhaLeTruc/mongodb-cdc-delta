"""Schema management for Delta Lake tables with caching and evolution."""

from typing import Optional, Dict, Callable, Any
from datetime import datetime, timedelta
import pyarrow as pa
from deltalake import DeltaTable
import structlog

from ..transformers.schema_inferrer import SchemaInferrer
from .schema_cache import SchemaCache

logger = structlog.get_logger(__name__)


class SchemaEvolutionMetrics:
    """Metrics for schema evolution operations."""

    def __init__(self):
        """Initialize metrics."""
        self.fields_added = 0
        self.types_widened = 0
        self.schema_versions_created = 0
        self.schema_evolutions = 0

    def reset(self):
        """Reset all metrics."""
        self.fields_added = 0
        self.types_widened = 0
        self.schema_versions_created = 0
        self.schema_evolutions = 0

    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary."""
        return {
            "fields_added": self.fields_added,
            "types_widened": self.types_widened,
            "schema_versions_created": self.schema_versions_created,
            "schema_evolutions": self.schema_evolutions
        }


class SchemaManager:
    """Manages Delta Lake table schemas with evolution support."""

    def __init__(self, storage_options: Dict[str, str], cache_ttl: int = 300):
        """
        Initialize schema manager.

        Args:
            storage_options: S3 storage options for Delta Lake
            cache_ttl: Schema cache TTL in seconds (default: 5 minutes)
        """
        self.storage_options = storage_options
        self.cache = SchemaCache(ttl_seconds=cache_ttl)
        self.metrics = SchemaEvolutionMetrics()
        self.schema_change_callbacks: list[Callable[[str, pa.Schema, pa.Schema], None]] = []
        self.schema_versions: Dict[str, int] = {}

    def get_table_schema(self, table_uri: str, use_cache: bool = True) -> Optional[pa.Schema]:
        """
        Get schema for an existing Delta table.

        Args:
            table_uri: Delta table URI (e.g., s3://bucket/table)
            use_cache: Whether to use cached schema

        Returns:
            PyArrow schema or None if table doesn't exist
        """
        if use_cache:
            cached_schema = self.cache.get(table_uri)
            if cached_schema is not None:
                return cached_schema

        try:
            table = DeltaTable(table_uri, storage_options=self.storage_options)
            schema = table.schema().to_pyarrow()
            self.cache.set(table_uri, schema)
            logger.info("table_schema_loaded", table_uri=table_uri, fields=len(schema))
            return schema
        except Exception as e:
            logger.debug("table_does_not_exist", table_uri=table_uri, error=str(e))
            return None

    def table_exists(self, table_uri: str) -> bool:
        """Check if Delta table exists."""
        return self.get_table_schema(table_uri, use_cache=False) is not None

    def ensure_schema_compatible(
        self,
        table_uri: str,
        new_schema: pa.Schema
    ) -> pa.Schema:
        """
        Ensure new schema is compatible with existing table schema.

        If table exists, merges schemas. If not, returns new schema as-is.

        Args:
            table_uri: Delta table URI
            new_schema: New schema to validate/merge

        Returns:
            Merged schema (or new schema if table doesn't exist)
        """
        existing_schema = self.get_table_schema(table_uri)

        if existing_schema is None:
            logger.info("new_table_schema_created", table_uri=table_uri)
            self.schema_versions[table_uri] = 1
            self.metrics.schema_versions_created += 1
            return new_schema

        merged_schema = SchemaInferrer.merge_schemas(existing_schema, new_schema)

        if existing_schema != merged_schema:
            # Schema evolved
            diff = SchemaInferrer.get_schema_diff(existing_schema, merged_schema)

            # Update metrics
            self.metrics.schema_evolutions += 1
            if diff.get("added_fields"):
                self.metrics.fields_added += len(diff["added_fields"])
            if diff.get("changed_fields"):
                self.metrics.types_widened += len(diff["changed_fields"])

            # Increment version
            current_version = self.schema_versions.get(table_uri, 1)
            new_version = current_version + 1
            self.schema_versions[table_uri] = new_version

            # Enhanced logging with schema diff
            logger.info(
                "schema_evolved",
                table_uri=table_uri,
                version=new_version,
                diff=diff,
                fields_added=len(diff.get("added_fields", {})),
                fields_removed=len(diff.get("removed_fields", {})),
                fields_changed=len(diff.get("changed_fields", {}))
            )

            # Trigger callbacks
            self._notify_schema_change(table_uri, existing_schema, merged_schema)

            self.cache.invalidate(table_uri)

        return merged_schema

    def register_schema_change_callback(
        self,
        callback: Callable[[str, pa.Schema, pa.Schema], None]
    ):
        """
        Register a callback to be notified of schema changes.

        Args:
            callback: Function to call on schema change.
                      Signature: (table_uri, old_schema, new_schema) -> None
        """
        self.schema_change_callbacks.append(callback)
        logger.info(
            "schema_change_callback_registered",
            callbacks_count=len(self.schema_change_callbacks)
        )

    def _notify_schema_change(
        self,
        table_uri: str,
        old_schema: pa.Schema,
        new_schema: pa.Schema
    ):
        """
        Notify all registered callbacks of a schema change.

        Args:
            table_uri: Table URI
            old_schema: Previous schema
            new_schema: New schema
        """
        for callback in self.schema_change_callbacks:
            try:
                callback(table_uri, old_schema, new_schema)
            except Exception as e:
                logger.error(
                    "schema_change_callback_failed",
                    table_uri=table_uri,
                    error=str(e)
                )

    def get_schema_version(self, table_uri: str) -> int:
        """
        Get the current schema version for a table.

        Args:
            table_uri: Delta table URI

        Returns:
            Current schema version (1 if unknown)
        """
        return self.schema_versions.get(table_uri, 1)

    def get_metrics(self) -> Dict[str, int]:
        """
        Get schema evolution metrics.

        Returns:
            Dictionary with metric counters
        """
        return self.metrics.to_dict()

    def reset_metrics(self):
        """Reset schema evolution metrics."""
        self.metrics.reset()
        logger.info("schema_manager_metrics_reset")

    def create_table_if_not_exists(
        self,
        table_uri: str,
        schema: pa.Schema,
        partition_by: Optional[list] = None
    ) -> None:
        """
        Create Delta table with given schema if it doesn't exist.

        Args:
            table_uri: Delta table URI
            schema: PyArrow schema
            partition_by: Optional partition columns
        """
        if self.table_exists(table_uri):
            logger.debug("table_already_exists", table_uri=table_uri)
            return

        try:
            from deltalake import write_deltalake

            empty_table = pa.Table.from_pydict({}, schema=schema)

            write_deltalake(
                table_uri,
                empty_table,
                mode="append",
                schema_mode="merge",
                partition_by=partition_by,
                storage_options=self.storage_options,
                engine="rust"
            )

            self.cache.set(table_uri, schema)
            logger.info("table_created", table_uri=table_uri, fields=len(schema))
        except Exception as e:
            logger.error("table_creation_failed", table_uri=table_uri, error=str(e))
            raise
