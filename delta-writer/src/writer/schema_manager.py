"""Schema management for Delta Lake tables with caching and evolution."""

from typing import Optional, Dict
from datetime import datetime, timedelta
import pyarrow as pa
from deltalake import DeltaTable
import structlog

from ..transformers.schema_inferrer import SchemaInferrer

logger = structlog.get_logger(__name__)


class SchemaCache:
    """TTL-based cache for Delta table schemas."""

    def __init__(self, ttl_seconds: int = 300):
        """Initialize cache with TTL (default: 5 minutes)."""
        self._cache: Dict[str, tuple[pa.Schema, datetime]] = {}
        self._ttl = timedelta(seconds=ttl_seconds)

    def get(self, table_uri: str) -> Optional[pa.Schema]:
        """Get cached schema if not expired."""
        if table_uri in self._cache:
            schema, cached_at = self._cache[table_uri]
            if datetime.now() - cached_at < self._ttl:
                logger.debug("schema_cache_hit", table_uri=table_uri)
                return schema
            else:
                logger.debug("schema_cache_expired", table_uri=table_uri)
                del self._cache[table_uri]
        return None

    def set(self, table_uri: str, schema: pa.Schema) -> None:
        """Cache schema with current timestamp."""
        self._cache[table_uri] = (schema, datetime.now())
        logger.debug("schema_cached", table_uri=table_uri, fields=len(schema))

    def invalidate(self, table_uri: str) -> None:
        """Invalidate cached schema."""
        if table_uri in self._cache:
            del self._cache[table_uri]
            logger.debug("schema_cache_invalidated", table_uri=table_uri)


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
            return new_schema

        merged_schema = SchemaInferrer.merge_schemas(existing_schema, new_schema)

        if existing_schema != merged_schema:
            logger.info(
                "schema_evolved",
                table_uri=table_uri,
                diff=SchemaInferrer.get_schema_diff(existing_schema, merged_schema)
            )
            self.cache.invalidate(table_uri)

        return merged_schema

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
