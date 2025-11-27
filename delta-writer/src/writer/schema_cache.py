"""TTL-based caching for Delta Lake table schemas.

This module provides a caching mechanism for Delta Lake schemas
to reduce the number of table metadata lookups.
"""

from typing import Optional, Dict, Tuple
from datetime import datetime, timedelta
import pyarrow as pa
import structlog

logger = structlog.get_logger(__name__)


class SchemaCacheMetrics:
    """Metrics for schema cache operations."""

    def __init__(self):
        """Initialize metrics."""
        self.hits = 0
        self.misses = 0
        self.expirations = 0
        self.invalidations = 0
        self.evictions = 0

    def reset(self):
        """Reset all metrics."""
        self.hits = 0
        self.misses = 0
        self.expirations = 0
        self.invalidations = 0
        self.evictions = 0

    def to_dict(self) -> Dict[str, int]:
        """Convert to dictionary."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "expirations": self.expirations,
            "invalidations": self.invalidations,
            "evictions": self.evictions,
            "hit_rate": self.hits / (self.hits + self.misses) if (self.hits + self.misses) > 0 else 0.0
        }


class SchemaCache:
    """
    TTL-based cache for Delta table schemas.

    This cache stores schemas with a time-to-live (TTL) to ensure
    stale schemas are not used. It also implements a maximum size
    limit with LRU eviction.
    """

    def __init__(self, ttl_seconds: int = 300, max_size: int = 100):
        """
        Initialize cache with TTL and size limit.

        Args:
            ttl_seconds: Time-to-live in seconds (default: 5 minutes)
            max_size: Maximum number of cached schemas (default: 100)
        """
        self._cache: Dict[str, Tuple[pa.Schema, datetime, int]] = {}
        self._ttl = timedelta(seconds=ttl_seconds)
        self._max_size = max_size
        self._access_counter = 0
        self.metrics = SchemaCacheMetrics()

        logger.info(
            "schema_cache_initialized",
            ttl_seconds=ttl_seconds,
            max_size=max_size
        )

    def get(self, table_uri: str) -> Optional[pa.Schema]:
        """
        Get cached schema if not expired.

        Args:
            table_uri: Delta table URI

        Returns:
            Cached schema, or None if not found or expired
        """
        if table_uri in self._cache:
            schema, cached_at, _ = self._cache[table_uri]

            if datetime.now() - cached_at < self._ttl:
                # Update access counter for LRU
                self._access_counter += 1
                self._cache[table_uri] = (schema, cached_at, self._access_counter)

                self.metrics.hits += 1
                logger.debug(
                    "schema_cache_hit",
                    table_uri=table_uri,
                    age_seconds=(datetime.now() - cached_at).total_seconds()
                )
                return schema
            else:
                # Expired
                self.metrics.expirations += 1
                logger.debug(
                    "schema_cache_expired",
                    table_uri=table_uri,
                    age_seconds=(datetime.now() - cached_at).total_seconds()
                )
                del self._cache[table_uri]

        self.metrics.misses += 1
        logger.debug("schema_cache_miss", table_uri=table_uri)
        return None

    def set(self, table_uri: str, schema: pa.Schema) -> None:
        """
        Cache schema with current timestamp.

        If cache is at max size, evict the least recently used entry.

        Args:
            table_uri: Delta table URI
            schema: Schema to cache
        """
        # Check if we need to evict
        if len(self._cache) >= self._max_size and table_uri not in self._cache:
            self._evict_lru()

        self._access_counter += 1
        self._cache[table_uri] = (schema, datetime.now(), self._access_counter)

        logger.debug(
            "schema_cached",
            table_uri=table_uri,
            fields=len(schema),
            cache_size=len(self._cache)
        )

    def _evict_lru(self):
        """Evict the least recently used entry from cache."""
        if not self._cache:
            return

        # Find entry with lowest access counter
        lru_uri = min(self._cache.items(), key=lambda x: x[1][2])[0]

        del self._cache[lru_uri]
        self.metrics.evictions += 1

        logger.info(
            "schema_cache_evicted_lru",
            table_uri=lru_uri,
            cache_size=len(self._cache)
        )

    def invalidate(self, table_uri: str) -> None:
        """
        Invalidate cached schema.

        Args:
            table_uri: Delta table URI
        """
        if table_uri in self._cache:
            del self._cache[table_uri]
            self.metrics.invalidations += 1
            logger.debug("schema_cache_invalidated", table_uri=table_uri)

    def clear(self):
        """Clear all cached schemas."""
        count = len(self._cache)
        self._cache.clear()
        logger.info("schema_cache_cleared", entries_removed=count)

    def get_statistics(self) -> Dict[str, any]:
        """
        Get cache statistics.

        Returns:
            Dictionary with cache statistics including metrics and state
        """
        metrics = self.metrics.to_dict()

        stats = {
            "metrics": metrics,
            "cache_size": len(self._cache),
            "max_size": self._max_size,
            "ttl_seconds": self._ttl.total_seconds(),
            "utilization": len(self._cache) / self._max_size if self._max_size > 0 else 0.0
        }

        logger.info("schema_cache_statistics", **stats)
        return stats

    def get_cached_tables(self) -> list[str]:
        """
        Get list of currently cached table URIs.

        Returns:
            List of table URIs currently in cache
        """
        return list(self._cache.keys())

    def get_cache_age(self, table_uri: str) -> Optional[float]:
        """
        Get age of cached entry in seconds.

        Args:
            table_uri: Delta table URI

        Returns:
            Age in seconds, or None if not cached
        """
        if table_uri in self._cache:
            _, cached_at, _ = self._cache[table_uri]
            return (datetime.now() - cached_at).total_seconds()
        return None

    def is_expired(self, table_uri: str) -> bool:
        """
        Check if a cached entry is expired without removing it.

        Args:
            table_uri: Delta table URI

        Returns:
            True if expired or not found, False if valid
        """
        if table_uri not in self._cache:
            return True

        _, cached_at, _ = self._cache[table_uri]
        return datetime.now() - cached_at >= self._ttl

    def reset_metrics(self):
        """Reset cache metrics."""
        self.metrics.reset()
        logger.info("schema_cache_metrics_reset")
