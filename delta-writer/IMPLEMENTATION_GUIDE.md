# Delta Writer Implementation Guide

This guide provides detailed instructions for implementing each component of the Delta Writer service.

## Overview

The Delta Writer service consumes CDC events from Kafka (produced by Debezium) and writes them to Delta Lake tables on MinIO storage.

**Architecture Flow**:
```
Kafka (Debezium events) → EventConsumer → EventHandler → DeltaWriter → MinIO (Delta Lake)
```

## Component Implementation Order

Follow this order to ensure dependencies are satisfied:

1. **Configuration** (T040)
2. **Transformers** (T041, T042)
3. **Storage** (T043)
4. **Writer** (T044, T045, T046)
5. **Consumer** (T047, T048)
6. **Main Entry Point** (T049)
7. **Docker** (T050, T051)

---

## T040: Configuration (src/config.py)

### Purpose
Centralized configuration using Pydantic Settings for Kafka, MinIO, and Delta Lake.

### Implementation

```python
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class KafkaConfig(BaseSettings):
    """Kafka connection configuration."""
    bootstrap_servers: str = Field(default="kafka:9092")
    consumer_group: str = Field(default="delta-writer")
    topic_pattern: str = Field(default="mongodb.*")
    auto_offset_reset: str = Field(default="earliest")
    enable_auto_commit: bool = Field(default=False)
    max_poll_records: int = Field(default=2000)

    model_config = SettingsConfigDict(env_prefix="KAFKA_")

class MinIOConfig(BaseSettings):
    """MinIO/S3 configuration."""
    endpoint: str = Field(default="http://minio:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin123")
    bucket: str = Field(default="lakehouse")
    region: str = Field(default="us-east-1")

    model_config = SettingsConfigDict(env_prefix="MINIO_")

class DeltaConfig(BaseSettings):
    """Delta Lake configuration."""
    table_base_path: str = Field(default="s3://lakehouse/tables")
    schema_cache_ttl: int = Field(default=300)  # 5 minutes
    write_mode: str = Field(default="append")
    partition_by: list[str] = Field(default_factory=list)

    model_config = SettingsConfigDict(env_prefix="DELTA_")

class Config(BaseSettings):
    """Main service configuration."""
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    minio: MinIOConfig = Field(default_factory=MinIOConfig)
    delta: DeltaConfig = Field(default_factory=DeltaConfig)

    log_level: str = Field(default="INFO")
    metrics_port: int = Field(default=8001)

    model_config = SettingsConfigDict(env_file=".env")
```

### Testing
- Test that config loads from environment variables
- Test default values
- Test validation (e.g., bootstrap_servers cannot be empty)

---

## T041: BSON to Delta Transformer (src/transformers/bson_to_delta.py)

### Purpose
Convert MongoDB BSON types to Arrow/Delta Lake types.

### Implementation

```python
from datetime import datetime
from typing import Any
import pyarrow as pa

# Type mapping from MongoDB BSON to Arrow
BSON_TO_ARROW_TYPE_MAP = {
    "string": pa.string(),
    "int": pa.int64(),
    "long": pa.int64(),
    "double": pa.float64(),
    "bool": pa.bool_(),
    "date": pa.timestamp("ms"),
    "objectId": pa.string(),
    "binary": pa.binary(),
    "decimal": pa.string(),  # Store as string for precision
}

def convert_bson_value(value: Any, target_type: pa.DataType | None = None) -> Any:
    """
    Convert BSON value to Arrow-compatible type.

    Args:
        value: The BSON value to convert
        target_type: Optional target Arrow type

    Returns:
        Converted value compatible with Arrow

    Implementation Steps:
    1. Handle None/null values
    2. Convert ObjectId to string (hex representation)
    3. Convert datetime to timestamp (milliseconds)
    4. Convert Decimal to string (preserve precision)
    5. Recursively convert nested documents
    6. Recursively convert arrays
    7. Handle binary data (base64 encode if needed)
    """
    # TODO: Implement conversion logic
    pass

def infer_arrow_type(value: Any) -> pa.DataType:
    """
    Infer Arrow type from Python value.

    Args:
        value: Python value

    Returns:
        Corresponding Arrow DataType

    Implementation Steps:
    1. Check type with isinstance()
    2. Handle nested structures (dict → struct, list → list)
    3. Return appropriate pa.DataType
    """
    # TODO: Implement type inference
    pass
```

### Key Considerations
- ObjectId: Convert to 24-character hex string
- Dates: Store as timestamp with millisecond precision
- Decimals: Store as strings to preserve precision
- Binary: Base64 encode or store as binary type
- Nested docs: Convert to pa.struct
- Arrays: Convert to pa.list_

---

## T042: Schema Inferrer (src/transformers/schema_inferrer.py)

### Purpose
Infer Delta Lake schema from MongoDB documents.

### Implementation

```python
from typing import Dict, List, Any
import pyarrow as pa

def infer_schema_from_documents(documents: List[Dict[str, Any]]) -> pa.Schema:
    """
    Infer Arrow schema from a list of MongoDB documents.

    Args:
        documents: List of MongoDB documents

    Returns:
        Inferred PyArrow schema

    Implementation Steps:
    1. Collect all unique field names from all documents
    2. For each field, infer type from non-null values
    3. Mark fields as nullable if any document is missing them
    4. Add CDC metadata fields (_cdc_op, _cdc_ts_ms, etc.)
    5. Return pa.Schema
    """
    # TODO: Implement schema inference
    pass

def merge_schemas(schema1: pa.Schema, schema2: pa.Schema) -> pa.Schema:
    """
    Merge two schemas for schema evolution.

    Args:
        schema1: Existing schema
        schema2: New schema with additional fields

    Returns:
        Merged schema

    Implementation Steps:
    1. Start with schema1 fields
    2. Add new fields from schema2 that don't exist in schema1
    3. Handle type conflicts (promote int to double, etc.)
    4. Ensure all new fields are nullable
    """
    # TODO: Implement schema merging
    pass
```

---

## T043: MinIO Client (src/storage/minio_client.py)

### Purpose
Wrapper around aioboto3 for S3-compatible operations.

### Implementation

```python
import aioboto3
from contextlib import asynccontextmanager

class MinIOClient:
    """Async MinIO client wrapper."""

    def __init__(self, endpoint: str, access_key: str, secret_key: str, region: str = "us-east-1"):
        """
        Initialize MinIO client.

        Implementation Steps:
        1. Create aioboto3.Session
        2. Store connection parameters
        3. Initialize client lazily
        """
        # TODO: Implement initialization
        pass

    @asynccontextmanager
    async def get_client(self):
        """
        Get S3 client context manager.

        Usage:
            async with client.get_client() as s3:
                await s3.put_object(...)
        """
        # TODO: Implement async context manager
        pass

    async def upload_file(self, file_path: str, bucket: str, key: str):
        """Upload file to MinIO."""
        # TODO: Implement upload
        pass

    async def list_objects(self, bucket: str, prefix: str):
        """List objects in bucket."""
        # TODO: Implement list
        pass
```

---

## T044: Schema Manager (src/writer/schema_manager.py)

### Purpose
Manage Delta Lake schemas with caching.

### Implementation

```python
from typing import Dict, Optional
import pyarrow as pa
import deltalake as dl
from datetime import datetime, timedelta

class SchemaCache:
    """TTL-based schema cache."""

    def __init__(self, ttl_seconds: int = 300):
        """Initialize cache with TTL."""
        self._cache: Dict[str, tuple[pa.Schema, datetime]] = {}
        self._ttl = timedelta(seconds=ttl_seconds)

    def get(self, table_path: str) -> Optional[pa.Schema]:
        """Get schema from cache if not expired."""
        # TODO: Implement get with TTL check
        pass

    def set(self, table_path: str, schema: pa.Schema):
        """Store schema in cache."""
        # TODO: Implement set
        pass

class SchemaManager:
    """Manage Delta Lake table schemas."""

    def __init__(self, cache_ttl: int = 300):
        self._cache = SchemaCache(cache_ttl)

    def get_table_schema(self, table_path: str) -> Optional[pa.Schema]:
        """
        Get table schema with caching.

        Implementation Steps:
        1. Check cache first
        2. If miss, load from Delta table
        3. Store in cache
        4. Return schema
        """
        # TODO: Implement
        pass

    def evolve_schema(self, table_path: str, new_schema: pa.Schema):
        """
        Evolve table schema (add new fields).

        Implementation Steps:
        1. Get current schema
        2. Merge with new schema
        3. Update Delta table schema
        4. Invalidate cache
        """
        # TODO: Implement
        pass
```

---

## T045: Delta Writer (src/writer/delta_writer.py)

### Purpose
Core Delta Lake write operations.

### Implementation

```python
import pyarrow as pa
import deltalake as dl
from typing import List, Dict, Any

class DeltaWriter:
    """Write data to Delta Lake tables."""

    def __init__(self, storage_options: Dict[str, str]):
        """
        Initialize Delta writer.

        Args:
            storage_options: S3/MinIO connection options
        """
        self._storage_options = storage_options

    async def write_batch(
        self,
        table_path: str,
        records: List[Dict[str, Any]],
        schema: pa.Schema,
        mode: str = "append"
    ) -> int:
        """
        Write batch of records to Delta table.

        Args:
            table_path: Full S3 path to table
            records: List of records to write
            schema: Arrow schema
            mode: Write mode (append, overwrite, upsert)

        Returns:
            Number of records written

        Implementation Steps:
        1. Convert records to PyArrow Table
        2. Validate against schema
        3. Create/load Delta table
        4. Write based on mode:
           - append: dl.write_deltalake(mode="append")
           - upsert: Use merge operation with _id as key
           - delete: Use merge with whenNotMatchedBySource
        5. Return record count
        """
        # TODO: Implement write logic
        pass

    async def upsert_batch(
        self,
        table_path: str,
        records: List[Dict[str, Any]],
        merge_keys: List[str]
    ):
        """
        Upsert records (insert or update).

        Implementation Steps:
        1. Read existing Delta table
        2. Create DataFrame from new records
        3. Use Delta Lake merge:
           table.merge(
               source=new_df,
               predicate="_id = source._id",
               source_alias="source",
               target_alias="target"
           ).when_matched_update_all().when_not_matched_insert_all().execute()
        """
        # TODO: Implement upsert
        pass
```

---

## T046-T049: Remaining Components

### T046: Batch Processor
**Purpose**: Accumulate Kafka records before writing to reduce I/O operations.

### T047: Event Handler
**Purpose**: Process individual Debezium change events, extract after/before fields.

### T048: Event Consumer
**Purpose**: Main Kafka consumer loop with exactly-once semantics.

### T049: Main Entry Point
**Purpose**: CLI entry point that initializes all components and starts consumer.

---

## Quick Implementation Checklist

- [ ] T040: config.py - Configuration with Pydantic
- [ ] T041: bson_to_delta.py - Type conversion
- [ ] T042: schema_inferrer.py - Schema inference
- [ ] T043: minio_client.py - S3 client wrapper
- [ ] T044: schema_manager.py - Schema caching
- [ ] T045: delta_writer.py - Core write logic
- [ ] T046: batch_processor.py - Batching logic
- [ ] T047: event_handler.py - Event processing
- [ ] T048: event_consumer.py - Kafka consumer
- [ ] T049: main.py - Entry point

---

## Testing Strategy

1. **Unit Tests**: Test each component in isolation
2. **Integration Tests**: Test with real Testcontainers
3. **E2E Tests**: Full pipeline with MongoDB → Kafka → Delta Lake

## Performance Targets

- **Throughput**: 10,000 events/sec
- **Latency**: P95 < 60 seconds
- **Memory**: < 2GB per process

## Next Steps

1. Implement T040 (config.py) first
2. Then T041-T042 (transformers)
3. Then T043-T045 (storage and writer)
4. Finally T046-T049 (consumer and main)
5. Test incrementally after each component
