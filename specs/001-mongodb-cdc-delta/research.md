# Research & Technology Decisions

**Feature**: MongoDB CDC to Delta Lake Pipeline
**Date**: 2025-11-26
**Status**: Complete

This document consolidates research findings and technology decisions for the MongoDB CDC to Delta Lake pipeline implementation.

---

## 1. Debezium MongoDB Connector Configuration

### Decision
Use Debezium MongoDB Connector 2.5+ with optimized configuration for high-throughput CDC from MongoDB 7.0+ to Kafka.

### Rationale
- Industry-standard CDC solution with proven reliability at scale
- Native MongoDB change streams support (oplog and change streams modes)
- Built-in support for exactly-once semantics with Kafka
- Active community and enterprise support options
- Handles schema evolution and complex MongoDB data types

### Best Practices

**Connector Configuration**:
```json
{
  "name": "mongodb-cdc-connector",
  "config": {
    "connector.class": "io.debezium.connector.mongodb.MongoDbConnector",
    "mongodb.connection.string": "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0",
    "mongodb.connection.mode": "replica_set",
    "topic.prefix": "mongodb",
    "collection.include.list": "mydb.users,mydb.orders",

    "snapshot.mode": "initial",
    "snapshot.fetch.size": 10000,
    "snapshot.max.threads": 4,

    "poll.interval.ms": 100,
    "max.batch.size": 2048,
    "max.queue.size": 16384,
    "tasks.max": 1,

    "tombstones.on.delete": true,
    "capture.mode": "change_streams_update_full",

    "heartbeat.interval.ms": 5000,
    "heartbeat.topics.prefix": "__debezium-heartbeat",

    "schema.name.adjustment.mode": "avro",
    "provide.transaction.metadata": true
  }
}
```

**Key Parameters**:
- `snapshot.mode: initial` - Perform initial snapshot then switch to change streams
- `snapshot.fetch.size: 10000` - Batch size for initial snapshot (balance memory vs speed)
- `snapshot.max.threads: 4` - Parallel snapshot threads (adjust based on MongoDB load)
- `poll.interval.ms: 100` - Frequent polling for low-latency CDC
- `max.batch.size: 2048` - Kafka batch size for throughput
- `max.queue.size: 16384` - Internal queue size (prevent backpressure)
- `tasks.max: 1` - Single task per connector (MongoDB change streams requirement)
- `capture.mode: change_streams_update_full` - Capture full document on updates

### Supported Topologies
- **Replica Set** (Recommended): 3+ node replica set with majority write concern
- **Sharded Cluster**: Supported but requires one connector per shard (use MongoDB router)
- **Standalone**: Not supported for CDC (change streams require replica set)

### Snapshot Strategies
- **Initial**: Snapshot existing data, then stream changes (recommended for existing databases)
- **Never**: Stream only new changes (for new databases or when snapshot not needed)
- **When Needed**: Snapshot if no offset exists (useful for testing)

**Performance**: Parallel snapshot with 4 threads can process ~1TB in 18-24 hours depending on document size and MongoDB load.

### MongoDB Oplog Considerations
- **Oplog Size**: Minimum 50GB recommended for production (supports 24-hour replay window)
- **Retention**: Configure oplog retention period based on expected downtime scenarios
- **Monitoring**: Alert when oplog window drops below 6 hours

### Alternatives Considered
- **Custom Change Streams Reader**: Rejected - reinventing the wheel, Debezium handles edge cases
- **MongoDB Atlas Triggers**: Rejected - vendor lock-in, limited throughput, no exactly-once guarantees
- **MongoDB Kafka Connector**: Rejected - less mature than Debezium, limited transformation support

### References
- [Debezium MongoDB Connector Documentation](https://debezium.io/documentation/reference/stable/connectors/mongodb.html)
- [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- [Debezium Performance Tuning](https://debezium.io/documentation/reference/stable/operations/tuning.html)

---

## 2. Delta Lake Schema Evolution

### Decision
Use delta-rs Python bindings with automatic schema merging enabled to handle MongoDB's schemaless collections.

### Rationale
- Native Rust implementation provides better performance than PySpark
- Supports schema evolution without external dependencies (Spark)
- Compatible with S3-compatible storage (MinIO)
- Supports time travel and ACID transactions
- Growing ecosystem with DuckDB integration

### Type Mappings

| MongoDB BSON Type | Delta Lake/Arrow Type | Notes |
|-------------------|----------------------|-------|
| ObjectId | String | Convert to hex string representation |
| String | String (UTF-8) | Direct mapping |
| Int32 | Int32 | Direct mapping |
| Int64 | Int64 | Direct mapping |
| Double | Float64 | Direct mapping |
| Decimal128 | Decimal128(38,10) | Preserve precision |
| Boolean | Boolean | Direct mapping |
| Date | Timestamp (microseconds) | Convert to UTC |
| Timestamp | Timestamp (microseconds) | MongoDB replication timestamp |
| Binary | Binary | Base64 or raw bytes |
| Array | List\<T\> | Recursive type mapping |
| Document (Nested) | Struct | Recursive schema inference |
| Null | Null | Preserve nullability |
| Undefined | Null | Convert to null |

### Schema Merge Strategy

```python
# Enable schema evolution
from deltalake import write_deltalake, DeltaTable

write_deltalake(
    table_uri="s3://bucket/table",
    data=dataframe,
    mode="append",
    schema_mode="merge",  # Automatically merge new fields
    engine="rust",
    storage_options=storage_options
)
```

**Schema Merge Rules**:
- New fields in source → automatically added to Delta table schema
- Missing fields in source → represented as null in Delta table
- Type conflicts → use widest type (e.g., Int32 + Int64 → Int64)
- Nested schema changes → recursively merged

### Nested Documents

MongoDB documents with nested structures are mapped to Delta Lake Struct types:

```python
# MongoDB document
{
  "_id": ObjectId("..."),
  "name": "John",
  "address": {
    "street": "123 Main St",
    "city": "NYC",
    "coordinates": {"lat": 40.7, "lon": -74.0}
  }
}

# Delta Lake schema
root
 |-- _id: string
 |-- name: string
 |-- address: struct
 |    |-- street: string
 |    |-- city: string
 |    |-- coordinates: struct
 |    |    |-- lat: double
 |    |    |-- lon: double
```

### Array Handling

MongoDB arrays are mapped to Delta Lake List types:

```python
# Simple array
{"tags": ["python", "mongodb", "cdc"]}
# → tags: list<string>

# Array of documents
{"items": [{"sku": "A1", "qty": 5}, {"sku": "B2", "qty": 3}]}
# → items: list<struct<sku: string, qty: int>>
```

### Partition Strategies

**Time-Based Partitioning** (Recommended for CDC):
```python
# Partition by ingestion date
partition_by=["_ingestion_date"]  # Format: YYYY-MM-DD
```

**Hybrid Partitioning** (For sharded MongoDB):
```python
# Partition by date and shard key
partition_by=["_ingestion_date", "_shard_key"]
```

**Performance Considerations**:
- Avoid over-partitioning (keep partitions >1GB for query performance)
- Daily partitions work well for most CDC workloads
- Use Z-ordering for frequently queried columns

### Schema Caching

```python
# Cache Delta table metadata to reduce MinIO roundtrips
class SchemaCache:
    def __init__(self, ttl_seconds=300):
        self._cache = {}
        self._ttl = ttl_seconds

    def get_schema(self, table_uri):
        if table_uri in self._cache:
            cached_time, schema = self._cache[table_uri]
            if time.time() - cached_time < self._ttl:
                return schema

        table = DeltaTable(table_uri)
        schema = table.schema()
        self._cache[table_uri] = (time.time(), schema)
        return schema
```

### Best Practices
- Enable schema evolution from day one (avoid schema lock-in)
- Use struct types for nested documents (preserve structure)
- Partition by ingestion date for CDC workloads
- Cache Delta table metadata to reduce S3 API calls
- Run OPTIMIZE and VACUUM operations weekly

### Alternatives Considered
- **Apache Iceberg**: Rejected - less mature Python support, more complex catalog management
- **Apache Hudi**: Rejected - Spark-dependent, overkill for append-heavy CDC workloads
- **Parquet Files**: Rejected - no ACID guarantees, no schema evolution, manual partitioning

### References
- [delta-rs Documentation](https://delta-io.github.io/delta-rs/)
- [Delta Lake Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)
- [Arrow Type System](https://arrow.apache.org/docs/python/api/datatypes.html)

---

## 3. Kafka Exactly-Once Semantics

### Decision
Enable Kafka exactly-once semantics (EOS) using transactional producers and idempotent consumers.

### Rationale
- Zero data loss requirement for enterprise CDC
- Prevents duplicate events during rebalancing or failures
- Supported natively by Kafka 0.11+ (mature feature)
- Compatible with Debezium and Kafka Connect
- Minimal performance overhead (<5% latency increase)

### Configuration

**Producer Settings** (Debezium/Kafka Connect):
```properties
# Enable exactly-once semantics
processing.guarantee=exactly_once_v2
enable.idempotence=true
transactional.id=mongodb-cdc-connector-${task.id}
transaction.timeout.ms=900000

# Ack settings for durability
acks=all
min.insync.replicas=2
retries=2147483647
max.in.flight.requests.per.connection=5

# Batching for performance
linger.ms=10
batch.size=16384
compression.type=snappy
```

**Consumer Settings** (Delta Lake Writer):
```properties
# Exactly-once consumption
isolation.level=read_committed
enable.auto.commit=false

# Manual offset management
max.poll.records=2000
max.poll.interval.ms=300000
session.timeout.ms=45000
heartbeat.interval.ms=15000

# Consumer group
group.id=deltalake-writer-group
auto.offset.reset=earliest
```

### Offset Management Strategy

**Transactional Commits**:
```python
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer(
    'mongodb.mydb.users',
    bootstrap_servers=['kafka:9092'],
    isolation_level='read_committed',
    enable_auto_commit=False,
    group_id='delta-writer'
)

for message in consumer:
    try:
        # 1. Process message → write to Delta Lake
        write_to_delta_lake(message.value)

        # 2. Commit offset transactionally
        consumer.commit()

    except Exception as e:
        # On failure, offset not committed → retry on next poll
        logger.error(f"Failed to process message: {e}")
        # Optionally seek back to failed offset
        consumer.seek(message.partition, message.offset)
```

### Transaction Coordination

For atomic Delta Lake writes with offset commits:

```python
# Option 1: Delta Lake transaction + manual offset tracking
with delta_table.transaction():
    # Write batch to Delta Lake
    delta_table.write(batch_data)

    # Track offset in Delta metadata or external store
    store_offset(topic, partition, offset)

# Option 2: Use Delta Lake _metadata for offset tracking
batch_data['_kafka_offset'] = offset
batch_data['_kafka_partition'] = partition
batch_data['_kafka_timestamp'] = timestamp
delta_table.write(batch_data)
```

### Handling Consumer Group Rebalancing

**Strategy**: Graceful shutdown with in-flight transaction completion

```python
import signal

class DeltaWriter:
    def __init__(self):
        self.running = True
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        logger.info("Graceful shutdown initiated")
        self.running = False

    def consume(self):
        while self.running:
            messages = consumer.poll(timeout_ms=1000)
            if not messages:
                continue

            # Process batch atomically
            batch = []
            for partition, records in messages.items():
                batch.extend(records)

            # Write to Delta Lake
            self.write_batch(batch)

            # Commit offsets after successful write
            consumer.commit()

        # On shutdown, complete in-flight transaction
        consumer.close()
```

### Performance Impact

**Throughput**: 5-10% reduction compared to at-least-once semantics
**Latency**: P95 latency increases by 10-20ms
**Acceptable Trade-off**: Zero data loss and zero duplicates worth the minimal overhead

### Best Practices
- Always use `acks=all` and `min.insync.replicas=2` for durability
- Set transaction timeout higher than max processing time
- Monitor `kafka.producer:type=producer-metrics,client-id=*` for transaction metrics
- Use batching to amortize transaction overhead
- Test rebalancing scenarios in staging environment

### Alternatives Considered
- **At-Least-Once**: Rejected - causes duplicate events, violates data integrity requirement
- **At-Most-Once**: Rejected - can lose data, unacceptable for CDC
- **Custom Deduplication**: Rejected - complex, error-prone, Kafka EOS is proven

### References
- [Kafka Exactly-Once Semantics](https://kafka.apache.org/documentation/#semantics)
- [Kafka Transactions](https://www.confluent.io/blog/transactions-apache-kafka/)
- [Kafka Connect Exactly-Once](https://cwiki.apache.org/confluence/display/KAFKA/KIP-618%3A+Exactly-Once+Support+for+Source+Connectors)

---

## 4. MinIO Performance Optimization

### Decision
Deploy MinIO in distributed mode with erasure coding for high availability and performance.

### Rationale
- S3-compatible API enables Delta Lake integration
- Distributed mode provides horizontal scalability
- Erasure coding ensures data durability without full replication overhead
- Sub-millisecond latency for object operations
- Open-source with production-grade reliability

### Configuration

**Distributed MinIO Setup** (4 nodes minimum):
```bash
# Start MinIO cluster (4 nodes, 16 drives)
minio server \
  http://minio{1...4}/data{1...4} \
  --console-address ":9001" \
  --address ":9000"
```

**Erasure Coding**:
- **EC:4**: 4 data shards + 4 parity shards (tolerate 4 drive failures)
- **Storage efficiency**: ~50% (vs 33% for 3-way replication)
- **Recommended**: EC:4 for production (N/2 parity)

**Object Size Optimization**:
```python
# Delta Lake write configuration
write_deltalake(
    table_uri="s3://bucket/table",
    data=dataframe,
    mode="append",
    file_options={
        "max_rows_per_file": 1000000,  # ~100MB files (optimal for queries)
        "min_rows_per_file": 500000,
        "max_rows_per_group": 100000    # Parquet row group size
    }
)
```

**Optimal File Sizes**:
- 64MB - 256MB: Ideal for query performance
- Avoid <10MB files (too many S3 LIST operations)
- Avoid >1GB files (slow time-travel queries)

### Connection Pooling

```python
import boto3
from botocore.config import Config

# Configure connection pooling
s3_config = Config(
    max_pool_connections=50,  # Connection pool size
    retries={
        'max_attempts': 3,
        'mode': 'adaptive'
    }
)

s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='...',
    aws_secret_access_key='...',
    config=s3_config
)
```

### Best Practices
- Use distributed mode (4+ nodes) for production
- Configure EC:4 erasure coding for durability
- Target 64MB-256MB object sizes for Delta Lake
- Enable connection pooling (50+ connections)
- Monitor per-bucket metrics (ops/sec, bandwidth)
- Use lifecycle policies to expire old logs/temp files
- Enable versioning for critical buckets

### High Availability

**Load Balancing**:
```nginx
# NGINX upstream for MinIO cluster
upstream minio {
    least_conn;
    server minio1:9000;
    server minio2:9000;
    server minio3:9000;
    server minio4:9000;
}
```

**Health Checks**:
```bash
# MinIO health endpoint
curl http://minio:9000/minio/health/live
```

### Alternatives Considered
- **AWS S3**: Rejected - requires cloud deployment, higher cost for high-frequency access
- **Ceph**: Rejected - complex setup, higher operational overhead
- **Local Filesystem**: Rejected - no distributed durability, doesn't scale

### References
- [MinIO Distributed Setup](https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-multi-node-multi-drive.html)
- [MinIO Erasure Coding](https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html)
- [Delta Lake on S3](https://delta-io.github.io/delta-rs/usage/writing/writing-to-s3-with-locking-provider/)

---

## 5. DuckDB Delta Lake Integration

### Decision
Use DuckDB with delta extension for interactive analytical queries on Delta Lake tables.

### Rationale
- Native Delta Lake support via delta extension
- Sub-second query performance on GB-scale data
- OLAP-optimized (columnar execution, vectorization)
- No external dependencies (embedded database)
- Zero-copy reads from S3-compatible storage

### Configuration

**Install Delta Extension**:
```sql
INSTALL delta;
LOAD delta;
```

**Query Delta Lake Tables**:
```sql
-- Query Delta table on MinIO
SELECT *
FROM delta_scan('s3://bucket/table',
                 s3_access_key_id='...',
                 s3_secret_access_key='...',
                 s3_endpoint='minio:9000',
                 s3_use_ssl=false)
WHERE _ingestion_date >= '2025-01-01'
LIMIT 100;
```

**Python Integration**:
```python
import duckdb

# Create connection with MinIO credentials
conn = duckdb.connect()
conn.execute("INSTALL delta; LOAD delta;")
conn.execute(f"""
    CREATE SECRET minio_secret (
        TYPE S3,
        KEY_ID '{access_key}',
        SECRET '{secret_key}',
        ENDPOINT 'minio:9000',
        USE_SSL false
    );
""")

# Query Delta table
result = conn.execute("""
    SELECT customer_id, COUNT(*) as order_count
    FROM delta_scan('s3://lakehouse/orders')
    WHERE order_date >= '2025-01-01'
    GROUP BY customer_id
    ORDER BY order_count DESC
    LIMIT 10;
""").fetchdf()  # Returns pandas DataFrame
```

### Query Optimization

**Predicate Pushdown**:
```sql
-- DuckDB pushes filters to Delta scan (reads only relevant files)
SELECT *
FROM delta_scan('s3://bucket/table')
WHERE _ingestion_date = '2025-01-15'  -- Partition pruning
  AND customer_id = 12345;             -- Filter pushdown
```

**Projection Pruning**:
```sql
-- Only read required columns (reduces I/O)
SELECT customer_id, order_total
FROM delta_scan('s3://bucket/orders');  -- Reads only 2 columns
```

**Time Travel Queries**:
```sql
-- Query historical version
SELECT *
FROM delta_scan('s3://bucket/table',
                 version => 42);  -- Specific version

-- Query as of timestamp
SELECT *
FROM delta_scan('s3://bucket/table',
                 timestamp => '2025-01-15 10:30:00');
```

### Memory Management

DuckDB automatically manages memory, but for large queries:

```python
# Configure memory limit
conn.execute("SET memory_limit='8GB';")

# Enable disk-based query execution for larger-than-memory datasets
conn.execute("SET temp_directory='/tmp/duckdb';")
```

### Best Practices
- Use predicate pushdown (filter on partition columns)
- Project only needed columns (avoid `SELECT *`)
- Materialize frequently queried results as CTEs or temp tables
- For very large results, use `COPY TO` instead of fetching to Python
- Monitor query plans with `EXPLAIN` for optimization opportunities

### Performance Expectations

| Dataset Size | Query Type | Latency |
|--------------|------------|---------|
| 10GB | Full scan | 2-5 seconds |
| 10GB | Partition filter | 0.1-0.5 seconds |
| 100GB | Partition filter | 0.5-2 seconds |
| 100GB | Aggregate | 5-15 seconds |

*Assumes SSD storage and 8-core CPU*

### Alternatives Considered
- **Apache Spark**: Rejected - heavyweight, requires cluster, overkill for interactive queries
- **Presto/Trino**: Rejected - requires separate cluster deployment, higher operational overhead
- **pandas + PyArrow**: Rejected - poor performance on large datasets, no query optimization

### References
- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB Delta Extension](https://duckdb.org/docs/extensions/delta.html)
- [DuckDB S3 Integration](https://duckdb.org/docs/extensions/httpfs.html)

---

## 6. Reconciliation Algorithm Design

### Decision
Use hash-based batch comparison with checkpoint-driven incremental reconciliation.

### Rationale
- Efficient for large datasets (1TB+ in 6 hours target)
- Parallelizable across partitions
- Low memory footprint (streaming batches)
- Supports both full and incremental reconciliation
- Detects both missing records and field-level mismatches

### Algorithm Overview

**High-Level Process**:
1. **Partition**: Divide collection into batches (by _id range or time window)
2. **Hash**: Compute hash of each batch from MongoDB and Delta Lake
3. **Compare**: Identify batches with hash mismatches
4. **Drill-Down**: For mismatched batches, compare individual records
5. **Report**: Generate discrepancy report (missing, mismatched, extra)
6. **Repair** (optional): Sync identified discrepancies

**Pseudocode**:
```python
def reconcile(collection, delta_table, batch_size=10000):
    # 1. Get total record counts
    mongo_count = mongodb.count(collection)
    delta_count = duckdb.count(delta_table)

    if mongo_count != delta_count:
        report.add_discrepancy("count_mismatch", mongo_count, delta_count)

    # 2. Partition by _id ranges
    id_ranges = partition_by_id(collection, batch_size)

    # 3. Parallel batch comparison
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for id_range in id_ranges:
            future = executor.submit(compare_batch, id_range)
            futures.append(future)

        # Wait for all batches
        for future in as_completed(futures):
            batch_result = future.result()
            if batch_result.has_discrepancies:
                report.add(batch_result)

    return report

def compare_batch(id_range):
    # Fetch batch from MongoDB
    mongo_docs = mongodb.find({
        "_id": {"$gte": id_range.start, "$lt": id_range.end}
    }).sort("_id", 1)

    # Fetch batch from Delta Lake
    delta_docs = duckdb.execute(f"""
        SELECT * FROM delta_scan('{table_uri}')
        WHERE _id >= '{id_range.start}' AND _id < '{id_range.end}'
        ORDER BY _id
    """).fetchall()

    # Compare hashes
    mongo_hash = hash_batch(mongo_docs)
    delta_hash = hash_batch(delta_docs)

    if mongo_hash == delta_hash:
        return BatchResult(matches=True)

    # Hash mismatch → drill down to record level
    return compare_records(mongo_docs, delta_docs)

def compare_records(mongo_docs, delta_docs):
    # Create lookup maps
    mongo_map = {doc['_id']: doc for doc in mongo_docs}
    delta_map = {doc['_id']: doc for doc in delta_docs}

    discrepancies = []

    # Find missing in Delta Lake
    for doc_id in mongo_map.keys() - delta_map.keys():
        discrepancies.append({
            "type": "missing_in_delta",
            "id": doc_id,
            "mongo_doc": mongo_map[doc_id]
        })

    # Find extra in Delta Lake
    for doc_id in delta_map.keys() - mongo_map.keys():
        discrepancies.append({
            "type": "extra_in_delta",
            "id": doc_id,
            "delta_doc": delta_map[doc_id]
        })

    # Find field mismatches
    for doc_id in mongo_map.keys() & delta_map.keys():
        mongo_doc = mongo_map[doc_id]
        delta_doc = delta_map[doc_id]

        if not docs_equal(mongo_doc, delta_doc):
            field_diffs = compute_field_diffs(mongo_doc, delta_doc)
            discrepancies.append({
                "type": "field_mismatch",
                "id": doc_id,
                "diffs": field_diffs
            })

    return BatchResult(matches=False, discrepancies=discrepancies)
```

### Batch Processing Strategy

**Batch Size Calculation**:
```python
# Target: 1TB collection in 6 hours
# 1TB = 1,000,000 MB
# 6 hours = 21,600 seconds
# Required throughput: 46 MB/s

# Assume average document size: 5KB
# Documents per collection: 200M
# Batch size for parallelism: 10K documents = 50MB batch

BATCH_SIZE = 10000  # documents
NUM_WORKERS = 8     # parallel workers
```

### Incremental Reconciliation

For efficiency, reconcile only recent changes:

```python
def incremental_reconcile(since_timestamp):
    # Only reconcile documents modified since last reconciliation
    mongo_docs = mongodb.find({
        "_last_modified": {"$gte": since_timestamp}
    })

    # Compare with Delta Lake using time travel
    delta_docs = duckdb.execute(f"""
        SELECT * FROM delta_scan('{table_uri}',
                                  timestamp => '{since_timestamp}')
    """).fetchall()

    return compare_records(mongo_docs, delta_docs)
```

### Memory Management

**Streaming Batches**:
```python
def stream_batches(collection, batch_size):
    cursor = mongodb.find().batch_size(batch_size)
    batch = []

    for doc in cursor:
        batch.append(doc)
        if len(batch) >= batch_size:
            yield batch
            batch = []

    if batch:
        yield batch
```

**Memory Limit**: <4GB per worker for 1TB collection reconciliation

### Parallel Processing

**Partition Strategy**:
- Partition by _id ranges (ensures even distribution)
- Use MongoDB $min/$max to determine partition boundaries
- Parallelize across partitions (8 workers)

### Performance Characteristics

- **Time Complexity**: O(n) where n = number of documents
- **Space Complexity**: O(batch_size) = O(10K) ~ 50MB per worker
- **Expected Duration**:
  - 1TB collection (200M docs): ~5-6 hours with 8 workers
  - 100GB collection (20M docs): ~30-40 minutes

### Best Practices
- Use _id-based partitioning for even distribution
- Parallelize batch comparison (4-8 workers)
- Implement checkpointing to resume failed reconciliations
- Store reconciliation reports for historical tracking
- Run incremental reconciliation for frequent checks (daily)
- Run full reconciliation less frequently (weekly)

### Alternatives Considered
- **Merkle Trees**: Rejected - complex implementation, overhead for building trees
- **Sampling**: Rejected - cannot guarantee 100% discrepancy detection
- **Row-by-row comparison**: Rejected - too slow for large datasets

### References
- [Data Reconciliation Patterns](https://martinfowler.com/articles/patterns-of-distributed-systems/reconciliation.html)
- [Efficient Large-Scale Data Comparison](https://www.usenix.org/conference/nsdi13/technical-sessions/presentation/li_xian)

---

## 7. FastAPI Production Deployment

### Decision
Use Uvicorn with Gunicorn workers for production FastAPI deployment.

### Rationale
- Uvicorn: High-performance ASGI server (async support)
- Gunicorn: Process manager (worker lifecycle, graceful reloads)
- Proven combination for production FastAPI apps
- Handles async I/O efficiently (critical for database/HTTP clients)
- Supports graceful shutdown and zero-downtime deployments

### Configuration

**Gunicorn + Uvicorn Workers**:
```bash
gunicorn api.main:app \
  --workers 4 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000 \
  --timeout 120 \
  --graceful-timeout 30 \
  --keep-alive 5 \
  --max-requests 10000 \
  --max-requests-jitter 1000 \
  --access-logfile - \
  --error-logfile - \
  --log-level info
```

**Worker Calculation**:
```python
# Rule of thumb: (2 * CPU cores) + 1
import multiprocessing
workers = (2 * multiprocessing.cpu_count()) + 1  # e.g., 9 workers for 4-core CPU
```

### Async Endpoint Best Practices

**I/O-Bound Operations** (use async):
```python
from fastapi import FastAPI
import httpx
import asyncpg

app = FastAPI()

@app.get("/pipelines/{id}")
async def get_pipeline(id: str):
    # Async database query
    async with db_pool.acquire() as conn:
        pipeline = await conn.fetchrow(
            "SELECT * FROM pipelines WHERE id = $1", id
        )
    return pipeline

@app.post("/pipelines/{id}/start")
async def start_pipeline(id: str):
    # Async HTTP call to Kafka Connect
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"http://kafka-connect:8083/connectors/{id}/resume"
        )
    return {"status": "started"}
```

**CPU-Bound Operations** (use background tasks):
```python
from fastapi import BackgroundTasks

@app.post("/reconciliation/jobs")
async def trigger_reconciliation(
    collection: str,
    background_tasks: BackgroundTasks
):
    # Schedule CPU-intensive task in background
    background_tasks.add_task(run_reconciliation, collection)
    return {"status": "scheduled", "collection": collection}
```

### Connection Pooling

**PostgreSQL (asyncpg)**:
```python
import asyncpg

# Create connection pool at startup
@app.on_event("startup")
async def startup():
    app.state.db_pool = await asyncpg.create_pool(
        host='postgres',
        port=5432,
        database='cdc_metadata',
        user='admin',
        password='...',
        min_size=10,
        max_size=50,
        command_timeout=60
    )

@app.on_event("shutdown")
async def shutdown():
    await app.state.db_pool.close()
```

**MongoDB (motor)**:
```python
from motor.motor_asyncio import AsyncIOMotorClient

@app.on_event("startup")
async def startup():
    app.state.mongo_client = AsyncIOMotorClient(
        'mongodb://mongo:27017',
        maxPoolSize=50,
        minPoolSize=10,
        serverSelectionTimeoutMS=5000
    )
```

**MinIO/S3 (aioboto3)**:
```python
import aioboto3

session = aioboto3.Session()

async def get_s3_client():
    async with session.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='...',
        aws_secret_access_key='...'
    ) as s3:
        yield s3
```

### Health Checks

**Liveness Probe** (is the service running?):
```python
@app.get("/api/v1/health")
async def health_check():
    return {"status": "ok"}
```

**Readiness Probe** (is the service ready to handle requests?):
```python
@app.get("/api/v1/ready")
async def readiness_check():
    checks = {
        "database": await check_database(),
        "kafka": await check_kafka(),
        "minio": await check_minio()
    }

    if all(checks.values()):
        return {"status": "ready", "checks": checks}
    else:
        raise HTTPException(status_code=503, detail=checks)
```

### Graceful Shutdown

```python
import signal
import asyncio

async def shutdown_handler():
    # Close database connections
    await app.state.db_pool.close()

    # Close HTTP clients
    await app.state.http_client.aclose()

    # Complete in-flight requests (Gunicorn handles this)
    logger.info("Graceful shutdown complete")

@app.on_event("shutdown")
async def shutdown():
    await shutdown_handler()
```

### Rate Limiting

```python
from slowapi import Limiter
from slowapi.util import get_remote_address

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter

@app.post("/api/v1/pipelines")
@limiter.limit("10/minute")  # 10 requests per minute per IP
async def create_pipeline(request: Request):
    ...
```

### Best Practices
- Use Gunicorn + Uvicorn workers for production
- Implement connection pooling for all external services
- Use async/await for I/O-bound operations
- Implement health checks (liveness + readiness)
- Handle graceful shutdown (close connections, complete requests)
- Enable request timeout (prevent hung requests)
- Implement rate limiting for public endpoints
- Use structured logging with correlation IDs

### Alternatives Considered
- **Pure Uvicorn**: Rejected - no process management, manual worker orchestration
- **Hypercorn**: Rejected - less mature than Uvicorn, smaller community
- **Daphne**: Rejected - Django-focused, not optimized for FastAPI

### References
- [FastAPI Deployment](https://fastapi.tiangolo.com/deployment/)
- [Gunicorn Configuration](https://docs.gunicorn.org/en/stable/settings.html)
- [Uvicorn Workers](https://www.uvicorn.org/deployment/#gunicorn)

---

## 8. HashiCorp Vault Integration

### Decision
Use HashiCorp Vault for centralized secrets management with dynamic database credentials.

### Rationale
- Industry-standard secrets management solution
- Supports dynamic secrets (auto-rotation without downtime)
- Audit logging of all secret access
- Integrates with Kubernetes, Docker, and standalone apps
- Open-source with enterprise options

### Configuration

**Vault Server Setup**:
```bash
# Development mode (for local testing)
vault server -dev

# Production mode
vault server -config=/etc/vault/config.hcl
```

**config.hcl**:
```hcl
storage "file" {
  path = "/vault/data"
}

listener "tcp" {
  address = "0.0.0.0:8200"
  tls_disable = 0
  tls_cert_file = "/vault/certs/cert.pem"
  tls_key_file = "/vault/certs/key.pem"
}

api_addr = "https://vault:8200"
cluster_addr = "https://vault:8201"
ui = true
```

### Dynamic Secrets for MongoDB

**Configure MongoDB Secret Engine**:
```bash
# Enable MongoDB secret engine
vault secrets enable database

# Configure MongoDB connection
vault write database/config/mongodb \
  plugin_name=mongodb-database-plugin \
  allowed_roles="cdc-reader" \
  connection_url="mongodb://{{username}}:{{password}}@mongo:27017/admin" \
  username="vault-admin" \
  password="vault-password"

# Create role with TTL
vault write database/roles/cdc-reader \
  db_name=mongodb \
  creation_statements='{ "db": "admin", "roles": [{ "role": "readAnyDatabase" }] }' \
  default_ttl="1h" \
  max_ttl="24h"
```

**Read Dynamic Credentials**:
```python
import hvac

# Initialize Vault client
client = hvac.Client(url='https://vault:8200')
client.token = os.getenv('VAULT_TOKEN')

# Get dynamic MongoDB credentials
mongo_creds = client.secrets.database.generate_credentials(
    name='cdc-reader'
)

username = mongo_creds['data']['username']
password = mongo_creds['data']['password']
lease_id = mongo_creds['lease_id']

# Use credentials (valid for 1 hour)
mongo_client = MongoClient(
    f"mongodb://{username}:{password}@mongo:27017"
)

# Renew lease before expiration
client.sys.renew_lease(lease_id)
```

### Secret Rotation Strategy

**Automatic Rotation** (Vault handles):
```python
import time
import threading

class VaultSecretManager:
    def __init__(self, vault_client, role_name):
        self.vault = vault_client
        self.role = role_name
        self.credentials = None
        self.lease_id = None
        self.renew_thread = None

    def get_credentials(self):
        # Get fresh credentials
        response = self.vault.secrets.database.generate_credentials(
            name=self.role
        )

        self.credentials = response['data']
        self.lease_id = response['lease_id']
        self.lease_duration = response['lease_duration']

        # Start renewal thread
        self.start_renewal()

        return self.credentials

    def start_renewal(self):
        # Renew at 50% of lease duration
        renew_interval = self.lease_duration * 0.5

        def renew_loop():
            while True:
                time.sleep(renew_interval)
                try:
                    self.vault.sys.renew_lease(self.lease_id)
                except Exception as e:
                    # Lease expired, get new credentials
                    self.get_credentials()

        self.renew_thread = threading.Thread(target=renew_loop, daemon=True)
        self.renew_thread.start()
```

### Vault Authentication Methods

**AppRole** (for services):
```python
# Authenticate using AppRole
approle_response = client.auth.approle.login(
    role_id=os.getenv('VAULT_ROLE_ID'),
    secret_id=os.getenv('VAULT_SECRET_ID')
)

client.token = approle_response['auth']['client_token']
```

**Kubernetes Auth** (for pods):
```python
# Read service account token
with open('/var/run/secrets/kubernetes.io/serviceaccount/token') as f:
    jwt = f.read()

# Authenticate
k8s_response = client.auth.kubernetes.login(
    role='cdc-pipeline',
    jwt=jwt
)

client.token = k8s_response['auth']['client_token']
```

### Best Practices
- Use dynamic secrets for databases (auto-rotation)
- Set reasonable TTLs (1-24 hours for database creds)
- Implement automatic lease renewal
- Use AppRole or Kubernetes auth (avoid long-lived tokens)
- Enable audit logging
- Encrypt Vault storage backend
- Use TLS for all Vault communication
- Implement secret caching with expiration

### Leasing and Renewal

**Lease Lifecycle**:
1. Request secret → Vault generates + returns with lease_id
2. Use secret for duration of TTL
3. Renew lease before expiration (at 50% of TTL)
4. On renewal failure → request new secret
5. On service shutdown → revoke lease

### Alternatives Considered
- **Environment Variables**: Rejected - no rotation, exposed in process listings
- **Kubernetes Secrets**: Rejected - no rotation, limited audit logging
- **AWS Secrets Manager**: Rejected - cloud vendor lock-in, higher cost
- **CyberArk**: Rejected - expensive, complex setup for open-source project

### References
- [Vault Documentation](https://www.vaultproject.io/docs)
- [Vault Database Secrets](https://www.vaultproject.io/docs/secrets/databases)
- [Vault AppRole Auth](https://www.vaultproject.io/docs/auth/approle)

---

## 9. Observability Stack Configuration

### Decision
Use Prometheus + Grafana + Jaeger + ELK stack for comprehensive observability.

### Rationale
- **Prometheus**: Industry-standard metrics collection and alerting
- **Grafana**: Powerful visualization and dashboarding
- **Jaeger**: Distributed tracing for complex request flows
- **ELK**: Centralized logging and log analysis
- All open-source, mature, and production-proven
- Integrates seamlessly with Python applications

### Prometheus Configuration

**Scraping Configuration** (prometheus.yml):
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  # FastAPI metrics
  - job_name: 'fastapi'
    static_configs:
      - targets: ['api:8000']
    metrics_path: '/metrics'

  # Kafka JMX metrics
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka:9101']

  # MongoDB metrics
  - job_name: 'mongodb'
    static_configs:
      - targets: ['mongodb-exporter:9216']

  # MinIO metrics
  - job_name: 'minio'
    static_configs:
      - targets: ['minio:9000']
    metrics_path: '/minio/v2/metrics/cluster'
```

**Python Metrics** (FastAPI):
```python
from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi import Response

# Define metrics
pipeline_created = Counter(
    'pipeline_created_total',
    'Total pipelines created',
    ['status']
)

request_duration = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration',
    ['method', 'endpoint']
)

active_pipelines = Gauge(
    'active_pipelines',
    'Number of active CDC pipelines'
)

# Expose metrics endpoint
@app.get("/metrics")
async def metrics():
    return Response(
        content=generate_latest(),
        media_type="text/plain"
    )

# Instrument endpoints
@app.post("/api/v1/pipelines")
async def create_pipeline(config: PipelineConfig):
    with request_duration.labels('POST', '/pipelines').time():
        try:
            pipeline = await pipeline_service.create(config)
            pipeline_created.labels(status='success').inc()
            active_pipelines.inc()
            return pipeline
        except Exception as e:
            pipeline_created.labels(status='error').inc()
            raise
```

### OpenTelemetry Instrumentation

**Auto-Instrumentation for FastAPI**:
```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

# Configure tracer
trace.set_tracer_provider(TracerProvider())
jaeger_exporter = JaegerExporter(
    agent_host_name="jaeger",
    agent_port=6831,
)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(jaeger_exporter)
)

# Auto-instrument FastAPI
FastAPIInstrumentor.instrument_app(app)

# Manual tracing for custom operations
tracer = trace.get_tracer(__name__)

async def write_to_delta_lake(data):
    with tracer.start_as_current_span("write_delta_lake") as span:
        span.set_attribute("table", data.table_name)
        span.set_attribute("record_count", len(data.records))

        # Write to Delta Lake
        result = await delta_writer.write(data)

        span.set_attribute("bytes_written", result.bytes)
        return result
```

### Jaeger Sampling Strategies

**Configuration**:
```yaml
# jaeger-config.yml
sampling:
  default_strategy:
    type: probabilistic
    param: 0.1  # Sample 10% of traces

  per_operation_strategies:
    default_sampling_probability: 0.1
    default_lower_bound_traces_per_second: 1.0

    per_operation_strategies:
      # Always trace errors
      - operation: "POST /api/v1/pipelines"
        probabilistic_sampling:
          sampling_probability: 1.0  # 100% sampling for critical ops

      # Lower sampling for high-volume endpoints
      - operation: "GET /api/v1/health"
        probabilistic_sampling:
          sampling_probability: 0.01  # 1% sampling
```

### ELK Stack Configuration

**Structured Logging** (structlog):
```python
import structlog

# Configure structlog
structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Log with structured context
logger.info(
    "pipeline_created",
    pipeline_id="abc123",
    collection="users",
    target_table="lakehouse.users",
    user_id="operator-1",
    trace_id=get_trace_id()
)
```

**Filebeat Configuration** (filebeat.yml):
```yaml
filebeat.inputs:
  - type: container
    paths:
      - '/var/lib/docker/containers/*/*.log'
    json.keys_under_root: true
    json.add_error_key: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "cdc-logs-%{+yyyy.MM.dd}"

setup.ilm.enabled: true
setup.ilm.rollover_alias: "cdc-logs"
setup.ilm.pattern: "cdc-logs-*"
```

**Elasticsearch Index Lifecycle**:
```json
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_size": "50GB",
            "max_age": "1d"
          }
        }
      },
      "delete": {
        "min_age": "30d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}
```

### Grafana Dashboards

**Example Dashboard JSON**:
```json
{
  "dashboard": {
    "title": "CDC Pipeline Metrics",
    "panels": [
      {
        "title": "Replication Lag",
        "targets": [
          {
            "expr": "kafka_consumer_lag{topic=~'mongodb.*'}",
            "legendFormat": "{{topic}}"
          }
        ],
        "type": "graph"
      },
      {
        "title": "Events Per Second",
        "targets": [
          {
            "expr": "rate(kafka_consumer_records_consumed_total[1m])",
            "legendFormat": "{{consumer_group}}"
          }
        ],
        "type": "graph"
      }
    ]
  }
}
```

### Best Practices
- Use structured logging (JSON format) for all services
- Implement correlation IDs across all components
- Sample high-volume traces (10% default, 100% for errors)
- Set up alerts for SLO violations
- Use Grafana dashboards for operational visibility
- Retain logs for 30 days (adjust for compliance requirements)
- Monitor Prometheus, Jaeger, Elasticsearch health

### Alternatives Considered
- **DataDog**: Rejected - expensive SaaS, vendor lock-in
- **New Relic**: Rejected - expensive, less flexible
- **CloudWatch + X-Ray**: Rejected - AWS-specific, higher cost

### References
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Jaeger Documentation](https://www.jaegertracing.io/docs/)
- [ELK Stack](https://www.elastic.co/what-is/elk-stack)

---

## 10. Test Data Generation

### Decision
Use Faker + Mimesis for realistic MongoDB test data generation.

### Rationale
- Faker: Rich API, supports many data types (names, addresses, emails, etc.)
- Mimesis: Fast performance, good for high-volume data generation
- Both support custom providers for domain-specific data
- Reproducible data with seed values
- Lightweight dependencies

### Configuration

**Install Libraries**:
```bash
pip install faker mimesis
```

**Basic Usage**:
```python
from faker import Faker
from mimesis import Person, Address, Datetime
import random

fake = Faker()
person = Person()
address = Address()
datetime_gen = Datetime()

# Generate sample document
def generate_user():
    return {
        "_id": fake.uuid4(),
        "name": person.full_name(),
        "email": person.email(),
        "phone": person.phone_number(),
        "address": {
            "street": address.street_name(),
            "city": address.city(),
            "country": address.country(),
            "coordinates": {
                "lat": float(address.latitude()),
                "lon": float(address.longitude())
            }
        },
        "created_at": datetime_gen.datetime(),
        "is_active": fake.boolean(chance_of_getting_true=75),
        "tags": [fake.word() for _ in range(random.randint(1, 5))]
    }
```

### Seeding MongoDB

**Bulk Insert**:
```python
from pymongo import MongoClient
from faker import Faker

client = MongoClient('mongodb://localhost:27017')
db = client['testdb']
collection = db['users']

fake = Faker()
Faker.seed(12345)  # Reproducible data

# Generate 1M documents
batch_size = 10000
total_docs = 1000000

for i in range(0, total_docs, batch_size):
    batch = [generate_user() for _ in range(batch_size)]
    collection.insert_many(batch)
    print(f"Inserted {i + batch_size} / {total_docs}")
```

### Generating Change Events

**Simulate Insert/Update/Delete Operations**:
```python
import time
import random

def generate_changes(collection, num_changes=1000, delay_ms=10):
    """Generate realistic change events"""
    for i in range(num_changes):
        operation = random.choices(
            ['insert', 'update', 'delete'],
            weights=[50, 40, 10]  # 50% inserts, 40% updates, 10% deletes
        )[0]

        if operation == 'insert':
            collection.insert_one(generate_user())

        elif operation == 'update':
            # Update random document
            doc = collection.find_one()
            if doc:
                collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"last_updated": datetime.now()}}
                )

        elif operation == 'delete':
            # Delete random document
            doc = collection.find_one()
            if doc:
                collection.delete_one({"_id": doc["_id"]})

        time.sleep(delay_ms / 1000)  # Simulate real-time changes
```

### Custom Providers

**Domain-Specific Data**:
```python
from faker.providers import BaseProvider

class OrderProvider(BaseProvider):
    def order_status(self):
        return self.random_element(['pending', 'processing', 'shipped', 'delivered', 'cancelled'])

    def payment_method(self):
        return self.random_element(['credit_card', 'debit_card', 'paypal', 'crypto'])

# Register provider
fake = Faker()
fake.add_provider(OrderProvider)

def generate_order():
    return {
        "_id": fake.uuid4(),
        "order_number": fake.ean13(),
        "customer_id": fake.uuid4(),
        "status": fake.order_status(),
        "payment_method": fake.payment_method(),
        "total": float(fake.pydecimal(left_digits=4, right_digits=2, positive=True)),
        "items": [
            {
                "sku": fake.ean8(),
                "name": fake.catch_phrase(),
                "quantity": fake.random_int(1, 10),
                "price": float(fake.pydecimal(left_digits=3, right_digits=2, positive=True))
            }
            for _ in range(fake.random_int(1, 5))
        ],
        "created_at": fake.date_time_this_year()
    }
```

### Performance Testing Data

**Generate High-Volume Test Data**:
```python
# Generate 10M documents for performance testing
def generate_large_dataset(collection_name, num_docs=10_000_000):
    collection = db[collection_name]
    collection.drop()  # Start fresh

    batch_size = 50000
    batches = num_docs // batch_size

    for i in range(batches):
        batch = [generate_user() for _ in range(batch_size)]
        collection.insert_many(batch, ordered=False)

        if (i + 1) % 10 == 0:
            print(f"Progress: {(i + 1) * batch_size:,} / {num_docs:,}")
```

### Reproducible Fixtures

**Test Fixtures with Seeds**:
```python
import pytest
from faker import Faker

@pytest.fixture
def fake_users():
    """Generate reproducible test users"""
    fake = Faker()
    Faker.seed(42)  # Same seed = same data

    return [generate_user() for _ in range(100)]

def test_cdc_pipeline(fake_users):
    # Insert users into MongoDB
    collection.insert_many(fake_users)

    # Verify they appear in Delta Lake
    assert delta_table.count() == 100
```

### Best Practices
- Use seeds for reproducible test data
- Generate realistic data (avoid lorem ipsum for production-like testing)
- Create custom providers for domain-specific data
- Use bulk inserts for performance (batch_size = 10K-50K)
- Vary data distribution (80% common cases, 20% edge cases)
- Include null values and missing fields (MongoDB is schemaless)
- Generate sufficient volume for performance testing (1M+ docs)

### Data Volume Requirements

| Test Type | Document Count | Purpose |
|-----------|----------------|---------|
| Unit Tests | 100-1,000 | Fast, focused tests |
| Integration Tests | 10,000-100,000 | Realistic workflows |
| Performance Tests | 1,000,000-10,000,000 | Throughput validation |
| Stress Tests | 10,000,000+ | Breaking point discovery |

### Alternatives Considered
- **Hand-Crafted Fixtures**: Rejected - time-consuming, limited variety
- **Production Data Copy**: Rejected - security/privacy concerns, requires anonymization
- **Random Data**: Rejected - unrealistic, doesn't expose edge cases

### References
- [Faker Documentation](https://faker.readthedocs.io/)
- [Mimesis Documentation](https://mimesis.name/)
- [PyMongo Bulk Operations](https://pymongo.readthedocs.io/en/stable/examples/bulk.html)

---

## Summary of Key Decisions

| Area | Technology | Key Rationale |
|------|-----------|---------------|
| CDC Connector | Debezium MongoDB Connector 2.5+ | Industry standard, proven at scale, handles edge cases |
| Schema Evolution | Delta Lake with delta-rs | Native Rust performance, ACID guarantees, schema merging |
| Exactly-Once | Kafka EOS with transactions | Zero data loss, prevents duplicates, minimal overhead |
| Object Storage | MinIO (distributed) | S3-compatible, erasure coding, high availability |
| Analytics | DuckDB with delta extension | Sub-second queries, OLAP-optimized, zero-copy reads |
| Reconciliation | Hash-based batch comparison | Efficient for TB-scale, parallelizable, low memory |
| API Framework | FastAPI + Uvicorn + Gunicorn | Async support, high performance, production-ready |
| Secrets Management | HashiCorp Vault | Dynamic secrets, auto-rotation, audit logging |
| Observability | Prometheus + Grafana + Jaeger + ELK | Comprehensive metrics, traces, logs; open-source |
| Test Data | Faker + Mimesis | Realistic data, custom providers, reproducible |

---

**Research Status**: ✅ Complete
**Next Phase**: Phase 1 - Design & Contracts
**Last Updated**: 2025-11-26
