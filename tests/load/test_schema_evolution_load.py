"""Load test for schema evolution with 2000+ documents.

This script tests schema evolution under load with evolving schemas
and monitors for errors and restarts.
"""

import time
import logging
from datetime import datetime, timedelta
from typing import Generator
import pymongo
import pyarrow as pa
from deltalake import DeltaTable
from testcontainers.mongodb import MongoDbContainer
from testcontainers.minio import MinioContainer

from delta_writer.src.writer.delta_writer import DeltaWriter
from delta_writer.src.transformers.bson_to_delta import BSONToDeltaConverter

# Configure logging to monitor for errors
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_documents_phase1(start_id: int, count: int) -> list[dict]:
    """
    Generate Phase 1 documents with basic schema.

    Schema: id, name, timestamp, status, value
    """
    docs = []
    for i in range(start_id, start_id + count):
        doc = {
            "_id": i,
            "name": f"record_{i:06d}",
            "timestamp": datetime.now() - timedelta(hours=count - i),
            "status": "active" if i % 2 == 0 else "inactive",
            "value": i * 100
        }
        docs.append(doc)
    return docs


def generate_documents_phase2(start_id: int, count: int) -> list[dict]:
    """
    Generate Phase 2 documents with expanded schema.

    Added: category, priority, metadata.version
    Type change: value becomes float
    """
    docs = []
    for i in range(start_id, start_id + count):
        doc = {
            "_id": i,
            "name": f"record_{i:06d}",
            "timestamp": datetime.now() - timedelta(hours=count - i),
            "status": "pending",
            "value": i * 100.5,  # Changed to float
            "category": f"CAT{i % 10}",  # New field
            "priority": i % 5,  # New field
            "metadata": {  # New nested field
                "version": 2,
                "created_by": f"user_{i % 50}"
            }
        }
        docs.append(doc)
    return docs


def generate_documents_phase3(start_id: int, count: int) -> list[dict]:
    """
    Generate Phase 3 documents with further schema evolution.

    Added: tags array, metrics struct, metadata.tags
    """
    docs = []
    for i in range(start_id, start_id + count):
        doc = {
            "_id": i,
            "name": f"record_{i:06d}",
            "timestamp": datetime.now() - timedelta(hours=count - i),
            "status": "completed",
            "value": i * 100.75,
            "category": f"CAT{i % 10}",
            "priority": i % 5,
            "metadata": {
                "version": 3,
                "created_by": f"user_{i % 50}",
                "tags": [f"tag{i % 10}", f"tag{(i + 1) % 10}"]  # New nested array
            },
            "metrics": {  # New top-level nested struct
                "score": i * 1.5,
                "count": i,
                "rate": i * 0.01
            },
            "tags": [f"main_tag{i % 5}"]  # New top-level array
        }
        docs.append(doc)
    return docs


def generate_documents_phase4(start_id: int, count: int) -> list[dict]:
    """
    Generate Phase 4 documents with complex nested evolution.

    Added: deep nested structures, arrays of structs
    """
    docs = []
    for i in range(start_id, start_id + count):
        doc = {
            "_id": i,
            "name": f"record_{i:06d}",
            "timestamp": datetime.now() - timedelta(hours=count - i),
            "status": "archived",
            "value": i * 101.0,
            "category": f"CAT{i % 10}",
            "priority": i % 5,
            "metadata": {
                "version": 4,
                "created_by": f"user_{i % 50}",
                "tags": [f"tag{i % 10}", f"tag{(i + 1) % 10}"],
                "audit": {  # New deeply nested struct
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(),
                    "change_count": i % 100
                }
            },
            "metrics": {
                "score": i * 1.5,
                "count": i,
                "rate": i * 0.01,
                "percentile": i * 0.99  # New field in existing struct
            },
            "tags": [f"main_tag{i % 5}"],
            "relations": [  # New array of structs
                {"target_id": i + 1, "type": "parent"},
                {"target_id": i + 2, "type": "sibling"}
            ]
        }
        docs.append(doc)
    return docs


def run_load_test():
    """Run the load test with 2000+ documents and schema evolution."""
    logger.info("=" * 80)
    logger.info("Starting schema evolution load test")
    logger.info("=" * 80)

    # Start containers
    logger.info("Starting MongoDB container...")
    mongodb_container = MongoDbContainer("mongo:7.0")
    mongodb_container.start()

    logger.info("Starting MinIO container...")
    minio_container = MinioContainer()
    minio_container.start()

    try:
        # Setup clients
        connection_url = mongodb_container.get_connection_url()
        mongo_client = pymongo.MongoClient(connection_url)
        db = mongo_client["load_test_db"]
        collection = db["schema_evolution"]

        storage_options = {
            "AWS_ACCESS_KEY_ID": minio_container.access_key,
            "AWS_SECRET_ACCESS_KEY": minio_container.secret_key,
            "AWS_ENDPOINT_URL": minio_container.get_config()["endpoint"],
            "AWS_ALLOW_HTTP": "true",
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }

        delta_writer = DeltaWriter(storage_options=storage_options)
        table_uri = "s3://test-bucket/schema_evolution_load"

        total_docs = 0
        total_duration = 0
        errors = []

        # Phase 1: 500 documents with basic schema
        logger.info("-" * 80)
        logger.info("PHASE 1: Writing 500 documents with basic schema")
        logger.info("-" * 80)

        phase1_docs = generate_documents_phase1(0, 500)
        collection.insert_many(phase1_docs)

        start_time = time.time()
        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in phase1_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "load_test.schema_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        try:
            stats = delta_writer.write_batch(table_uri, converted_docs)
            phase1_duration = time.time() - start_time
            total_docs += len(phase1_docs)
            total_duration += phase1_duration

            logger.info(f"Phase 1 completed: {len(phase1_docs)} docs in {phase1_duration:.2f}s")
            logger.info(f"Throughput: {stats['records_per_second']:.2f} docs/sec")
            logger.info(f"Schema version: {stats.get('schema_version', 'N/A')}")
        except Exception as e:
            logger.error(f"Phase 1 failed: {e}")
            errors.append(f"Phase 1: {e}")

        # Phase 2: 500 documents with expanded schema
        logger.info("-" * 80)
        logger.info("PHASE 2: Writing 500 documents with expanded schema")
        logger.info("-" * 80)

        phase2_docs = generate_documents_phase2(500, 500)
        collection.insert_many(phase2_docs)

        start_time = time.time()
        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in phase2_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 500
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "load_test.schema_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        try:
            stats = delta_writer.write_batch(table_uri, converted_docs)
            phase2_duration = time.time() - start_time
            total_docs += len(phase2_docs)
            total_duration += phase2_duration

            logger.info(f"Phase 2 completed: {len(phase2_docs)} docs in {phase2_duration:.2f}s")
            logger.info(f"Throughput: {stats['records_per_second']:.2f} docs/sec")
            logger.info(f"Schema version: {stats.get('schema_version', 'N/A')}")
            logger.info(f"Fields added: {stats.get('schema_fields_added', 0)}")
            logger.info(f"Types widened: {stats.get('schema_types_widened', 0)}")
        except Exception as e:
            logger.error(f"Phase 2 failed: {e}")
            errors.append(f"Phase 2: {e}")

        # Phase 3: 500 documents with further evolution
        logger.info("-" * 80)
        logger.info("PHASE 3: Writing 500 documents with further schema evolution")
        logger.info("-" * 80)

        phase3_docs = generate_documents_phase3(1000, 500)
        collection.insert_many(phase3_docs)

        start_time = time.time()
        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in phase3_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1000
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "load_test.schema_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        try:
            stats = delta_writer.write_batch(table_uri, converted_docs)
            phase3_duration = time.time() - start_time
            total_docs += len(phase3_docs)
            total_duration += phase3_duration

            logger.info(f"Phase 3 completed: {len(phase3_docs)} docs in {phase3_duration:.2f}s")
            logger.info(f"Throughput: {stats['records_per_second']:.2f} docs/sec")
            logger.info(f"Schema version: {stats.get('schema_version', 'N/A')}")
        except Exception as e:
            logger.error(f"Phase 3 failed: {e}")
            errors.append(f"Phase 3: {e}")

        # Phase 4: 600 documents with complex nested evolution
        logger.info("-" * 80)
        logger.info("PHASE 4: Writing 600 documents with complex nested evolution")
        logger.info("-" * 80)

        phase4_docs = generate_documents_phase4(1500, 600)
        collection.insert_many(phase4_docs)

        start_time = time.time()
        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in phase4_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1500
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "load_test.schema_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        try:
            stats = delta_writer.write_batch(table_uri, converted_docs)
            phase4_duration = time.time() - start_time
            total_docs += len(phase4_docs)
            total_duration += phase4_duration

            logger.info(f"Phase 4 completed: {len(phase4_docs)} docs in {phase4_duration:.2f}s")
            logger.info(f"Throughput: {stats['records_per_second']:.2f} docs/sec")
            logger.info(f"Schema version: {stats.get('schema_version', 'N/A')}")
        except Exception as e:
            logger.error(f"Phase 4 failed: {e}")
            errors.append(f"Phase 4: {e}")

        # Verification
        logger.info("=" * 80)
        logger.info("VERIFICATION")
        logger.info("=" * 80)

        table = DeltaTable(table_uri, storage_options=storage_options)
        df = table.to_pandas()

        logger.info(f"Total documents written: {total_docs}")
        logger.info(f"Documents in Delta Lake: {len(df)}")
        logger.info(f"Total duration: {total_duration:.2f}s")
        logger.info(f"Average throughput: {total_docs / total_duration:.2f} docs/sec")

        # Check schema
        schema = table.schema().to_pyarrow()
        logger.info(f"Final schema has {len(schema)} fields")

        expected_fields = {
            "_id", "name", "timestamp", "status", "value",
            "category", "priority", "metadata", "metrics", "tags", "relations",
            "_cdc_timestamp", "_cdc_operation", "_ingestion_timestamp",
            "_kafka_offset", "_kafka_partition", "_kafka_topic", "_ingestion_date"
        }

        actual_fields = {f.name for f in schema}
        missing_fields = expected_fields - actual_fields

        if missing_fields:
            logger.error(f"Missing fields in final schema: {missing_fields}")
            errors.append(f"Missing fields: {missing_fields}")
        else:
            logger.info("All expected fields present in schema")

        # Check data integrity
        unique_ids = df["_id"].nunique()
        if unique_ids != total_docs:
            logger.error(f"Data integrity issue: {unique_ids} unique IDs, expected {total_docs}")
            errors.append(f"ID count mismatch: {unique_ids} vs {total_docs}")
        else:
            logger.info(f"Data integrity verified: {unique_ids} unique documents")

        # Schema manager metrics
        schema_metrics = delta_writer.schema_manager.get_metrics()
        logger.info("=" * 80)
        logger.info("SCHEMA EVOLUTION METRICS")
        logger.info("=" * 80)
        for metric, value in schema_metrics.items():
            logger.info(f"{metric}: {value}")

        # Cache statistics
        cache_stats = delta_writer.schema_manager.cache.get_statistics()
        logger.info("=" * 80)
        logger.info("CACHE STATISTICS")
        logger.info("=" * 80)
        for stat, value in cache_stats.items():
            if isinstance(value, dict):
                logger.info(f"{stat}:")
                for k, v in value.items():
                    logger.info(f"  {k}: {v}")
            else:
                logger.info(f"{stat}: {value}")

        # Summary
        logger.info("=" * 80)
        logger.info("LOAD TEST SUMMARY")
        logger.info("=" * 80)
        if errors:
            logger.error(f"Load test completed with {len(errors)} errors:")
            for error in errors:
                logger.error(f"  - {error}")
        else:
            logger.info("Load test completed successfully with no errors!")

        logger.info(f"Total documents: {total_docs}")
        logger.info(f"Total duration: {total_duration:.2f}s")
        logger.info(f"Average throughput: {total_docs / total_duration:.2f} docs/sec")

    finally:
        # Cleanup
        logger.info("Cleaning up containers...")
        mongo_client.close()
        mongodb_container.stop()
        minio_container.stop()
        logger.info("Load test complete!")


if __name__ == "__main__":
    run_load_test()
