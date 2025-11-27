"""Integration tests for schema evolution with new field addition."""

import pytest
import time
from datetime import datetime
from typing import Generator
from testcontainers.mongodb import MongoDbContainer
from testcontainers.kafka import KafkaContainer
from testcontainers.minio import MinioContainer
import pymongo
import pyarrow as pa
from deltalake import DeltaTable

from delta_writer.src.writer.delta_writer import DeltaWriter
from delta_writer.src.transformers.bson_to_delta import BSONToDeltaConverter
from delta_writer.src.transformers.schema_inferrer import SchemaInferrer


@pytest.fixture(scope="module")
def mongodb_container() -> Generator[MongoDbContainer, None, None]:
    """Start MongoDB container for integration tests."""
    container = MongoDbContainer("mongo:7.0")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def minio_container() -> Generator[MinioContainer, None, None]:
    """Start MinIO container for integration tests."""
    container = MinioContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture
def mongodb_client(mongodb_container):
    """Get MongoDB client."""
    connection_url = mongodb_container.get_connection_url()
    client = pymongo.MongoClient(connection_url)
    yield client
    client.close()


@pytest.fixture
def storage_options(minio_container):
    """Get MinIO storage options."""
    return {
        "AWS_ACCESS_KEY_ID": minio_container.access_key,
        "AWS_SECRET_ACCESS_KEY": minio_container.secret_key,
        "AWS_ENDPOINT_URL": minio_container.get_config()["endpoint"],
        "AWS_ALLOW_HTTP": "true",
        "AWS_REGION": "us-east-1",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


@pytest.fixture
def delta_writer(storage_options):
    """Get DeltaWriter instance."""
    return DeltaWriter(storage_options=storage_options)


class TestSchemaEvolutionNewFields:
    """Test schema evolution when new fields are added."""

    def test_add_single_new_field(self, mongodb_client, delta_writer, storage_options):
        """Test adding a single new field to documents."""
        db = mongodb_client["test_db"]
        collection = db["users"]

        # Insert initial documents with basic schema
        initial_docs = [
            {"_id": 1, "name": "Alice", "age": 30},
            {"_id": 2, "name": "Bob", "age": 25},
        ]
        collection.insert_many(initial_docs)

        # Convert and write to Delta Lake
        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.users"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/users_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Verify initial schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        initial_schema = table.schema().to_pyarrow()
        initial_field_names = {f.name for f in initial_schema}

        assert "name" in initial_field_names
        assert "age" in initial_field_names
        assert "email" not in initial_field_names  # Not yet added

        # Insert documents with new field
        new_docs = [
            {"_id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com"},
            {"_id": 4, "name": "Diana", "age": 28, "email": "diana@example.com"},
        ]
        collection.insert_many(new_docs)

        # Convert and write new documents
        converted_new_docs = [BSONToDeltaConverter.convert_document(doc) for doc in new_docs]
        for doc in converted_new_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = len(initial_docs)
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.users"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_new_docs)

        # Verify schema evolved
        table = DeltaTable(table_uri, storage_options=storage_options)
        evolved_schema = table.schema().to_pyarrow()
        evolved_field_names = {f.name for f in evolved_schema}

        assert "name" in evolved_field_names
        assert "age" in evolved_field_names
        assert "email" in evolved_field_names  # New field added

        # Verify all documents are queryable
        df = table.to_pandas()
        assert len(df) == 4

        # Check that old documents have null for new field
        old_docs_df = df[df["_id"].isin([1, 2])]
        assert old_docs_df["email"].isna().all()

        # Check that new documents have email values
        new_docs_df = df[df["_id"].isin([3, 4])]
        assert not new_docs_df["email"].isna().any()

    def test_add_multiple_new_fields(self, mongodb_client, delta_writer, storage_options):
        """Test adding multiple new fields at once."""
        db = mongodb_client["test_db"]
        collection = db["products"]

        # Initial documents
        initial_docs = [
            {"_id": 1, "name": "Product A", "price": 100.0},
            {"_id": 2, "name": "Product B", "price": 200.0},
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.products"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/products_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Add documents with multiple new fields
        new_docs = [
            {
                "_id": 3,
                "name": "Product C",
                "price": 150.0,
                "category": "Electronics",
                "in_stock": True,
                "tags": ["new", "featured"]
            }
        ]
        collection.insert_many(new_docs)

        converted_new_docs = [BSONToDeltaConverter.convert_document(doc) for doc in new_docs]
        for doc in converted_new_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = len(initial_docs)
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.products"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_new_docs)

        # Verify all new fields are in schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        schema = table.schema().to_pyarrow()
        field_names = {f.name for f in schema}

        assert "name" in field_names
        assert "price" in field_names
        assert "category" in field_names
        assert "in_stock" in field_names
        assert "tags" in field_names

        # Verify data integrity
        df = table.to_pandas()
        assert len(df) == 3

    def test_add_nested_field(self, mongodb_client, delta_writer, storage_options):
        """Test adding nested fields to existing schema."""
        db = mongodb_client["test_db"]
        collection = db["orders"]

        # Initial documents with simple structure
        initial_docs = [
            {"_id": 1, "order_id": "ORD001", "total": 100.0},
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.orders"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/orders_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Add document with nested field
        new_docs = [
            {
                "_id": 2,
                "order_id": "ORD002",
                "total": 200.0,
                "customer": {
                    "name": "John Doe",
                    "email": "john@example.com"
                }
            }
        ]
        collection.insert_many(new_docs)

        converted_new_docs = [BSONToDeltaConverter.convert_document(doc) for doc in new_docs]
        for doc in converted_new_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.orders"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_new_docs)

        # Verify nested field in schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        schema = table.schema().to_pyarrow()

        customer_field = next((f for f in schema if f.name == "customer"), None)
        assert customer_field is not None
        assert pa.types.is_struct(customer_field.type)

        # Verify data
        df = table.to_pandas()
        assert len(df) == 2

    def test_schema_evolution_preserves_data_types(self, mongodb_client, delta_writer, storage_options):
        """Test that schema evolution preserves existing data types."""
        db = mongodb_client["test_db"]
        collection = db["metrics"]

        # Initial documents
        initial_docs = [
            {"_id": 1, "metric_name": "cpu_usage", "value": 75},
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.metrics"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/metrics_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Get initial schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        initial_schema = table.schema().to_pyarrow()

        # Add document with new field
        new_docs = [
            {"_id": 2, "metric_name": "memory_usage", "value": 80, "unit": "percent"},
        ]
        collection.insert_many(new_docs)

        converted_new_docs = [BSONToDeltaConverter.convert_document(doc) for doc in new_docs]
        for doc in converted_new_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.metrics"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_new_docs)

        # Get evolved schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        evolved_schema = table.schema().to_pyarrow()

        # Verify existing field types are preserved
        for field in initial_schema:
            if field.name in [f.name for f in evolved_schema]:
                evolved_field = next(f for f in evolved_schema if f.name == field.name)
                # Types should be compatible (might be widened but not incompatible)
                assert evolved_field.type == field.type or \
                       SchemaInferrer._types_compatible(field.type, evolved_field.type)

    def test_concurrent_schema_evolution(self, mongodb_client, delta_writer, storage_options):
        """Test handling multiple batches with different schemas."""
        db = mongodb_client["test_db"]
        collection = db["events"]

        table_uri = "s3://test-bucket/events_v1"

        # Batch 1: Basic fields
        batch1 = [
            {"_id": 1, "event_type": "click", "timestamp": datetime.now()},
        ]

        # Batch 2: Add new field
        batch2 = [
            {"_id": 2, "event_type": "view", "timestamp": datetime.now(), "user_id": 123},
        ]

        # Batch 3: Add another field
        batch3 = [
            {"_id": 3, "event_type": "purchase", "timestamp": datetime.now(), "user_id": 456, "amount": 99.99},
        ]

        for batch in [batch1, batch2, batch3]:
            collection.insert_many(batch)

            converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in batch]
            for doc in converted_docs:
                doc["_cdc_timestamp"] = datetime.now()
                doc["_cdc_operation"] = "insert"
                doc["_ingestion_timestamp"] = datetime.now()
                doc["_kafka_offset"] = 0
                doc["_kafka_partition"] = 0
                doc["_kafka_topic"] = "test.events"
                doc["_ingestion_date"] = datetime.now().date().isoformat()

            delta_writer.write_batch(table_uri, converted_docs)

        # Verify final schema has all fields
        table = DeltaTable(table_uri, storage_options=storage_options)
        schema = table.schema().to_pyarrow()
        field_names = {f.name for f in schema}

        assert "event_type" in field_names
        assert "timestamp" in field_names
        assert "user_id" in field_names
        assert "amount" in field_names

        # Verify all documents are present
        df = table.to_pandas()
        assert len(df) == 3
