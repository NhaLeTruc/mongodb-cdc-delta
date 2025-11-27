"""Integration tests for type evolution and type conflict resolution."""

import pytest
from datetime import datetime
from typing import Generator
from testcontainers.mongodb import MongoDbContainer
from testcontainers.minio import MinioContainer
import pymongo
import pyarrow as pa
from deltalake import DeltaTable

from delta_writer.src.writer.delta_writer import DeltaWriter
from delta_writer.src.transformers.bson_to_delta import BSONToDeltaConverter


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


class TestTypeEvolution:
    """Test type evolution and type widening."""

    def test_int32_to_int64_widening(self, mongodb_client, delta_writer, storage_options):
        """Test automatic widening from int32 to int64."""
        db = mongodb_client["test_db"]
        collection = db["counters"]

        # Insert documents with int32 values (small integers)
        initial_docs = [
            {"_id": 1, "counter": 100},
            {"_id": 2, "counter": 200},
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.counters"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/counters_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Verify initial type is int32
        table = DeltaTable(table_uri, storage_options=storage_options)
        initial_schema = table.schema().to_pyarrow()
        counter_field = next(f for f in initial_schema if f.name == "counter")
        assert counter_field.type == pa.int32()

        # Insert document with int64 value (large integer)
        large_docs = [
            {"_id": 3, "counter": 9223372036854775807},  # Max int64
        ]
        collection.insert_many(large_docs)

        converted_large_docs = [BSONToDeltaConverter.convert_document(doc) for doc in large_docs]
        for doc in converted_large_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 2
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.counters"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_large_docs)

        # Verify type widened to int64
        table = DeltaTable(table_uri, storage_options=storage_options)
        evolved_schema = table.schema().to_pyarrow()
        counter_field = next(f for f in evolved_schema if f.name == "counter")
        assert counter_field.type == pa.int64()

        # Verify all data is preserved
        df = table.to_pandas()
        assert len(df) == 3
        assert df["counter"].tolist() == [100, 200, 9223372036854775807]

    def test_int_to_float_widening(self, mongodb_client, delta_writer, storage_options):
        """Test automatic widening from int to float."""
        db = mongodb_client["test_db"]
        collection = db["measurements"]

        # Insert documents with integer values
        initial_docs = [
            {"_id": 1, "value": 10},
            {"_id": 2, "value": 20},
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.measurements"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/measurements_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Insert documents with float values
        float_docs = [
            {"_id": 3, "value": 30.5},
            {"_id": 4, "value": 40.7},
        ]
        collection.insert_many(float_docs)

        converted_float_docs = [BSONToDeltaConverter.convert_document(doc) for doc in float_docs]
        for doc in converted_float_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 2
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.measurements"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_float_docs)

        # Verify type widened to float64
        table = DeltaTable(table_uri, storage_options=storage_options)
        schema = table.schema().to_pyarrow()
        value_field = next(f for f in schema if f.name == "value")
        assert value_field.type == pa.float64()

        # Verify all data is preserved
        df = table.to_pandas()
        assert len(df) == 4

    def test_nested_struct_evolution(self, mongodb_client, delta_writer, storage_options):
        """Test evolution of nested struct types."""
        db = mongodb_client["test_db"]
        collection = db["profiles"]

        # Initial documents with simple nested structure
        initial_docs = [
            {
                "_id": 1,
                "user": {
                    "id": 100
                }
            }
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.profiles"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/profiles_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Add document with expanded nested structure
        expanded_docs = [
            {
                "_id": 2,
                "user": {
                    "id": 200,
                    "name": "Alice",
                    "settings": {
                        "theme": "dark",
                        "notifications": True
                    }
                }
            }
        ]
        collection.insert_many(expanded_docs)

        converted_expanded_docs = [BSONToDeltaConverter.convert_document(doc) for doc in expanded_docs]
        for doc in converted_expanded_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.profiles"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_expanded_docs)

        # Verify nested struct evolved
        table = DeltaTable(table_uri, storage_options=storage_options)
        schema = table.schema().to_pyarrow()

        user_field = next(f for f in schema if f.name == "user")
        assert pa.types.is_struct(user_field.type)

        user_struct_fields = {f.name for f in user_field.type}
        assert "id" in user_struct_fields
        assert "name" in user_struct_fields
        assert "settings" in user_struct_fields

        # Verify nested settings struct
        settings_field = user_field.type.field("settings")
        assert pa.types.is_struct(settings_field.type)

        # Verify data
        df = table.to_pandas()
        assert len(df) == 2

    def test_list_type_evolution(self, mongodb_client, delta_writer, storage_options):
        """Test evolution of list element types."""
        db = mongodb_client["test_db"]
        collection = db["arrays"]

        # Initial documents with int list
        initial_docs = [
            {"_id": 1, "values": [1, 2, 3]},
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.arrays"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/arrays_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Verify initial list type
        table = DeltaTable(table_uri, storage_options=storage_options)
        initial_schema = table.schema().to_pyarrow()
        values_field = next(f for f in initial_schema if f.name == "values")
        assert pa.types.is_list(values_field.type)
        assert values_field.type.value_type == pa.int32()

        # Add document with larger int values
        large_docs = [
            {"_id": 2, "values": [9223372036854775807]},  # int64 value
        ]
        collection.insert_many(large_docs)

        converted_large_docs = [BSONToDeltaConverter.convert_document(doc) for doc in large_docs]
        for doc in converted_large_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.arrays"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_large_docs)

        # Verify list element type widened
        table = DeltaTable(table_uri, storage_options=storage_options)
        evolved_schema = table.schema().to_pyarrow()
        values_field = next(f for f in evolved_schema if f.name == "values")
        assert pa.types.is_list(values_field.type)
        assert values_field.type.value_type == pa.int64()

    def test_no_data_loss_during_type_evolution(self, mongodb_client, delta_writer, storage_options):
        """Test that no data is lost during type evolution."""
        db = mongodb_client["test_db"]
        collection = db["data_integrity"]

        table_uri = "s3://test-bucket/data_integrity_v1"
        all_docs = []

        # Insert 50 documents with int32 values
        for i in range(50):
            doc = {"_id": i, "value": i * 10}
            all_docs.append(doc)

        collection.insert_many(all_docs[:50])

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in all_docs[:50]]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.data_integrity"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_docs)

        # Insert 50 more documents with int64 values
        large_docs = []
        for i in range(50, 100):
            doc = {"_id": i, "value": 9223372036854775800 + i}
            large_docs.append(doc)
            all_docs.append(doc)

        collection.insert_many(large_docs)

        converted_large_docs = [BSONToDeltaConverter.convert_document(doc) for doc in large_docs]
        for doc in converted_large_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 50
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.data_integrity"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_large_docs)

        # Verify all 100 documents are present
        table = DeltaTable(table_uri, storage_options=storage_options)
        df = table.to_pandas()
        assert len(df) == 100

        # Verify no duplicates
        assert df["_id"].nunique() == 100

        # Verify value ranges
        small_values = df[df["_id"] < 50]["value"]
        assert small_values.min() == 0
        assert small_values.max() == 490

        large_values = df[df["_id"] >= 50]["value"]
        assert large_values.min() >= 9223372036854775800

    def test_type_evolution_with_100_plus_documents(self, mongodb_client, delta_writer, storage_options):
        """Test type evolution with 100+ documents as per requirements."""
        db = mongodb_client["test_db"]
        collection = db["large_dataset"]

        table_uri = "s3://test-bucket/large_dataset_v1"

        # Insert 100 documents with consistent int32 schema
        batch1 = []
        for i in range(100):
            doc = {
                "_id": i,
                "count": i,
                "score": i * 1.5,
                "category": f"cat_{i % 10}"
            }
            batch1.append(doc)

        collection.insert_many(batch1)

        converted_batch1 = [BSONToDeltaConverter.convert_document(doc) for doc in batch1]
        for doc in converted_batch1:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.large_dataset"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_batch1)

        # Insert 50 more documents with evolved schema (int64 for count)
        batch2 = []
        for i in range(100, 150):
            doc = {
                "_id": i,
                "count": 9223372036854775800 + i,  # int64 value
                "score": i * 2.5,
                "category": f"cat_{i % 10}",
                "new_field": f"value_{i}"  # New field
            }
            batch2.append(doc)

        collection.insert_many(batch2)

        converted_batch2 = [BSONToDeltaConverter.convert_document(doc) for doc in batch2]
        for doc in converted_batch2:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 100
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.large_dataset"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_batch2)

        # Verify final state
        table = DeltaTable(table_uri, storage_options=storage_options)
        df = table.to_pandas()

        # Verify count
        assert len(df) == 150

        # Verify schema evolution
        schema = table.schema().to_pyarrow()
        field_names = {f.name for f in schema}

        assert "count" in field_names
        assert "score" in field_names
        assert "category" in field_names
        assert "new_field" in field_names

        # Verify type widening
        count_field = next(f for f in schema if f.name == "count")
        assert count_field.type == pa.int64()

        # Verify data integrity
        assert df["_id"].nunique() == 150
        assert df["category"].nunique() == 10

    def test_multiple_type_changes_same_batch(self, mongodb_client, delta_writer, storage_options):
        """Test multiple fields changing types in the same batch."""
        db = mongodb_client["test_db"]
        collection = db["multi_evolution"]

        # Initial documents
        initial_docs = [
            {
                "_id": 1,
                "field_a": 100,
                "field_b": 200,
                "field_c": "text"
            }
        ]
        collection.insert_many(initial_docs)

        converted_docs = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.multi_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        table_uri = "s3://test-bucket/multi_evolution_v1"
        delta_writer.write_batch(table_uri, converted_docs)

        # Document with multiple type changes
        evolved_docs = [
            {
                "_id": 2,
                "field_a": 9223372036854775807,  # int32 -> int64
                "field_b": 200.5,  # int -> float
                "field_c": "text"
            }
        ]
        collection.insert_many(evolved_docs)

        converted_evolved_docs = [BSONToDeltaConverter.convert_document(doc) for doc in evolved_docs]
        for doc in converted_evolved_docs:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.multi_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_evolved_docs)

        # Verify both type changes
        table = DeltaTable(table_uri, storage_options=storage_options)
        schema = table.schema().to_pyarrow()

        field_a = next(f for f in schema if f.name == "field_a")
        assert field_a.type == pa.int64()

        field_b = next(f for f in schema if f.name == "field_b")
        assert field_b.type == pa.float64()

        # Verify data
        df = table.to_pandas()
        assert len(df) == 2
