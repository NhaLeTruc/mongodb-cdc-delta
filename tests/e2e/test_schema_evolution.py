"""End-to-end tests for complex nested schema evolution.

This module tests schema evolution with complex nested documents,
multi-level nesting, array element type changes, and data integrity
across 200+ documents.
"""

import pytest
from datetime import datetime, timedelta
from typing import Generator
import time
from testcontainers.mongodb import MongoDbContainer
from testcontainers.minio import MinioContainer
import pymongo
import pyarrow as pa
from deltalake import DeltaTable

from delta_writer.src.writer.delta_writer import DeltaWriter
from delta_writer.src.transformers.bson_to_delta import BSONToDeltaConverter


@pytest.fixture(scope="module")
def mongodb_container() -> Generator[MongoDbContainer, None, None]:
    """Start MongoDB container for E2E tests."""
    container = MongoDbContainer("mongo:7.0")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def minio_container() -> Generator[MinioContainer, None, None]:
    """Start MinIO container for E2E tests."""
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


class TestComplexNestedSchemaEvolution:
    """Test complex nested schema evolution scenarios."""

    def test_three_level_nested_document_evolution(
        self,
        mongodb_client,
        delta_writer,
        storage_options
    ):
        """Test schema evolution with 3-level nested documents."""
        db = mongodb_client["test_db"]
        collection = db["deep_nested"]

        table_uri = "s3://test-bucket/deep_nested_e2e"

        # Phase 1: Insert documents with 1-level nesting
        phase1_docs = []
        for i in range(50):
            doc = {
                "_id": i,
                "record_id": f"REC{i:04d}",
                "metadata": {
                    "created_at": datetime.now(),
                    "version": 1
                }
            }
            phase1_docs.append(doc)

        collection.insert_many(phase1_docs)

        converted_phase1 = [BSONToDeltaConverter.convert_document(doc) for doc in phase1_docs]
        for doc in converted_phase1:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.deep_nested"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase1)

        # Verify Phase 1 schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        phase1_schema = table.schema().to_pyarrow()

        metadata_field = next(f for f in phase1_schema if f.name == "metadata")
        assert pa.types.is_struct(metadata_field.type)
        metadata_fields = {f.name for f in metadata_field.type}
        assert "created_at" in metadata_fields
        assert "version" in metadata_fields

        # Phase 2: Add 2-level nesting
        phase2_docs = []
        for i in range(50, 100):
            doc = {
                "_id": i,
                "record_id": f"REC{i:04d}",
                "metadata": {
                    "created_at": datetime.now(),
                    "version": 2,
                    "author": {
                        "id": i * 10,
                        "name": f"Author {i}"
                    }
                }
            }
            phase2_docs.append(doc)

        collection.insert_many(phase2_docs)

        converted_phase2 = [BSONToDeltaConverter.convert_document(doc) for doc in phase2_docs]
        for doc in converted_phase2:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 50
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.deep_nested"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase2)

        # Verify Phase 2 schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        phase2_schema = table.schema().to_pyarrow()

        metadata_field = next(f for f in phase2_schema if f.name == "metadata")
        assert pa.types.is_struct(metadata_field.type)

        author_field = metadata_field.type.field("author")
        assert author_field is not None
        assert pa.types.is_struct(author_field.type)

        author_struct_fields = {f.name for f in author_field.type}
        assert "id" in author_struct_fields
        assert "name" in author_struct_fields

        # Phase 3: Add 3-level nesting
        phase3_docs = []
        for i in range(100, 150):
            doc = {
                "_id": i,
                "record_id": f"REC{i:04d}",
                "metadata": {
                    "created_at": datetime.now(),
                    "version": 3,
                    "author": {
                        "id": i * 10,
                        "name": f"Author {i}",
                        "contact": {
                            "email": f"author{i}@example.com",
                            "phone": f"+1-555-{i:04d}"
                        }
                    }
                }
            }
            phase3_docs.append(doc)

        collection.insert_many(phase3_docs)

        converted_phase3 = [BSONToDeltaConverter.convert_document(doc) for doc in phase3_docs]
        for doc in converted_phase3:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 100
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.deep_nested"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase3)

        # Verify final 3-level nested schema
        table = DeltaTable(table_uri, storage_options=storage_options)
        final_schema = table.schema().to_pyarrow()

        metadata_field = next(f for f in final_schema if f.name == "metadata")
        author_field = metadata_field.type.field("author")
        contact_field = author_field.type.field("contact")

        assert contact_field is not None
        assert pa.types.is_struct(contact_field.type)

        contact_struct_fields = {f.name for f in contact_field.type}
        assert "email" in contact_struct_fields
        assert "phone" in contact_struct_fields

        # Verify all 150 documents are queryable
        df = table.to_pandas()
        assert len(df) == 150
        assert df["_id"].nunique() == 150

    def test_add_fields_at_different_nesting_levels(
        self,
        mongodb_client,
        delta_writer,
        storage_options
    ):
        """Test adding fields at different levels of nesting simultaneously."""
        db = mongodb_client["test_db"]
        collection = db["multi_level_add"]

        table_uri = "s3://test-bucket/multi_level_add_e2e"

        # Initial simple document
        initial_docs = [
            {
                "_id": 1,
                "name": "Record 1",
                "data": {
                    "value": 100
                }
            }
        ]

        collection.insert_many(initial_docs)

        converted_initial = [BSONToDeltaConverter.convert_document(doc) for doc in initial_docs]
        for doc in converted_initial:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.multi_level_add"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_initial)

        # Add fields at multiple levels
        evolved_docs = []
        for i in range(2, 52):
            doc = {
                "_id": i,
                "name": f"Record {i}",
                "category": f"CAT{i % 5}",  # New top-level field
                "data": {
                    "value": 100 + i,
                    "unit": "units",  # New field in nested level
                    "metrics": {  # New nested object
                        "count": i * 10,
                        "average": i * 2.5
                    }
                },
                "tags": [f"tag{i}", f"tag{i+1}"]  # New top-level array
            }
            evolved_docs.append(doc)

        collection.insert_many(evolved_docs)

        converted_evolved = [BSONToDeltaConverter.convert_document(doc) for doc in evolved_docs]
        for doc in converted_evolved:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 1
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.multi_level_add"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_evolved)

        # Verify all fields at all levels
        table = DeltaTable(table_uri, storage_options=storage_options)
        schema = table.schema().to_pyarrow()

        top_level_fields = {f.name for f in schema}
        assert "name" in top_level_fields
        assert "category" in top_level_fields
        assert "data" in top_level_fields
        assert "tags" in top_level_fields

        # Verify nested data structure
        data_field = next(f for f in schema if f.name == "data")
        assert pa.types.is_struct(data_field.type)

        data_fields = {f.name for f in data_field.type}
        assert "value" in data_fields
        assert "unit" in data_fields
        assert "metrics" in data_fields

        # Verify double-nested metrics
        metrics_field = data_field.type.field("metrics")
        assert pa.types.is_struct(metrics_field.type)

        metrics_fields = {f.name for f in metrics_field.type}
        assert "count" in metrics_fields
        assert "average" in metrics_fields

        # Verify data integrity
        df = table.to_pandas()
        assert len(df) == 51

    def test_array_element_type_evolution(
        self,
        mongodb_client,
        delta_writer,
        storage_options
    ):
        """Test evolution of array element types."""
        db = mongodb_client["test_db"]
        collection = db["array_evolution"]

        table_uri = "s3://test-bucket/array_evolution_e2e"

        # Phase 1: Arrays with simple types
        phase1_docs = []
        for i in range(40):
            doc = {
                "_id": i,
                "int_array": [i, i + 1, i + 2],
                "string_array": [f"item{i}", f"item{i+1}"]
            }
            phase1_docs.append(doc)

        collection.insert_many(phase1_docs)

        converted_phase1 = [BSONToDeltaConverter.convert_document(doc) for doc in phase1_docs]
        for doc in converted_phase1:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 0
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.array_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase1)

        # Verify initial array types
        table = DeltaTable(table_uri, storage_options=storage_options)
        phase1_schema = table.schema().to_pyarrow()

        int_array_field = next(f for f in phase1_schema if f.name == "int_array")
        assert pa.types.is_list(int_array_field.type)
        assert int_array_field.type.value_type == pa.int32()

        # Phase 2: Int array gets larger ints (type widening)
        phase2_docs = []
        for i in range(40, 80):
            doc = {
                "_id": i,
                "int_array": [9223372036854775800 + i],  # int64 values
                "string_array": [f"item{i}"]
            }
            phase2_docs.append(doc)

        collection.insert_many(phase2_docs)

        converted_phase2 = [BSONToDeltaConverter.convert_document(doc) for doc in phase2_docs]
        for doc in converted_phase2:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 40
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.array_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase2)

        # Verify type widening occurred
        table = DeltaTable(table_uri, storage_options=storage_options)
        phase2_schema = table.schema().to_pyarrow()

        int_array_field = next(f for f in phase2_schema if f.name == "int_array")
        assert pa.types.is_list(int_array_field.type)
        assert int_array_field.type.value_type == pa.int64()

        # Phase 3: Add arrays with struct elements
        phase3_docs = []
        for i in range(80, 120):
            doc = {
                "_id": i,
                "int_array": [i],
                "string_array": [f"item{i}"],
                "struct_array": [
                    {"key": f"k{i}", "value": i * 10},
                    {"key": f"k{i+1}", "value": (i + 1) * 10}
                ]
            }
            phase3_docs.append(doc)

        collection.insert_many(phase3_docs)

        converted_phase3 = [BSONToDeltaConverter.convert_document(doc) for doc in phase3_docs]
        for doc in converted_phase3:
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 80
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.array_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase3)

        # Verify struct array was added
        table = DeltaTable(table_uri, storage_options=storage_options)
        final_schema = table.schema().to_pyarrow()

        field_names = {f.name for f in final_schema}
        assert "struct_array" in field_names

        struct_array_field = next(f for f in final_schema if f.name == "struct_array")
        assert pa.types.is_list(struct_array_field.type)
        assert pa.types.is_struct(struct_array_field.type.value_type)

        # Verify all 120 documents
        df = table.to_pandas()
        assert len(df) == 120

    def test_200_plus_documents_complex_evolution(
        self,
        mongodb_client,
        delta_writer,
        storage_options
    ):
        """Test complex schema evolution with 200+ documents."""
        db = mongodb_client["test_db"]
        collection = db["large_complex_evolution"]

        table_uri = "s3://test-bucket/large_complex_e2e"

        # Phase 1: 80 documents with basic nested structure
        phase1_docs = []
        for i in range(80):
            doc = {
                "_id": i,
                "entity_id": f"ENT{i:05d}",
                "timestamp": datetime.now() - timedelta(hours=80 - i),
                "status": "active" if i % 2 == 0 else "inactive",
                "properties": {
                    "name": f"Entity {i}",
                    "type": f"Type{i % 5}"
                }
            }
            phase1_docs.append(doc)

        collection.insert_many(phase1_docs)

        converted_phase1 = [BSONToDeltaConverter.convert_document(doc) for doc in phase1_docs]
        for idx, doc in enumerate(converted_phase1):
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = idx
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.large_complex_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase1)

        # Phase 2: 80 documents with expanded schema
        phase2_docs = []
        for i in range(80, 160):
            doc = {
                "_id": i,
                "entity_id": f"ENT{i:05d}",
                "timestamp": datetime.now() - timedelta(hours=160 - i),
                "status": "pending",
                "priority": i % 10,  # New field
                "properties": {
                    "name": f"Entity {i}",
                    "type": f"Type{i % 5}",
                    "metadata": {  # New nested field
                        "created_by": f"user{i}",
                        "version": 2
                    }
                },
                "metrics": {  # New top-level nested object
                    "score": i * 1.5,
                    "count": i
                }
            }
            phase2_docs.append(doc)

        collection.insert_many(phase2_docs)

        converted_phase2 = [BSONToDeltaConverter.convert_document(doc) for doc in phase2_docs]
        for idx, doc in enumerate(converted_phase2):
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 80 + idx
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.large_complex_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase2)

        # Phase 3: 60 documents with further evolution
        phase3_docs = []
        for i in range(160, 220):
            doc = {
                "_id": i,
                "entity_id": f"ENT{i:05d}",
                "timestamp": datetime.now() - timedelta(hours=220 - i),
                "status": "completed",
                "priority": i % 10,
                "properties": {
                    "name": f"Entity {i}",
                    "type": f"Type{i % 5}",
                    "metadata": {
                        "created_by": f"user{i}",
                        "version": 3,
                        "tags": [f"tag{i}", f"tag{i+1}"]  # New array in nested metadata
                    },
                    "config": {  # Another new nested object
                        "enabled": True,
                        "threshold": i * 0.1
                    }
                },
                "metrics": {
                    "score": i * 2.5,
                    "count": i,
                    "rate": i * 0.05  # New field in existing nested object
                },
                "relations": [  # New array of struct
                    {"target_id": i + 1, "relation_type": "parent"},
                    {"target_id": i + 2, "relation_type": "sibling"}
                ]
            }
            phase3_docs.append(doc)

        collection.insert_many(phase3_docs)

        converted_phase3 = [BSONToDeltaConverter.convert_document(doc) for doc in phase3_docs]
        for idx, doc in enumerate(converted_phase3):
            doc["_cdc_timestamp"] = datetime.now()
            doc["_cdc_operation"] = "insert"
            doc["_ingestion_timestamp"] = datetime.now()
            doc["_kafka_offset"] = 160 + idx
            doc["_kafka_partition"] = 0
            doc["_kafka_topic"] = "test.large_complex_evolution"
            doc["_ingestion_date"] = datetime.now().date().isoformat()

        delta_writer.write_batch(table_uri, converted_phase3)

        # Comprehensive verification
        table = DeltaTable(table_uri, storage_options=storage_options)
        final_schema = table.schema().to_pyarrow()

        # Verify top-level fields
        top_fields = {f.name for f in final_schema}
        assert "entity_id" in top_fields
        assert "timestamp" in top_fields
        assert "status" in top_fields
        assert "priority" in top_fields
        assert "properties" in top_fields
        assert "metrics" in top_fields
        assert "relations" in top_fields

        # Verify properties structure
        properties_field = next(f for f in final_schema if f.name == "properties")
        assert pa.types.is_struct(properties_field.type)

        props_fields = {f.name for f in properties_field.type}
        assert "name" in props_fields
        assert "type" in props_fields
        assert "metadata" in props_fields
        assert "config" in props_fields

        # Verify nested metadata in properties
        metadata_field = properties_field.type.field("metadata")
        assert pa.types.is_struct(metadata_field.type)

        metadata_fields = {f.name for f in metadata_field.type}
        assert "created_by" in metadata_fields
        assert "version" in metadata_fields
        assert "tags" in metadata_fields

        # Verify metrics structure
        metrics_field = next(f for f in final_schema if f.name == "metrics")
        assert pa.types.is_struct(metrics_field.type)

        metrics_fields = {f.name for f in metrics_field.type}
        assert "score" in metrics_fields
        assert "count" in metrics_fields
        assert "rate" in metrics_fields

        # Verify relations array
        relations_field = next(f for f in final_schema if f.name == "relations")
        assert pa.types.is_list(relations_field.type)
        assert pa.types.is_struct(relations_field.type.value_type)

        # Verify all 220 documents are present and queryable
        df = table.to_pandas()
        assert len(df) == 220
        assert df["_id"].nunique() == 220

        # Verify data integrity across phases
        phase1_data = df[df["_id"] < 80]
        assert len(phase1_data) == 80
        assert phase1_data["priority"].isna().all()  # Phase 1 didn't have priority

        phase2_data = df[(df["_id"] >= 80) & (df["_id"] < 160)]
        assert len(phase2_data) == 80
        assert not phase2_data["priority"].isna().any()  # Phase 2 has priority

        phase3_data = df[df["_id"] >= 160]
        assert len(phase3_data) == 60

    def test_schema_evolution_correctness(
        self,
        mongodb_client,
        delta_writer,
        storage_options
    ):
        """Test that schema evolution maintains data correctness."""
        db = mongodb_client["test_db"]
        collection = db["correctness_check"]

        table_uri = "s3://test-bucket/correctness_e2e"

        all_inserted_docs = []

        # Insert batches with evolving schema
        for batch_idx in range(5):
            batch_docs = []

            for i in range(batch_idx * 10, (batch_idx + 1) * 10):
                doc = {
                    "_id": i,
                    "batch": batch_idx,
                    "value": i * 100
                }

                # Add more fields as batches progress
                if batch_idx >= 1:
                    doc["extra1"] = f"extra1_{i}"
                if batch_idx >= 2:
                    doc["extra2"] = i * 2
                if batch_idx >= 3:
                    doc["nested"] = {"field1": i, "field2": f"nested_{i}"}
                if batch_idx >= 4:
                    doc["array_field"] = [i, i + 1, i + 2]

                batch_docs.append(doc)
                all_inserted_docs.append(doc)

            collection.insert_many(batch_docs)

            converted_batch = [BSONToDeltaConverter.convert_document(doc) for doc in batch_docs]
            for doc in converted_batch:
                doc["_cdc_timestamp"] = datetime.now()
                doc["_cdc_operation"] = "insert"
                doc["_ingestion_timestamp"] = datetime.now()
                doc["_kafka_offset"] = batch_idx * 10
                doc["_kafka_partition"] = 0
                doc["_kafka_topic"] = "test.correctness_check"
                doc["_ingestion_date"] = datetime.now().date().isoformat()

            delta_writer.write_batch(table_uri, converted_batch)

        # Verify all documents and data correctness
        table = DeltaTable(table_uri, storage_options=storage_options)
        df = table.to_pandas()

        assert len(df) == 50
        assert df["_id"].nunique() == 50

        # Verify each batch has correct data
        for batch_idx in range(5):
            batch_data = df[df["batch"] == batch_idx]
            assert len(batch_data) == 10

            # Verify value field is correct for all
            for _, row in batch_data.iterrows():
                assert row["value"] == row["_id"] * 100

            # Verify conditional fields
            if batch_idx >= 1:
                assert not batch_data["extra1"].isna().any()
            else:
                assert batch_data["extra1"].isna().all()

            if batch_idx >= 2:
                assert not batch_data["extra2"].isna().any()
            else:
                assert batch_data["extra2"].isna().all()
