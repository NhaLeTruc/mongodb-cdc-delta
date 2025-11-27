"""Integration test for MongoDB delete -> Delta Lake replication."""

import pytest
import time
from datetime import datetime
from pymongo import MongoClient
from deltalake import DeltaTable
import pyarrow.compute as pc

from tests.testcontainers.containers import (
    MongoDBContainer,
    KafkaContainer,
    MinIOContainer,
)


@pytest.fixture(scope="module")
def mongodb_container():
    """Start MongoDB container."""
    container = MongoDBContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def kafka_container():
    """Start Kafka container."""
    container = KafkaContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="module")
def minio_container():
    """Start MinIO container."""
    container = MinIOContainer()
    container.start()
    yield container
    container.stop()


@pytest.fixture
def mongo_client(mongodb_container):
    """Get MongoDB client."""
    client = MongoClient(mongodb_container.get_connection_string())
    yield client
    client.close()


@pytest.fixture
def test_db(mongo_client):
    """Get test database."""
    db = mongo_client["testdb"]
    yield db
    mongo_client.drop_database("testdb")


def test_delete_document_replicates_to_delta(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test that deleting a document in MongoDB creates a delete event in Delta Lake.

    Steps:
        1. Insert a document in MongoDB
        2. Delete the document
        3. Wait for CDC to process the changes
        4. Verify delete event appears in Delta Lake
    """
    collection = test_db["sessions"]

    document = {
        "_id": "session_001",
        "user_id": "user_001",
        "started_at": datetime.now(),
        "active": True
    }

    collection.insert_one(document)
    print(f"Inserted document with _id: {document['_id']}")

    time.sleep(30)

    collection.delete_one({"_id": "session_001"})
    print("Deleted document")

    time.sleep(60)

    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_sessions"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()

        print(f"Delta table records: {len(df)}")

        id_filter = pc.equal(df["_id"], "session_001")
        filtered_df = df.filter(id_filter)

        records = filtered_df.to_pylist()

        delete_records = [r for r in records if r["_cdc_operation"] == "delete"]
        assert len(delete_records) >= 1, "Expected at least one delete record"

        print("Test passed: Delete successfully replicated to Delta Lake")

    except Exception as e:
        pytest.fail(f"Failed to read from Delta Lake: {str(e)}")


def test_bulk_delete_replicates(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test that bulk deletes replicate to Delta Lake.

    Steps:
        1. Insert multiple documents
        2. Delete them in bulk
        3. Verify delete events appear in Delta Lake
    """
    collection = test_db["temp_data"]

    documents = []
    for i in range(20):
        doc = {
            "_id": f"temp_{i:03d}",
            "data": f"value_{i}",
            "created_at": datetime.now()
        }
        documents.append(doc)

    collection.insert_many(documents)
    print(f"Inserted {len(documents)} documents")

    time.sleep(30)

    result = collection.delete_many({"_id": {"$regex": "^temp_"}})
    print(f"Deleted {result.deleted_count} documents")

    time.sleep(60)

    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_temp_data"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()

        delete_filter = pc.equal(df["_cdc_operation"], "delete")
        delete_df = df.filter(delete_filter)

        assert len(delete_df) >= 20, f"Expected at least 20 delete records, found {len(delete_df)}"

        print("Test passed: Bulk delete successfully replicated")

    except Exception as e:
        pytest.fail(f"Failed to read from Delta Lake: {str(e)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
