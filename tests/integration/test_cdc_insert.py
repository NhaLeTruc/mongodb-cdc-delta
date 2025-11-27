"""Integration test for MongoDB insert -> Delta Lake replication."""

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


def test_insert_document_replicates_to_delta(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test that inserting a document in MongoDB replicates to Delta Lake.

    Steps:
        1. Insert a document in MongoDB
        2. Wait for CDC to process the change
        3. Verify the document appears in Delta Lake
    """
    collection = test_db["users"]

    test_document = {
        "_id": "test_user_001",
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30,
        "created_at": datetime.now(),
        "active": True,
        "tags": ["premium", "verified"],
        "metadata": {
            "signup_source": "web",
            "referral_code": "ABC123"
        }
    }

    collection.insert_one(test_document)
    print(f"Inserted document with _id: {test_document['_id']}")

    time.sleep(60)

    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_users"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()

        print(f"Delta table records: {len(df)}")
        print(f"Schema: {df.schema}")

        id_filter = pc.equal(df["_id"], test_document["_id"])
        filtered_df = df.filter(id_filter)

        assert len(filtered_df) == 1, f"Expected 1 record, found {len(filtered_df)}"

        record = filtered_df.to_pylist()[0]
        assert record["name"] == test_document["name"]
        assert record["email"] == test_document["email"]
        assert record["age"] == test_document["age"]
        assert record["active"] == test_document["active"]
        assert record["_cdc_operation"] == "insert"

        print("Test passed: Document successfully replicated to Delta Lake")

    except Exception as e:
        pytest.fail(f"Failed to read from Delta Lake: {str(e)}")


def test_batch_insert_replicates(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test that batch inserting documents replicates to Delta Lake.

    Steps:
        1. Insert 100 documents in MongoDB
        2. Wait for CDC to process
        3. Verify all documents appear in Delta Lake
    """
    collection = test_db["orders"]

    documents = []
    for i in range(100):
        doc = {
            "_id": f"order_{i:03d}",
            "user_id": f"user_{i % 10}",
            "amount": 100.0 + i,
            "status": "pending",
            "created_at": datetime.now()
        }
        documents.append(doc)

    result = collection.insert_many(documents)
    print(f"Inserted {len(result.inserted_ids)} documents")

    time.sleep(60)

    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_orders"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()

        print(f"Delta table records: {len(df)}")

        assert len(df) >= 100, f"Expected at least 100 records, found {len(df)}"

        insert_filter = pc.equal(df["_cdc_operation"], "insert")
        insert_df = df.filter(insert_filter)
        assert len(insert_df) >= 100, f"Expected at least 100 inserts, found {len(insert_df)}"

        print("Test passed: Batch insert successfully replicated to Delta Lake")

    except Exception as e:
        pytest.fail(f"Failed to read from Delta Lake: {str(e)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
