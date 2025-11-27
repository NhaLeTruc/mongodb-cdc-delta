"""End-to-end test for full CDC flow from MongoDB to Delta Lake."""

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


def test_full_cdc_flow(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test complete CDC flow: insert -> update -> delete.

    This E2E test verifies that all CDC operations work end-to-end:
        1. Insert documents in MongoDB
        2. Update some documents
        3. Delete some documents
        4. Verify all operations appear correctly in Delta Lake
    """
    collection = test_db["e2e_users"]

    print("Step 1: Insert documents")
    users = []
    for i in range(50):
        user = {
            "_id": f"user_{i:03d}",
            "name": f"User {i}",
            "email": f"user{i}@example.com",
            "status": "active",
            "created_at": datetime.now(),
            "login_count": 0
        }
        users.append(user)

    collection.insert_many(users)
    print(f"Inserted {len(users)} users")

    time.sleep(30)

    print("Step 2: Update documents")
    for i in range(0, 25, 5):
        collection.update_one(
            {"_id": f"user_{i:03d}"},
            {
                "$set": {
                    "status": "premium",
                    "login_count": 10 + i,
                    "updated_at": datetime.now()
                }
            }
        )
    print("Updated 5 users to premium status")

    time.sleep(30)

    print("Step 3: Delete documents")
    for i in range(45, 50):
        collection.delete_one({"_id": f"user_{i:03d}"})
    print("Deleted 5 users")

    time.sleep(60)

    print("Step 4: Verify in Delta Lake")
    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_e2e_users"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()

        print(f"Total records in Delta Lake: {len(df)}")
        print(f"Schema: {df.schema}")

        insert_filter = pc.equal(df["_cdc_operation"], "insert")
        inserts = df.filter(insert_filter)
        print(f"Insert operations: {len(inserts)}")
        assert len(inserts) >= 50, f"Expected at least 50 inserts, found {len(inserts)}"

        update_filter = pc.equal(df["_cdc_operation"], "update")
        updates = df.filter(update_filter)
        print(f"Update operations: {len(updates)}")
        assert len(updates) >= 5, f"Expected at least 5 updates, found {len(updates)}"

        delete_filter = pc.equal(df["_cdc_operation"], "delete")
        deletes = df.filter(delete_filter)
        print(f"Delete operations: {len(deletes)}")
        assert len(deletes) >= 5, f"Expected at least 5 deletes, found {len(deletes)}"

        premium_filter = pc.equal(df["status"], "premium")
        premium_updates = df.filter(update_filter & premium_filter)
        assert len(premium_updates) >= 5, "Expected premium status updates"

        for record in premium_updates.to_pylist():
            assert record["login_count"] >= 10, "Expected login count to be updated"

        print("\nTest passed: Full CDC flow working end-to-end!")
        print(f"  - Inserts: {len(inserts)}")
        print(f"  - Updates: {len(updates)}")
        print(f"  - Deletes: {len(deletes)}")

    except Exception as e:
        pytest.fail(f"E2E test failed: {str(e)}")


def test_schema_evolution_in_cdc_flow(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test schema evolution during CDC flow.

    Steps:
        1. Insert documents with initial schema
        2. Insert documents with new fields
        3. Verify Delta Lake schema evolves correctly
    """
    collection = test_db["evolving_schema"]

    print("Step 1: Insert documents with initial schema")
    initial_docs = [
        {
            "_id": f"doc_{i}",
            "name": f"Document {i}",
            "version": 1,
            "created_at": datetime.now()
        }
        for i in range(10)
    ]
    collection.insert_many(initial_docs)

    time.sleep(30)

    print("Step 2: Insert documents with evolved schema")
    evolved_docs = [
        {
            "_id": f"doc_evolved_{i}",
            "name": f"Evolved Document {i}",
            "version": 2,
            "created_at": datetime.now(),
            "new_field": f"new_value_{i}",
            "tags": ["tag1", "tag2"],
            "metadata": {
                "source": "api",
                "validated": True
            }
        }
        for i in range(10)
    ]
    collection.insert_many(evolved_docs)

    time.sleep(60)

    print("Step 3: Verify schema evolution")
    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_evolving_schema"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()
        schema = df.schema

        print(f"Delta table has {len(df)} records")
        print(f"Schema fields: {[field.name for field in schema]}")

        assert "name" in schema.names
        assert "version" in schema.names
        assert "created_at" in schema.names
        assert "new_field" in schema.names
        assert "tags" in schema.names
        assert "metadata" in schema.names

        new_field_filter = pc.is_valid(df["new_field"])
        records_with_new_field = df.filter(new_field_filter)
        assert len(records_with_new_field) >= 10, "Expected new field in evolved documents"

        print("Test passed: Schema evolution working correctly!")

    except Exception as e:
        pytest.fail(f"Schema evolution test failed: {str(e)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
