"""Integration test for MongoDB update -> Delta Lake replication."""

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


def test_update_document_replicates_to_delta(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test that updating a document in MongoDB replicates to Delta Lake.

    Steps:
        1. Insert a document in MongoDB
        2. Update the document
        3. Wait for CDC to process the changes
        4. Verify both insert and update appear in Delta Lake
    """
    collection = test_db["products"]

    initial_document = {
        "_id": "product_001",
        "name": "Widget",
        "price": 19.99,
        "stock": 100,
        "active": True,
        "created_at": datetime.now()
    }

    collection.insert_one(initial_document)
    print(f"Inserted document with _id: {initial_document['_id']}")

    time.sleep(30)

    collection.update_one(
        {"_id": "product_001"},
        {
            "$set": {
                "price": 24.99,
                "stock": 150,
                "updated_at": datetime.now()
            }
        }
    )
    print("Updated document price and stock")

    time.sleep(60)

    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_products"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()

        print(f"Delta table records: {len(df)}")

        id_filter = pc.equal(df["_id"], "product_001")
        filtered_df = df.filter(id_filter)

        assert len(filtered_df) >= 1, f"Expected at least 1 record, found {len(filtered_df)}"

        records = filtered_df.to_pylist()

        update_records = [r for r in records if r["_cdc_operation"] == "update"]
        assert len(update_records) >= 1, "Expected at least one update record"

        latest_record = sorted(records, key=lambda r: r["_cdc_timestamp"])[-1]
        assert latest_record["price"] == 24.99
        assert latest_record["stock"] == 150

        print("Test passed: Update successfully replicated to Delta Lake")

    except Exception as e:
        pytest.fail(f"Failed to read from Delta Lake: {str(e)}")


def test_multiple_updates_replicate(
    mongodb_container,
    kafka_container,
    minio_container,
    test_db
):
    """
    Test that multiple updates to the same document replicate.

    Steps:
        1. Insert a document
        2. Update it multiple times
        3. Verify all updates appear in Delta Lake
    """
    collection = test_db["inventory"]

    collection.insert_one({
        "_id": "item_001",
        "quantity": 100,
        "version": 1
    })

    time.sleep(30)

    for i in range(2, 6):
        collection.update_one(
            {"_id": "item_001"},
            {
                "$set": {
                    "quantity": 100 + i * 10,
                    "version": i
                }
            }
        )
        time.sleep(5)

    time.sleep(60)

    storage_options = minio_container.get_storage_options()
    table_uri = "s3://lakehouse/tables/testdb_inventory"

    try:
        delta_table = DeltaTable(table_uri, storage_options=storage_options)
        df = delta_table.to_pyarrow_table()

        id_filter = pc.equal(df["_id"], "item_001")
        filtered_df = df.filter(id_filter)

        records = filtered_df.to_pylist()
        update_records = [r for r in records if r["_cdc_operation"] == "update"]

        assert len(update_records) >= 4, f"Expected at least 4 updates, found {len(update_records)}"

        print("Test passed: Multiple updates successfully replicated")

    except Exception as e:
        pytest.fail(f"Failed to read from Delta Lake: {str(e)}")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
