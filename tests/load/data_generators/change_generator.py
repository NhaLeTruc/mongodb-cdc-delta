"""Change event generator for simulating CDC workloads.

Generates insert, update, and delete operations to test CDC pipeline.
"""

import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pymongo import MongoClient
from pymongo.collection import Collection


class ChangeGenerator:
    """Generator for MongoDB change events."""

    def __init__(
        self,
        connection_string: str,
        database: str,
        collection_name: str,
    ) -> None:
        """Initialize change generator.

        Args:
            connection_string: MongoDB connection string
            database: Database name
            collection_name: Collection name
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database]
        self.collection: Collection = self.db[collection_name]

    def generate_insert(self) -> Dict[str, Any]:
        """Generate and execute an insert operation.

        Returns:
            Inserted document
        """
        document = {
            "name": f"User-{random.randint(1000, 9999)}",
            "email": f"user{random.randint(1000, 9999)}@example.com",
            "age": random.randint(18, 80),
            "status": random.choice(["active", "inactive", "pending"]),
            "created_at": datetime.utcnow(),
        }

        result = self.collection.insert_one(document)
        document["_id"] = result.inserted_id
        return document

    def generate_update(self) -> Optional[Dict[str, Any]]:
        """Generate and execute an update operation.

        Returns:
            Updated document or None if no document found
        """
        # Find a random document
        pipeline = [{"$sample": {"size": 1}}]
        documents = list(self.collection.aggregate(pipeline))

        if not documents:
            return None

        document = documents[0]
        doc_id = document["_id"]

        # Perform update
        updates = {
            "status": random.choice(["active", "inactive", "suspended"]),
            "updated_at": datetime.utcnow(),
        }

        self.collection.update_one({"_id": doc_id}, {"$set": updates})
        document.update(updates)
        return document

    def generate_delete(self) -> Optional[Dict[str, Any]]:
        """Generate and execute a delete operation.

        Returns:
            Deleted document or None if no document found
        """
        # Find a random document
        pipeline = [{"$sample": {"size": 1}}]
        documents = list(self.collection.aggregate(pipeline))

        if not documents:
            return None

        document = documents[0]
        doc_id = document["_id"]

        # Perform delete
        self.collection.delete_one({"_id": doc_id})
        return document

    def generate_changes(
        self,
        num_changes: int,
        insert_weight: int = 50,
        update_weight: int = 40,
        delete_weight: int = 10,
        delay_ms: int = 10,
    ) -> List[Dict[str, Any]]:
        """Generate a sequence of random changes.

        Args:
            num_changes: Number of changes to generate
            insert_weight: Relative weight for inserts (0-100)
            update_weight: Relative weight for updates (0-100)
            delete_weight: Relative weight for deletes (0-100)
            delay_ms: Delay between operations in milliseconds

        Returns:
            List of generated change events
        """
        operations = ["insert", "update", "delete"]
        weights = [insert_weight, update_weight, delete_weight]
        changes = []

        for i in range(num_changes):
            operation = random.choices(operations, weights=weights)[0]

            if operation == "insert":
                result = self.generate_insert()
                changes.append({"operation": "insert", "document": result})
            elif operation == "update":
                result = self.generate_update()
                if result:
                    changes.append({"operation": "update", "document": result})
            elif operation == "delete":
                result = self.generate_delete()
                if result:
                    changes.append({"operation": "delete", "document": result})

            if delay_ms > 0:
                time.sleep(delay_ms / 1000)

            if (i + 1) % 100 == 0:
                print(f"Generated {i + 1} / {num_changes} changes")

        return changes

    def close(self) -> None:
        """Close MongoDB connection."""
        self.client.close()

    def __enter__(self) -> "ChangeGenerator":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()


def simulate_changes(
    connection_string: str,
    database: str,
    collection_name: str,
    duration_seconds: int = 60,
    target_rate_per_second: int = 100,
) -> int:
    """Simulate continuous change events for a duration.

    Args:
        connection_string: MongoDB connection string
        database: Database name
        collection_name: Collection name
        duration_seconds: Duration to run simulation
        target_rate_per_second: Target change rate per second

    Returns:
        Total number of changes generated
    """
    total_changes = 0
    delay_ms = 1000 / target_rate_per_second if target_rate_per_second > 0 else 10

    with ChangeGenerator(connection_string, database, collection_name) as generator:
        start_time = time.time()
        end_time = start_time + duration_seconds

        while time.time() < end_time:
            generator.generate_changes(
                num_changes=1,
                delay_ms=int(delay_ms),
            )
            total_changes += 1

        elapsed = time.time() - start_time
        actual_rate = total_changes / elapsed

        print(f"\nSimulation complete:")
        print(f"  Duration: {elapsed:.2f} seconds")
        print(f"  Total changes: {total_changes}")
        print(f"  Actual rate: {actual_rate:.2f} changes/second")

    return total_changes
