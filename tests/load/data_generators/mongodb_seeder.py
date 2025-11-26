"""MongoDB data seeder using Faker and Mimesis.

Generates realistic test data for MongoDB collections.
"""

import random
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from faker import Faker
from pymongo import MongoClient
from pymongo.collection import Collection


def generate_user_document(fake: Optional[Faker] = None) -> Dict[str, Any]:
    """Generate a realistic user document.

    Args:
        fake: Faker instance (creates new one if not provided)

    Returns:
        User document dictionary
    """
    if fake is None:
        fake = Faker()

    return {
        "name": fake.name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "age": random.randint(18, 80),
        "address": {
            "street": fake.street_address(),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "zip": fake.zipcode(),
            "country": fake.country_code(),
            "coordinates": {
                "lat": float(fake.latitude()),
                "lon": float(fake.longitude()),
            },
        },
        "tags": [fake.word() for _ in range(random.randint(1, 5))],
        "is_active": fake.boolean(chance_of_getting_true=75),
        "created_at": fake.date_time_between(start_date="-2y", end_date="now"),
        "updated_at": datetime.utcnow(),
        "metadata": {
            "source": "test_generator",
            "version": "1.0",
        },
    }


class MongoDBSeeder:
    """MongoDB data seeder for load testing."""

    def __init__(
        self,
        connection_string: str,
        database: str,
        collection_name: str,
        seed: Optional[int] = None,
    ) -> None:
        """Initialize MongoDB seeder.

        Args:
            connection_string: MongoDB connection string
            database: Database name
            collection_name: Collection name
            seed: Random seed for reproducible data
        """
        self.client = MongoClient(connection_string)
        self.db = self.client[database]
        self.collection: Collection = self.db[collection_name]

        self.fake = Faker()
        if seed is not None:
            Faker.seed(seed)
            random.seed(seed)

    def seed_users(self, count: int, batch_size: int = 10000) -> int:
        """Seed user documents in batches.

        Args:
            count: Total number of documents to insert
            batch_size: Batch size for bulk inserts

        Returns:
            Number of documents inserted
        """
        inserted = 0

        for i in range(0, count, batch_size):
            batch_count = min(batch_size, count - i)
            batch = [generate_user_document(self.fake) for _ in range(batch_count)]

            result = self.collection.insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)

            if (i + batch_count) % 50000 == 0:
                print(f"Inserted {i + batch_count:,} / {count:,} documents")

        return inserted

    def seed_orders(self, count: int, batch_size: int = 10000) -> int:
        """Seed order documents.

        Args:
            count: Total number of documents to insert
            batch_size: Batch size for bulk inserts

        Returns:
            Number of documents inserted
        """
        inserted = 0

        for i in range(0, count, batch_size):
            batch_count = min(batch_size, count - i)
            batch = []

            for _ in range(batch_count):
                order = {
                    "order_number": self.fake.ean13(),
                    "customer_email": self.fake.email(),
                    "status": random.choice(
                        ["pending", "processing", "shipped", "delivered", "cancelled"]
                    ),
                    "payment_method": random.choice(
                        ["credit_card", "debit_card", "paypal", "crypto"]
                    ),
                    "total": round(random.uniform(10, 5000), 2),
                    "items": [
                        {
                            "sku": self.fake.ean8(),
                            "name": self.fake.catch_phrase(),
                            "quantity": random.randint(1, 10),
                            "price": round(random.uniform(5, 500), 2),
                        }
                        for _ in range(random.randint(1, 5))
                    ],
                    "created_at": self.fake.date_time_between(
                        start_date="-1y", end_date="now"
                    ),
                    "updated_at": datetime.utcnow(),
                }
                batch.append(order)

            result = self.collection.insert_many(batch, ordered=False)
            inserted += len(result.inserted_ids)

            if (i + batch_count) % 50000 == 0:
                print(f"Inserted {i + batch_count:,} / {count:,} orders")

        return inserted

    def clear_collection(self) -> int:
        """Clear all documents from collection.

        Returns:
            Number of documents deleted
        """
        result = self.collection.delete_many({})
        return result.deleted_count

    def close(self) -> None:
        """Close MongoDB connection."""
        self.client.close()

    def __enter__(self) -> "MongoDBSeeder":
        """Context manager entry."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        self.close()
