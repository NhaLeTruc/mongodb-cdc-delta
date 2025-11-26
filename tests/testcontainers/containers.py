"""Reusable Testcontainers configurations for integration tests.

Provides pre-configured containers for MongoDB, Kafka, and MinIO.
"""

import time
from typing import Optional

from testcontainers.core.container import DockerContainer
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.kafka import KafkaContainer as BaseKafkaContainer
from testcontainers.mongodb import MongoDbContainer as BaseMongoDbContainer


class MongoDBContainer(BaseMongoDbContainer):
    """MongoDB container configured for CDC with replica set."""

    def __init__(
        self,
        image: str = "mongo:7.0",
        replica_set: str = "rs0-test",
        **kwargs: object,
    ) -> None:
        """Initialize MongoDB container with replica set.

        Args:
            image: MongoDB image tag
            replica_set: Replica set name
            **kwargs: Additional container arguments
        """
        super().__init__(image=image, **kwargs)
        self.replica_set = replica_set
        self.with_command(f"--replSet {replica_set} --bind_ip_all")

    def start(self) -> "MongoDBContainer":
        """Start MongoDB container and initialize replica set.

        Returns:
            Started container instance
        """
        super().start()

        # Wait for MongoDB to be ready
        time.sleep(5)

        # Initialize replica set
        connection_string = self.get_connection_url()
        from pymongo import MongoClient

        client = MongoClient(connection_string)
        try:
            client.admin.command("replSetInitiate", {
                "_id": self.replica_set,
                "members": [{"_id": 0, "host": "localhost:27017"}]
            })

            # Wait for replica set to be ready
            for _ in range(30):
                try:
                    status = client.admin.command("replSetGetStatus")
                    if status["members"][0]["stateStr"] == "PRIMARY":
                        break
                except Exception:
                    pass
                time.sleep(1)
        finally:
            client.close()

        return self


class KafkaContainer(BaseKafkaContainer):
    """Kafka container with optimized test configuration."""

    def __init__(
        self,
        image: str = "confluentinc/cp-kafka:7.6.0",
        **kwargs: object,
    ) -> None:
        """Initialize Kafka container.

        Args:
            image: Kafka image tag
            **kwargs: Additional container arguments
        """
        super().__init__(image=image, **kwargs)

        # Optimize for faster startup in tests
        self.with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
        self.with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        self.with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        self.with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")


class MinIOContainer(DockerContainer):
    """MinIO container for S3-compatible object storage."""

    def __init__(
        self,
        image: str = "minio/minio:RELEASE.2024-01-01T16-36-33Z",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        **kwargs: object,
    ) -> None:
        """Initialize MinIO container.

        Args:
            image: MinIO image tag
            access_key: MinIO access key
            secret_key: MinIO secret key
            **kwargs: Additional container arguments
        """
        super().__init__(image=image, **kwargs)
        self.access_key = access_key
        self.secret_key = secret_key

        self.with_exposed_ports(9000)
        self.with_command("server /data")
        self.with_env("MINIO_ROOT_USER", access_key)
        self.with_env("MINIO_ROOT_PASSWORD", secret_key)

    def get_connection_url(self) -> str:
        """Get MinIO connection URL.

        Returns:
            MinIO endpoint URL
        """
        host = self.get_container_host_ip()
        port = self.get_exposed_port(9000)
        return f"http://{host}:{port}"

    def start(self) -> "MinIOContainer":
        """Start MinIO container.

        Returns:
            Started container instance
        """
        super().start()
        wait_for_logs(self, "API:")
        time.sleep(2)
        return self


# Singleton container instances for test session
_mongodb_container: Optional[MongoDBContainer] = None
_kafka_container: Optional[KafkaContainer] = None
_minio_container: Optional[MinIOContainer] = None


def get_mongodb_container() -> MongoDBContainer:
    """Get or create MongoDB container instance.

    Returns:
        MongoDBContainer instance
    """
    global _mongodb_container
    if _mongodb_container is None:
        _mongodb_container = MongoDBContainer()
        _mongodb_container.start()
    return _mongodb_container


def get_kafka_container() -> KafkaContainer:
    """Get or create Kafka container instance.

    Returns:
        KafkaContainer instance
    """
    global _kafka_container
    if _kafka_container is None:
        _kafka_container = KafkaContainer()
        _kafka_container.start()
    return _kafka_container


def get_minio_container() -> MinIOContainer:
    """Get or create MinIO container instance.

    Returns:
        MinIOContainer instance
    """
    global _minio_container
    if _minio_container is None:
        _minio_container = MinIOContainer()
        _minio_container.start()
    return _minio_container
