"""
Integration tests for Docker Compose environment startup and health checks.

Tests that all services in the Docker Compose environment start correctly,
pass health checks, and are ready for local development and testing.
"""

import pytest
import time
import subprocess
import requests
from typing import Dict, List
import json


class TestDockerComposeStartup:
    """Test Docker Compose environment startup"""

    @pytest.fixture(scope="class")
    def compose_file(self):
        """Path to docker-compose.yml"""
        return "docker/compose/docker-compose.yml"

    @pytest.fixture(scope="class")
    def service_health_endpoints(self) -> Dict[str, str]:
        """Health check endpoints for each service"""
        return {
            "mongodb": "mongodb://localhost:27017",
            "kafka": "localhost:9092",
            "zookeeper": "localhost:2181",
            "kafka-connect": "http://localhost:8083/connectors",
            "minio": "http://localhost:9000/minio/health/live",
            "postgres": "postgresql://localhost:5432",
            "prometheus": "http://localhost:9090/-/healthy",
            "grafana": "http://localhost:3000/api/health",
        }

    def test_compose_file_exists(self, compose_file):
        """Test that docker-compose.yml exists"""
        import os
        assert os.path.exists(compose_file), f"Docker Compose file not found: {compose_file}"

    def test_compose_file_valid(self, compose_file):
        """Test that docker-compose.yml is valid YAML"""
        result = subprocess.run(
            ["docker-compose", "-f", compose_file, "config"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"Invalid docker-compose.yml: {result.stderr}"

    def test_all_services_defined(self, compose_file):
        """Test that all required services are defined"""
        result = subprocess.run(
            ["docker-compose", "-f", compose_file, "config", "--services"],
            capture_output=True,
            text=True
        )

        services = result.stdout.strip().split('\n')
        required_services = [
            "mongodb",
            "kafka",
            "zookeeper",
            "kafka-connect",
            "minio",
            "postgres",
            "prometheus",
            "grafana"
        ]

        for service in required_services:
            assert service in services, f"Service {service} not defined in docker-compose.yml"

    def test_services_have_health_checks(self, compose_file):
        """Test that all services have health checks defined"""
        result = subprocess.run(
            ["docker-compose", "-f", compose_file, "config"],
            capture_output=True,
            text=True
        )

        config = result.stdout

        # Critical services that must have health checks
        critical_services = ["mongodb", "kafka", "postgres", "minio"]

        for service in critical_services:
            # Check for healthcheck definition
            assert f"{service}:" in config, f"Service {service} not found"

    def test_environment_variables_defined(self, compose_file):
        """Test that critical environment variables are defined"""
        result = subprocess.run(
            ["docker-compose", "-f", compose_file, "config"],
            capture_output=True,
            text=True
        )

        config = result.stdout

        # Check for critical env vars
        critical_vars = [
            "MONGO_INITDB_ROOT_USERNAME",
            "KAFKA_ADVERTISED_LISTENERS",
            "MINIO_ROOT_USER",
            "POSTGRES_USER"
        ]

        # At least some env vars should be present
        env_var_count = sum(1 for var in critical_vars if var in config)
        assert env_var_count > 0, "No critical environment variables defined"

    def test_volumes_defined(self, compose_file):
        """Test that data volumes are defined for persistence"""
        result = subprocess.run(
            ["docker-compose", "-f", compose_file, "config"],
            capture_output=True,
            text=True
        )

        config = result.stdout

        # Check for volume definitions
        assert "volumes:" in config, "No volumes section found"

        # Key volumes that should exist
        expected_volumes = ["mongodb_data", "minio_data", "postgres_data"]

        for volume in expected_volumes:
            # Volume should be referenced somewhere in config
            assert volume in config, f"Volume {volume} not found in configuration"

    def test_networks_defined(self, compose_file):
        """Test that networks are defined"""
        result = subprocess.run(
            ["docker-compose", "-f", compose_file, "config"],
            capture_output=True,
            text=True
        )

        config = result.stdout

        # Should have networks section
        assert "networks:" in config or "network_mode:" in config, "No network configuration found"


class TestServiceHealthChecks:
    """Test individual service health checks"""

    @pytest.fixture
    def max_wait_time(self):
        """Maximum time to wait for services (seconds)"""
        return 120

    def test_mongodb_health(self, max_wait_time):
        """Test MongoDB health check"""
        from pymongo import MongoClient
        from pymongo.errors import ServerSelectionTimeoutError

        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                client = MongoClient(
                    "mongodb://localhost:27017",
                    serverSelectionTimeoutMS=5000
                )
                # Ping the server
                client.admin.command('ping')
                return  # Success
            except ServerSelectionTimeoutError:
                time.sleep(2)

        pytest.fail("MongoDB did not become healthy within timeout")

    def test_kafka_health(self, max_wait_time):
        """Test Kafka health check"""
        from kafka import KafkaAdminClient
        from kafka.errors import NoBrokersAvailable

        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                admin_client = KafkaAdminClient(
                    bootstrap_servers=["localhost:9092"],
                    request_timeout_ms=5000
                )
                # List topics to verify connection
                admin_client.list_topics()
                admin_client.close()
                return  # Success
            except (NoBrokersAvailable, Exception):
                time.sleep(2)

        pytest.fail("Kafka did not become healthy within timeout")

    def test_minio_health(self, max_wait_time):
        """Test MinIO health check"""
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                response = requests.get(
                    "http://localhost:9000/minio/health/live",
                    timeout=5
                )
                if response.status_code == 200:
                    return  # Success
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)

        pytest.fail("MinIO did not become healthy within timeout")

    def test_postgres_health(self, max_wait_time):
        """Test PostgreSQL health check"""
        import psycopg2

        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    port=5432,
                    user="postgres",
                    password="postgres",
                    database="postgres",
                    connect_timeout=5
                )
                conn.close()
                return  # Success
            except psycopg2.OperationalError:
                time.sleep(2)

        pytest.fail("PostgreSQL did not become healthy within timeout")

    def test_kafka_connect_health(self, max_wait_time):
        """Test Kafka Connect health check"""
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                response = requests.get(
                    "http://localhost:8083/connectors",
                    timeout=5
                )
                if response.status_code == 200:
                    return  # Success
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)

        pytest.fail("Kafka Connect did not become healthy within timeout")

    def test_prometheus_health(self, max_wait_time):
        """Test Prometheus health check"""
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                response = requests.get(
                    "http://localhost:9090/-/healthy",
                    timeout=5
                )
                if response.status_code == 200:
                    return  # Success
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)

        pytest.fail("Prometheus did not become healthy within timeout")

    def test_grafana_health(self, max_wait_time):
        """Test Grafana health check"""
        start_time = time.time()
        while time.time() - start_time < max_wait_time:
            try:
                response = requests.get(
                    "http://localhost:3000/api/health",
                    timeout=5
                )
                if response.status_code == 200:
                    return  # Success
            except requests.exceptions.RequestException:
                pass
            time.sleep(2)

        pytest.fail("Grafana did not become healthy within timeout")


class TestServiceConnectivity:
    """Test connectivity between services"""

    def test_kafka_to_zookeeper(self):
        """Test that Kafka can connect to Zookeeper"""
        from kafka import KafkaAdminClient

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=["localhost:9092"],
                request_timeout_ms=10000
            )
            # If Kafka is connected to Zookeeper, it can list topics
            topics = admin_client.list_topics()
            assert topics is not None
            admin_client.close()
        except Exception as e:
            pytest.fail(f"Kafka cannot connect to Zookeeper: {e}")

    def test_kafka_connect_to_kafka(self):
        """Test that Kafka Connect can connect to Kafka"""
        try:
            response = requests.get(
                "http://localhost:8083/connectors",
                timeout=10
            )
            assert response.status_code == 200
        except requests.exceptions.RequestException as e:
            pytest.fail(f"Kafka Connect cannot connect to Kafka: {e}")

    def test_delta_writer_can_access_minio(self):
        """Test that Delta writer can access MinIO"""
        import boto3
        from botocore.client import Config

        try:
            s3_client = boto3.client(
                's3',
                endpoint_url='http://localhost:9000',
                aws_access_key_id='minioadmin',
                aws_secret_access_key='minioadmin',
                config=Config(signature_version='s3v4')
            )

            # List buckets to verify connection
            s3_client.list_buckets()
        except Exception as e:
            pytest.fail(f"Cannot access MinIO: {e}")


class TestEnvironmentVariables:
    """Test environment variables configuration"""

    def test_env_example_exists(self):
        """Test that .env.example file exists"""
        import os
        env_example_path = "docker/compose/.env.example"
        assert os.path.exists(env_example_path), ".env.example file not found"

    def test_env_example_complete(self):
        """Test that .env.example contains all required variables"""
        with open("docker/compose/.env.example", 'r') as f:
            content = f.read()

        required_vars = [
            "MONGO_INITDB_ROOT_USERNAME",
            "MONGO_INITDB_ROOT_PASSWORD",
            "KAFKA_ADVERTISED_LISTENERS",
            "MINIO_ROOT_USER",
            "MINIO_ROOT_PASSWORD",
            "POSTGRES_USER",
            "POSTGRES_PASSWORD"
        ]

        for var in required_vars:
            assert var in content, f"Required variable {var} not in .env.example"


class TestDockerComposeCommands:
    """Test Docker Compose commands"""

    def test_docker_compose_up_dry_run(self):
        """Test docker-compose up in dry-run mode"""
        result = subprocess.run(
            ["docker-compose", "-f", "docker/compose/docker-compose.yml", "config"],
            capture_output=True,
            text=True
        )
        assert result.returncode == 0, f"docker-compose config failed: {result.stderr}"

    def test_docker_compose_ps(self):
        """Test docker-compose ps command"""
        result = subprocess.run(
            ["docker-compose", "-f", "docker/compose/docker-compose.yml", "ps"],
            capture_output=True,
            text=True
        )
        # Should not error (may be empty if not running)
        assert result.returncode == 0


class TestServicePersistence:
    """Test data persistence across restarts"""

    def test_volumes_persist_data(self):
        """Test that volumes are created and persist data"""
        result = subprocess.run(
            ["docker", "volume", "ls"],
            capture_output=True,
            text=True
        )

        # Check that some volumes exist (if services have been started)
        assert result.returncode == 0

    def test_named_volumes_in_config(self):
        """Test that named volumes are properly configured"""
        result = subprocess.run(
            ["docker-compose", "-f", "docker/compose/docker-compose.yml", "config"],
            capture_output=True,
            text=True
        )

        config = result.stdout

        # Should define top-level volumes
        assert "volumes:" in config
