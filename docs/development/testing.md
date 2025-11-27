# Testing Guide

This guide covers testing strategies, test execution, and best practices for the MongoDB CDC to Delta Lake pipeline.

## Table of Contents

- [Test Categories](#test-categories)
- [Running Tests](#running-tests)
- [Writing Tests](#writing-tests)
- [Test Environment](#test-environment)
- [Coverage Requirements](#coverage-requirements)
- [Continuous Integration](#continuous-integration)

## Test Categories

### Unit Tests (`tests/unit/`)

**Purpose**: Test individual functions and classes in isolation

**Characteristics**:
- Fast (< 1 second per test)
- No external dependencies (mocked)
- High test count (hundreds)
- Run on every commit

**Examples**:
- `test_retry.py`: Retry logic and exponential backoff
- `test_dlq.py`: Dead Letter Queue handling
- `test_checkpointing.py`: Checkpoint management
- `test_bson_to_delta.py`: BSON to Delta type conversion
- `test_schema_manager.py`: Schema merging and evolution

**Running**:
```bash
pytest tests/unit -v
```

### Integration Tests (`tests/integration/`)

**Purpose**: Test integration between components

**Characteristics**:
- Moderate speed (1-30 seconds per test)
- Use Testcontainers for real services
- Test realistic workflows
- Run before merge to main

**Examples**:
- `test_retry_minio.py`: MinIO failure and recovery
- `test_corrupted_data.py`: Corrupted event handling
- `test_crash_recovery.py`: Crash and checkpoint recovery
- `test_cdc_insert.py`: MongoDB insert → Delta Lake
- `test_cdc_update.py`: MongoDB update → Delta Lake
- `test_schema_evolution.py`: Schema changes

**Running**:
```bash
pytest tests/integration -v
```

### End-to-End Tests (`tests/e2e/`)

**Purpose**: Test complete user workflows

**Characteristics**:
- Slow (30+ seconds per test)
- Use full Docker Compose environment
- Test production-like scenarios
- Run nightly or on release

**Examples**:
- `test_cdc_flow.py`: Full CDC pipeline flow
- `test_local_suite.py`: Local test suite execution
- `test_pipeline_management.py`: Pipeline lifecycle
- `test_reconciliation.py`: Full reconciliation workflow

**Running**:
```bash
pytest tests/e2e -v
```

### Load/Performance Tests (`tests/load/`)

**Purpose**: Test system performance and scalability

**Characteristics**:
- Very slow (minutes to hours)
- High resource usage
- Generate realistic load
- Run periodically or on demand

**Examples**:
- `test_throughput.py`: Verify 10K events/sec
- `test_latency.py`: Verify P95 < 60s lag
- `test_schema_evolution_load.py`: Schema evolution under load

**Running**:
```bash
pytest tests/load -v --timeout=3600
```

## Running Tests

### Quick Start

```bash
# All tests (requires Docker)
make test-local

# Unit tests only (fast)
pytest tests/unit

# Specific test file
pytest tests/unit/test_retry.py

# Specific test function
pytest tests/unit/test_retry.py::TestExponentialBackoff::test_retry_on_transient_failure

# With verbose output
pytest tests/unit -v

# With coverage
pytest tests/unit --cov=delta_writer --cov-report=html
```

### Test Markers

Tests are marked by category:

```bash
# Run only unit tests
pytest -m unit

# Run only integration tests
pytest -m integration

# Run only E2E tests
pytest -m e2e

# Run only slow tests
pytest -m slow

# Skip slow tests
pytest -m "not slow"
```

### Filtering Tests

```bash
# Run tests matching keyword
pytest -k "retry"

# Run tests not matching keyword
pytest -k "not minio"

# Multiple keywords
pytest -k "retry or checkpoint"
```

### Parallel Execution

```bash
# Install pytest-xdist
pip install pytest-xdist

# Run tests in parallel
pytest -n auto  # Auto-detect CPU count
pytest -n 4     # Use 4 workers
```

### Test Output

```bash
# Show print statements
pytest -s

# Show local variables on failure
pytest -l

# Stop on first failure
pytest -x

# Drop into debugger on failure
pytest --pdb

# Show slowest tests
pytest --durations=10
```

## Writing Tests

### Unit Test Template

```python
"""
Unit tests for <module>.

Tests the <functionality> without external dependencies.
"""

import pytest
from unittest.mock import Mock, AsyncMock, patch


class TestFeature:
    """Test <feature> functionality"""

    @pytest.fixture
    def mock_dependency(self):
        """Mock external dependency"""
        return Mock()

    def test_basic_functionality(self, mock_dependency):
        """Test basic functionality"""
        # Arrange
        input_data = {"key": "value"}

        # Act
        result = function_under_test(input_data, mock_dependency)

        # Assert
        assert result == expected_output
        mock_dependency.method.assert_called_once()

    @pytest.mark.asyncio
    async def test_async_functionality(self):
        """Test async functionality"""
        mock_async = AsyncMock()

        result = await async_function(mock_async)

        assert result is not None
        mock_async.assert_called_once()

    def test_error_handling(self):
        """Test error handling"""
        with pytest.raises(ValueError, match="Expected error"):
            function_that_should_fail()

    @pytest.mark.parametrize("input,expected", [
        (1, 2),
        (2, 4),
        (3, 6)
    ])
    def test_multiple_inputs(self, input, expected):
        """Test with multiple inputs"""
        assert double(input) == expected
```

### Integration Test Template

```python
"""
Integration tests for <feature>.

Tests integration with real services using Testcontainers.
"""

import pytest
from testcontainers.mongodb import MongoDbContainer
from testcontainers.kafka import KafkaContainer


class TestIntegration:
    """Integration tests for <feature>"""

    @pytest.fixture(scope="class")
    def mongodb_container(self):
        """MongoDB container for testing"""
        with MongoDbContainer("mongo:7.0") as mongodb:
            yield mongodb

    @pytest.fixture(scope="class")
    def kafka_container(self):
        """Kafka container for testing"""
        with KafkaContainer() as kafka:
            yield kafka

    def test_integration_workflow(self, mongodb_container, kafka_container):
        """Test full integration workflow"""
        # Arrange
        mongo_client = mongodb_container.get_connection_client()
        db = mongo_client["testdb"]
        collection = db["testcol"]

        # Act
        collection.insert_one({"_id": "test", "data": "value"})

        # Assert
        document = collection.find_one({"_id": "test"})
        assert document["data"] == "value"
```

### E2E Test Template

```python
"""
End-to-end tests for <feature>.

Tests complete workflows in Docker Compose environment.
"""

import pytest
import time
import requests


class TestE2E:
    """E2E tests for <feature>"""

    @pytest.mark.e2e
    @pytest.mark.slow
    def test_complete_workflow(self):
        """Test complete end-to-end workflow"""
        # Given: Services are running
        health = requests.get("http://localhost:9090/-/healthy")
        assert health.status_code == 200

        # When: Data is inserted
        # ... insert test data

        # Then: Data appears in Delta Lake
        # ... verify in Delta Lake
        time.sleep(5)  # Allow for processing

        assert result is not None
```

## Test Environment

### Prerequisites

```bash
# Install test dependencies
pip install -r tests/requirements.txt

# Key packages:
# - pytest: Test framework
# - pytest-asyncio: Async test support
# - pytest-cov: Coverage reporting
# - pytest-xdist: Parallel execution
# - testcontainers: Docker containers for tests
# - requests: HTTP client for API tests
# - pymongo: MongoDB client
# - kafka-python: Kafka client
```

### Environment Variables for Tests

Create `tests/.env`:

```env
# Test environment
TEST_MONGODB_URI=mongodb://localhost:27017/testdb
TEST_KAFKA_BOOTSTRAP_SERVERS=localhost:9092
TEST_MINIO_ENDPOINT=localhost:9000
TEST_POSTGRES_URI=postgresql://postgres:postgres@localhost:5432/testdb

# Test configuration
TEST_TIMEOUT=300
TEST_DATA_SEED=42
```

### Test Fixtures Directory

```
tests/fixtures/
├── sample_documents.json          # Sample MongoDB documents
├── schema_evolution_samples.json  # Schema evolution test cases
├── corrupted_events.json          # Corrupted event examples
└── large_documents.json           # Performance test data
```

### Testcontainers Configuration

Testcontainers automatically starts Docker containers for tests:

```python
@pytest.fixture(scope="session")
def mongodb():
    """MongoDB container for all tests"""
    with MongoDbContainer("mongo:7.0") as container:
        container.with_env("MONGO_REPLICA_SET_MODE", "primary")
        yield container

@pytest.fixture(scope="session")
def kafka():
    """Kafka container for all tests"""
    with KafkaContainer() as container:
        yield container
```

## Coverage Requirements

### Target Coverage

- **Overall**: ≥ 80%
- **Critical paths**: ≥ 90%
- **New code**: ≥ 85%

### Running Coverage

```bash
# Generate coverage report
pytest --cov=delta_writer --cov=api --cov=reconciliation

# Generate HTML report
pytest --cov=delta_writer --cov-report=html

# Open HTML report
open htmlcov/index.html

# Show missing lines
pytest --cov=delta_writer --cov-report=term-missing

# Fail if coverage below threshold
pytest --cov=delta_writer --cov-fail-under=80
```

### Coverage Configuration

In `pytest.ini`:

```ini
[pytest]
addopts =
    --cov=delta_writer
    --cov=api
    --cov=reconciliation
    --cov-report=term-missing
    --cov-report=html
    --cov-fail-under=80
```

## Continuous Integration

### Pre-commit Hooks

Install pre-commit hooks:

```bash
pip install pre-commit
pre-commit install
```

Hooks run automatically on `git commit`:
- Black (code formatting)
- Ruff (linting)
- mypy (type checking)
- pytest (fast unit tests)

### GitHub Actions Workflow

`.github/workflows/test.yml`:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.10'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r tests/requirements.txt

      - name: Run unit tests
        run: pytest tests/unit -v --cov

      - name: Run integration tests
        run: pytest tests/integration -v

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

## Best Practices

### Test Organization

- **One class per module under test**
- **One test method per behavior**
- **Use descriptive test names**: `test_retry_on_connection_error_with_exponential_backoff`
- **Arrange-Act-Assert pattern**

### Test Data

- **Use fixtures for common test data**
- **Generate realistic data with Faker/Mimesis**
- **Clean up test data after tests**
- **Use deterministic random seeds**: `Faker.seed(42)`

### Mocking

- **Mock external services (APIs, databases)**
- **Don't mock code under test**
- **Verify mock calls**: `mock.assert_called_once_with(...)`
- **Use `patch` for temporary mocks**

### Async Testing

```python
@pytest.mark.asyncio
async def test_async_operation():
    """Test async operation"""
    result = await async_function()
    assert result is not None
```

### Parametrized Tests

```python
@pytest.mark.parametrize("input,expected", [
    ({"key": "value"}, True),
    ({"key": None}, False),
    ({}, False)
])
def test_validation(input, expected):
    """Test validation with multiple inputs"""
    assert validate(input) == expected
```

### Test Timeouts

```python
@pytest.mark.timeout(10)  # Timeout after 10 seconds
def test_slow_operation():
    """Test that should complete quickly"""
    slow_operation()
```

## Debugging Tests

### Using pytest debugger

```bash
# Drop into debugger on failure
pytest --pdb

# Drop into debugger at start
pytest --trace
```

### Print debugging

```bash
# Show print statements
pytest -s

# Show captured output on failure
pytest --tb=short
```

### Logging in tests

```python
import logging

def test_with_logging(caplog):
    """Test with logging capture"""
    with caplog.at_level(logging.INFO):
        function_that_logs()

    assert "Expected log message" in caplog.text
```

## Common Issues

### Tests Hang

**Problem**: Tests hang indefinitely

**Solutions**:
- Use `pytest-timeout`: `pytest --timeout=30`
- Check for infinite loops
- Verify async operations complete
- Check for resource leaks

### Tests Flaky

**Problem**: Tests pass/fail randomly

**Solutions**:
- Avoid time-dependent assertions
- Use deterministic test data
- Increase timeouts for slow operations
- Mock time-dependent code
- Check for race conditions

### Tests Too Slow

**Problem**: Tests take too long to run

**Solutions**:
- Run only changed tests locally
- Use pytest markers to skip slow tests
- Run tests in parallel: `pytest -n auto`
- Mock slow external services
- Use smaller test datasets

## Next Steps

- Read [Setup Guide](setup.md) for environment setup
- Review [Architecture Documentation](../architecture/overview.md)
- Check [Runbooks](../runbooks/) for operational procedures
- See [Contributing Guide](../../CONTRIBUTING.md) for development workflow
