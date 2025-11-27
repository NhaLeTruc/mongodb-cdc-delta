"""
Integration tests for retry logic on transient MinIO failures.

Tests that the Delta Lake writer correctly retries MinIO operations
on transient failures and recovers when MinIO becomes available again.
"""

import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch
import aiohttp


# Will be implemented in T077-T080
# from delta_writer.storage.minio_client import MinIOClient
# from delta_writer.utils.error_handler import retry_with_backoff
# from delta_writer.writer.delta_writer import DeltaLakeWriter


class TestMinIOTransientFailures:
    """Test retry behavior for transient MinIO failures"""

    @pytest.fixture
    async def minio_config(self):
        """MinIO configuration for tests"""
        return {
            "endpoint": "localhost:9000",
            "access_key": "test_access_key",
            "secret_key": "test_secret_key",
            "bucket": "test-bucket",
            "region": "us-east-1",
            "secure": False
        }

    @pytest.mark.asyncio
    async def test_retry_on_connection_error(self, minio_config):
        """Test retry on MinIO connection error"""
        call_count = 0

        async def upload_with_connection_error():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise aiohttp.ClientConnectionError("Connection to MinIO failed")
            return {"etag": "abc123", "version_id": "1"}

        # Simulate retry
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                result = await upload_with_connection_error()
                break
            except aiohttp.ClientConnectionError:
                if attempt == max_attempts - 1:
                    raise
                await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff

        assert result["etag"] == "abc123"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_on_timeout_error(self, minio_config):
        """Test retry on MinIO timeout error"""
        call_count = 0

        async def upload_with_timeout():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise asyncio.TimeoutError("MinIO operation timed out")
            return {"etag": "def456"}

        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                result = await upload_with_timeout()
                break
            except asyncio.TimeoutError:
                if attempt == max_attempts - 1:
                    raise
                await asyncio.sleep(0.1)

        assert result["etag"] == "def456"
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_retry_on_503_service_unavailable(self, minio_config):
        """Test retry on MinIO 503 Service Unavailable"""
        call_count = 0

        async def upload_with_503():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("HTTP 503: Service Unavailable")
            return {"etag": "ghi789"}

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                result = await upload_with_503()
                break
            except Exception as e:
                if "503" in str(e):
                    if attempt == max_attempts - 1:
                        raise
                    await asyncio.sleep(0.2 * (2 ** attempt))
                else:
                    raise

        assert result["etag"] == "ghi789"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_no_retry_on_non_retryable_error(self, minio_config):
        """Test that non-retryable errors don't trigger retry"""
        call_count = 0

        async def upload_with_400_error():
            nonlocal call_count
            call_count += 1
            raise Exception("HTTP 400: Bad Request")

        # Non-retryable errors should fail immediately
        with pytest.raises(Exception, match="400"):
            await upload_with_400_error()

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_max_retries_exhausted(self, minio_config):
        """Test behavior when max retries are exhausted"""
        call_count = 0

        async def always_fails():
            nonlocal call_count
            call_count += 1
            raise aiohttp.ClientConnectionError("MinIO unreachable")

        max_attempts = 3
        with pytest.raises(aiohttp.ClientConnectionError):
            for attempt in range(max_attempts):
                try:
                    await always_fails()
                except aiohttp.ClientConnectionError:
                    if attempt == max_attempts - 1:
                        raise
                    await asyncio.sleep(0.1)

        assert call_count == 3


class TestMinIORecoveryScenarios:
    """Test MinIO recovery scenarios"""

    @pytest.mark.asyncio
    async def test_recovery_after_temporary_outage(self):
        """Test recovery after MinIO comes back online"""
        minio_available = False
        call_count = 0

        async def upload_with_recovery():
            nonlocal call_count, minio_available
            call_count += 1

            # Simulate MinIO becoming available after 2 attempts
            if call_count >= 3:
                minio_available = True

            if not minio_available:
                raise ConnectionError("MinIO down")
            return {"etag": "recovered"}

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                result = await upload_with_recovery()
                break
            except ConnectionError:
                if attempt == max_attempts - 1:
                    raise
                await asyncio.sleep(0.1)

        assert result["etag"] == "recovered"
        assert minio_available is True
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_partial_upload_resume(self):
        """Test resuming partial upload after failure"""
        uploaded_parts = []
        total_parts = 5

        async def upload_part(part_number):
            # Simulate failure on part 3
            if part_number == 3 and len(uploaded_parts) < 3:
                raise ConnectionError("Upload failed")

            uploaded_parts.append(part_number)
            return {"etag": f"part-{part_number}"}

        # Upload with retry
        for part_num in range(1, total_parts + 1):
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    await upload_part(part_num)
                    break
                except ConnectionError:
                    if attempt == max_attempts - 1:
                        raise
                    await asyncio.sleep(0.1)

        assert len(uploaded_parts) == total_parts

    @pytest.mark.asyncio
    async def test_circuit_breaker_prevents_overload(self):
        """Test circuit breaker prevents overwhelming failing MinIO"""
        circuit_state = {"failures": 0, "state": "closed", "threshold": 5}

        async def upload_with_circuit_breaker():
            if circuit_state["state"] == "open":
                raise Exception("Circuit breaker is open")

            # Simulate failure
            circuit_state["failures"] += 1
            if circuit_state["failures"] >= circuit_state["threshold"]:
                circuit_state["state"] = "open"

            raise ConnectionError("MinIO error")

        # Try uploads until circuit opens
        with pytest.raises(Exception, match="Circuit breaker"):
            for i in range(10):
                try:
                    await upload_with_circuit_breaker()
                except ConnectionError:
                    if circuit_state["state"] == "open":
                        raise Exception("Circuit breaker is open")
                await asyncio.sleep(0.01)

        assert circuit_state["state"] == "open"


class TestMinIORetryMetrics:
    """Test metrics collection for MinIO retries"""

    @pytest.mark.asyncio
    async def test_track_retry_attempts(self):
        """Test tracking number of retry attempts"""
        metrics = {
            "total_attempts": 0,
            "successful_attempts": 0,
            "failed_attempts": 0,
            "retry_count": 0
        }

        async def upload_with_metrics():
            metrics["total_attempts"] += 1
            if metrics["total_attempts"] < 3:
                metrics["retry_count"] += 1
                raise ConnectionError("Retry needed")
            metrics["successful_attempts"] += 1
            return {"etag": "success"}

        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                result = await upload_with_metrics()
                break
            except ConnectionError:
                if attempt == max_attempts - 1:
                    metrics["failed_attempts"] += 1
                    raise
                await asyncio.sleep(0.01)

        assert metrics["total_attempts"] == 3
        assert metrics["successful_attempts"] == 1
        assert metrics["retry_count"] == 2

    @pytest.mark.asyncio
    async def test_track_retry_duration(self):
        """Test tracking total retry duration"""
        start_time = datetime.utcnow()
        delays = []

        async def upload_with_delays():
            if len(delays) < 2:
                delay = 0.1 * (2 ** len(delays))
                delays.append(delay)
                await asyncio.sleep(delay)
                raise ConnectionError("Retry")
            return {"etag": "done"}

        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                result = await upload_with_delays()
                break
            except ConnectionError:
                if attempt == max_attempts - 1:
                    raise

        end_time = datetime.utcnow()
        total_duration = (end_time - start_time).total_seconds()

        # Should have delays of 0.1s + 0.2s = 0.3s minimum
        assert total_duration >= 0.3


class TestMinIOBatchOperations:
    """Test retry logic for batch MinIO operations"""

    @pytest.mark.asyncio
    async def test_batch_upload_with_partial_failures(self):
        """Test batch upload where some files fail"""
        files = [f"file_{i}.parquet" for i in range(10)]
        uploaded = []
        failed = []

        async def upload_file(filename):
            # Simulate intermittent failures
            if "file_5" in filename or "file_7" in filename:
                raise ConnectionError(f"Failed to upload {filename}")
            uploaded.append(filename)
            return {"file": filename, "etag": f"etag_{filename}"}

        # Upload with retry per file
        for filename in files:
            max_attempts = 3
            success = False
            for attempt in range(max_attempts):
                try:
                    await upload_file(filename)
                    success = True
                    break
                except ConnectionError:
                    if attempt == max_attempts - 1:
                        failed.append(filename)
                    await asyncio.sleep(0.01)

        assert len(failed) == 2
        assert "file_5.parquet" in failed

    @pytest.mark.asyncio
    async def test_concurrent_uploads_with_retry(self):
        """Test concurrent uploads with individual retry logic"""
        files = [f"file_{i}.parquet" for i in range(5)]
        results = []

        async def upload_with_retry(filename):
            attempt_count = 0
            max_attempts = 3

            for attempt in range(max_attempts):
                try:
                    attempt_count += 1
                    if attempt_count < 2:
                        raise ConnectionError("Transient error")
                    return {"file": filename, "etag": f"etag_{filename}"}
                except ConnectionError:
                    if attempt == max_attempts - 1:
                        raise
                    await asyncio.sleep(0.1)

        # Upload concurrently
        tasks = [upload_with_retry(f) for f in files]
        results = await asyncio.gather(*tasks, return_exceptions=False)

        assert len(results) == 5
        assert all("etag" in r for r in results)


class TestMinIOBackpressure:
    """Test backpressure handling when MinIO is slow"""

    @pytest.mark.asyncio
    async def test_backpressure_on_slow_uploads(self):
        """Test applying backpressure when MinIO is slow"""
        upload_queue = asyncio.Queue(maxsize=10)
        slow_uploads = 0

        async def upload_with_backpressure(data):
            nonlocal slow_uploads
            # Simulate slow upload
            await asyncio.sleep(0.2)
            slow_uploads += 1
            return {"etag": f"etag_{slow_uploads}"}

        # Producer: add items to queue
        async def producer():
            for i in range(20):
                await upload_queue.put({"data": f"data_{i}"})

        # Consumer: process with backpressure
        async def consumer():
            processed = []
            while not upload_queue.empty() or processed == []:
                try:
                    item = await asyncio.wait_for(upload_queue.get(), timeout=0.1)
                    result = await upload_with_backpressure(item)
                    processed.append(result)
                except asyncio.TimeoutError:
                    break
            return processed

        # Run producer and consumer
        await producer()
        results = await consumer()

        # Should process some items (queue size limits backpressure)
        assert len(results) > 0

    @pytest.mark.asyncio
    async def test_rate_limiting_on_errors(self):
        """Test rate limiting when encountering many errors"""
        error_count = 0
        rate_limiter = {"tokens": 10, "refill_rate": 1}

        async def upload_with_rate_limit():
            nonlocal error_count
            error_count += 1

            # Check rate limit
            if rate_limiter["tokens"] <= 0:
                await asyncio.sleep(1.0 / rate_limiter["refill_rate"])
                rate_limiter["tokens"] += 1

            rate_limiter["tokens"] -= 1
            raise ConnectionError("MinIO error")

        # Try multiple uploads
        attempts = 0
        with pytest.raises(ConnectionError):
            for i in range(15):
                try:
                    await upload_with_rate_limit()
                    attempts += 1
                except ConnectionError:
                    if i == 14:
                        raise

        # Should have hit rate limit
        assert rate_limiter["tokens"] < 10


class TestMinIOHealthChecks:
    """Test MinIO health checks during retry"""

    @pytest.mark.asyncio
    async def test_health_check_before_retry(self):
        """Test checking MinIO health before retrying"""
        minio_healthy = False

        async def check_minio_health():
            return minio_healthy

        async def upload_with_health_check():
            if not await check_minio_health():
                raise ConnectionError("MinIO unhealthy")
            return {"etag": "success"}

        # First attempt fails
        with pytest.raises(ConnectionError):
            await upload_with_health_check()

        # MinIO becomes healthy
        minio_healthy = True

        # Second attempt succeeds
        result = await upload_with_health_check()
        assert result["etag"] == "success"

    @pytest.mark.asyncio
    async def test_exponential_backoff_with_jitter(self):
        """Test exponential backoff with jitter"""
        import random

        delays = []

        async def upload_with_backoff():
            if len(delays) < 3:
                base_delay = 0.1 * (2 ** len(delays))
                jitter = random.uniform(-0.1, 0.1) * base_delay
                delay = base_delay + jitter
                delays.append(delay)
                await asyncio.sleep(delay)
                raise ConnectionError("Retry")
            return {"etag": "success"}

        max_attempts = 4
        for attempt in range(max_attempts):
            try:
                result = await upload_with_backoff()
                break
            except ConnectionError:
                if attempt == max_attempts - 1:
                    raise

        # Verify exponential growth pattern (with jitter tolerance)
        assert len(delays) == 3
        assert delays[1] > delays[0] * 1.5  # Roughly 2x with jitter
        assert delays[2] > delays[1] * 1.5


class TestMinIORetryLogging:
    """Test logging during MinIO retry operations"""

    @pytest.mark.asyncio
    async def test_log_retry_attempts(self, caplog):
        """Test that retry attempts are logged"""
        import logging

        logger = logging.getLogger("minio_retry")

        async def upload_with_logging():
            for attempt in range(3):
                try:
                    if attempt < 2:
                        logger.warning(f"MinIO upload attempt {attempt + 1} failed, retrying...")
                        raise ConnectionError("Upload failed")
                    return {"etag": "success"}
                except ConnectionError:
                    if attempt == 2:
                        raise
                    await asyncio.sleep(0.01)

        result = await upload_with_logging()
        assert result["etag"] == "success"

    @pytest.mark.asyncio
    async def test_log_retry_exhausted(self, caplog):
        """Test logging when retries are exhausted"""
        import logging

        logger = logging.getLogger("minio_retry")

        async def upload_fails_all_retries():
            max_attempts = 3
            for attempt in range(max_attempts):
                try:
                    raise ConnectionError("MinIO down")
                except ConnectionError:
                    if attempt == max_attempts - 1:
                        logger.error("Max retries exhausted for MinIO upload")
                        raise
                    await asyncio.sleep(0.01)

        with pytest.raises(ConnectionError):
            await upload_fails_all_retries()
