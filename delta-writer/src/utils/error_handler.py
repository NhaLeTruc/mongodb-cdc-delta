"""
Error handling utilities with exponential backoff retry logic.

Provides retry decorators and error classification for transient vs
permanent failures in MinIO, Kafka, and other network operations.
"""

import asyncio
import functools
import logging
import random
import time
from datetime import datetime
from enum import Enum
from typing import Callable, Optional, Type, Tuple, Any
from dataclasses import dataclass, field

import aiohttp
from kafka.errors import KafkaError, KafkaConnectionError, KafkaTimeoutError


logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """Error category classification"""
    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"
    RATE_LIMITED = "rate_limited"


@dataclass
class RetryConfig:
    """Configuration for retry behavior"""
    max_attempts: int = 3
    initial_delay: float = 0.1  # seconds
    max_delay: float = 60.0  # seconds
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_range: float = 0.2  # +/- 20%


@dataclass
class RetryMetrics:
    """Metrics for retry operations"""
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    retry_count: int = 0
    total_retry_duration_ms: float = 0.0
    last_error: Optional[str] = None
    last_error_timestamp: Optional[datetime] = None


# Retryable exception types
RETRYABLE_EXCEPTIONS = (
    # Network errors
    ConnectionError,
    TimeoutError,
    OSError,
    # HTTP errors
    aiohttp.ClientConnectionError,
    aiohttp.ClientConnectorError,
    aiohttp.ServerTimeoutError,
    # Kafka errors
    KafkaConnectionError,
    KafkaTimeoutError,
)


# Non-retryable exception types
NON_RETRYABLE_EXCEPTIONS = (
    ValueError,
    TypeError,
    KeyError,
    AttributeError,
    RuntimeError,
)


def classify_error(exception: Exception) -> ErrorCategory:
    """
    Classify an exception as retryable or non-retryable.

    Args:
        exception: The exception to classify

    Returns:
        ErrorCategory indicating retry behavior
    """
    # Check HTTP status codes if available
    if hasattr(exception, 'status'):
        status_code = exception.status
        if status_code in {408, 429, 500, 502, 503, 504}:
            return ErrorCategory.RETRYABLE
        elif status_code == 429:
            return ErrorCategory.RATE_LIMITED
        elif 400 <= status_code < 500:
            return ErrorCategory.NON_RETRYABLE

    # Check exception type
    if isinstance(exception, RETRYABLE_EXCEPTIONS):
        return ErrorCategory.RETRYABLE

    if isinstance(exception, NON_RETRYABLE_EXCEPTIONS):
        return ErrorCategory.NON_RETRYABLE

    # Check error message for common patterns
    error_msg = str(exception).lower()
    retryable_patterns = [
        'connection',
        'timeout',
        'unavailable',
        'temporary',
        'transient'
    ]

    if any(pattern in error_msg for pattern in retryable_patterns):
        return ErrorCategory.RETRYABLE

    # Default to non-retryable for safety
    return ErrorCategory.NON_RETRYABLE


def calculate_delay(
    attempt: int,
    config: RetryConfig
) -> float:
    """
    Calculate delay for retry attempt with exponential backoff and jitter.

    Args:
        attempt: Current attempt number (0-indexed)
        config: Retry configuration

    Returns:
        Delay in seconds
    """
    # Calculate base delay with exponential backoff
    delay = config.initial_delay * (config.exponential_base ** attempt)

    # Cap at max delay
    delay = min(delay, config.max_delay)

    # Add jitter if enabled
    if config.jitter:
        jitter = random.uniform(-config.jitter_range, config.jitter_range)
        delay = delay * (1 + jitter)

    return max(0, delay)


def retry_with_backoff(
    config: Optional[RetryConfig] = None,
    retryable_exceptions: Optional[Tuple[Type[Exception], ...]] = None,
    on_retry: Optional[Callable] = None,
    metrics: Optional[RetryMetrics] = None
):
    """
    Decorator for retrying operations with exponential backoff.

    Args:
        config: Retry configuration (uses defaults if None)
        retryable_exceptions: Tuple of exception types to retry
        on_retry: Optional callback called on each retry
        metrics: Optional metrics object to track retry stats

    Returns:
        Decorated function with retry logic

    Example:
        @retry_with_backoff(RetryConfig(max_attempts=5))
        async def upload_to_minio(data):
            return await minio_client.upload(data)
    """
    if config is None:
        config = RetryConfig()

    if retryable_exceptions is None:
        retryable_exceptions = RETRYABLE_EXCEPTIONS

    if metrics is None:
        metrics = RetryMetrics()

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(config.max_attempts):
                start_time = time.time()

                try:
                    metrics.total_attempts += 1
                    result = await func(*args, **kwargs)
                    metrics.successful_attempts += 1
                    return result

                except Exception as e:
                    last_exception = e
                    error_category = classify_error(e)

                    # Update metrics
                    metrics.last_error = str(e)
                    metrics.last_error_timestamp = datetime.utcnow()

                    # Don't retry non-retryable errors
                    if error_category == ErrorCategory.NON_RETRYABLE:
                        logger.error(
                            f"Non-retryable error in {func.__name__}: {e}",
                            extra={
                                "function": func.__name__,
                                "attempt": attempt + 1,
                                "error_type": type(e).__name__
                            }
                        )
                        metrics.failed_attempts += 1
                        raise

                    # Last attempt - raise the exception
                    if attempt == config.max_attempts - 1:
                        logger.error(
                            f"Max retries exhausted for {func.__name__}: {e}",
                            extra={
                                "function": func.__name__,
                                "max_attempts": config.max_attempts,
                                "total_retry_duration_ms": metrics.total_retry_duration_ms,
                                "error_type": type(e).__name__
                            }
                        )
                        metrics.failed_attempts += 1
                        raise

                    # Calculate delay and retry
                    delay = calculate_delay(attempt, config)
                    metrics.retry_count += 1
                    metrics.total_retry_duration_ms += delay * 1000

                    logger.warning(
                        f"Retrying {func.__name__} after {delay:.2f}s (attempt {attempt + 1}/{config.max_attempts}): {e}",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt + 1,
                            "max_attempts": config.max_attempts,
                            "delay_seconds": delay,
                            "error_type": type(e).__name__,
                            "error_category": error_category.value
                        }
                    )

                    # Call retry callback if provided
                    if on_retry:
                        on_retry(attempt, e, delay)

                    # Wait before retry
                    await asyncio.sleep(delay)

                finally:
                    elapsed = (time.time() - start_time) * 1000

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(config.max_attempts):
                start_time = time.time()

                try:
                    metrics.total_attempts += 1
                    result = func(*args, **kwargs)
                    metrics.successful_attempts += 1
                    return result

                except Exception as e:
                    last_exception = e
                    error_category = classify_error(e)

                    # Update metrics
                    metrics.last_error = str(e)
                    metrics.last_error_timestamp = datetime.utcnow()

                    # Don't retry non-retryable errors
                    if error_category == ErrorCategory.NON_RETRYABLE:
                        logger.error(
                            f"Non-retryable error in {func.__name__}: {e}",
                            extra={
                                "function": func.__name__,
                                "attempt": attempt + 1,
                                "error_type": type(e).__name__
                            }
                        )
                        metrics.failed_attempts += 1
                        raise

                    # Last attempt - raise the exception
                    if attempt == config.max_attempts - 1:
                        logger.error(
                            f"Max retries exhausted for {func.__name__}: {e}",
                            extra={
                                "function": func.__name__,
                                "max_attempts": config.max_attempts,
                                "total_retry_duration_ms": metrics.total_retry_duration_ms,
                                "error_type": type(e).__name__
                            }
                        )
                        metrics.failed_attempts += 1
                        raise

                    # Calculate delay and retry
                    delay = calculate_delay(attempt, config)
                    metrics.retry_count += 1
                    metrics.total_retry_duration_ms += delay * 1000

                    logger.warning(
                        f"Retrying {func.__name__} after {delay:.2f}s (attempt {attempt + 1}/{config.max_attempts}): {e}",
                        extra={
                            "function": func.__name__,
                            "attempt": attempt + 1,
                            "max_attempts": config.max_attempts,
                            "delay_seconds": delay,
                            "error_type": type(e).__name__,
                            "error_category": error_category.value
                        }
                    )

                    # Call retry callback if provided
                    if on_retry:
                        on_retry(attempt, e, delay)

                    # Wait before retry
                    time.sleep(delay)

                finally:
                    elapsed = (time.time() - start_time) * 1000

        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


@dataclass
class CircuitBreakerState:
    """State for circuit breaker pattern"""
    failure_count: int = 0
    last_failure_time: Optional[datetime] = None
    state: str = "closed"  # closed, open, half-open
    failure_threshold: int = 5
    timeout_seconds: int = 60


class CircuitBreaker:
    """
    Circuit breaker to prevent overwhelming failing services.

    States:
    - CLOSED: Normal operation, requests go through
    - OPEN: Too many failures, reject requests immediately
    - HALF_OPEN: After timeout, allow one request to test recovery
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_seconds: int = 60,
        recovery_timeout: int = 30
    ):
        self.state = CircuitBreakerState(
            failure_threshold=failure_threshold,
            timeout_seconds=timeout_seconds
        )
        self.recovery_timeout = recovery_timeout

    def check_state(self) -> str:
        """Check and update circuit breaker state"""
        if self.state.state == "open":
            # Check if timeout has elapsed
            if self.state.last_failure_time:
                elapsed = (datetime.utcnow() - self.state.last_failure_time).total_seconds()
                if elapsed > self.state.timeout_seconds:
                    self.state.state = "half-open"
                    logger.info("Circuit breaker transitioning to half-open state")

        return self.state.state

    def record_success(self):
        """Record successful operation"""
        if self.state.state == "half-open":
            # Recovery successful, close circuit
            self.state.state = "closed"
            self.state.failure_count = 0
            logger.info("Circuit breaker closed after successful recovery")
        elif self.state.state == "closed":
            # Reset failure count on success
            self.state.failure_count = 0

    def record_failure(self):
        """Record failed operation"""
        self.state.failure_count += 1
        self.state.last_failure_time = datetime.utcnow()

        if self.state.failure_count >= self.state.failure_threshold:
            if self.state.state != "open":
                self.state.state = "open"
                logger.warning(
                    f"Circuit breaker opened after {self.state.failure_count} failures",
                    extra={
                        "failure_count": self.state.failure_count,
                        "threshold": self.state.failure_threshold
                    }
                )

    def is_call_permitted(self) -> bool:
        """Check if call is permitted based on circuit state"""
        current_state = self.check_state()

        if current_state == "open":
            return False
        elif current_state == "half-open":
            # Allow one request to test recovery
            return True
        else:  # closed
            return True


def with_circuit_breaker(circuit_breaker: CircuitBreaker):
    """
    Decorator to apply circuit breaker pattern.

    Args:
        circuit_breaker: CircuitBreaker instance to use

    Example:
        cb = CircuitBreaker(failure_threshold=5, timeout_seconds=60)

        @with_circuit_breaker(cb)
        async def call_external_service():
            ...
    """

    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not circuit_breaker.is_call_permitted():
                raise Exception("Circuit breaker is open, rejecting call")

            try:
                result = await func(*args, **kwargs)
                circuit_breaker.record_success()
                return result
            except Exception as e:
                circuit_breaker.record_failure()
                raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not circuit_breaker.is_call_permitted():
                raise Exception("Circuit breaker is open, rejecting call")

            try:
                result = func(*args, **kwargs)
                circuit_breaker.record_success()
                return result
            except Exception as e:
                circuit_breaker.record_failure()
                raise

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
