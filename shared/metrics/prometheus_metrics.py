"""Prometheus metrics definitions and helpers.

Provides common metric definitions for CDC pipeline components.
"""

from typing import Callable

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    REGISTRY,
    CollectorRegistry,
)


class CDCMetrics:
    """CDC Pipeline metrics."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        """Initialize CDC metrics.

        Args:
            registry: Prometheus registry to use
        """
        # Events processed
        self.events_processed = Counter(
            "cdc_events_processed_total",
            "Total number of CDC events processed",
            ["service", "collection", "operation"],
            registry=registry,
        )

        # Events failed
        self.events_failed = Counter(
            "cdc_events_failed_total",
            "Total number of CDC events that failed processing",
            ["service", "collection", "error_type"],
            registry=registry,
        )

        # Processing duration
        self.processing_duration = Histogram(
            "cdc_processing_duration_seconds",
            "Time spent processing CDC events",
            ["service", "collection", "operation"],
            buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0],
            registry=registry,
        )

        # Replication lag
        self.replication_lag = Gauge(
            "cdc_replication_lag_seconds",
            "Replication lag in seconds",
            ["service", "collection"],
            registry=registry,
        )

        # Batch size
        self.batch_size = Histogram(
            "cdc_batch_size",
            "Number of events in processing batch",
            ["service", "collection"],
            buckets=[1, 10, 50, 100, 500, 1000, 2000, 5000, 10000],
            registry=registry,
        )

        # Dead letter queue size
        self.dlq_messages = Gauge(
            "dlq_messages_total",
            "Number of messages in dead letter queue",
            ["service", "collection"],
            registry=registry,
        )

        # Schema evolution events
        self.schema_evolution = Counter(
            "cdc_schema_evolution_total",
            "Number of schema evolution events",
            ["service", "collection", "evolution_type"],
            registry=registry,
        )


class ReconciliationMetrics:
    """Reconciliation engine metrics."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        """Initialize reconciliation metrics.

        Args:
            registry: Prometheus registry to use
        """
        # Jobs started
        self.jobs_started = Counter(
            "reconciliation_jobs_started_total",
            "Total number of reconciliation jobs started",
            ["collection", "job_type"],
            registry=registry,
        )

        # Jobs completed
        self.jobs_completed = Counter(
            "reconciliation_jobs_completed_total",
            "Total number of reconciliation jobs completed",
            ["collection", "job_type", "status"],
            registry=registry,
        )

        # Job duration
        self.job_duration = Histogram(
            "reconciliation_job_duration_seconds",
            "Time spent on reconciliation jobs",
            ["collection", "job_type"],
            buckets=[60, 300, 600, 1800, 3600, 7200, 14400, 21600],  # 1m to 6h
            registry=registry,
        )

        # Records compared
        self.records_compared = Counter(
            "reconciliation_records_compared_total",
            "Total number of records compared",
            ["collection"],
            registry=registry,
        )

        # Discrepancies found
        self.discrepancies_found = Counter(
            "reconciliation_discrepancies_found_total",
            "Total number of discrepancies found",
            ["collection", "discrepancy_type"],
            registry=registry,
        )

        # Job status
        self.job_status = Gauge(
            "reconciliation_job_status",
            "Current status of reconciliation jobs (1=running, 0=stopped)",
            ["job_id", "collection"],
            registry=registry,
        )


def setup_metrics() -> tuple[CDCMetrics, ReconciliationMetrics]:
    """Setup and return metric instances.

    Returns:
        Tuple of (CDCMetrics, ReconciliationMetrics)
    """
    cdc_metrics = CDCMetrics()
    reconciliation_metrics = ReconciliationMetrics()
    return cdc_metrics, reconciliation_metrics


def get_metrics_handler() -> Callable[[], bytes]:
    """Get metrics handler for HTTP endpoint.

    Returns:
        Function that generates Prometheus metrics output
    """

    def metrics_handler() -> bytes:
        return generate_latest(REGISTRY)

    return metrics_handler
