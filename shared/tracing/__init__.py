"""Distributed tracing module using OpenTelemetry."""

from .otel_config import configure_tracing, get_tracer, trace_function

__all__ = ["configure_tracing", "get_tracer", "trace_function"]
