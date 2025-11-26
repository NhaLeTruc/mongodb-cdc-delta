"""OpenTelemetry configuration for distributed tracing.

Provides automatic instrumentation and trace export to Jaeger.
"""

import functools
from typing import Any, Callable, Optional, TypeVar

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode

F = TypeVar("F", bound=Callable[..., Any])


def configure_tracing(
    service_name: str,
    jaeger_host: str = "jaeger",
    jaeger_port: int = 6831,
    sampling_rate: float = 0.1,
) -> TracerProvider:
    """Configure OpenTelemetry tracing for the service.

    Args:
        service_name: Name of the service (e.g., "delta-writer")
        jaeger_host: Jaeger agent hostname
        jaeger_port: Jaeger agent port
        sampling_rate: Sampling rate (0.0 to 1.0)

    Returns:
        Configured TracerProvider
    """
    # Create resource with service information
    resource = Resource(
        attributes={
            "service.name": service_name,
            "service.namespace": "mongodb-cdc-delta",
            "service.version": "1.0.0",
        }
    )

    # Configure tracer provider
    provider = TracerProvider(resource=resource)

    # Configure Jaeger exporter
    jaeger_exporter = JaegerExporter(
        agent_host_name=jaeger_host,
        agent_port=jaeger_port,
    )

    # Add span processor
    provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

    # Set as global tracer provider
    trace.set_tracer_provider(provider)

    return provider


def get_tracer(name: str) -> trace.Tracer:
    """Get a tracer instance.

    Args:
        name: Tracer name (typically __name__)

    Returns:
        Tracer instance
    """
    return trace.get_tracer(name)


def trace_function(span_name: Optional[str] = None) -> Callable[[F], F]:
    """Decorator to automatically trace a function.

    Args:
        span_name: Optional custom span name (defaults to function name)

    Returns:
        Decorated function with automatic tracing
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
            name = span_name or func.__name__
            tracer = get_tracer(func.__module__)

            with tracer.start_as_current_span(name) as span:
                try:
                    # Add function metadata to span
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)

                    # Execute function
                    result = func(*args, **kwargs)

                    # Mark span as successful
                    span.set_status(Status(StatusCode.OK))

                    return result
                except Exception as exc:
                    # Record exception in span
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise

        @functools.wraps(func)
        async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
            name = span_name or func.__name__
            tracer = get_tracer(func.__module__)

            with tracer.start_as_current_span(name) as span:
                try:
                    # Add function metadata to span
                    span.set_attribute("function.name", func.__name__)
                    span.set_attribute("function.module", func.__module__)

                    # Execute async function
                    result = await func(*args, **kwargs)

                    # Mark span as successful
                    span.set_status(Status(StatusCode.OK))

                    return result
                except Exception as exc:
                    # Record exception in span
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise

        # Return appropriate wrapper based on function type
        if functools.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        else:
            return sync_wrapper  # type: ignore

    return decorator


class TracingMixin:
    """Mixin to add tracing capabilities to classes."""

    @property
    def tracer(self) -> trace.Tracer:
        """Get tracer for this class.

        Returns:
            Tracer instance with class context
        """
        return get_tracer(self.__class__.__module__)

    def start_span(self, name: str, **attributes: Any) -> trace.Span:
        """Start a new span with the given name and attributes.

        Args:
            name: Span name
            **attributes: Span attributes

        Returns:
            Active span
        """
        span = self.tracer.start_as_current_span(name)
        for key, value in attributes.items():
            span.set_attribute(key, value)
        return span
