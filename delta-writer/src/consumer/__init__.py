"""Kafka consumer components for CDC processing."""

from .event_handler import EventHandler
from .event_consumer import EventConsumer

__all__ = ["EventHandler", "EventConsumer"]
