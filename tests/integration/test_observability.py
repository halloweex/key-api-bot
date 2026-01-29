"""
Integration tests for core/observability.py

Tests structured logging, correlation IDs, and metrics collection.
"""
import logging
import json
import pytest
import time as time_module

from core.observability import (
    generate_correlation_id,
    set_correlation_id,
    get_correlation_id,
    Timer,
    MetricsCollector,
    StructuredFormatter,
    get_logger,
)


class TestCorrelationId:
    """Tests for correlation ID management."""

    def test_generate_correlation_id_not_empty(self):
        """Generated ID is not empty."""
        cid = generate_correlation_id()
        assert cid is not None
        assert len(cid) > 0

    def test_set_and_get_correlation_id(self):
        """Can set and retrieve correlation ID."""
        test_id = "test-correlation-123"
        set_correlation_id(test_id)
        assert get_correlation_id() == test_id

    def test_correlation_id_isolation(self):
        """Correlation IDs are context-isolated."""
        set_correlation_id("context-1")
        assert get_correlation_id() == "context-1"


class TestTimer:
    """Tests for Timer context manager."""

    def test_measures_elapsed_time(self):
        """Timer measures elapsed time correctly."""
        with Timer("test_operation") as timer:
            time_module.sleep(0.05)

        assert timer.elapsed_ms >= 45  # At least 45ms
        assert timer.elapsed_ms < 200  # But not too long (allow some slack)

    def test_timer_name(self):
        """Timer stores operation name."""
        with Timer("my_operation") as timer:
            pass

        assert timer.name == "my_operation"


class TestMetricsCollector:
    """Tests for MetricsCollector class."""

    def test_record_request(self):
        """Records request counts by endpoint."""
        metrics = MetricsCollector()
        metrics.record_request("/api/health")
        metrics.record_request("/api/health")
        metrics.record_request("/api/orders")

        stats = metrics.get_stats()
        assert stats["requests"]["/api/health"] == 2
        assert stats["requests"]["/api/orders"] == 1

    def test_record_error(self):
        """Records error counts by type."""
        metrics = MetricsCollector()
        metrics.record_error("ValueError")
        metrics.record_error("TimeoutError")
        metrics.record_error("ValueError")

        stats = metrics.get_stats()
        assert stats["errors"]["ValueError"] == 2
        assert stats["errors"]["TimeoutError"] == 1

    def test_record_timing(self):
        """Records timing statistics."""
        metrics = MetricsCollector()
        metrics.record_timing("/api/test", 100.0)
        metrics.record_timing("/api/test", 200.0)
        metrics.record_timing("/api/test", 150.0)

        stats = metrics.get_stats()
        timings = stats["timing"]["/api/test"]
        assert timings["count"] == 3
        assert timings["avg_ms"] == 150.0
        assert timings["min_ms"] == 100.0
        assert timings["max_ms"] == 200.0

    def test_reset_stats(self):
        """Reset clears all statistics."""
        metrics = MetricsCollector()
        metrics.record_request("/api/test")
        metrics.record_error("Error")
        metrics.record_timing("/api/test", 100.0)

        metrics.reset()
        stats = metrics.get_stats()

        assert len(stats["requests"]) == 0
        assert len(stats["errors"]) == 0
        assert len(stats["timing"]) == 0

    def test_get_stats_empty(self):
        """Returns valid stats when empty."""
        metrics = MetricsCollector()
        stats = metrics.get_stats()

        assert len(stats["requests"]) == 0
        assert len(stats["errors"]) == 0
        assert isinstance(stats["requests"], dict)


class TestStructuredFormatter:
    """Tests for JSON log formatter."""

    def test_formats_as_json(self):
        """Outputs valid JSON."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        assert parsed["message"] == "Test message"
        assert parsed["level"] == "INFO"
        assert parsed["logger"] == "test.logger"

    def test_includes_timestamp(self):
        """JSON includes ISO timestamp."""
        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        assert "timestamp" in parsed
        # ISO format check
        assert "T" in parsed["timestamp"]

    def test_includes_correlation_id(self):
        """JSON includes correlation ID when set."""
        set_correlation_id("test-correlation-456")

        formatter = StructuredFormatter()
        record = logging.LogRecord(
            name="test",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test",
            args=(),
            exc_info=None,
        )

        output = formatter.format(record)
        parsed = json.loads(output)

        assert parsed["correlation_id"] == "test-correlation-456"


class TestGetLogger:
    """Tests for get_logger function."""

    def test_returns_logger(self):
        """Returns a logger instance."""
        logger = get_logger("test.module")
        assert isinstance(logger, logging.Logger)
        assert logger.name == "test.module"

    def test_logger_has_name(self):
        """Logger has correct name."""
        logger = get_logger("my.custom.logger")
        assert logger.name == "my.custom.logger"
