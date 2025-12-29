"""Unit tests for tracing utilities"""
import pytest
import uuid
from datetime import datetime, UTC
from unittest.mock import Mock, patch
import time

from utils.tracing import (
    TraceSpan,
    trace_span,
    get_trace_context
)
from utils.request_context import RequestContext


class TestTraceSpan:
    """Test TraceSpan class"""
    
    def test_trace_span_initialization(self):
        """Test TraceSpan initialization"""
        span = TraceSpan("test_span")
        
        assert span.name == "test_span"
        assert span.span_id is not None
        assert span.trace_id is not None
        assert span.parent_span_id is None
        assert span.start_time is None
        assert span.end_time is None
        assert span.duration_ms is None
        assert isinstance(span.tags, dict)
        assert isinstance(span.logs, list)
    
    def test_trace_span_with_parent(self):
        """Test TraceSpan with parent span ID"""
        parent_id = str(uuid.uuid4())
        span = TraceSpan("child_span", parent_span_id=parent_id)
        
        assert span.parent_span_id == parent_id
    
    def test_trace_span_with_trace_id(self):
        """Test TraceSpan with explicit trace ID"""
        trace_id = str(uuid.uuid4())
        span = TraceSpan("test_span", trace_id=trace_id)
        
        assert span.trace_id == trace_id
    
    def test_trace_span_start(self):
        """Test TraceSpan start method"""
        span = TraceSpan("test_span")
        span.start()
        
        assert span.start_time is not None
        assert isinstance(span.start_time, datetime)
    
    def test_trace_span_finish(self):
        """Test TraceSpan finish method"""
        span = TraceSpan("test_span")
        span.start()
        time.sleep(0.01)  # Small delay to ensure duration > 0
        span.finish()
        
        assert span.end_time is not None
        assert isinstance(span.end_time, datetime)
        assert span.duration_ms is not None
        assert span.duration_ms > 0
    
    def test_trace_span_finish_without_start(self):
        """Test TraceSpan finish without start"""
        span = TraceSpan("test_span")
        span.finish()
        
        assert span.end_time is not None
        assert span.duration_ms is None  # No start time, so no duration
    
    def test_trace_span_set_tag(self):
        """Test TraceSpan set_tag method"""
        span = TraceSpan("test_span")
        span.set_tag("key1", "value1")
        span.set_tag("key2", 123)
        span.set_tag("key3", True)
        
        assert span.tags["key1"] == "value1"
        assert span.tags["key2"] == 123
        assert span.tags["key3"] is True
    
    def test_trace_span_set_tag_overwrite(self):
        """Test TraceSpan set_tag overwrites existing tags"""
        span = TraceSpan("test_span")
        span.set_tag("key", "value1")
        span.set_tag("key", "value2")
        
        assert span.tags["key"] == "value2"
    
    def test_trace_span_log(self):
        """Test TraceSpan log method"""
        span = TraceSpan("test_span")
        span.log("Test message", level="INFO", extra="data")
        
        assert len(span.logs) == 1
        assert span.logs[0]["message"] == "Test message"
        assert span.logs[0]["level"] == "INFO"
        assert span.logs[0]["extra"] == "data"
        assert "timestamp" in span.logs[0]
    
    def test_trace_span_log_multiple(self):
        """Test TraceSpan log with multiple entries"""
        span = TraceSpan("test_span")
        span.log("Message 1", level="INFO")
        span.log("Message 2", level="WARNING")
        span.log("Message 3", level="ERROR")
        
        assert len(span.logs) == 3
        assert span.logs[0]["level"] == "INFO"
        assert span.logs[1]["level"] == "WARNING"
        assert span.logs[2]["level"] == "ERROR"
    
    def test_trace_span_to_dict(self):
        """Test TraceSpan to_dict method"""
        span = TraceSpan("test_span", parent_span_id="parent-123")
        span.start()
        span.set_tag("key", "value")
        span.log("Test message", level="INFO")
        span.finish()
        
        span_dict = span.to_dict()
        
        assert isinstance(span_dict, dict)
        assert span_dict["name"] == "test_span"
        assert span_dict["span_id"] == span.span_id
        assert span_dict["trace_id"] == span.trace_id
        assert span_dict["parent_span_id"] == "parent-123"
        assert span_dict["start_time"] is not None
        assert span_dict["end_time"] is not None
        assert span_dict["duration_ms"] is not None
        assert span_dict["tags"]["key"] == "value"
        assert len(span_dict["logs"]) == 1
    
    def test_trace_span_to_dict_before_start(self):
        """Test TraceSpan to_dict before start"""
        span = TraceSpan("test_span")
        span_dict = span.to_dict()
        
        assert span_dict["start_time"] is None
        assert span_dict["end_time"] is None
        assert span_dict["duration_ms"] is None


class TestTraceSpanContextManager:
    """Test trace_span context manager"""
    
    def test_trace_span_context_manager_success(self):
        """Test trace_span context manager on success"""
        with trace_span("test_operation") as span:
            assert isinstance(span, TraceSpan)
            assert span.name == "test_operation"
            assert span.start_time is not None
        
        # After context exit
        assert span.end_time is not None
        assert span.duration_ms is not None
    
    def test_trace_span_context_manager_with_component(self):
        """Test trace_span context manager with component tag"""
        with trace_span("test_operation", component="test_service") as span:
            assert span.tags["component"] == "test_service"
    
    def test_trace_span_context_manager_with_tags(self):
        """Test trace_span context manager with additional tags"""
        with trace_span(
            "test_operation",
            component="test_service",
            query_hash="abc123",
            user_id="user456"
        ) as span:
            assert span.tags["component"] == "test_service"
            assert span.tags["query_hash"] == "abc123"
            assert span.tags["user_id"] == "user456"
    
    def test_trace_span_context_manager_on_exception(self):
        """Test trace_span context manager on exception"""
        with pytest.raises(ValueError):
            with trace_span("test_operation") as span:
                raise ValueError("Test error")
        
        # Span should be finished and tagged with error
        assert span.end_time is not None
        assert span.tags["error"] is True
        assert span.tags["error_type"] == "ValueError"
        assert len(span.logs) > 0
        assert any(log["level"] == "ERROR" for log in span.logs)
    
    def test_trace_span_context_manager_nested(self):
        """Test nested trace_span context managers"""
        RequestContext.clear()
        request_id = RequestContext.set_request_id("test-request-123")
        
        with trace_span("parent_operation") as parent_span:
            with trace_span("child_operation") as child_span:
                assert child_span.parent_span_id is None  # Not automatically set
                # Both should use the same trace_id from request context
                assert child_span.trace_id == parent_span.trace_id
                assert child_span.trace_id == request_id
        
        assert parent_span.end_time is not None
        assert child_span.end_time is not None
    
    def test_trace_span_context_manager_duration(self):
        """Test trace_span context manager measures duration"""
        with trace_span("test_operation") as span:
            time.sleep(0.01)  # Small delay
        
        assert span.duration_ms is not None
        assert span.duration_ms >= 10  # At least 10ms


class TestGetTraceContext:
    """Test get_trace_context function"""
    
    def test_get_trace_context_with_request_id(self):
        """Test get_trace_context when request ID is set"""
        RequestContext.clear()
        request_id = RequestContext.set_request_id("test-request-123")
        
        context = get_trace_context()
        
        assert isinstance(context, dict)
        assert context["trace_id"] == request_id
        assert context["request_id"] == request_id
    
    def test_get_trace_context_without_request_id(self):
        """Test get_trace_context when no request ID is set"""
        RequestContext.clear()
        
        context = get_trace_context()
        
        assert isinstance(context, dict)
        assert context["trace_id"] is None
        assert context["request_id"] is None
    
    def test_get_trace_context_returns_dict(self):
        """Test that get_trace_context returns a dictionary"""
        context = get_trace_context()
        
        assert isinstance(context, dict)
        assert "trace_id" in context
        assert "request_id" in context


class TestTracingEdgeCases:
    """Test edge cases for tracing utilities"""
    
    def test_trace_span_with_empty_name(self):
        """Test TraceSpan with empty name"""
        span = TraceSpan("")
        assert span.name == ""
    
    def test_trace_span_with_unicode_name(self):
        """Test TraceSpan with unicode name"""
        span = TraceSpan("операция_测试")
        assert span.name == "операция_测试"
    
    def test_trace_span_with_very_long_name(self):
        """Test TraceSpan with very long name"""
        long_name = "a" * 1000
        span = TraceSpan(long_name)
        assert span.name == long_name
    
    def test_trace_span_set_tag_with_none_value(self):
        """Test TraceSpan set_tag with None value"""
        span = TraceSpan("test_span")
        span.set_tag("key", None)
        
        assert span.tags["key"] is None
    
    def test_trace_span_set_tag_with_dict_value(self):
        """Test TraceSpan set_tag with dict value"""
        span = TraceSpan("test_span")
        span.set_tag("key", {"nested": "value"})
        
        assert span.tags["key"] == {"nested": "value"}
    
    def test_trace_span_log_with_empty_message(self):
        """Test TraceSpan log with empty message"""
        span = TraceSpan("test_span")
        span.log("", level="INFO")
        
        assert len(span.logs) == 1
        assert span.logs[0]["message"] == ""
    
    def test_trace_span_multiple_start_finish(self):
        """Test TraceSpan with multiple start/finish calls"""
        span = TraceSpan("test_span")
        span.start()
        span.finish()
        first_duration = span.duration_ms
        
        span.start()
        span.finish()
        second_duration = span.duration_ms
        
        # Second duration should overwrite first
        assert second_duration is not None
    
    def test_trace_span_context_manager_with_none_component(self):
        """Test trace_span context manager with None component"""
        with trace_span("test_operation", component=None) as span:
            # Component should not be set if None
            assert "component" not in span.tags or span.tags.get("component") is None
    
    def test_trace_span_context_manager_exception_propagation(self):
        """Test that trace_span context manager propagates exceptions"""
        with pytest.raises(RuntimeError):
            with trace_span("test_operation") as span:
                raise RuntimeError("Test error")
        
        # Exception should be propagated
        assert span.tags.get("error") is True
    
    def test_trace_span_unique_span_ids(self):
        """Test that TraceSpan generates unique span IDs"""
        spans = [TraceSpan("test_span") for _ in range(10)]
        span_ids = [span.span_id for span in spans]
        
        # All span IDs should be unique
        assert len(span_ids) == len(set(span_ids))
    
    def test_trace_span_uses_request_context_trace_id(self):
        """Test that TraceSpan uses request context trace ID"""
        RequestContext.clear()
        request_id = RequestContext.set_request_id("test-request-123")
        
        span = TraceSpan("test_span")
        
        # Should use request ID as trace ID
        assert span.trace_id == request_id

