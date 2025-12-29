"""Request tracing utilities for distributed tracing"""
import uuid
from typing import Optional, Dict, Any
from contextlib import contextmanager
from datetime import datetime, UTC
import time

from utils.request_context import RequestContext


class TraceSpan:
    """Represents a span in a distributed trace"""
    
    def __init__(
        self,
        name: str,
        parent_span_id: Optional[str] = None,
        trace_id: Optional[str] = None
    ):
        self.name = name
        self.span_id = str(uuid.uuid4())
        self.trace_id = trace_id or RequestContext.get_request_id() or str(uuid.uuid4())
        self.parent_span_id = parent_span_id
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.duration_ms: Optional[float] = None
        self.tags: Dict[str, Any] = {}
        self.logs: list = []
    
    def start(self):
        """Start the span"""
        self.start_time = datetime.now(UTC)
        return self
    
    def finish(self):
        """Finish the span and calculate duration"""
        self.end_time = datetime.now(UTC)
        if self.start_time:
            delta = self.end_time - self.start_time
            self.duration_ms = delta.total_seconds() * 1000
        return self
    
    def set_tag(self, key: str, value: Any):
        """Set a tag on the span"""
        self.tags[key] = value
        return self
    
    def log(self, message: str, level: str = "INFO", **kwargs):
        """Add a log entry to the span"""
        self.logs.append({
            "timestamp": datetime.now(UTC).isoformat(),
            "level": level,
            "message": message,
            **kwargs
        })
        return self
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert span to dictionary for logging"""
        return {
            "span_id": self.span_id,
            "trace_id": self.trace_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_ms": self.duration_ms,
            "tags": self.tags,
            "logs": self.logs
        }


@contextmanager
def trace_span(name: str, component: Optional[str] = None, **tags):
    """
    Context manager for creating a trace span
    
    Usage:
        with trace_span("execute_query", component="bigquery_service") as span:
            # Your code here
            span.set_tag("query_hash", query_hash)
            result = execute_query()
    """
    span = TraceSpan(name)
    if component:
        span.set_tag("component", component)
    for key, value in tags.items():
        span.set_tag(key, value)
    
    span.start()
    try:
        yield span
    except Exception as e:
        span.set_tag("error", True)
        span.set_tag("error_type", type(e).__name__)
        span.log(f"Error in {name}: {str(e)}", level="ERROR")
        raise
    finally:
        span.finish()


def get_trace_context() -> Dict[str, Any]:
    """
    Get current trace context for propagation
    
    Returns:
        Dictionary with trace context (trace_id, span_id)
    """
    request_id = RequestContext.get_request_id()
    return {
        "trace_id": request_id,
        "request_id": request_id
    }

