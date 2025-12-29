"""Unit tests for request context management"""
import pytest
import contextvars
import uuid
from datetime import datetime, UTC
from unittest.mock import Mock, patch

from utils.request_context import (
    RequestContext,
    get_request_context
)


class TestRequestContextRequestId:
    """Test RequestContext request ID management"""
    
    def test_generate_request_id(self):
        """Test generate_request_id creates UUID string"""
        request_id = RequestContext.generate_request_id()
        
        assert isinstance(request_id, str)
        assert len(request_id) > 0
        # Should be valid UUID format
        uuid.UUID(request_id)  # Should not raise
    
    def test_generate_request_id_unique(self):
        """Test that generate_request_id creates unique IDs"""
        ids = [RequestContext.generate_request_id() for _ in range(10)]
        
        # All IDs should be unique
        assert len(ids) == len(set(ids))
    
    def test_set_request_id_generates_new(self):
        """Test set_request_id generates new ID when None"""
        RequestContext.clear()
        
        request_id = RequestContext.set_request_id()
        
        assert request_id is not None
        assert isinstance(request_id, str)
        assert RequestContext.get_request_id() == request_id
    
    def test_set_request_id_with_existing(self):
        """Test set_request_id with existing ID"""
        RequestContext.clear()
        existing_id = "existing-request-123"
        
        request_id = RequestContext.set_request_id(existing_id)
        
        assert request_id == existing_id
        assert RequestContext.get_request_id() == existing_id
    
    def test_get_request_id_when_not_set(self):
        """Test get_request_id when not set"""
        RequestContext.clear()
        
        request_id = RequestContext.get_request_id()
        
        assert request_id is None
    
    def test_get_request_id_after_set(self):
        """Test get_request_id after set"""
        RequestContext.clear()
        test_id = "test-request-123"
        
        RequestContext.set_request_id(test_id)
        request_id = RequestContext.get_request_id()
        
        assert request_id == test_id


class TestRequestContextSessionId:
    """Test RequestContext session ID management"""
    
    def test_set_session_id(self):
        """Test set_session_id"""
        RequestContext.clear()
        session_id = "test-session-456"
        
        RequestContext.set_session_id(session_id)
        result = RequestContext.get_session_id()
        
        assert result == session_id
    
    def test_get_session_id_when_not_set(self):
        """Test get_session_id when not set"""
        RequestContext.clear()
        
        session_id = RequestContext.get_session_id()
        
        assert session_id is None
    
    def test_set_session_id_overwrites(self):
        """Test set_session_id overwrites existing"""
        RequestContext.clear()
        
        RequestContext.set_session_id("session1")
        RequestContext.set_session_id("session2")
        
        assert RequestContext.get_session_id() == "session2"


class TestRequestContextUserId:
    """Test RequestContext user ID management"""
    
    def test_set_user_id(self):
        """Test set_user_id"""
        RequestContext.clear()
        user_id = "test-user-789"
        
        RequestContext.set_user_id(user_id)
        result = RequestContext.get_user_id()
        
        assert result == user_id
    
    def test_get_user_id_when_not_set(self):
        """Test get_user_id when not set"""
        RequestContext.clear()
        
        user_id = RequestContext.get_user_id()
        
        assert user_id is None
    
    def test_set_user_id_overwrites(self):
        """Test set_user_id overwrites existing"""
        RequestContext.clear()
        
        RequestContext.set_user_id("user1")
        RequestContext.set_user_id("user2")
        
        assert RequestContext.get_user_id() == "user2"


class TestRequestContextStartTime:
    """Test RequestContext start time management"""
    
    def test_set_start_time_default(self):
        """Test set_start_time with default (current time)"""
        RequestContext.clear()
        
        RequestContext.set_start_time()
        start_time = RequestContext.get_start_time()
        
        assert start_time is not None
        assert isinstance(start_time, datetime)
        # Should be recent (within last second)
        now = datetime.now(UTC)
        assert abs((now - start_time).total_seconds()) < 1
    
    def test_set_start_time_explicit(self):
        """Test set_start_time with explicit time"""
        RequestContext.clear()
        test_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        
        RequestContext.set_start_time(test_time)
        start_time = RequestContext.get_start_time()
        
        assert start_time == test_time
    
    def test_get_start_time_when_not_set(self):
        """Test get_start_time when not set"""
        RequestContext.clear()
        
        start_time = RequestContext.get_start_time()
        
        assert start_time is None


class TestRequestContextData:
    """Test RequestContext context data management"""
    
    def test_set_context_data(self):
        """Test set_context_data"""
        RequestContext.clear()
        
        RequestContext.set_context_data("key1", "value1")
        RequestContext.set_context_data("key2", 123)
        RequestContext.set_context_data("key3", True)
        
        data = RequestContext.get_context_data()
        
        assert data["key1"] == "value1"
        assert data["key2"] == 123
        assert data["key3"] is True
    
    def test_get_context_data_when_empty(self):
        """Test get_context_data when empty"""
        RequestContext.clear()
        
        data = RequestContext.get_context_data()
        
        assert isinstance(data, dict)
        assert len(data) == 0
    
    def test_set_context_data_overwrites(self):
        """Test set_context_data overwrites existing"""
        RequestContext.clear()
        
        RequestContext.set_context_data("key", "value1")
        RequestContext.set_context_data("key", "value2")
        
        data = RequestContext.get_context_data()
        assert data["key"] == "value2"
    
    def test_set_context_data_multiple_keys(self):
        """Test set_context_data with multiple keys"""
        RequestContext.clear()
        
        RequestContext.set_context_data("key1", "value1")
        RequestContext.set_context_data("key2", "value2")
        RequestContext.set_context_data("key3", "value3")
        
        data = RequestContext.get_context_data()
        assert len(data) == 3
        assert data["key1"] == "value1"
        assert data["key2"] == "value2"
        assert data["key3"] == "value3"


class TestRequestContextClear:
    """Test RequestContext clear method"""
    
    def test_clear_removes_all_context(self):
        """Test clear removes all context"""
        RequestContext.set_request_id("test-request")
        RequestContext.set_session_id("test-session")
        RequestContext.set_user_id("test-user")
        RequestContext.set_start_time()
        RequestContext.set_context_data("key", "value")
        
        RequestContext.clear()
        
        assert RequestContext.get_request_id() is None
        assert RequestContext.get_session_id() is None
        assert RequestContext.get_user_id() is None
        assert RequestContext.get_start_time() is None
        assert len(RequestContext.get_context_data()) == 0


class TestRequestContextGetContextDict:
    """Test RequestContext get_context_dict method"""
    
    def test_get_context_dict_with_all_fields(self):
        """Test get_context_dict with all fields set"""
        RequestContext.clear()
        request_id = RequestContext.set_request_id("test-request")
        RequestContext.set_session_id("test-session")
        RequestContext.set_user_id("test-user")
        test_time = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        RequestContext.set_start_time(test_time)
        RequestContext.set_context_data("key", "value")
        
        context = RequestContext.get_context_dict()
        
        assert isinstance(context, dict)
        assert context["request_id"] == request_id
        assert context["session_id"] == "test-session"
        assert context["user_id"] == "test-user"
        assert context["start_time"] == test_time.isoformat()
        assert context["key"] == "value"
    
    def test_get_context_dict_with_partial_fields(self):
        """Test get_context_dict with partial fields"""
        RequestContext.clear()
        RequestContext.set_request_id("test-request")
        # Don't set other fields
        
        context = RequestContext.get_context_dict()
        
        assert context["request_id"] == "test-request"
        assert context["session_id"] is None
        assert context["user_id"] is None
        assert "start_time" not in context or context.get("start_time") is None
    
    def test_get_context_dict_when_empty(self):
        """Test get_context_dict when context is empty"""
        RequestContext.clear()
        
        context = RequestContext.get_context_dict()
        
        assert isinstance(context, dict)
        assert context["request_id"] is None
        assert context["session_id"] is None
        assert context["user_id"] is None


class TestGetRequestContext:
    """Test get_request_context convenience function"""
    
    def test_get_request_context_returns_dict(self):
        """Test that get_request_context returns a dictionary"""
        RequestContext.clear()
        
        context = get_request_context()
        
        assert isinstance(context, dict)
    
    def test_get_request_context_calls_get_context_dict(self):
        """Test that get_request_context calls get_context_dict"""
        RequestContext.clear()
        RequestContext.set_request_id("test-request")
        
        context = get_request_context()
        
        assert context["request_id"] == "test-request"


class TestRequestContextEdgeCases:
    """Test edge cases for request context"""
    
    def test_set_request_id_with_empty_string(self):
        """Test set_request_id with empty string"""
        RequestContext.clear()
        
        request_id = RequestContext.set_request_id("")
        assert request_id == ""
        assert RequestContext.get_request_id() == ""
    
    def test_set_session_id_with_empty_string(self):
        """Test set_session_id with empty string"""
        RequestContext.clear()
        
        RequestContext.set_session_id("")
        assert RequestContext.get_session_id() == ""
    
    def test_set_user_id_with_empty_string(self):
        """Test set_user_id with empty string"""
        RequestContext.clear()
        
        RequestContext.set_user_id("")
        assert RequestContext.get_user_id() == ""
    
    def test_set_context_data_with_none_value(self):
        """Test set_context_data with None value"""
        RequestContext.clear()
        
        RequestContext.set_context_data("key", None)
        data = RequestContext.get_context_data()
        
        assert data["key"] is None
    
    def test_set_context_data_with_dict_value(self):
        """Test set_context_data with dict value"""
        RequestContext.clear()
        
        RequestContext.set_context_data("key", {"nested": "value"})
        data = RequestContext.get_context_data()
        
        assert data["key"] == {"nested": "value"}
    
    def test_set_context_data_with_list_value(self):
        """Test set_context_data with list value"""
        RequestContext.clear()
        
        RequestContext.set_context_data("key", [1, 2, 3])
        data = RequestContext.get_context_data()
        
        assert data["key"] == [1, 2, 3]
    
    def test_get_context_dict_with_unicode_values(self):
        """Test get_context_dict with unicode values"""
        RequestContext.clear()
        RequestContext.set_request_id("тест-123")
        RequestContext.set_session_id("сессия-456")
        RequestContext.set_user_id("пользователь-789")
        
        context = RequestContext.get_context_dict()
        
        assert context["request_id"] == "тест-123"
        assert context["session_id"] == "сессия-456"
        assert context["user_id"] == "пользователь-789"
    
    def test_context_isolation_after_clear(self):
        """Test that context is isolated after clear"""
        RequestContext.set_request_id("request1")
        RequestContext.set_session_id("session1")
        
        RequestContext.clear()
        
        assert RequestContext.get_request_id() is None
        assert RequestContext.get_session_id() is None
    
    def test_multiple_context_data_keys(self):
        """Test setting many context data keys"""
        RequestContext.clear()
        
        for i in range(100):
            RequestContext.set_context_data(f"key_{i}", f"value_{i}")
        
        data = RequestContext.get_context_data()
        assert len(data) == 100
        assert data["key_0"] == "value_0"
        assert data["key_99"] == "value_99"
    
    def test_get_context_dict_start_time_format(self):
        """Test that get_context_dict formats start_time as ISO string"""
        RequestContext.clear()
        test_time = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        RequestContext.set_start_time(test_time)
        
        context = RequestContext.get_context_dict()
        
        assert context["start_time"] == test_time.isoformat()
        assert isinstance(context["start_time"], str)

