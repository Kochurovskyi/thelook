"""Request context management for distributed tracing"""
import contextvars
import uuid
from typing import Optional, Dict, Any
from datetime import datetime, UTC

# Context variables for request tracking
_request_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('request_id', default=None)
_session_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('session_id', default=None)
_user_id: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar('user_id', default=None)
_start_time: contextvars.ContextVar[Optional[datetime]] = contextvars.ContextVar('start_time', default=None)
_context_data: contextvars.ContextVar[Optional[Dict[str, Any]]] = contextvars.ContextVar('context_data', default=None)


class RequestContext:
    """Manages request context for distributed tracing"""
    
    @staticmethod
    def generate_request_id() -> str:
        """Generate a unique request ID"""
        return str(uuid.uuid4())
    
    @staticmethod
    def set_request_id(request_id: Optional[str] = None) -> str:
        """
        Set request ID in context
        
        Args:
            request_id: Optional request ID. If None, generates a new one
            
        Returns:
            The request ID (newly generated or provided)
        """
        if request_id is None:
            request_id = RequestContext.generate_request_id()
        _request_id.set(request_id)
        return request_id
    
    @staticmethod
    def get_request_id() -> Optional[str]:
        """Get current request ID from context"""
        return _request_id.get()
    
    @staticmethod
    def set_session_id(session_id: str):
        """Set session ID in context"""
        _session_id.set(session_id)
    
    @staticmethod
    def get_session_id() -> Optional[str]:
        """Get current session ID from context"""
        return _session_id.get()
    
    @staticmethod
    def set_user_id(user_id: str):
        """Set user ID in context"""
        _user_id.set(user_id)
    
    @staticmethod
    def get_user_id() -> Optional[str]:
        """Get current user ID from context"""
        return _user_id.get()
    
    @staticmethod
    def set_start_time(start_time: Optional[datetime] = None):
        """Set request start time"""
        if start_time is None:
            start_time = datetime.now(UTC)
        _start_time.set(start_time)
    
    @staticmethod
    def get_start_time() -> Optional[datetime]:
        """Get request start time"""
        return _start_time.get()
    
    @staticmethod
    def set_context_data(key: str, value: Any):
        """Set additional context data"""
        data = _context_data.get() or {}
        data[key] = value
        _context_data.set(data)
    
    @staticmethod
    def get_context_data() -> Dict[str, Any]:
        """Get all context data"""
        return _context_data.get() or {}
    
    @staticmethod
    def clear():
        """Clear all context variables"""
        _request_id.set(None)
        _session_id.set(None)
        _user_id.set(None)
        _start_time.set(None)
        _context_data.set(None)
    
    @staticmethod
    def get_context_dict() -> Dict[str, Any]:
        """
        Get all context as a dictionary for logging
        
        Returns:
            Dictionary with all context information
        """
        context = {
            "request_id": RequestContext.get_request_id(),
            "session_id": RequestContext.get_session_id(),
            "user_id": RequestContext.get_user_id(),
        }
        
        start_time = RequestContext.get_start_time()
        if start_time:
            context["start_time"] = start_time.isoformat()
        
        context_data = RequestContext.get_context_data()
        if context_data:
            context.update(context_data)
        
        return context


def get_request_context() -> Dict[str, Any]:
    """Convenience function to get request context"""
    return RequestContext.get_context_dict()

