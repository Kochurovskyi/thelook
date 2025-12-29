"""Main logging setup and utilities"""
import logging
from typing import Optional, Dict, Any
import structlog

from utils.logging_config import setup_logging_config, get_logger
from utils.request_context import RequestContext, get_request_context

# Initialize logging on import
_initialized = False


def initialize_logging(
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_file: Optional[str] = None
):
    """
    Initialize the logging system
    
    This should be called once at application startup
    
    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format type ('json' or 'console')
        log_file: Path to log file
    """
    global _initialized
    if _initialized:
        return
    
    setup_logging_config(
        log_level=log_level,
        log_format=log_format,
        log_file=log_file
    )
    _initialized = True


def get_logger_for_module(module_name: str) -> structlog.BoundLogger:
    """
    Get a logger for a module with automatic context binding
    
    Args:
        module_name: Module name (typically __name__)
        
    Returns:
        Configured logger with context
    """
    if not _initialized:
        initialize_logging()
    
    logger = get_logger(module_name)
    
    # Bind request context automatically
    context = get_request_context()
    if context:
        logger = logger.bind(**context)
    
    return logger


class ComponentLogger:
    """Logger wrapper for components with automatic context binding"""
    
    def __init__(self, component: str):
        """
        Initialize component logger
        
        Args:
            component: Component name (e.g., 'bigquery_service')
        """
        self.component = component
        if not _initialized:
            initialize_logging()
        self._logger = get_logger_for_module(component)
    
    def _bind_component(self, **kwargs) -> structlog.BoundLogger:
        """Bind component name and additional context"""
        return self._logger.bind(component=self.component, **kwargs)
    
    def debug(self, message: str, **kwargs):
        """Log debug message"""
        self._bind_component(**kwargs).debug(message)
    
    def info(self, message: str, **kwargs):
        """Log info message"""
        self._bind_component(**kwargs).info(message)
    
    def warning(self, message: str, **kwargs):
        """Log warning message"""
        self._bind_component(**kwargs).warning(message)
    
    def error(self, message: str, **kwargs):
        """Log error message"""
        self._bind_component(**kwargs).error(message)
    
    def exception(self, message: str, exc_info=True, **kwargs):
        """Log exception with traceback"""
        self._bind_component(**kwargs).exception(message, exc_info=exc_info)
    
    def critical(self, message: str, **kwargs):
        """Log critical message"""
        self._bind_component(**kwargs).critical(message)


# Initialize logging on module import
initialize_logging()

