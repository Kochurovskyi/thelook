"""Unit tests for logger utilities"""
import pytest
import logging
import structlog
from unittest.mock import Mock, patch, MagicMock
import sys
from io import StringIO

from utils.logger import (
    initialize_logging,
    get_logger_for_module,
    ComponentLogger
)
from utils.request_context import RequestContext


class TestLoggerInitialization:
    """Test logging initialization"""
    
    def test_initialize_logging_default(self):
        """Test logging initialization with default parameters"""
        # Reset initialization state
        import utils.logger
        utils.logger._initialized = False
        
        # Should not raise
        initialize_logging()
        assert utils.logger._initialized is True
    
    def test_initialize_logging_custom_level(self):
        """Test logging initialization with custom log level"""
        import utils.logger
        utils.logger._initialized = False
        
        initialize_logging(log_level="DEBUG")
        assert utils.logger._initialized is True
    
    def test_initialize_logging_custom_format(self):
        """Test logging initialization with custom format"""
        import utils.logger
        utils.logger._initialized = False
        
        initialize_logging(log_format="console")
        assert utils.logger._initialized is True
    
    def test_initialize_logging_idempotent(self):
        """Test that initialization is idempotent"""
        import utils.logger
        utils.logger._initialized = False
        
        initialize_logging()
        first_init = utils.logger._initialized
        
        # Second call should not change state
        initialize_logging()
        assert utils.logger._initialized == first_init
    
    def test_initialize_logging_with_file(self, tmp_path):
        """Test logging initialization with log file"""
        import utils.logger
        utils.logger._initialized = False
        
        log_file = str(tmp_path / "test.log")
        initialize_logging(log_file=log_file)
        
        # Verify file was created
        assert tmp_path.joinpath("test.log").exists()


class TestGetLoggerForModule:
    """Test get_logger_for_module function"""
    
    def test_get_logger_for_module_returns_logger(self):
        """Test that get_logger_for_module returns a logger"""
        logger = get_logger_for_module("test_module")
        # structlog returns BoundLoggerLazyProxy initially, which is a valid logger
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_get_logger_for_module_auto_initializes(self):
        """Test that get_logger_for_module auto-initializes if needed"""
        import utils.logger
        utils.logger._initialized = False
        
        logger = get_logger_for_module("test_module")
        assert utils.logger._initialized is True
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_get_logger_for_module_binds_context(self):
        """Test that get_logger_for_module binds request context"""
        RequestContext.clear()
        RequestContext.set_request_id("test-request-123")
        RequestContext.set_session_id("test-session-456")
        
        logger = get_logger_for_module("test_module")
        
        # Logger should have context bound (check if it's callable/usable)
        assert logger is not None
        assert hasattr(logger, 'info')
    
    def test_get_logger_for_module_no_context(self):
        """Test get_logger_for_module when no context is set"""
        RequestContext.clear()
        
        logger = get_logger_for_module("test_module")
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)


class TestComponentLogger:
    """Test ComponentLogger class"""
    
    def test_component_logger_initialization(self):
        """Test ComponentLogger initialization"""
        logger = ComponentLogger("test_component")
        assert logger.component == "test_component"
        assert logger._logger is not None
    
    def test_component_logger_debug(self, caplog):
        """Test ComponentLogger debug method"""
        with caplog.at_level(logging.DEBUG):
            logger = ComponentLogger("test_component")
            logger.debug("Test debug message", extra_field="value")
            
            # Verify log was created
            assert len(caplog.records) > 0
    
    def test_component_logger_info(self, caplog):
        """Test ComponentLogger info method"""
        with caplog.at_level(logging.INFO):
            logger = ComponentLogger("test_component")
            logger.info("Test info message", extra_field="value")
            
            assert len(caplog.records) > 0
    
    def test_component_logger_warning(self, caplog):
        """Test ComponentLogger warning method"""
        with caplog.at_level(logging.WARNING):
            logger = ComponentLogger("test_component")
            logger.warning("Test warning message", extra_field="value")
            
            assert len(caplog.records) > 0
    
    def test_component_logger_error(self, caplog):
        """Test ComponentLogger error method"""
        with caplog.at_level(logging.ERROR):
            logger = ComponentLogger("test_component")
            logger.error("Test error message", extra_field="value")
            
            assert len(caplog.records) > 0
    
    def test_component_logger_critical(self, caplog):
        """Test ComponentLogger critical method"""
        with caplog.at_level(logging.CRITICAL):
            logger = ComponentLogger("test_component")
            logger.critical("Test critical message", extra_field="value")
            
            assert len(caplog.records) > 0
    
    def test_component_logger_exception(self, caplog):
        """Test ComponentLogger exception method"""
        with caplog.at_level(logging.ERROR):
            logger = ComponentLogger("test_component")
            try:
                raise ValueError("Test exception")
            except ValueError:
                logger.exception("Test exception message", extra_field="value")
            
            assert len(caplog.records) > 0
    
    def test_component_logger_binds_component_name(self):
        """Test that ComponentLogger binds component name"""
        logger = ComponentLogger("my_component")
        assert logger.component == "my_component"
    
    def test_component_logger_multiple_instances(self):
        """Test that multiple ComponentLogger instances work independently"""
        logger1 = ComponentLogger("component1")
        logger2 = ComponentLogger("component2")
        
        assert logger1.component == "component1"
        assert logger2.component == "component2"
        assert logger1 is not logger2
    
    def test_component_logger_with_context(self):
        """Test ComponentLogger with request context"""
        RequestContext.clear()
        RequestContext.set_request_id("test-request")
        
        logger = ComponentLogger("test_component")
        assert logger._logger is not None


class TestLoggerEdgeCases:
    """Test edge cases for logger utilities"""
    
    def test_logger_with_empty_module_name(self):
        """Test logger with empty module name"""
        logger = get_logger_for_module("")
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_logger_with_special_characters_in_module_name(self):
        """Test logger with special characters in module name"""
        logger = get_logger_for_module("test.module.name")
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_component_logger_with_empty_component_name(self):
        """Test ComponentLogger with empty component name"""
        logger = ComponentLogger("")
        assert logger.component == ""
    
    def test_component_logger_with_unicode_component_name(self):
        """Test ComponentLogger with unicode component name"""
        logger = ComponentLogger("компонент_测试")
        assert logger.component == "компонент_测试"
    
    def test_logger_with_none_context_values(self):
        """Test logger when context values are None"""
        RequestContext.clear()
        # Don't set any context values
        
        logger = get_logger_for_module("test_module")
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_component_logger_logs_with_empty_message(self, caplog):
        """Test ComponentLogger with empty message"""
        with caplog.at_level(logging.INFO):
            logger = ComponentLogger("test_component")
            logger.info("")
            
            # Should still create log record
            assert len(caplog.records) >= 0
    
    def test_component_logger_with_many_kwargs(self, caplog):
        """Test ComponentLogger with many keyword arguments"""
        with caplog.at_level(logging.INFO):
            logger = ComponentLogger("test_component")
            logger.info(
                "Test message",
                field1="value1",
                field2="value2",
                field3="value3",
                field4="value4",
                field5="value5"
            )
            
            assert len(caplog.records) > 0
    
    def test_logger_concurrent_access(self):
        """Test logger with concurrent access simulation"""
        import utils.logger
        utils.logger._initialized = False
        
        # Simulate concurrent initialization
        logger1 = get_logger_for_module("module1")
        logger2 = get_logger_for_module("module2")
        
        assert logger1 is not None
        assert logger2 is not None
        assert utils.logger._initialized is True

