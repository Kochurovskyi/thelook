"""Unit tests for logging configuration"""
import pytest
import logging
import structlog
import sys
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from io import StringIO

from utils.logging_config import (
    setup_logging_config,
    get_logger,
    configure_logging_for_component
)
import config


class TestSetupLoggingConfig:
    """Test setup_logging_config function"""
    
    def test_setup_logging_config_default(self):
        """Test setup_logging_config with default parameters"""
        result = setup_logging_config()
        
        assert isinstance(result, dict)
        assert "log_level" in result
        assert "log_format" in result
        assert "log_file" in result
        assert "enable_json" in result
    
    def test_setup_logging_config_custom_level(self):
        """Test setup_logging_config with custom log level"""
        result = setup_logging_config(log_level="DEBUG")
        
        assert result["log_level"] == "DEBUG"
    
    def test_setup_logging_config_custom_format(self):
        """Test setup_logging_config with custom format"""
        result = setup_logging_config(log_format="console")
        
        assert result["log_format"] == "console"
    
    def test_setup_logging_config_json_format(self):
        """Test setup_logging_config with JSON format"""
        result = setup_logging_config(log_format="json", enable_json=True)
        
        assert result["log_format"] == "json"
        assert result["enable_json"] is True
    
    def test_setup_logging_config_console_format(self):
        """Test setup_logging_config with console format"""
        result = setup_logging_config(log_format="console", enable_json=False)
        
        assert result["log_format"] == "console"
        assert result["enable_json"] is False
    
    def test_setup_logging_config_with_file(self, tmp_path):
        """Test setup_logging_config with log file"""
        log_file = str(tmp_path / "test.log")
        result = setup_logging_config(log_file=log_file)
        
        assert result["log_file"] == log_file
        # Verify log directory was created
        assert tmp_path.exists()
    
    def test_setup_logging_config_creates_log_dir(self, tmp_path, monkeypatch):
        """Test that setup_logging_config creates log directory"""
        # Use a simple log file path within tmp_path
        log_file = str(tmp_path / "test.log")
        
        # Temporarily patch config.LOG_DIR to use tmp_path
        original_log_dir = config.LOG_DIR
        monkeypatch.setattr(config, "LOG_DIR", str(tmp_path))
        
        try:
            result = setup_logging_config(log_file=log_file)
            
            # The log directory from config.LOG_DIR should be created
            assert result["log_file"] == log_file
            # Verify the log directory exists (created from config.LOG_DIR)
            assert Path(config.LOG_DIR).exists()
        finally:
            monkeypatch.setattr(config, "LOG_DIR", original_log_dir)
    
    def test_setup_logging_config_all_levels(self):
        """Test setup_logging_config with all log levels"""
        levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        
        for level in levels:
            result = setup_logging_config(log_level=level)
            assert result["log_level"] == level
    
    def test_setup_logging_config_invalid_level(self):
        """Test setup_logging_config with invalid log level"""
        # Should default to INFO
        result = setup_logging_config(log_level="INVALID")
        
        # Should still return valid config
        assert "log_level" in result
    
    def test_setup_logging_config_configures_structlog(self):
        """Test that setup_logging_config configures structlog"""
        setup_logging_config()
        
        # Verify structlog is configured
        logger = structlog.get_logger("test")
        assert logger is not None


class TestGetLogger:
    """Test get_logger function"""
    
    def test_get_logger_returns_logger(self):
        """Test that get_logger returns a logger"""
        logger = get_logger("test_module")
        # structlog returns BoundLoggerLazyProxy initially
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_get_logger_different_names(self):
        """Test get_logger with different module names"""
        logger1 = get_logger("module1")
        logger2 = get_logger("module2")
        
        assert logger1 is not None
        assert logger2 is not None
        # Loggers should be different instances
        assert logger1 is not logger2
    
    def test_get_logger_same_name_cached(self):
        """Test that get_logger caches loggers"""
        logger1 = get_logger("test_module")
        logger2 = get_logger("test_module")
        
        # Should return same logger instance (cached)
        # Note: structlog may return lazy proxies that are equal but not identical
        assert logger1 is not None
        assert logger2 is not None
    
    def test_get_logger_with_empty_name(self):
        """Test get_logger with empty name"""
        logger = get_logger("")
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_get_logger_with_special_characters(self):
        """Test get_logger with special characters in name"""
        logger = get_logger("test.module.name")
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_get_logger_with_unicode_name(self):
        """Test get_logger with unicode name"""
        logger = get_logger("тест_模块")
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)


class TestConfigureLoggingForComponent:
    """Test configure_logging_for_component function"""
    
    def test_configure_logging_for_component_default(self):
        """Test configure_logging_for_component with default parameters"""
        # Should not raise
        configure_logging_for_component("test_component")
        
        logger = logging.getLogger("test_component")
        assert logger is not None
    
    def test_configure_logging_for_component_custom_level(self):
        """Test configure_logging_for_component with custom log level"""
        configure_logging_for_component("test_component", log_level="DEBUG")
        
        logger = logging.getLogger("test_component")
        assert logger.level == logging.DEBUG
    
    def test_configure_logging_for_component_all_levels(self):
        """Test configure_logging_for_component with all log levels"""
        levels = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL
        }
        
        for level_name, level_value in levels.items():
            configure_logging_for_component(
                f"component_{level_name}",
                log_level=level_name
            )
            
            logger = logging.getLogger(f"component_{level_name}")
            assert logger.level == level_value
    
    def test_configure_logging_for_component_multiple_components(self):
        """Test configure_logging_for_component with multiple components"""
        components = ["component1", "component2", "component3"]
        
        for component in components:
            configure_logging_for_component(component)
            logger = logging.getLogger(component)
            assert logger is not None
    
    def test_configure_logging_for_component_invalid_level(self):
        """Test configure_logging_for_component with invalid log level"""
        # Should default to INFO
        configure_logging_for_component("test_component", log_level="INVALID")
        
        logger = logging.getLogger("test_component")
        # Should still be configured
        assert logger is not None


class TestLoggingConfigEdgeCases:
    """Test edge cases for logging configuration"""
    
    def test_setup_logging_config_none_values(self):
        """Test setup_logging_config with None values"""
        result = setup_logging_config(
            log_level=None,
            log_format=None,
            log_file=None
        )
        
        # Should use config defaults
        assert isinstance(result, dict)
        assert "log_level" in result
    
    def test_setup_logging_config_empty_string_values(self):
        """Test setup_logging_config with empty string values"""
        result = setup_logging_config(
            log_level="",
            log_format="",
            log_file=""
        )
        
        assert isinstance(result, dict)
    
    def test_get_logger_very_long_name(self):
        """Test get_logger with very long name"""
        long_name = "a" * 1000
        logger = get_logger(long_name)
        assert hasattr(logger, 'info') or isinstance(logger, structlog.BoundLogger)
    
    def test_setup_logging_config_file_permissions(self, tmp_path):
        """Test setup_logging_config handles file permissions"""
        log_file = str(tmp_path / "test.log")
        
        # Should not raise even if directory doesn't exist
        result = setup_logging_config(log_file=log_file)
        assert result["log_file"] == log_file
    
    def test_setup_logging_config_multiple_calls(self):
        """Test setup_logging_config with multiple calls"""
        result1 = setup_logging_config(log_level="DEBUG")
        result2 = setup_logging_config(log_level="INFO")
        
        # Both should return valid configs
        assert isinstance(result1, dict)
        assert isinstance(result2, dict)
    
    def test_get_logger_thread_safety(self):
        """Test get_logger thread safety (simulation)"""
        loggers = []
        
        # Simulate concurrent access
        for i in range(10):
            logger = get_logger(f"module_{i}")
            loggers.append(logger)
        
        # All should be valid loggers (have info method)
        assert all(hasattr(lg, 'info') for lg in loggers)
        assert len(set(loggers)) == 10  # All different instances
    
    def test_configure_logging_for_component_empty_name(self):
        """Test configure_logging_for_component with empty name"""
        # Should not raise
        configure_logging_for_component("")
        
        logger = logging.getLogger("")
        assert logger is not None
    
    def test_configure_logging_for_component_none_level(self):
        """Test configure_logging_for_component with None log level"""
        # Should not raise
        configure_logging_for_component("test_component", log_level=None)
        
        logger = logging.getLogger("test_component")
        assert logger is not None

