"""Logging configuration management"""
import os
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
import structlog
from structlog.stdlib import LoggerFactory

import config


def setup_logging_config(
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_file: Optional[str] = None,
    enable_json: bool = True
) -> Dict[str, Any]:
    """
    Configure logging system
    
    Args:
        log_level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Format type ('json' or 'console')
        log_file: Path to log file
        enable_json: Whether to use JSON format
        
    Returns:
        Dictionary with configuration
    """
    log_level = log_level or config.LOG_LEVEL
    log_format = log_format or config.LOG_FORMAT
    log_file = log_file or config.LOG_FILE
    
    # Create log directory if it doesn't exist
    log_dir = Path(config.LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Configure standard library logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )
    
    # Configure structlog processors
    processors = [
        structlog.contextvars.merge_contextvars,  # Merge context variables
        structlog.stdlib.add_log_level,  # Add log level
        structlog.stdlib.add_logger_name,  # Add logger name
        structlog.processors.TimeStamper(fmt="iso"),  # Add ISO timestamp
        structlog.processors.StackInfoRenderer(),  # Add stack info
        structlog.processors.format_exc_info,  # Format exceptions
    ]
    
    # Add JSON or console renderer
    if enable_json or log_format == "json":
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(
            structlog.dev.ConsoleRenderer(colors=True)
        )
    
    # Configure structlog
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )
    
    # Set up file handler if log file is specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(getattr(logging, log_level.upper(), logging.INFO))
        file_formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(file_formatter)
        
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
    
    return {
        "log_level": log_level,
        "log_format": log_format,
        "log_file": log_file,
        "enable_json": enable_json
    }


def get_logger(name: str) -> structlog.BoundLogger:
    """
    Get a configured logger instance
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured structlog logger
    """
    return structlog.get_logger(name)


def configure_logging_for_component(component: str, log_level: Optional[str] = None):
    """
    Configure logging for a specific component
    
    Args:
        component: Component name (e.g., 'bigquery_service')
        log_level: Optional log level override
    """
    logger = logging.getLogger(component)
    if log_level:
        logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

