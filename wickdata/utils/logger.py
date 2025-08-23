"""
Logging infrastructure for WickData
"""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, Optional


class Logger:
    """Logger wrapper for WickData"""

    def __init__(self, name: str, level: str = "INFO") -> None:
        """
        Initialize logger

        Args:
            name: Logger name
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, level.upper()))

        # Remove existing handlers
        self.logger.handlers = []

        # Create console handler with formatting
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(getattr(logging, level.upper()))

        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

        # Add handler to logger
        self.logger.addHandler(handler)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message"""
        extra_info = self._format_kwargs(kwargs)
        if extra_info:
            message = f"{message} | {extra_info}"
        self.logger.debug(message)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message"""
        extra_info = self._format_kwargs(kwargs)
        if extra_info:
            message = f"{message} | {extra_info}"
        self.logger.info(message)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message"""
        extra_info = self._format_kwargs(kwargs)
        if extra_info:
            message = f"{message} | {extra_info}"
        self.logger.warning(message)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message"""
        extra_info = self._format_kwargs(kwargs)
        if extra_info:
            message = f"{message} | {extra_info}"
        self.logger.error(message)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message"""
        extra_info = self._format_kwargs(kwargs)
        if extra_info:
            message = f"{message} | {extra_info}"
        self.logger.critical(message)

    def set_level(self, level: str) -> None:
        """
        Set log level

        Args:
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        """
        log_level = getattr(logging, level.upper())
        self.logger.setLevel(log_level)
        for handler in self.logger.handlers:
            handler.setLevel(log_level)

    def _format_kwargs(self, kwargs: Dict[str, Any]) -> str:
        """Format kwargs for logging"""
        if not kwargs:
            return ""

        parts = []
        for key, value in kwargs.items():
            if isinstance(value, datetime):
                value = value.isoformat()
            elif isinstance(value, (list, dict)) and len(str(value)) > 100:
                value = f"{type(value).__name__}[{len(value)}]"
            parts.append(f"{key}={value}")

        return " ".join(parts)

    @classmethod
    def create_logger(cls, name: str, level: Optional[str] = None) -> "Logger":
        """
        Create a logger instance

        Args:
            name: Logger name
            level: Log level (optional)

        Returns:
            Logger instance
        """
        if level is None:
            level = "INFO"
        return cls(name, level)
