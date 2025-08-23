"""
Custom error classes for WickData
"""

from typing import Any, Dict, Optional


class WickDataError(Exception):
    """Base error class for WickData"""

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "WICKDATA_ERROR"
        self.details = details or {}

    def __str__(self) -> str:
        if self.details:
            return f"{self.error_code}: {self.message} - {self.details}"
        return f"{self.error_code}: {self.message}"


class ExchangeError(WickDataError):
    """Exchange-specific errors"""

    def __init__(
        self,
        message: str,
        exchange: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, "EXCHANGE_ERROR", details)
        self.exchange = exchange
        if exchange and self.details is not None:
            self.details["exchange"] = exchange


class ValidationError(WickDataError):
    """Input validation errors"""

    def __init__(
        self,
        message: str,
        field: Optional[str] = None,
        value: Optional[Any] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, "VALIDATION_ERROR", details)
        self.field = field
        self.value = value
        if field and self.details is not None:
            self.details["field"] = field
            if value is not None:
                self.details["value"] = value


class RateLimitError(WickDataError):
    """Rate limiting errors"""

    def __init__(
        self,
        message: str,
        retry_after: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, "RATE_LIMIT_ERROR", details)
        self.retry_after = retry_after
        if retry_after and self.details is not None:
            self.details["retry_after"] = retry_after


class NetworkError(WickDataError):
    """Network connectivity errors"""

    def __init__(
        self,
        message: str,
        url: Optional[str] = None,
        status_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, "NETWORK_ERROR", details)
        self.url = url
        self.status_code = status_code
        if self.details is not None:
            if url:
                self.details["url"] = url
            if status_code:
                self.details["status_code"] = status_code


class DatabaseError(WickDataError):
    """Database operation errors"""

    def __init__(
        self,
        message: str,
        operation: Optional[str] = None,
        table: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, "DATABASE_ERROR", details)
        self.operation = operation
        self.table = table
        if self.details is not None:
            if operation:
                self.details["operation"] = operation
            if table:
                self.details["table"] = table


class ConfigurationError(WickDataError):
    """Configuration errors"""

    def __init__(
        self,
        message: str,
        config_key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, "CONFIGURATION_ERROR", details)
        self.config_key = config_key
        if config_key and self.details is not None:
            self.details["config_key"] = config_key


class DataGapError(WickDataError):
    """Data gap related errors"""

    def __init__(
        self,
        message: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message, "DATA_GAP_ERROR", details)
        self.start_time = start_time
        self.end_time = end_time
        if self.details is not None:
            if start_time:
                self.details["start_time"] = start_time
            if end_time:
                self.details["end_time"] = end_time
