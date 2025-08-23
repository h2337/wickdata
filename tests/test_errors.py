"""
Unit tests for error handling and custom exceptions
"""

from wickdata.core.errors import (
    ConfigurationError,
    DatabaseError,
    DataGapError,
    ExchangeError,
    NetworkError,
    RateLimitError,
    ValidationError,
    WickDataError,
)


class TestWickDataError:
    """Test base error class"""

    def test_basic_error(self):
        """Test basic error creation"""
        error = WickDataError("Something went wrong")
        assert str(error).endswith("Something went wrong")
        assert error.message == "Something went wrong"
        assert error.error_code == "WICKDATA_ERROR"
        assert error.details == {}

    def test_error_with_code_and_details(self):
        """Test error with custom code and details"""
        error = WickDataError("Custom error", error_code="CUSTOM_CODE", details={"key": "value"})
        assert error.error_code == "CUSTOM_CODE"
        assert error.details == {"key": "value"}
        assert "CUSTOM_CODE" in str(error)
        assert "Custom error" in str(error)


class TestExchangeError:
    """Test exchange-specific error"""

    def test_exchange_error(self):
        """Test exchange error creation"""
        error = ExchangeError("API failed", exchange="binance")
        assert error.message == "API failed"
        assert error.error_code == "EXCHANGE_ERROR"
        assert error.exchange == "binance"
        assert error.details["exchange"] == "binance"

    def test_exchange_error_without_exchange(self):
        """Test exchange error without exchange name"""
        error = ExchangeError("API failed")
        assert error.exchange is None
        assert "exchange" not in error.details


class TestValidationError:
    """Test validation error"""

    def test_validation_error_basic(self):
        """Test basic validation error"""
        error = ValidationError("Invalid input")
        assert error.message == "Invalid input"
        assert error.error_code == "VALIDATION_ERROR"

    def test_validation_error_with_field_and_value(self):
        """Test validation error with field and value"""
        error = ValidationError("Invalid symbol", field="symbol", value="INVALID")
        assert error.field == "symbol"
        assert error.value == "INVALID"
        assert error.details["field"] == "symbol"
        assert error.details["value"] == "INVALID"

    def test_validation_error_none_value(self):
        """Test validation error with None value"""
        error = ValidationError("Required field", field="name", value=None)
        assert error.field == "name"
        assert error.value is None
        assert "value" not in error.details  # None values not added to details


class TestRateLimitError:
    """Test rate limit error"""

    def test_rate_limit_error(self):
        """Test rate limit error creation"""
        error = RateLimitError("Too many requests")
        assert error.message == "Too many requests"
        assert error.error_code == "RATE_LIMIT_ERROR"
        assert error.retry_after is None

    def test_rate_limit_error_with_retry_after(self):
        """Test rate limit error with retry_after"""
        error = RateLimitError("Rate limited", retry_after=5.5)
        assert error.retry_after == 5.5
        assert error.details["retry_after"] == 5.5


class TestNetworkError:
    """Test network error"""

    def test_network_error_basic(self):
        """Test basic network error"""
        error = NetworkError("Connection failed")
        assert error.message == "Connection failed"
        assert error.error_code == "NETWORK_ERROR"

    def test_network_error_with_url_and_status(self):
        """Test network error with URL and status code"""
        error = NetworkError("Request failed", url="https://api.example.com", status_code=404)
        assert error.url == "https://api.example.com"
        assert error.status_code == 404
        assert error.details["url"] == "https://api.example.com"
        assert error.details["status_code"] == 404


class TestDatabaseError:
    """Test database error"""

    def test_database_error_basic(self):
        """Test basic database error"""
        error = DatabaseError("Query failed")
        assert error.message == "Query failed"
        assert error.error_code == "DATABASE_ERROR"

    def test_database_error_with_operation_and_table(self):
        """Test database error with operation and table"""
        error = DatabaseError("Insert failed", operation="insert", table="candles")
        assert error.operation == "insert"
        assert error.table == "candles"
        assert error.details["operation"] == "insert"
        assert error.details["table"] == "candles"


class TestConfigurationError:
    """Test configuration error"""

    def test_configuration_error_basic(self):
        """Test basic configuration error"""
        error = ConfigurationError("Invalid config")
        assert error.message == "Invalid config"
        assert error.error_code == "CONFIGURATION_ERROR"

    def test_configuration_error_with_key(self):
        """Test configuration error with config key"""
        error = ConfigurationError("Missing API key", config_key="exchanges.binance.api_key")
        assert error.config_key == "exchanges.binance.api_key"
        assert error.details["config_key"] == "exchanges.binance.api_key"


class TestDataGapError:
    """Test data gap error"""

    def test_data_gap_error_basic(self):
        """Test basic data gap error"""
        error = DataGapError("Gap detected")
        assert error.message == "Gap detected"
        assert error.error_code == "DATA_GAP_ERROR"

    def test_data_gap_error_with_timestamps(self):
        """Test data gap error with timestamps"""
        error = DataGapError("Large gap found", start_time=1704067200000, end_time=1704070800000)
        assert error.start_time == 1704067200000
        assert error.end_time == 1704070800000
        assert error.details["start_time"] == 1704067200000
        assert error.details["end_time"] == 1704070800000


class TestErrorInheritance:
    """Test error inheritance and isinstance checks"""

    def test_all_errors_inherit_from_base(self):
        """Test that all custom errors inherit from WickDataError"""
        errors = [
            ExchangeError("test"),
            ValidationError("test"),
            RateLimitError("test"),
            NetworkError("test"),
            DatabaseError("test"),
            ConfigurationError("test"),
            DataGapError("test"),
        ]

        for error in errors:
            assert isinstance(error, WickDataError)
            assert isinstance(error, Exception)
