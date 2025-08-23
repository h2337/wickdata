"""
Unit tests for DataValidationService
"""

from unittest.mock import Mock

import pytest

from wickdata.models.candle import Candle
from wickdata.models.data_request import DataRequest
from wickdata.models.timeframe import Timeframe
from wickdata.services.data_validation_service import DataValidationService
from wickdata.utils.logger import Logger


@pytest.fixture
def mock_logger():
    """Create mock logger"""
    return Mock(spec=Logger)


@pytest.fixture
def validation_service(mock_logger):
    """Create DataValidationService instance"""
    return DataValidationService(logger=mock_logger)


@pytest.fixture
def valid_candles():
    """Create valid candles for testing"""
    return [
        Candle(
            timestamp=1609459200000, open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0
        ),
        Candle(
            timestamp=1609459260000, open=105.0, high=115.0, low=95.0, close=110.0, volume=1100.0
        ),
        Candle(
            timestamp=1609459320000, open=110.0, high=120.0, low=100.0, close=115.0, volume=1200.0
        ),
    ]


@pytest.fixture
def invalid_candles():
    """Create candles with various validation issues"""
    return [
        Candle(
            timestamp=-1, open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0
        ),  # Invalid timestamp
        Candle(
            timestamp=1609459260000, open=-10.0, high=110.0, low=90.0, close=105.0, volume=1100.0
        ),  # Negative price
        Candle(
            timestamp=1609459320000, open=110.0, high=100.0, low=120.0, close=115.0, volume=-100.0
        ),  # High < Low, negative volume
    ]


class TestDataValidationService:

    def test_validate_candles_valid(self, validation_service, valid_candles):
        """Test validation of valid candles"""
        result = validation_service.validate_candles(valid_candles)

        assert result.is_valid
        assert len(result.errors) == 0
        assert len(result.warnings) == 0
        assert len(result.invalid_candles) == 0

    def test_validate_candles_empty(self, validation_service):
        """Test validation of empty candle list"""
        result = validation_service.validate_candles([])

        assert result.is_valid
        assert len(result.errors) == 0
        assert len(result.warnings) == 1
        assert "Empty candle list" in result.warnings[0]

    def test_validate_candles_invalid_timestamp(self, validation_service):
        """Test validation catches invalid timestamps"""
        candles = [
            Candle(timestamp=0, open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0),
            Candle(timestamp=-100, open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0),
        ]

        result = validation_service.validate_candles(candles)

        assert not result.is_valid
        assert len(result.errors) == 2
        assert 0 in result.invalid_candles
        assert 1 in result.invalid_candles

    def test_validate_candles_invalid_prices(self, validation_service):
        """Test validation catches invalid prices"""
        candles = [
            Candle(
                timestamp=1609459200000, open=0, high=110.0, low=90.0, close=105.0, volume=1000.0
            ),
            Candle(
                timestamp=1609459260000,
                open=100.0,
                high=-10.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),
            Candle(
                timestamp=1609459320000, open=100.0, high=110.0, low=0, close=105.0, volume=1000.0
            ),
            Candle(
                timestamp=1609459380000, open=100.0, high=110.0, low=90.0, close=-5.0, volume=1000.0
            ),
        ]

        result = validation_service.validate_candles(candles)

        assert not result.is_valid
        assert len(result.errors) >= 4  # May have additional errors for OHLC violations
        assert all(i in result.invalid_candles for i in range(4))

    def test_validate_candles_negative_volume(self, validation_service):
        """Test validation catches negative volume"""
        candles = [
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=-1000.0,
            ),
        ]

        result = validation_service.validate_candles(candles)

        assert not result.is_valid
        assert len(result.errors) == 1
        assert "Negative volume" in result.errors[0]
        assert 0 in result.invalid_candles

    def test_validate_candles_ohlc_violations(self, validation_service):
        """Test validation catches OHLC relationship violations"""
        candles = [
            # High < Low
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=90.0,
                low=110.0,
                close=105.0,
                volume=1000.0,
            ),
            # High < Open
            Candle(
                timestamp=1609459260000,
                open=120.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),
            # High < Close
            Candle(
                timestamp=1609459320000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=120.0,
                volume=1000.0,
            ),
            # Low > Open
            Candle(
                timestamp=1609459380000, open=80.0, high=110.0, low=90.0, close=105.0, volume=1000.0
            ),
            # Low > Close
            Candle(
                timestamp=1609459440000,
                open=100.0,
                high=110.0,
                low=120.0,
                close=105.0,
                volume=1000.0,
            ),
        ]

        result = validation_service.validate_candles(candles)

        assert not result.is_valid
        assert len(result.errors) >= 5
        assert all(i in result.invalid_candles for i in range(5))

    def test_validate_candles_duplicate_timestamps(self, validation_service):
        """Test validation detects duplicate timestamps"""
        candles = [
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),
            Candle(
                timestamp=1609459200000,
                open=105.0,
                high=115.0,
                low=95.0,
                close=110.0,
                volume=1100.0,
            ),
            Candle(
                timestamp=1609459320000,
                open=110.0,
                high=120.0,
                low=100.0,
                close=115.0,
                volume=1200.0,
            ),
        ]

        result = validation_service.validate_candles(candles)

        assert not result.is_valid
        assert any("duplicate timestamps" in error for error in result.errors)

    def test_validate_candles_not_chronological(self, validation_service):
        """Test validation detects non-chronological order"""
        candles = [
            Candle(
                timestamp=1609459320000,
                open=110.0,
                high=120.0,
                low=100.0,
                close=115.0,
                volume=1200.0,
            ),
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),
            Candle(
                timestamp=1609459260000,
                open=105.0,
                high=115.0,
                low=95.0,
                close=110.0,
                volume=1100.0,
            ),
        ]

        result = validation_service.validate_candles(candles)

        assert len(result.warnings) > 0
        assert any("chronological order" in warning for warning in result.warnings)

    def test_validate_data_request(self, validation_service):
        """Test data request validation"""
        from datetime import datetime

        request = DataRequest(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_date=datetime.fromtimestamp(1609459200),
            end_date=datetime.fromtimestamp(1609545600),
        )

        # Should not raise exception for valid request
        validation_service.validate_data_request(request)

    def test_sanitize_candles_empty(self, validation_service):
        """Test sanitizing empty candle list"""
        result = validation_service.sanitize_candles([])
        assert result == []

    def test_sanitize_candles_removes_invalid(
        self, validation_service, valid_candles, invalid_candles
    ):
        """Test sanitizing removes invalid candles"""
        mixed_candles = valid_candles + invalid_candles

        result = validation_service.sanitize_candles(mixed_candles)

        # Should only keep valid candles
        assert len(result) == len(valid_candles)
        assert all(c.timestamp > 0 for c in result)
        assert all(c.open > 0 and c.high > 0 and c.low > 0 and c.close > 0 for c in result)
        assert all(c.volume >= 0 for c in result)

    def test_sanitize_candles_removes_duplicates(self, validation_service):
        """Test sanitizing removes duplicate timestamps"""
        candles = [
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),
            Candle(
                timestamp=1609459200000,
                open=105.0,
                high=115.0,
                low=95.0,
                close=110.0,
                volume=1100.0,
            ),  # Duplicate
            Candle(
                timestamp=1609459260000,
                open=110.0,
                high=120.0,
                low=100.0,
                close=115.0,
                volume=1200.0,
            ),
        ]

        result = validation_service.sanitize_candles(candles)

        # Should keep only unique timestamps (keeps last occurrence)
        assert len(result) == 2
        timestamps = [c.timestamp for c in result]
        assert len(timestamps) == len(set(timestamps))
        # Check that it kept the last occurrence of duplicate
        duplicate_candle = next(c for c in result if c.timestamp == 1609459200000)
        assert duplicate_candle.open == 105.0  # From second candle

    def test_sanitize_candles_sorts_chronologically(self, validation_service):
        """Test sanitizing sorts candles by timestamp"""
        candles = [
            Candle(
                timestamp=1609459320000,
                open=110.0,
                high=120.0,
                low=100.0,
                close=115.0,
                volume=1200.0,
            ),
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),
            Candle(
                timestamp=1609459260000,
                open=105.0,
                high=115.0,
                low=95.0,
                close=110.0,
                volume=1100.0,
            ),
        ]

        result = validation_service.sanitize_candles(candles)

        # Should be sorted by timestamp
        assert len(result) == 3
        assert result[0].timestamp < result[1].timestamp < result[2].timestamp
        assert result[0].timestamp == 1609459200000
        assert result[1].timestamp == 1609459260000
        assert result[2].timestamp == 1609459320000

    def test_sanitize_candles_complex_scenario(self, validation_service):
        """Test sanitizing with multiple issues"""
        candles = [
            # Valid
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),
            # Duplicate (should keep this one)
            Candle(
                timestamp=1609459200000,
                open=102.0,
                high=112.0,
                low=92.0,
                close=107.0,
                volume=1050.0,
            ),
            # Invalid price
            Candle(
                timestamp=1609459260000,
                open=-10.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1100.0,
            ),
            # Valid but out of order
            Candle(
                timestamp=1609459380000,
                open=115.0,
                high=125.0,
                low=105.0,
                close=120.0,
                volume=1300.0,
            ),
            # Valid
            Candle(
                timestamp=1609459320000,
                open=110.0,
                high=120.0,
                low=100.0,
                close=115.0,
                volume=1200.0,
            ),
            # Invalid timestamp
            Candle(timestamp=0, open=100.0, high=110.0, low=90.0, close=105.0, volume=1000.0),
        ]

        result = validation_service.sanitize_candles(candles)

        # Should have 3 valid candles, sorted, no duplicates
        assert len(result) == 3
        assert result[0].timestamp == 1609459200000
        assert result[0].open == 102.0  # Kept the second duplicate
        assert result[1].timestamp == 1609459320000
        assert result[2].timestamp == 1609459380000
