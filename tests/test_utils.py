"""
Unit tests for utility functions
"""

import pytest

from wickdata.core.errors import ValidationError
from wickdata.models import Timeframe
from wickdata.utils import (
    TimeframeUtils,
    is_valid_exchange,
    is_valid_symbol,
    is_valid_timeframe,
    sanitize_symbol,
    validate_exchange_name,
    validate_symbol,
    validate_timeframe,
)


class TestTimeframeUtils:
    def test_to_milliseconds(self):
        assert TimeframeUtils.to_milliseconds("1m") == 60000
        assert TimeframeUtils.to_milliseconds(Timeframe.ONE_HOUR) == 3600000
        assert TimeframeUtils.to_milliseconds("1d") == 86400000

    def test_to_seconds(self):
        assert TimeframeUtils.to_seconds("1m") == 60
        assert TimeframeUtils.to_seconds(Timeframe.ONE_HOUR) == 3600
        assert TimeframeUtils.to_seconds("1d") == 86400

    def test_to_minutes(self):
        assert TimeframeUtils.to_minutes("1m") == 1
        assert TimeframeUtils.to_minutes(Timeframe.ONE_HOUR) == 60
        assert TimeframeUtils.to_minutes("1d") == 1440

    def test_get_candle_count(self):
        # 1 hour timeframe, 24 hours = 24 candles
        count = TimeframeUtils.get_candle_count(0, 86400000, Timeframe.ONE_HOUR)  # 24 hours in ms
        assert count == 24

        # 5 minute timeframe, 1 hour = 12 candles
        count = TimeframeUtils.get_candle_count(0, 3600000, "5m")  # 1 hour in ms
        assert count == 12

    def test_align_timestamp(self):
        # Test alignment to hour boundary
        timestamp = 1609459230000  # 30 seconds past hour
        aligned = TimeframeUtils.align_timestamp(timestamp, Timeframe.ONE_HOUR)
        assert aligned == 1609459200000  # Aligned to hour

        # Test already aligned timestamp
        timestamp = 1609459200000  # Exactly on hour
        aligned = TimeframeUtils.align_timestamp(timestamp, Timeframe.ONE_HOUR)
        assert aligned == timestamp

    def test_is_aligned(self):
        # Test aligned timestamp
        assert TimeframeUtils.is_aligned(1609459200000, Timeframe.ONE_HOUR)

        # Test unaligned timestamp
        assert not TimeframeUtils.is_aligned(1609459230000, Timeframe.ONE_HOUR)


class TestValidation:
    def test_validate_symbol(self):
        # Valid symbols
        assert validate_symbol("BTC/USDT") == "BTC/USDT"
        assert validate_symbol("eth/usdt") == "ETH/USDT"  # Converts to uppercase
        assert validate_symbol("BTC-USDT") == "BTC/USDT"  # Converts separator
        assert validate_symbol("BTC_USDT") == "BTC/USDT"  # Converts separator

        # Invalid symbols
        with pytest.raises(ValidationError):
            validate_symbol("")

        with pytest.raises(ValidationError):
            validate_symbol("BTCUSDT")  # No separator

        with pytest.raises(ValidationError):
            validate_symbol("BTC/")  # Missing quote

    def test_validate_exchange_name(self):
        # Valid exchange names
        assert validate_exchange_name("binance") == "binance"
        assert validate_exchange_name("BINANCE") == "binance"  # Converts to lowercase
        assert validate_exchange_name("kraken_futures") == "kraken_futures"

        # Invalid exchange names
        with pytest.raises(ValidationError):
            validate_exchange_name("")

        with pytest.raises(ValidationError):
            validate_exchange_name("123exchange")  # Starts with number

        with pytest.raises(ValidationError):
            validate_exchange_name("exchange-name")  # Contains hyphen

    def test_validate_timeframe(self):
        # Valid timeframes
        assert validate_timeframe("1m") == Timeframe.ONE_MINUTE
        assert validate_timeframe("1h") == Timeframe.ONE_HOUR
        assert validate_timeframe(Timeframe.ONE_DAY) == Timeframe.ONE_DAY

        # Invalid timeframes
        with pytest.raises(ValidationError):
            validate_timeframe("")

        with pytest.raises(ValidationError):
            validate_timeframe("2m")  # Not supported

        with pytest.raises(ValidationError):
            validate_timeframe("invalid")

    def test_sanitize_symbol(self):
        assert sanitize_symbol("BTC/USDT") == "BTC/USDT"
        assert sanitize_symbol("btc-usdt") == "BTC/USDT"
        assert sanitize_symbol("BTC_USDT") == "BTC/USDT"
        assert sanitize_symbol("BTC@USDT") == "BTCUSDT"  # Removes invalid char
        assert sanitize_symbol("") == ""

    def test_is_valid_symbol(self):
        assert is_valid_symbol("BTC/USDT")
        assert is_valid_symbol("ETH-USD")
        assert not is_valid_symbol("BTCUSDT")
        assert not is_valid_symbol("")

    def test_is_valid_exchange(self):
        assert is_valid_exchange("binance")
        assert is_valid_exchange("coinbase_pro")
        assert not is_valid_exchange("123exchange")
        assert not is_valid_exchange("")

    def test_is_valid_timeframe(self):
        assert is_valid_timeframe("1m")
        assert is_valid_timeframe("1h")
        assert is_valid_timeframe("1d")
        assert not is_valid_timeframe("2m")
        assert not is_valid_timeframe("invalid")
