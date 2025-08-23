"""
Unit tests for builder patterns
"""

from datetime import datetime

import pytest

from wickdata.builders import DataRequestBuilder
from wickdata.core.errors import ValidationError
from wickdata.models import Timeframe


class TestDataRequestBuilder:
    def test_basic_build(self):
        request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("BTC/USDT")
            .with_timeframe("1h")
            .with_date_range(datetime(2024, 1, 1), datetime(2024, 1, 31))
            .build()
        )

        assert request.exchange == "binance"
        assert request.symbol == "BTC/USDT"
        assert request.timeframe == Timeframe.ONE_HOUR
        assert request.start_date == datetime(2024, 1, 1)
        assert request.end_date == datetime(2024, 1, 31)

    def test_with_last_days(self):
        request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("ETH/USDT")
            .with_timeframe(Timeframe.ONE_DAY)
            .with_last_days(7)
            .build()
        )

        assert request.exchange == "binance"
        assert request.symbol == "ETH/USDT"
        assert request.timeframe == Timeframe.ONE_DAY

        # Check that dates are approximately correct
        days_diff = (request.end_date - request.start_date).days
        assert 6 <= days_diff <= 7

    def test_with_last_hours(self):
        request = (
            DataRequestBuilder.create()
            .with_exchange("kraken")
            .with_symbol("XRP/USD")
            .with_timeframe("5m")
            .with_last_hours(24)
            .build()
        )

        assert request.exchange == "kraken"
        assert request.symbol == "XRP/USD"
        assert request.timeframe == Timeframe.FIVE_MINUTES

        # Check that dates are approximately correct
        hours_diff = (request.end_date - request.start_date).total_seconds() / 3600
        assert 23 <= hours_diff <= 24

    def test_with_month_to_date(self):
        request = (
            DataRequestBuilder.create()
            .with_exchange("coinbase")
            .with_symbol("SOL/USD")
            .with_timeframe("4h")
            .with_month_to_date()
            .build()
        )

        assert request.exchange == "coinbase"
        assert request.start_date.day == 1
        assert request.start_date.hour == 0
        assert request.start_date.minute == 0

    def test_with_year_to_date(self):
        request = (
            DataRequestBuilder.create()
            .with_exchange("bybit")
            .with_symbol("DOGE/USDT")
            .with_timeframe("1d")
            .with_year_to_date()
            .build()
        )

        assert request.exchange == "bybit"
        assert request.start_date.month == 1
        assert request.start_date.day == 1
        assert request.start_date.hour == 0

    def test_with_custom_settings(self):
        request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("BTC/USDT")
            .with_timeframe("1h")
            .with_last_days(1)
            .with_batch_size(100)
            .with_concurrent_fetchers(5)
            .with_rate_limit_delay(0.5)
            .build()
        )

        assert request.batch_size == 100
        assert request.concurrent_fetchers == 5
        assert request.rate_limit_delay == 0.5

    def test_missing_required_fields(self):
        builder = DataRequestBuilder.create()

        # Missing exchange
        with pytest.raises(ValidationError) as exc_info:
            builder.with_symbol("BTC/USDT").with_timeframe("1h").with_last_days(1).build()
        assert "exchange" in str(exc_info.value).lower()

        # Missing symbol
        builder = DataRequestBuilder.create()
        with pytest.raises(ValidationError) as exc_info:
            builder.with_exchange("binance").with_timeframe("1h").with_last_days(1).build()
        assert "symbol" in str(exc_info.value).lower()

        # Missing timeframe
        builder = DataRequestBuilder.create()
        with pytest.raises(ValidationError) as exc_info:
            builder.with_exchange("binance").with_symbol("BTC/USDT").with_last_days(1).build()
        assert "timeframe" in str(exc_info.value).lower()

        # Missing dates
        builder = DataRequestBuilder.create()
        with pytest.raises(ValidationError) as exc_info:
            builder.with_exchange("binance").with_symbol("BTC/USDT").with_timeframe("1h").build()
        assert "date" in str(exc_info.value).lower()

    def test_date_string_parsing(self):
        request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("BTC/USDT")
            .with_timeframe("1h")
            .with_date_range("2024-01-01", "2024-01-31")
            .build()
        )

        assert request.start_date == datetime(2024, 1, 1)
        assert request.end_date == datetime(2024, 1, 31)
