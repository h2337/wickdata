"""
Unit tests for data models
"""

from datetime import datetime

import pytest

from wickdata.models import (
    Candle,
    DataGap,
    DataRange,
    DataRequest,
    StreamOptions,
    Timeframe,
)


class TestCandle:
    def test_candle_creation(self):
        candle = Candle(
            timestamp=1609459200000,
            open=29000.0,
            high=29500.0,
            low=28500.0,
            close=29200.0,
            volume=1000.0,
        )

        assert candle.timestamp == 1609459200000
        assert candle.open == 29000.0
        assert candle.high == 29500.0
        assert candle.low == 28500.0
        assert candle.close == 29200.0
        assert candle.volume == 1000.0

    def test_candle_to_dict(self):
        candle = Candle(
            timestamp=1609459200000,
            open=29000.0,
            high=29500.0,
            low=28500.0,
            close=29200.0,
            volume=1000.0,
        )

        data = candle.to_dict()
        assert data["timestamp"] == 1609459200000
        assert data["open"] == 29000.0
        assert data["volume"] == 1000.0

    def test_candle_from_ccxt(self):
        ccxt_candle = [1609459200000, 29000.0, 29500.0, 28500.0, 29200.0, 1000.0]
        candle = Candle.from_ccxt(ccxt_candle)

        assert candle.timestamp == 1609459200000
        assert candle.open == 29000.0
        assert candle.close == 29200.0
        assert candle.volume == 1000.0


class TestTimeframe:
    def test_timeframe_values(self):
        assert Timeframe.ONE_MINUTE.value == "1m"
        assert Timeframe.ONE_HOUR.value == "1h"
        assert Timeframe.ONE_DAY.value == "1d"

    def test_timeframe_to_minutes(self):
        assert Timeframe.ONE_MINUTE.to_minutes() == 1
        assert Timeframe.FIVE_MINUTES.to_minutes() == 5
        assert Timeframe.ONE_HOUR.to_minutes() == 60
        assert Timeframe.ONE_DAY.to_minutes() == 1440

    def test_timeframe_to_milliseconds(self):
        assert Timeframe.ONE_MINUTE.to_milliseconds() == 60000
        assert Timeframe.ONE_HOUR.to_milliseconds() == 3600000

    def test_timeframe_from_string(self):
        tf = Timeframe.from_string("1h")
        assert tf == Timeframe.ONE_HOUR

        with pytest.raises(ValueError):
            Timeframe.from_string("invalid")


class TestDataRequest:
    def test_data_request_creation(self):
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)

        request = DataRequest(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_date=start,
            end_date=end,
        )

        assert request.exchange == "binance"
        assert request.symbol == "BTC/USDT"
        assert request.timeframe == Timeframe.ONE_HOUR
        assert request.batch_size == 500
        assert request.concurrent_fetchers == 3

    def test_data_request_validation(self):
        start = datetime(2024, 1, 31)
        end = datetime(2024, 1, 1)

        with pytest.raises(ValueError):
            DataRequest(
                exchange="binance",
                symbol="BTC/USDT",
                timeframe=Timeframe.ONE_HOUR,
                start_date=start,
                end_date=end,
            )


class TestDataGap:
    def test_data_gap_creation(self):
        gap = DataGap(
            start_time=1609459200000,
            end_time=1609545600000,
            candle_count=24,
        )

        assert gap.start_time == 1609459200000
        assert gap.end_time == 1609545600000
        assert gap.candle_count == 24

    def test_data_gap_datetime_conversion(self):
        gap = DataGap(
            start_time=1609459200000,
            end_time=1609545600000,
            candle_count=24,
        )

        start_dt = gap.get_start_datetime()
        end_dt = gap.get_end_datetime()

        assert isinstance(start_dt, datetime)
        assert isinstance(end_dt, datetime)
        assert end_dt > start_dt


class TestDataRange:
    def test_data_range_creation(self):
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)

        range_obj = DataRange(start, end)

        assert range_obj.start == start
        assert range_obj.end == end

    def test_data_range_validation(self):
        start = datetime(2024, 1, 31)
        end = datetime(2024, 1, 1)

        with pytest.raises(ValueError):
            DataRange(start, end)

    def test_data_range_contains(self):
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 31)
        range_obj = DataRange(start, end)

        mid_timestamp = int(datetime(2024, 1, 15).timestamp() * 1000)
        assert range_obj.contains(mid_timestamp)

        outside_timestamp = int(datetime(2024, 2, 15).timestamp() * 1000)
        assert not range_obj.contains(outside_timestamp)


class TestStreamOptions:
    def test_stream_options_defaults(self):
        options = StreamOptions()

        assert options.batch_size == 1000
        assert options.delay_ms == 0
        assert options.realtime is False
        assert options.max_size is None

    def test_stream_options_validation(self):
        with pytest.raises(ValueError):
            StreamOptions(batch_size=0)

        with pytest.raises(ValueError):
            StreamOptions(delay_ms=-1)

        with pytest.raises(ValueError):
            StreamOptions(max_size=0)
