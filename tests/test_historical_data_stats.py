"""
Unit tests for HistoricalDataStats and DataRange models
"""

from datetime import datetime

import pytest

from wickdata.models.data_gap import DataGap
from wickdata.models.data_range import DataRange
from wickdata.models.historical_data_stats import HistoricalDataStats
from wickdata.models.timeframe import Timeframe


class TestDataRange:
    """Tests for DataRange model"""

    def test_data_range_creation(self):
        """Test creating a valid DataRange"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 2, 0, 0, 0)

        data_range = DataRange(start=start, end=end)

        assert data_range.start == start
        assert data_range.end == end

    def test_data_range_invalid_order(self):
        """Test that start > end raises ValueError"""
        start = datetime(2021, 1, 2, 0, 0, 0)
        end = datetime(2021, 1, 1, 0, 0, 0)

        with pytest.raises(ValueError) as exc_info:
            DataRange(start=start, end=end)
        assert "start must be before or equal to end" in str(exc_info.value)

    def test_data_range_equal_start_end(self):
        """Test that start == end is valid"""
        timestamp = datetime(2021, 1, 1, 0, 0, 0)

        data_range = DataRange(start=timestamp, end=timestamp)

        assert data_range.start == data_range.end

    def test_get_start_timestamp(self):
        """Test getting start timestamp in milliseconds"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 2, 0, 0, 0)

        data_range = DataRange(start=start, end=end)

        expected = int(start.timestamp() * 1000)
        assert data_range.get_start_timestamp() == expected

    def test_get_end_timestamp(self):
        """Test getting end timestamp in milliseconds"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 2, 0, 0, 0)

        data_range = DataRange(start=start, end=end)

        expected = int(end.timestamp() * 1000)
        assert data_range.get_end_timestamp() == expected

    def test_get_duration_seconds(self):
        """Test getting duration in seconds"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 1, 1, 0, 0)  # 1 hour later

        data_range = DataRange(start=start, end=end)

        assert data_range.get_duration_seconds() == 3600.0

    def test_get_duration_seconds_days(self):
        """Test getting duration for multiple days"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 3, 0, 0, 0)  # 2 days later

        data_range = DataRange(start=start, end=end)

        assert data_range.get_duration_seconds() == 2 * 24 * 3600.0

    def test_contains_timestamp_in_range(self):
        """Test checking if timestamp is within range"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 2, 0, 0, 0)

        data_range = DataRange(start=start, end=end)

        # Timestamp in the middle
        middle = datetime(2021, 1, 1, 12, 0, 0)
        middle_ts = int(middle.timestamp() * 1000)
        assert data_range.contains(middle_ts) is True

        # Start boundary
        assert data_range.contains(data_range.get_start_timestamp()) is True

        # End boundary
        assert data_range.contains(data_range.get_end_timestamp()) is True

    def test_contains_timestamp_out_of_range(self):
        """Test checking if timestamp is outside range"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 2, 0, 0, 0)

        data_range = DataRange(start=start, end=end)

        # Before start
        before = datetime(2020, 12, 31, 0, 0, 0)
        before_ts = int(before.timestamp() * 1000)
        assert data_range.contains(before_ts) is False

        # After end
        after = datetime(2021, 1, 3, 0, 0, 0)
        after_ts = int(after.timestamp() * 1000)
        assert data_range.contains(after_ts) is False

    def test_overlaps_with_overlapping_range(self):
        """Test checking overlap with another range"""
        range1 = DataRange(start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 3, 0, 0, 0))

        # Partial overlap
        range2 = DataRange(start=datetime(2021, 1, 2, 0, 0, 0), end=datetime(2021, 1, 4, 0, 0, 0))
        assert range1.overlaps(range2) is True
        assert range2.overlaps(range1) is True

        # Complete overlap (range2 inside range1)
        range3 = DataRange(start=datetime(2021, 1, 1, 12, 0, 0), end=datetime(2021, 1, 2, 12, 0, 0))
        assert range1.overlaps(range3) is True
        assert range3.overlaps(range1) is True

        # Same range
        range4 = DataRange(start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 3, 0, 0, 0))
        assert range1.overlaps(range4) is True

    def test_overlaps_with_non_overlapping_range(self):
        """Test checking no overlap with another range"""
        range1 = DataRange(start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 2, 0, 0, 0))

        # Range before
        range2 = DataRange(
            start=datetime(2020, 12, 29, 0, 0, 0), end=datetime(2020, 12, 31, 0, 0, 0)
        )
        assert range1.overlaps(range2) is False
        assert range2.overlaps(range1) is False

        # Range after
        range3 = DataRange(start=datetime(2021, 1, 3, 0, 0, 0), end=datetime(2021, 1, 4, 0, 0, 0))
        assert range1.overlaps(range3) is False
        assert range3.overlaps(range1) is False

    def test_overlaps_edge_cases(self):
        """Test overlap edge cases where ranges touch"""
        range1 = DataRange(start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 2, 0, 0, 0))

        # End touches start
        range2 = DataRange(start=datetime(2021, 1, 2, 0, 0, 0), end=datetime(2021, 1, 3, 0, 0, 0))
        # Touching at boundary is considered overlap
        assert range1.overlaps(range2) is True
        assert range2.overlaps(range1) is True

    def test_repr(self):
        """Test string representation"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 2, 0, 0, 0)

        data_range = DataRange(start=start, end=end)

        repr_str = repr(data_range)
        assert "DataRange" in repr_str
        assert str(start) in repr_str
        assert str(end) in repr_str


class TestHistoricalDataStats:
    """Tests for HistoricalDataStats model"""

    @pytest.fixture
    def sample_data_range(self):
        """Create a sample data range"""
        return DataRange(
            start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 2, 0, 0, 0)  # 24 hours
        )

    @pytest.fixture
    def sample_gaps(self):
        """Create sample data gaps"""
        return [
            DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=1),
            DataGap(start_time=1609459380000, end_time=1609459500000, candle_count=2),
            DataGap(start_time=1609459620000, end_time=1609459800000, candle_count=3),
        ]

    @pytest.fixture
    def stats_with_gap_list(self, sample_data_range, sample_gaps):
        """Create stats with list of gaps"""
        return HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=1434,  # 24 hours * 60 minutes - 6 gaps
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=sample_gaps,
        )

    @pytest.fixture
    def stats_with_gap_count(self, sample_data_range):
        """Create stats with gap count"""
        return HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            total_candles=23,  # 24 hours - 1 gap
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=1,  # Just the count
        )

    def test_creation_with_gap_list(self, stats_with_gap_list, sample_gaps):
        """Test creating stats with list of gaps"""
        assert stats_with_gap_list.exchange == "binance"
        assert stats_with_gap_list.symbol == "BTC/USDT"
        assert stats_with_gap_list.timeframe == Timeframe.ONE_MINUTE
        assert stats_with_gap_list.total_candles == 1434
        assert stats_with_gap_list.gaps == sample_gaps

    def test_creation_with_gap_count(self, stats_with_gap_count):
        """Test creating stats with gap count"""
        assert stats_with_gap_count.exchange == "binance"
        assert stats_with_gap_count.symbol == "BTC/USDT"
        assert stats_with_gap_count.timeframe == Timeframe.ONE_HOUR
        assert stats_with_gap_count.total_candles == 23
        assert stats_with_gap_count.gaps == 1

    def test_get_gap_count_from_list(self, stats_with_gap_list):
        """Test getting gap count from list"""
        assert stats_with_gap_list.get_gap_count() == 3

    def test_get_gap_count_from_int(self, stats_with_gap_count):
        """Test getting gap count from integer"""
        assert stats_with_gap_count.get_gap_count() == 1

    def test_get_gap_count_empty_list(self, sample_data_range):
        """Test getting gap count from empty list"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=1440,
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats.get_gap_count() == 0

    def test_get_coverage_percentage_full(self, sample_data_range):
        """Test coverage percentage with full data"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=1440,  # Exactly 24 hours of 1-minute candles
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats.get_coverage_percentage() == 100.0

    def test_get_coverage_percentage_partial(self, sample_data_range):
        """Test coverage percentage with partial data"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=720,  # Half of expected
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats.get_coverage_percentage() == 50.0

    def test_get_coverage_percentage_zero_candles(self, sample_data_range):
        """Test coverage percentage with zero candles"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=0,
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats.get_coverage_percentage() == 0.0

    def test_get_coverage_percentage_exceeds_100(self, sample_data_range):
        """Test coverage percentage is capped at 100%"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=2000,  # More than expected
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats.get_coverage_percentage() == 100.0

    def test_get_coverage_percentage_different_timeframes(self):
        """Test coverage percentage with different timeframes"""
        # 1 hour timeframe for 24 hours
        data_range = DataRange(
            start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 2, 0, 0, 0)
        )

        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            total_candles=12,  # Half of 24 hours
            date_range=data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats.get_coverage_percentage() == 50.0

    def test_calculate_expected_candles_one_minute(self, sample_data_range):
        """Test calculating expected candles for 1-minute timeframe"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=0,
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        # 24 hours * 60 minutes = 1440 candles
        assert stats._calculate_expected_candles() == 1440

    def test_calculate_expected_candles_one_hour(self, sample_data_range):
        """Test calculating expected candles for 1-hour timeframe"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            total_candles=0,
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        # 24 hours = 24 candles
        assert stats._calculate_expected_candles() == 24

    def test_calculate_expected_candles_one_day(self):
        """Test calculating expected candles for 1-day timeframe"""
        data_range = DataRange(
            start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 8, 0, 0, 0)  # 7 days
        )

        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_DAY,
            total_candles=0,
            date_range=data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        # 7 days = 7 candles
        assert stats._calculate_expected_candles() == 7

    def test_calculate_expected_candles_zero_duration(self):
        """Test calculating expected candles for zero duration"""
        data_range = DataRange(
            start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 1, 0, 0, 0)  # Same time
        )

        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=0,
            date_range=data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats._calculate_expected_candles() == 0
        # Coverage should be 0% when total_candles is 0 (even if no candles are expected)
        assert stats.get_coverage_percentage() == 0.0

        # But if we have some candles with zero expected, it should be 100%
        stats_with_candles = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=1,  # Have 1 candle
            date_range=data_range,
            last_updated=datetime.now(),
            gaps=[],
        )
        assert stats_with_candles._calculate_expected_candles() == 0
        assert stats_with_candles.get_coverage_percentage() == 100.0

    def test_repr(self, stats_with_gap_list):
        """Test string representation"""
        repr_str = repr(stats_with_gap_list)

        assert "HistoricalDataStats" in repr_str
        assert "exchange=binance" in repr_str
        assert "symbol=BTC/USDT" in repr_str
        assert "timeframe=" in repr_str
        assert "candles=1434" in repr_str
        assert "coverage=" in repr_str
        assert "%" in repr_str

    def test_repr_with_zero_coverage(self, sample_data_range):
        """Test string representation with zero coverage"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=0,
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )

        repr_str = repr(stats)
        assert "coverage=0.0%" in repr_str

    def test_repr_with_full_coverage(self, sample_data_range):
        """Test string representation with full coverage"""
        stats = HistoricalDataStats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_MINUTE,
            total_candles=1440,
            date_range=sample_data_range,
            last_updated=datetime.now(),
            gaps=[],
        )

        repr_str = repr(stats)
        assert "coverage=100.0%" in repr_str

    def test_with_various_timeframes(self):
        """Test stats with various timeframe combinations"""
        timeframes = [
            (Timeframe.ONE_MINUTE, 60),
            (Timeframe.FIVE_MINUTES, 12),
            (Timeframe.FIFTEEN_MINUTES, 4),
            (Timeframe.THIRTY_MINUTES, 2),
            (Timeframe.ONE_HOUR, 1),
        ]

        data_range = DataRange(
            start=datetime(2021, 1, 1, 0, 0, 0), end=datetime(2021, 1, 1, 1, 0, 0)  # 1 hour
        )

        for timeframe, expected_candles in timeframes:
            stats = HistoricalDataStats(
                exchange="test",
                symbol="TEST/USDT",
                timeframe=timeframe,
                total_candles=expected_candles,
                date_range=data_range,
                last_updated=datetime.now(),
                gaps=0,
            )
            assert stats._calculate_expected_candles() == expected_candles
            assert stats.get_coverage_percentage() == 100.0
