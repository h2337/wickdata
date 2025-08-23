"""
Unit tests for CandleQueryBuilder
"""

from datetime import datetime
from unittest.mock import AsyncMock, Mock

import pytest

from wickdata.builders.candle_query_builder import CandleQueryBuilder
from wickdata.core.errors import ValidationError
from wickdata.database.candle_repository import CandleRepository
from wickdata.models.candle import Candle
from wickdata.models.timeframe import Timeframe


@pytest.fixture
def mock_repository():
    """Create mock candle repository"""
    repository = Mock(spec=CandleRepository)
    repository.get_candles = AsyncMock()
    repository.count_candles = AsyncMock()
    return repository


@pytest.fixture
def query_builder(mock_repository):
    """Create CandleQueryBuilder instance"""
    return CandleQueryBuilder(repository=mock_repository)


@pytest.fixture
def sample_candles():
    """Create sample candles for testing"""
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


class TestCandleQueryBuilder:

    def test_exchange_sets_filter(self, query_builder):
        """Test setting exchange filter"""
        result = query_builder.exchange("binance")

        assert result is query_builder  # Returns self for chaining
        assert query_builder._exchange == "binance"

    def test_symbol_sets_filter(self, query_builder):
        """Test setting symbol filter"""
        result = query_builder.symbol("BTC/USDT")

        assert result is query_builder
        assert query_builder._symbol == "BTC/USDT"

    def test_timeframe_sets_filter(self, query_builder):
        """Test setting timeframe filter"""
        result = query_builder.timeframe(Timeframe.ONE_HOUR)

        assert result is query_builder
        assert query_builder._timeframe == Timeframe.ONE_HOUR

    def test_date_range_sets_timestamps(self, query_builder):
        """Test setting date range converts to timestamps"""
        start = datetime(2021, 1, 1, 0, 0, 0)
        end = datetime(2021, 1, 2, 0, 0, 0)

        result = query_builder.date_range(start, end)

        assert result is query_builder
        assert query_builder._start_time == int(start.timestamp() * 1000)
        assert query_builder._end_time == int(end.timestamp() * 1000)

    def test_timestamp_range_sets_directly(self, query_builder):
        """Test setting timestamp range directly"""
        start = 1609459200000
        end = 1609545600000

        result = query_builder.timestamp_range(start, end)

        assert result is query_builder
        assert query_builder._start_time == start
        assert query_builder._end_time == end

    def test_limit_sets_count(self, query_builder):
        """Test setting result limit"""
        result = query_builder.limit(100)

        assert result is query_builder
        assert query_builder._limit == 100

    def test_offset_sets_skip(self, query_builder):
        """Test setting result offset"""
        result = query_builder.offset(50)

        assert result is query_builder
        assert query_builder._offset == 50

    def test_order_by_sets_ordering(self, query_builder):
        """Test setting order parameters"""
        result = query_builder.order_by("volume", "desc")

        assert result is query_builder
        assert query_builder._order_field == "volume"
        assert query_builder._order_direction == "desc"

    def test_order_by_normalizes_direction(self, query_builder):
        """Test order direction is normalized to lowercase"""
        query_builder.order_by("timestamp", "DESC")
        assert query_builder._order_direction == "desc"

        query_builder.order_by("timestamp", "ASC")
        assert query_builder._order_direction == "asc"

    def test_method_chaining(self, query_builder):
        """Test fluent interface method chaining"""
        result = (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .limit(100)
            .offset(10)
            .order_by("timestamp", "desc")
        )

        assert result is query_builder
        assert query_builder._exchange == "binance"
        assert query_builder._symbol == "BTC/USDT"
        assert query_builder._timeframe == Timeframe.ONE_HOUR
        assert query_builder._limit == 100
        assert query_builder._offset == 10
        assert query_builder._order_field == "timestamp"
        assert query_builder._order_direction == "desc"

    @pytest.mark.asyncio
    async def test_execute_validates_required_fields(self, query_builder):
        """Test execute validates required fields"""
        # Missing exchange
        with pytest.raises(ValidationError) as exc_info:
            await query_builder.execute()
        assert "Exchange is required" in str(exc_info.value)

        # Missing symbol
        query_builder.exchange("binance")
        with pytest.raises(ValidationError) as exc_info:
            await query_builder.execute()
        assert "Symbol is required" in str(exc_info.value)

        # Missing timeframe
        query_builder.symbol("BTC/USDT")
        with pytest.raises(ValidationError) as exc_info:
            await query_builder.execute()
        assert "Timeframe is required" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_execute_with_all_parameters(
        self, query_builder, mock_repository, sample_candles
    ):
        """Test execute with all parameters set"""
        mock_repository.get_candles.return_value = sample_candles

        start = 1609459200000
        end = 1609545600000

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .timestamp_range(start, end)
            .limit(100)
            .offset(10)
            .execute()
        )

        assert result == sample_candles
        mock_repository.get_candles.assert_called_once_with(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_time=start,
            end_time=end,
            limit=100,
            offset=10,
        )

    @pytest.mark.asyncio
    async def test_execute_sets_default_time_range(self, query_builder, mock_repository):
        """Test execute sets default time range if not specified"""
        mock_repository.get_candles.return_value = []

        await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .execute()
        )

        call_args = mock_repository.get_candles.call_args
        assert call_args.kwargs["start_time"] == 0
        assert call_args.kwargs["end_time"] > 0  # Should be current time

    @pytest.mark.asyncio
    async def test_execute_descending_order(self, query_builder, mock_repository, sample_candles):
        """Test execute reverses results for descending order"""
        mock_repository.get_candles.return_value = sample_candles.copy()

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .order_by("timestamp", "desc")
            .execute()
        )

        # Results should be reversed
        assert result[0].timestamp == sample_candles[-1].timestamp
        assert result[-1].timestamp == sample_candles[0].timestamp

    @pytest.mark.asyncio
    async def test_execute_ascending_order(self, query_builder, mock_repository, sample_candles):
        """Test execute maintains order for ascending"""
        mock_repository.get_candles.return_value = sample_candles.copy()

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .order_by("timestamp", "asc")
            .execute()
        )

        # Results should maintain order
        assert result == sample_candles

    @pytest.mark.asyncio
    async def test_count_validates_required_fields(self, query_builder):
        """Test count validates required fields"""
        with pytest.raises(ValidationError):
            await query_builder.count()

    @pytest.mark.asyncio
    async def test_count_returns_candle_count(self, query_builder, mock_repository):
        """Test count returns number of matching candles"""
        mock_repository.count_candles.return_value = 42

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .count()
        )

        assert result == 42
        mock_repository.count_candles.assert_called_once_with(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_time=None,
            end_time=None,
        )

    @pytest.mark.asyncio
    async def test_count_with_time_range(self, query_builder, mock_repository):
        """Test count with time range filter"""
        mock_repository.count_candles.return_value = 10

        start = 1609459200000
        end = 1609545600000

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .timestamp_range(start, end)
            .count()
        )

        assert result == 10
        mock_repository.count_candles.assert_called_once_with(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_time=start,
            end_time=end,
        )

    @pytest.mark.asyncio
    async def test_exists_returns_true_when_candles_exist(self, query_builder, mock_repository):
        """Test exists returns True when candles exist"""
        mock_repository.count_candles.return_value = 5

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .exists()
        )

        assert result is True

    @pytest.mark.asyncio
    async def test_exists_returns_false_when_no_candles(self, query_builder, mock_repository):
        """Test exists returns False when no candles exist"""
        mock_repository.count_candles.return_value = 0

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .exists()
        )

        assert result is False

    @pytest.mark.asyncio
    async def test_stats_validates_required_fields(self, query_builder):
        """Test stats validates required fields"""
        with pytest.raises(ValidationError):
            await query_builder.stats()

    @pytest.mark.asyncio
    async def test_stats_with_candles(self, query_builder, mock_repository, sample_candles):
        """Test stats calculation with candles"""
        mock_repository.get_candles.return_value = sample_candles

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .stats()
        )

        assert result["count"] == 3
        assert result["first_timestamp"] == sample_candles[0].timestamp
        assert result["last_timestamp"] == sample_candles[-1].timestamp
        assert result["min_price"] == 90.0  # Lowest low
        assert result["max_price"] == 120.0  # Highest high
        assert result["total_volume"] == 3300.0  # Sum of volumes

    @pytest.mark.asyncio
    async def test_stats_with_no_candles(self, query_builder, mock_repository):
        """Test stats with no candles returns empty stats"""
        mock_repository.get_candles.return_value = []

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .stats()
        )

        assert result["count"] == 0
        assert result["first_timestamp"] is None
        assert result["last_timestamp"] is None
        assert result["min_price"] is None
        assert result["max_price"] is None
        assert result["total_volume"] == 0

    @pytest.mark.asyncio
    async def test_stats_price_calculation(self, query_builder, mock_repository):
        """Test stats correctly calculates min/max prices from highs and lows"""
        candles = [
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=150.0,
                low=50.0,
                close=105.0,
                volume=1000.0,
            ),
            Candle(
                timestamp=1609459260000,
                open=105.0,
                high=125.0,
                low=75.0,
                close=110.0,
                volume=1100.0,
            ),
        ]
        mock_repository.get_candles.return_value = candles

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .stats()
        )

        # Min should be lowest low, max should be highest high
        assert result["min_price"] == 50.0
        assert result["max_price"] == 150.0

    def test_initial_state(self, query_builder):
        """Test initial state of query builder"""
        assert query_builder._exchange is None
        assert query_builder._symbol is None
        assert query_builder._timeframe is None
        assert query_builder._start_time is None
        assert query_builder._end_time is None
        assert query_builder._limit is None
        assert query_builder._offset is None
        assert query_builder._order_field == "timestamp"
        assert query_builder._order_direction == "asc"

    @pytest.mark.asyncio
    async def test_execute_empty_result(self, query_builder, mock_repository):
        """Test execute handles empty result set"""
        mock_repository.get_candles.return_value = []

        result = await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .execute()
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_multiple_queries_with_same_builder(
        self, query_builder, mock_repository, sample_candles
    ):
        """Test that the same builder can be used for multiple queries"""
        mock_repository.get_candles.return_value = sample_candles
        mock_repository.count_candles.return_value = len(sample_candles)

        # Configure builder once
        query_builder.exchange("binance").symbol("BTC/USDT").timeframe(Timeframe.ONE_HOUR)

        # Execute multiple operations
        result1 = await query_builder.execute()
        count = await query_builder.count()
        exists = await query_builder.exists()
        stats = await query_builder.stats()

        assert result1 == sample_candles
        assert count == 3
        assert exists is True
        assert stats["count"] == 3

    @pytest.mark.asyncio
    async def test_date_range_with_datetime(self, query_builder, mock_repository):
        """Test date_range method with datetime objects"""
        mock_repository.get_candles.return_value = []

        start_date = datetime(2021, 1, 1, 12, 0, 0)
        end_date = datetime(2021, 1, 2, 12, 0, 0)

        await (
            query_builder.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .date_range(start_date, end_date)
            .execute()
        )

        call_args = mock_repository.get_candles.call_args
        assert call_args.kwargs["start_time"] == int(start_date.timestamp() * 1000)
        assert call_args.kwargs["end_time"] == int(end_date.timestamp() * 1000)
