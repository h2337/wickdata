"""
Unit tests for DataFetcherService
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from wickdata.exchanges.exchange_adapter import ExchangeAdapter
from wickdata.models.candle import Candle
from wickdata.models.data_gap import DataGap
from wickdata.models.progress_info import ProgressInfo, ProgressStage
from wickdata.models.timeframe import Timeframe
from wickdata.services.data_fetcher_service import DataFetcherService
from wickdata.services.data_validation_service import DataValidationService
from wickdata.services.retry_service import RetryService
from wickdata.utils.logger import Logger


@pytest.fixture
def mock_exchange():
    """Create mock exchange adapter"""
    exchange = Mock(spec=ExchangeAdapter)
    exchange.fetch_ohlcv = AsyncMock()
    return exchange


@pytest.fixture
def mock_retry_service():
    """Create mock retry service"""
    retry_service = Mock(spec=RetryService)
    retry_service.execute = AsyncMock()
    return retry_service


@pytest.fixture
def mock_validation_service():
    """Create mock validation service"""
    validation_service = Mock(spec=DataValidationService)
    validation_service.sanitize_candles = Mock()
    return validation_service


@pytest.fixture
def mock_logger():
    """Create mock logger"""
    return Mock(spec=Logger)


@pytest.fixture
def data_fetcher_service(mock_exchange, mock_retry_service, mock_validation_service, mock_logger):
    """Create DataFetcherService instance with mocked dependencies"""
    return DataFetcherService(
        exchange=mock_exchange,
        retry_service=mock_retry_service,
        validation_service=mock_validation_service,
        logger=mock_logger,
    )


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


@pytest.fixture
def sample_gap():
    """Create sample data gap"""
    return DataGap(start_time=1609459200000, end_time=1609459320000, candle_count=3)


class TestDataFetcherService:

    @pytest.mark.asyncio
    async def test_fetch_gap_success(
        self,
        data_fetcher_service,
        mock_retry_service,
        mock_validation_service,
        sample_candles,
        sample_gap,
    ):
        """Test successful gap fetching"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        mock_retry_service.execute.return_value = sample_candles
        mock_validation_service.sanitize_candles.return_value = sample_candles

        # Execute
        result = await data_fetcher_service.fetch_gap(symbol, timeframe, sample_gap)

        # Assert
        assert result == sample_candles
        mock_retry_service.execute.assert_called_once()
        mock_validation_service.sanitize_candles.assert_called()

    @pytest.mark.asyncio
    async def test_fetch_gap_with_progress_callback(
        self,
        data_fetcher_service,
        mock_retry_service,
        mock_validation_service,
        sample_candles,
        sample_gap,
    ):
        """Test gap fetching with progress callback"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        progress_callback = Mock()
        mock_retry_service.execute.return_value = sample_candles
        mock_validation_service.sanitize_candles.return_value = sample_candles

        # Execute
        result = await data_fetcher_service.fetch_gap(
            symbol, timeframe, sample_gap, progress_callback=progress_callback
        )

        # Assert
        assert result == sample_candles
        progress_callback.assert_called()
        progress_info = progress_callback.call_args[0][0]
        assert isinstance(progress_info, ProgressInfo)
        assert progress_info.stage == ProgressStage.DOWNLOADING

    @pytest.mark.asyncio
    async def test_fetch_gap_filters_out_of_range_candles(
        self, data_fetcher_service, mock_retry_service, mock_validation_service
    ):
        """Test that candles outside gap range are filtered"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        gap = DataGap(start_time=1609459260000, end_time=1609459320000, candle_count=2)

        all_candles = [
            Candle(
                timestamp=1609459200000,
                open=100.0,
                high=110.0,
                low=90.0,
                close=105.0,
                volume=1000.0,
            ),  # Before gap
            Candle(
                timestamp=1609459260000,
                open=105.0,
                high=115.0,
                low=95.0,
                close=110.0,
                volume=1100.0,
            ),  # In gap
            Candle(
                timestamp=1609459320000,
                open=110.0,
                high=120.0,
                low=100.0,
                close=115.0,
                volume=1200.0,
            ),  # In gap
            Candle(
                timestamp=1609459380000,
                open=115.0,
                high=125.0,
                low=105.0,
                close=120.0,
                volume=1300.0,
            ),  # After gap
        ]

        mock_retry_service.execute.return_value = all_candles
        mock_validation_service.sanitize_candles.side_effect = lambda x: x

        # Execute
        result = await data_fetcher_service.fetch_gap(symbol, timeframe, gap)

        # Assert
        assert len(result) == 2
        assert result[0].timestamp == 1609459260000
        assert result[1].timestamp == 1609459320000

    @pytest.mark.asyncio
    async def test_fetch_gap_handles_exception(
        self, data_fetcher_service, mock_retry_service, sample_gap
    ):
        """Test gap fetching handles exceptions gracefully"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        mock_retry_service.execute.side_effect = Exception("Network error")

        # Execute
        result = await data_fetcher_service.fetch_gap(symbol, timeframe, sample_gap)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_multiple_gaps_success(
        self, data_fetcher_service, mock_retry_service, mock_validation_service, sample_candles
    ):
        """Test fetching multiple gaps concurrently"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        gaps = [
            DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=1),
            DataGap(start_time=1609459320000, end_time=1609459380000, candle_count=1),
        ]

        # Mock fetch_gap to return different candles for each gap
        async def mock_fetch_gap(sym, tf, gap, batch_size):
            if gap == gaps[0]:
                return [sample_candles[0]]
            else:
                return [sample_candles[1]]

        data_fetcher_service.fetch_gap = AsyncMock(side_effect=mock_fetch_gap)
        mock_validation_service.sanitize_candles.side_effect = lambda x: x

        # Execute
        result = await data_fetcher_service.fetch_multiple_gaps(symbol, timeframe, gaps)

        # Assert
        assert len(result) == 2
        assert data_fetcher_service.fetch_gap.call_count == 2

    @pytest.mark.asyncio
    async def test_fetch_multiple_gaps_empty_list(self, data_fetcher_service):
        """Test fetching with empty gaps list"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        gaps = []

        # Execute
        result = await data_fetcher_service.fetch_multiple_gaps(symbol, timeframe, gaps)

        # Assert
        assert result == []

    @pytest.mark.asyncio
    async def test_fetch_multiple_gaps_with_progress(
        self, data_fetcher_service, mock_validation_service, sample_candles
    ):
        """Test fetching multiple gaps with progress callback"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        gaps = [
            DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=2),
            DataGap(start_time=1609459320000, end_time=1609459380000, candle_count=1),
        ]
        progress_callback = Mock()

        # Mock fetch_gap
        async def mock_fetch_gap(sym, tf, gap, batch_size):
            return sample_candles[:2] if gap == gaps[0] else sample_candles[2:3]

        data_fetcher_service.fetch_gap = AsyncMock(side_effect=mock_fetch_gap)
        mock_validation_service.sanitize_candles.side_effect = lambda x: x

        # Execute
        result = await data_fetcher_service.fetch_multiple_gaps(
            symbol, timeframe, gaps, progress_callback=progress_callback
        )

        # Assert
        progress_callback.assert_called()
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_fetch_multiple_gaps_handles_exceptions(
        self, data_fetcher_service, mock_validation_service
    ):
        """Test multiple gaps fetching handles partial failures"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        gaps = [
            DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=1),
            DataGap(start_time=1609459320000, end_time=1609459380000, candle_count=1),
        ]

        # Mock fetch_gap to fail for second gap
        async def mock_fetch_gap(sym, tf, gap, batch_size):
            if gap == gaps[0]:
                return [
                    Candle(
                        timestamp=1609459200000,
                        open=100.0,
                        high=110.0,
                        low=90.0,
                        close=105.0,
                        volume=1000.0,
                    )
                ]
            else:
                raise Exception("Network error")

        data_fetcher_service.fetch_gap = AsyncMock(side_effect=mock_fetch_gap)
        mock_validation_service.sanitize_candles.side_effect = lambda x: x

        # Execute
        result = await data_fetcher_service.fetch_multiple_gaps(symbol, timeframe, gaps)

        # Assert - should still return candles from successful gap
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_fetch_latest_success(
        self, data_fetcher_service, mock_retry_service, mock_validation_service, sample_candles
    ):
        """Test fetching latest candles"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        lookback_candles = 100
        mock_retry_service.execute.return_value = sample_candles
        mock_validation_service.sanitize_candles.return_value = sample_candles

        # Execute
        result = await data_fetcher_service.fetch_latest(symbol, timeframe, lookback_candles)

        # Assert
        assert result == sample_candles
        mock_retry_service.execute.assert_called_once_with(
            data_fetcher_service.exchange.fetch_ohlcv,
            f"fetch_latest_{symbol}_{timeframe}",
            symbol,
            timeframe,
            None,
            lookback_candles,
        )

    @pytest.mark.asyncio
    async def test_fetch_latest_handles_exception(self, data_fetcher_service, mock_retry_service):
        """Test fetch_latest raises exception on failure"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        mock_retry_service.execute.side_effect = Exception("Exchange error")

        # Execute & Assert
        with pytest.raises(Exception) as exc_info:
            await data_fetcher_service.fetch_latest(symbol, timeframe)
        assert str(exc_info.value) == "Exchange error"

    @pytest.mark.asyncio
    async def test_concurrent_fetchers_limit(self, data_fetcher_service, mock_validation_service):
        """Test that concurrent fetchers limit is respected"""
        # Setup
        symbol = "BTC/USDT"
        timeframe = Timeframe.ONE_MINUTE
        gaps = [
            DataGap(start_time=i * 1000, end_time=(i + 1) * 1000, candle_count=1) for i in range(10)
        ]
        concurrent_fetchers = 3

        # Track concurrent executions
        concurrent_count = 0
        max_concurrent = 0

        async def mock_fetch_gap(sym, tf, gap, batch_size):
            nonlocal concurrent_count, max_concurrent
            concurrent_count += 1
            max_concurrent = max(max_concurrent, concurrent_count)
            await asyncio.sleep(0.01)  # Simulate some work
            concurrent_count -= 1
            return []

        data_fetcher_service.fetch_gap = AsyncMock(side_effect=mock_fetch_gap)
        mock_validation_service.sanitize_candles.side_effect = lambda x: x

        # Execute
        await data_fetcher_service.fetch_multiple_gaps(
            symbol, timeframe, gaps, concurrent_fetchers=concurrent_fetchers
        )

        # Assert
        assert max_concurrent <= concurrent_fetchers
        assert data_fetcher_service.fetch_gap.call_count == len(gaps)
