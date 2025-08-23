"""
Unit tests for DataManager component
"""

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from wickdata.core.data_manager import DataManager
from wickdata.models import (
    Candle,
    DataGap,
    DataRequest,
    DatasetInfo,
    Timeframe,
)
from wickdata.models.progress_info import ProgressInfo, ProgressStage


class TestDataManager:
    """Test cases for DataManager"""

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository"""
        return AsyncMock()

    @pytest.fixture
    def mock_exchange_manager(self):
        """Create a mock exchange manager"""
        manager = Mock()
        exchange = Mock()
        manager.get_exchange.return_value = exchange
        return manager

    @pytest.fixture
    def data_manager(self, mock_repository, mock_exchange_manager):
        """Create a DataManager instance with mocks"""
        return DataManager(mock_repository, mock_exchange_manager)

    @pytest.mark.asyncio
    async def test_fetch_historical_data_no_gaps(self, data_manager, mock_repository):
        """Test fetching historical data when no gaps exist"""
        # Setup request
        request = DataRequest(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
        )

        # Mock no gaps
        mock_repository.find_data_gaps.return_value = []
        mock_repository.count_candles.return_value = 24

        # Execute
        stats = await data_manager.fetch_historical_data(request)

        # Verify
        assert stats.exchange == "binance"
        assert stats.symbol == "BTC/USDT"
        assert stats.total_candles == 24
        assert len(stats.gaps) == 0
        mock_repository.find_data_gaps.assert_called()

    @pytest.mark.asyncio
    async def test_fetch_historical_data_with_gaps(
        self, data_manager, mock_repository, mock_exchange_manager
    ):
        """Test fetching historical data with gaps to fill"""
        request = DataRequest(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
        )

        # Mock gaps
        gaps = [DataGap(1704067200000, 1704070800000, 1)]
        mock_repository.find_data_gaps.return_value = gaps

        # Mock fetcher service
        with patch("wickdata.core.data_manager.DataFetcherService") as mock_fetcher_class:
            mock_fetcher = AsyncMock()
            mock_fetcher.fetch_multiple_gaps.return_value = [
                Candle(1704067200000, 50000, 50100, 49900, 50050, 1000)
            ]
            mock_fetcher_class.return_value = mock_fetcher

            mock_repository.insert_candles.return_value = 1
            mock_repository.count_candles.return_value = 24

            # Execute
            stats = await data_manager.fetch_historical_data(request)

            # Verify
            assert stats.total_candles == 24
            mock_fetcher.fetch_multiple_gaps.assert_called_once()
            mock_repository.insert_candles.assert_called_once()

    @pytest.mark.asyncio
    async def test_fetch_historical_data_with_progress(self, data_manager, mock_repository):
        """Test progress callback during fetch"""
        request = DataRequest(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_date=datetime(2024, 1, 1),
            end_date=datetime(2024, 1, 2),
        )

        progress_calls = []

        def progress_callback(info: ProgressInfo):
            progress_calls.append(info.stage)

        mock_repository.find_data_gaps.return_value = []
        mock_repository.count_candles.return_value = 24

        await data_manager.fetch_historical_data(request, progress_callback)

        # Should have initialization, analyzing, and complete stages
        assert ProgressStage.INITIALIZING in progress_calls
        assert ProgressStage.ANALYZING in progress_calls
        assert ProgressStage.COMPLETE in progress_calls

    @pytest.mark.asyncio
    async def test_update_latest_data(self, data_manager, mock_repository, mock_exchange_manager):
        """Test updating with latest data"""
        with patch("wickdata.core.data_manager.DataFetcherService") as mock_fetcher_class:
            mock_fetcher = AsyncMock()
            mock_fetcher.fetch_latest.return_value = [
                Candle(1704067200000, 50000, 50100, 49900, 50050, 1000),
                Candle(1704070800000, 50050, 50150, 49950, 50100, 1100),
            ]
            mock_fetcher_class.return_value = mock_fetcher

            mock_repository.insert_candles.return_value = 2

            # Execute
            inserted = await data_manager.update_latest_data(
                "binance", "BTC/USDT", Timeframe.ONE_HOUR, 100
            )

            # Verify
            assert inserted == 2
            mock_fetcher.fetch_latest.assert_called_once_with("BTC/USDT", Timeframe.ONE_HOUR, 100)
            mock_repository.insert_candles.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_historical_data(self, data_manager, mock_repository):
        """Test retrieving historical data"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)

        expected_candles = [
            Candle(1704067200000, 50000, 50100, 49900, 50050, 1000),
            Candle(1704070800000, 50050, 50150, 49950, 50100, 1100),
        ]
        mock_repository.get_candles.return_value = expected_candles

        # Execute
        candles = await data_manager.get_historical_data(
            "binance", "BTC/USDT", Timeframe.ONE_HOUR, start_date, end_date, limit=10
        )

        # Verify
        assert candles == expected_candles
        mock_repository.get_candles.assert_called_once()
        call_args = mock_repository.get_candles.call_args
        # Check that limit was passed
        assert call_args.kwargs.get("limit") == 10 or (
            len(call_args.args) > 5 and call_args.args[5] == 10
        )

    @pytest.mark.asyncio
    async def test_find_missing_data(self, data_manager, mock_repository):
        """Test finding missing data gaps"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 2)

        expected_gaps = [
            DataGap(1704067200000, 1704070800000, 1),
            DataGap(1704074400000, 1704078000000, 1),
        ]
        mock_repository.find_data_gaps.return_value = expected_gaps

        # Execute
        gaps = await data_manager.find_missing_data(
            "binance", "BTC/USDT", Timeframe.ONE_HOUR, start_date, end_date
        )

        # Verify
        assert gaps == expected_gaps
        mock_repository.find_data_gaps.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_data_stats(self, data_manager, mock_repository):
        """Test getting data statistics"""
        from wickdata.models.dataset_metadata import DatasetMetadata

        metadata = DatasetMetadata(
            id=1,
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1h",
            first_timestamp=1704067200000,
            last_timestamp=1704153600000,
            candle_count=24,
            last_fetch_at=datetime.now(),
            created_at=datetime.now(),
        )
        mock_repository.get_metadata.return_value = metadata
        mock_repository.find_data_gaps.return_value = []

        # Execute
        stats = await data_manager.get_data_stats("binance", "BTC/USDT", Timeframe.ONE_HOUR)

        # Verify
        assert stats is not None
        assert stats.exchange == "binance"
        assert stats.symbol == "BTC/USDT"
        assert stats.total_candles == 24
        assert stats.gaps == 0  # Count of gaps

    @pytest.mark.asyncio
    async def test_get_data_stats_no_data(self, data_manager, mock_repository):
        """Test getting stats when no data exists"""
        mock_repository.get_metadata.return_value = None

        stats = await data_manager.get_data_stats("binance", "BTC/USDT", Timeframe.ONE_HOUR)

        assert stats is None

    @pytest.mark.asyncio
    async def test_get_available_datasets(self, data_manager, mock_repository):
        """Test getting available datasets"""
        from wickdata.models.dataset_metadata import DatasetMetadata

        metadata_list = [
            DatasetMetadata(
                id=1,
                exchange="binance",
                symbol="BTC/USDT",
                timeframe="1h",
                first_timestamp=1704067200000,
                last_timestamp=1704153600000,
                candle_count=24,
                last_fetch_at=datetime.now(),
                created_at=datetime.now(),
            ),
            DatasetMetadata(
                id=2,
                exchange="binance",
                symbol="ETH/USDT",
                timeframe="1h",
                first_timestamp=1704067200000,
                last_timestamp=1704153600000,
                candle_count=24,
                last_fetch_at=datetime.now(),
                created_at=datetime.now(),
            ),
        ]
        mock_repository.list_datasets.return_value = metadata_list

        # Execute
        datasets = await data_manager.get_available_datasets()

        # Verify
        assert len(datasets) == 2
        assert all(isinstance(d, DatasetInfo) for d in datasets)
        assert datasets[0].exchange == "binance"
        assert datasets[0].symbol == "BTC/USDT"
        assert datasets[1].symbol == "ETH/USDT"

    @pytest.mark.asyncio
    async def test_delete_data(self, data_manager, mock_repository):
        """Test deleting data"""
        mock_repository.delete_candles.return_value = 100

        # Execute
        deleted = await data_manager.delete_data(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
        )

        # Verify
        assert deleted == 100
        mock_repository.delete_candles.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_all_data(self, data_manager, mock_repository):
        """Test deleting all data for a dataset"""
        mock_repository.delete_candles.return_value = 1000

        # Execute
        deleted = await data_manager.delete_data("binance", "BTC/USDT", Timeframe.ONE_HOUR)

        # Verify
        assert deleted == 1000
        mock_repository.delete_candles.assert_called_once_with(
            "binance", "BTC/USDT", Timeframe.ONE_HOUR, None, None
        )
