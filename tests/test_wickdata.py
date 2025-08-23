"""
Unit tests for main WickData class
"""

from unittest.mock import AsyncMock, patch

import pytest

from wickdata import DatabaseConfig, ExchangeConfig, WickData, WickDataConfig
from wickdata.utils.logger import Logger


class TestWickData:
    """Test cases for WickData main class"""

    @pytest.mark.asyncio
    async def test_initialization_with_dict_config(self):
        """Test WickData initialization with dictionary of exchange configs"""
        exchange_configs = {
            "binance": ExchangeConfig(exchange="binance"),
            "kraken": ExchangeConfig(exchange="kraken"),
        }

        wickdata = WickData(exchange_configs)
        assert isinstance(wickdata.config, WickDataConfig)
        assert len(wickdata.config.exchanges) == 2
        assert "binance" in wickdata.config.exchanges
        assert not wickdata._initialized

    def test_initialization_with_wickdata_config(self):
        """Test WickData initialization with WickDataConfig"""
        config = WickDataConfig(
            exchanges={"binance": ExchangeConfig(exchange="binance")},
            database=DatabaseConfig(provider="sqlite", url="sqlite:///test.db"),
            log_level="DEBUG",
        )

        wickdata = WickData(config)
        assert wickdata.config == config
        assert wickdata.config.log_level == "DEBUG"
        assert not wickdata._initialized

    def test_custom_logger(self):
        """Test WickData with custom logger"""
        custom_logger = Logger("CustomLogger", level="WARNING")
        wickdata = WickData({}, logger=custom_logger)
        assert wickdata.logger == custom_logger

    @pytest.mark.asyncio
    async def test_initialize_success(self):
        """Test successful initialization"""
        with (
            patch("wickdata.core.wickdata.DatabaseFactory") as mock_db_factory,
            patch("wickdata.core.wickdata.CandleRepository"),
            patch("wickdata.core.wickdata.ExchangeManager") as mock_exchange_mgr,
            patch("wickdata.core.wickdata.DataManager"),
            patch("wickdata.core.wickdata.DataStreamer"),
        ):

            # Setup mocks
            mock_db = AsyncMock()
            mock_db_factory.create.return_value = mock_db

            mock_exchange = AsyncMock()
            mock_exchange_mgr.return_value = mock_exchange

            # Create and initialize WickData
            wickdata = WickData({"binance": ExchangeConfig(exchange="binance")})
            await wickdata.initialize()

            # Verify initialization
            assert wickdata._initialized
            mock_db.connect.assert_called_once()
            mock_db.initialize.assert_called_once()
            mock_exchange.add_exchange.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_idempotent(self):
        """Test that initialize is idempotent"""
        with (
            patch("wickdata.core.wickdata.DatabaseFactory") as mock_db_factory,
            patch("wickdata.core.wickdata.CandleRepository"),
            patch("wickdata.core.wickdata.ExchangeManager"),
            patch("wickdata.core.wickdata.DataManager"),
            patch("wickdata.core.wickdata.DataStreamer"),
        ):

            mock_db = AsyncMock()
            mock_db_factory.create.return_value = mock_db

            wickdata = WickData({})
            await wickdata.initialize()
            await wickdata.initialize()  # Second call should do nothing

            # Database should only be connected once
            mock_db.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_initialize_failure_cleanup(self):
        """Test cleanup on initialization failure"""
        with patch("wickdata.core.wickdata.DatabaseFactory") as mock_db_factory:
            mock_db = AsyncMock()
            mock_db.connect.side_effect = Exception("Connection failed")
            mock_db_factory.create.return_value = mock_db

            wickdata = WickData({})
            with pytest.raises(Exception, match="Connection failed"):
                await wickdata.initialize()

            # Should attempt cleanup
            assert not wickdata._initialized

    def test_get_data_manager_not_initialized(self):
        """Test getting data manager before initialization"""
        wickdata = WickData({})
        with pytest.raises(RuntimeError, match="not initialized"):
            wickdata.get_data_manager()

    def test_get_data_streamer_not_initialized(self):
        """Test getting data streamer before initialization"""
        wickdata = WickData({})
        with pytest.raises(RuntimeError, match="not initialized"):
            wickdata.get_data_streamer()

    def test_get_repository_not_initialized(self):
        """Test getting repository before initialization"""
        wickdata = WickData({})
        with pytest.raises(RuntimeError, match="not initialized"):
            wickdata.get_repository()

    def test_get_exchange_manager_not_initialized(self):
        """Test getting exchange manager before initialization"""
        wickdata = WickData({})
        with pytest.raises(RuntimeError, match="not initialized"):
            wickdata.get_exchange_manager()

    @pytest.mark.asyncio
    async def test_get_components_after_initialization(self):
        """Test getting components after initialization"""
        with (
            patch("wickdata.core.wickdata.DatabaseFactory"),
            patch("wickdata.core.wickdata.CandleRepository") as mock_repo,
            patch("wickdata.core.wickdata.ExchangeManager") as mock_exchange_mgr,
            patch("wickdata.core.wickdata.DataManager") as mock_data_mgr,
            patch("wickdata.core.wickdata.DataStreamer") as mock_streamer,
        ):

            wickdata = WickData({})
            wickdata._initialized = True  # Simulate initialization
            wickdata.data_manager = mock_data_mgr()
            wickdata.data_streamer = mock_streamer()
            wickdata.repository = mock_repo()
            wickdata.exchange_manager = mock_exchange_mgr()

            assert wickdata.get_data_manager() == wickdata.data_manager
            assert wickdata.get_data_streamer() == wickdata.data_streamer
            assert wickdata.get_repository() == wickdata.repository
            assert wickdata.get_exchange_manager() == wickdata.exchange_manager

    @pytest.mark.asyncio
    async def test_close(self):
        """Test closing WickData"""
        with (
            patch("wickdata.core.wickdata.DatabaseFactory") as mock_db_factory,
            patch("wickdata.core.wickdata.ExchangeManager"),
        ):

            mock_db = AsyncMock()
            mock_db_factory.create.return_value = mock_db
            mock_exchange = AsyncMock()

            wickdata = WickData({})
            wickdata.database = mock_db
            wickdata.exchange_manager = mock_exchange
            wickdata._initialized = True

            await wickdata.close()

            mock_exchange.close_all.assert_called_once()
            mock_db.disconnect.assert_called_once()
            assert not wickdata._initialized

    @pytest.mark.asyncio
    async def test_context_manager(self):
        """Test WickData as async context manager"""
        with (
            patch("wickdata.core.wickdata.DatabaseFactory") as mock_db_factory,
            patch("wickdata.core.wickdata.CandleRepository"),
            patch("wickdata.core.wickdata.ExchangeManager") as mock_exchange_mgr,
            patch("wickdata.core.wickdata.DataManager"),
            patch("wickdata.core.wickdata.DataStreamer"),
        ):

            mock_db = AsyncMock()
            mock_db_factory.create.return_value = mock_db
            mock_exchange = AsyncMock()
            mock_exchange_mgr.return_value = mock_exchange

            async with WickData({}) as wickdata:
                assert wickdata._initialized
                mock_db.connect.assert_called_once()

            # Should be closed after context
            mock_exchange.close_all.assert_called_once()
            mock_db.disconnect.assert_called_once()
