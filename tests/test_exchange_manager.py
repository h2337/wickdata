"""
Unit tests for ExchangeManager
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from wickdata.core.errors import ConfigurationError
from wickdata.exchanges.exchange_adapter import ExchangeAdapter
from wickdata.exchanges.exchange_manager import ExchangeManager
from wickdata.models.config import ExchangeConfig
from wickdata.utils.logger import Logger


@pytest.fixture
def mock_logger():
    """Create mock logger"""
    return Mock(spec=Logger)


@pytest.fixture
def exchange_manager(mock_logger):
    """Create ExchangeManager instance"""
    return ExchangeManager(logger=mock_logger)


@pytest.fixture
def binance_config():
    """Create Binance exchange configuration"""
    return ExchangeConfig(
        exchange="binance", api_key="binance_key", secret="binance_secret", enable_rate_limit=True
    )


@pytest.fixture
def kraken_config():
    """Create Kraken exchange configuration"""
    return ExchangeConfig(
        exchange="kraken", api_key="kraken_key", secret="kraken_secret", enable_rate_limit=True
    )


@pytest.fixture
def mock_exchange_adapter():
    """Create mock ExchangeAdapter"""
    adapter = Mock(spec=ExchangeAdapter)
    adapter.connect = AsyncMock()
    adapter.disconnect = AsyncMock()
    return adapter


class TestExchangeManager:

    def test_initialization(self, exchange_manager):
        """Test ExchangeManager initialization"""
        assert exchange_manager.exchanges == {}
        assert exchange_manager.logger is not None

    def test_initialization_with_default_logger(self):
        """Test initialization with default logger"""
        manager = ExchangeManager()
        assert manager.logger is not None
        assert manager.exchanges == {}

    @pytest.mark.asyncio
    async def test_add_exchange_success(
        self, exchange_manager, binance_config, mock_exchange_adapter
    ):
        """Test adding an exchange successfully"""
        with patch("wickdata.exchanges.exchange_manager.ExchangeAdapter") as mock_adapter:
            mock_adapter.return_value = mock_exchange_adapter

            result = await exchange_manager.add_exchange(binance_config)

            assert result == mock_exchange_adapter
            assert exchange_manager.exchanges["binance"] == mock_exchange_adapter
            mock_adapter.assert_called_once_with(binance_config, exchange_manager.logger)
            mock_exchange_adapter.connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_add_exchange_replace_existing(
        self, exchange_manager, binance_config, mock_exchange_adapter
    ):
        """Test replacing an existing exchange"""
        # Add initial exchange
        existing_adapter = Mock(spec=ExchangeAdapter)
        existing_adapter.disconnect = AsyncMock()
        exchange_manager.exchanges["binance"] = existing_adapter

        with patch("wickdata.exchanges.exchange_manager.ExchangeAdapter") as mock_adapter:
            mock_adapter.return_value = mock_exchange_adapter

            result = await exchange_manager.add_exchange(binance_config)

            # Should disconnect existing adapter
            existing_adapter.disconnect.assert_called_once()
            # Should add new adapter
            assert result == mock_exchange_adapter
            assert exchange_manager.exchanges["binance"] == mock_exchange_adapter

    @pytest.mark.asyncio
    async def test_add_multiple_exchanges(self, exchange_manager, binance_config, kraken_config):
        """Test adding multiple exchanges"""
        mock_binance = Mock(spec=ExchangeAdapter)
        mock_binance.connect = AsyncMock()
        mock_kraken = Mock(spec=ExchangeAdapter)
        mock_kraken.connect = AsyncMock()

        with patch("wickdata.exchanges.exchange_manager.ExchangeAdapter") as mock_adapter:
            mock_adapter.side_effect = [mock_binance, mock_kraken]

            await exchange_manager.add_exchange(binance_config)
            await exchange_manager.add_exchange(kraken_config)

            assert len(exchange_manager.exchanges) == 2
            assert exchange_manager.exchanges["binance"] == mock_binance
            assert exchange_manager.exchanges["kraken"] == mock_kraken

    @pytest.mark.asyncio
    async def test_remove_exchange_exists(self, exchange_manager, mock_exchange_adapter):
        """Test removing an existing exchange"""
        exchange_manager.exchanges["binance"] = mock_exchange_adapter

        await exchange_manager.remove_exchange("binance")

        assert "binance" not in exchange_manager.exchanges
        mock_exchange_adapter.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_remove_exchange_not_exists(self, exchange_manager):
        """Test removing non-existing exchange"""
        # Should not raise error
        await exchange_manager.remove_exchange("nonexistent")

    def test_get_exchange_exists(self, exchange_manager, mock_exchange_adapter):
        """Test getting an existing exchange"""
        exchange_manager.exchanges["binance"] = mock_exchange_adapter

        result = exchange_manager.get_exchange("binance")

        assert result == mock_exchange_adapter

    def test_get_exchange_not_exists(self, exchange_manager):
        """Test getting non-existing exchange"""
        with pytest.raises(ConfigurationError) as exc_info:
            exchange_manager.get_exchange("nonexistent")

        assert "Exchange 'nonexistent' not configured" in str(exc_info.value)
        assert exc_info.value.details["config_key"] == "exchanges.nonexistent"

    def test_has_exchange_exists(self, exchange_manager, mock_exchange_adapter):
        """Test checking if exchange exists"""
        exchange_manager.exchanges["binance"] = mock_exchange_adapter

        assert exchange_manager.has_exchange("binance") is True

    def test_has_exchange_not_exists(self, exchange_manager):
        """Test checking if non-existing exchange exists"""
        assert exchange_manager.has_exchange("nonexistent") is False

    def test_list_exchanges_empty(self, exchange_manager):
        """Test listing exchanges when none configured"""
        assert exchange_manager.list_exchanges() == []

    def test_list_exchanges_multiple(self, exchange_manager):
        """Test listing multiple exchanges"""
        exchange_manager.exchanges["binance"] = Mock()
        exchange_manager.exchanges["kraken"] = Mock()
        exchange_manager.exchanges["coinbase"] = Mock()

        result = exchange_manager.list_exchanges()

        assert len(result) == 3
        assert "binance" in result
        assert "kraken" in result
        assert "coinbase" in result

    @pytest.mark.asyncio
    async def test_close_all_empty(self, exchange_manager):
        """Test closing all when no exchanges"""
        await exchange_manager.close_all()
        assert exchange_manager.exchanges == {}

    @pytest.mark.asyncio
    async def test_close_all_multiple(self, exchange_manager):
        """Test closing all exchanges"""
        # Create mock adapters
        mock_binance = Mock(spec=ExchangeAdapter)
        mock_binance.disconnect = AsyncMock()
        mock_kraken = Mock(spec=ExchangeAdapter)
        mock_kraken.disconnect = AsyncMock()

        exchange_manager.exchanges["binance"] = mock_binance
        exchange_manager.exchanges["kraken"] = mock_kraken

        await exchange_manager.close_all()

        # All exchanges should be disconnected and removed
        assert exchange_manager.exchanges == {}
        mock_binance.disconnect.assert_called_once()
        mock_kraken.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_all_maintains_order(self, exchange_manager):
        """Test that close_all removes exchanges properly"""
        # Create mock adapters
        adapters = {}
        for name in ["exchange1", "exchange2", "exchange3"]:
            adapter = Mock(spec=ExchangeAdapter)
            adapter.disconnect = AsyncMock()
            adapters[name] = adapter
            exchange_manager.exchanges[name] = adapter

        await exchange_manager.close_all()

        # All should be removed
        assert exchange_manager.exchanges == {}
        for adapter in adapters.values():
            adapter.disconnect.assert_called_once()

    @pytest.mark.asyncio
    async def test_logging_add_exchange(
        self, exchange_manager, binance_config, mock_exchange_adapter, mock_logger
    ):
        """Test logging when adding exchange"""
        with patch("wickdata.exchanges.exchange_manager.ExchangeAdapter") as mock_adapter:
            mock_adapter.return_value = mock_exchange_adapter

            await exchange_manager.add_exchange(binance_config)

            mock_logger.info.assert_called()
            assert "Added exchange: binance" in str(mock_logger.info.call_args)

    @pytest.mark.asyncio
    async def test_logging_replace_exchange(
        self, exchange_manager, binance_config, mock_exchange_adapter, mock_logger
    ):
        """Test logging when replacing exchange"""
        existing_adapter = Mock(spec=ExchangeAdapter)
        existing_adapter.disconnect = AsyncMock()
        exchange_manager.exchanges["binance"] = existing_adapter

        with patch("wickdata.exchanges.exchange_manager.ExchangeAdapter") as mock_adapter:
            mock_adapter.return_value = mock_exchange_adapter

            await exchange_manager.add_exchange(binance_config)

            mock_logger.warning.assert_called()
            assert "already exists" in str(mock_logger.warning.call_args)

    @pytest.mark.asyncio
    async def test_logging_remove_exchange(
        self, exchange_manager, mock_exchange_adapter, mock_logger
    ):
        """Test logging when removing exchange"""
        exchange_manager.exchanges["binance"] = mock_exchange_adapter

        await exchange_manager.remove_exchange("binance")

        mock_logger.info.assert_called()
        assert "Removed exchange: binance" in str(mock_logger.info.call_args)

    @pytest.mark.asyncio
    async def test_logging_close_all(self, exchange_manager, mock_logger):
        """Test logging when closing all exchanges"""
        await exchange_manager.close_all()

        mock_logger.info.assert_called()
        assert "Closed all exchanges" in str(mock_logger.info.call_args)

    @pytest.mark.asyncio
    async def test_integration_workflow(self, exchange_manager, binance_config, kraken_config):
        """Test complete workflow of adding, getting, and removing exchanges"""
        mock_binance = Mock(spec=ExchangeAdapter)
        mock_binance.connect = AsyncMock()
        mock_binance.disconnect = AsyncMock()
        mock_kraken = Mock(spec=ExchangeAdapter)
        mock_kraken.connect = AsyncMock()
        mock_kraken.disconnect = AsyncMock()

        with patch("wickdata.exchanges.exchange_manager.ExchangeAdapter") as mock_adapter:
            mock_adapter.side_effect = [mock_binance, mock_kraken]

            # Add exchanges
            await exchange_manager.add_exchange(binance_config)
            await exchange_manager.add_exchange(kraken_config)

            # Check they exist
            assert exchange_manager.has_exchange("binance")
            assert exchange_manager.has_exchange("kraken")
            assert len(exchange_manager.list_exchanges()) == 2

            # Get exchanges
            assert exchange_manager.get_exchange("binance") == mock_binance
            assert exchange_manager.get_exchange("kraken") == mock_kraken

            # Remove one
            await exchange_manager.remove_exchange("binance")
            assert not exchange_manager.has_exchange("binance")
            assert exchange_manager.has_exchange("kraken")
            assert len(exchange_manager.list_exchanges()) == 1

            # Close all
            await exchange_manager.close_all()
            assert len(exchange_manager.exchanges) == 0
