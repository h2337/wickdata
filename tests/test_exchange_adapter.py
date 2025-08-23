"""
Unit tests for ExchangeAdapter
"""

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from wickdata.core.errors import ExchangeError, NetworkError, RateLimitError
from wickdata.exchanges.exchange_adapter import ExchangeAdapter
from wickdata.models.candle import Candle
from wickdata.models.config import ExchangeConfig
from wickdata.models.timeframe import Timeframe
from wickdata.utils.logger import Logger


@pytest.fixture
def exchange_config():
    """Create exchange configuration"""
    return ExchangeConfig(
        exchange="binance",
        api_key="test_key",
        secret="test_secret",
        enable_rate_limit=True,
        options={"test": "option"},
    )


@pytest.fixture
def mock_logger():
    """Create mock logger"""
    return Mock(spec=Logger)


@pytest.fixture
def exchange_adapter(exchange_config, mock_logger):
    """Create ExchangeAdapter instance"""
    return ExchangeAdapter(config=exchange_config, logger=mock_logger)


@pytest.fixture
def mock_ccxt_exchange():
    """Create mock CCXT exchange"""
    mock_exchange = MagicMock()
    mock_exchange.close = AsyncMock()
    mock_exchange.load_markets = AsyncMock()
    mock_exchange.fetch_ohlcv = AsyncMock()
    mock_exchange.fetch_ticker = AsyncMock()
    mock_exchange.id = "binance"
    mock_exchange.name = "Binance"
    mock_exchange.rateLimit = 50
    mock_exchange.has = {"fetchOHLCV": True, "fetchTicker": True}
    mock_exchange.timeframes = {"1m": "1m", "5m": "5m", "1h": "1h", "1d": "1d"}
    mock_exchange.markets = {"BTC/USDT": {"symbol": "BTC/USDT"}, "ETH/USDT": {"symbol": "ETH/USDT"}}
    return mock_exchange


class TestExchangeAdapter:

    @pytest.mark.asyncio
    async def test_connect_success(self, exchange_adapter, mock_ccxt_exchange):
        """Test successful connection to exchange"""
        with patch("ccxt.async_support.binance") as mock_binance_class:
            mock_binance_class.return_value = mock_ccxt_exchange
            mock_ccxt_exchange.load_markets.return_value = {"BTC/USDT": {}}

            await exchange_adapter.connect()

            assert exchange_adapter.exchange == mock_ccxt_exchange
            assert exchange_adapter._markets_loaded is True
            mock_ccxt_exchange.load_markets.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_unsupported_exchange(self, mock_logger):
        """Test connecting to unsupported exchange"""
        config = ExchangeConfig(exchange="nonexistent_exchange")
        adapter = ExchangeAdapter(config=config, logger=mock_logger)

        with pytest.raises(ExchangeError) as exc_info:
            await adapter.connect()
        assert "not supported by CCXT" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_connect_general_exception(self, exchange_adapter, mock_ccxt_exchange):
        """Test connection failure with general exception"""
        with patch("ccxt.async_support.binance") as mock_binance_class:
            mock_binance_class.return_value = mock_ccxt_exchange
            mock_ccxt_exchange.load_markets.side_effect = Exception("Connection failed")

            with pytest.raises(ExchangeError) as exc_info:
                await exchange_adapter.connect()
            assert "Failed to connect" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_disconnect(self, exchange_adapter, mock_ccxt_exchange):
        """Test disconnecting from exchange"""
        exchange_adapter.exchange = mock_ccxt_exchange

        await exchange_adapter.disconnect()

        mock_ccxt_exchange.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_disconnect_no_exchange(self, exchange_adapter):
        """Test disconnecting when not connected"""
        await exchange_adapter.disconnect()  # Should not raise

    @pytest.mark.asyncio
    async def test_load_markets_not_connected(self, exchange_adapter):
        """Test loading markets when not connected"""
        with pytest.raises(ExchangeError) as exc_info:
            await exchange_adapter.load_markets()
        assert "Exchange not connected" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_load_markets_success(self, exchange_adapter, mock_ccxt_exchange):
        """Test loading markets successfully"""
        exchange_adapter.exchange = mock_ccxt_exchange
        markets = {"BTC/USDT": {}, "ETH/USDT": {}}
        mock_ccxt_exchange.load_markets.return_value = markets

        result = await exchange_adapter.load_markets()

        assert result == markets
        assert exchange_adapter._markets_loaded is True
        mock_ccxt_exchange.load_markets.assert_called_once_with(False)

    @pytest.mark.asyncio
    async def test_load_markets_reload(self, exchange_adapter, mock_ccxt_exchange):
        """Test reloading markets"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = True
        markets = {"BTC/USDT": {}, "ETH/USDT": {}}
        mock_ccxt_exchange.load_markets.return_value = markets

        result = await exchange_adapter.load_markets(reload=True)

        assert result == markets
        mock_ccxt_exchange.load_markets.assert_called_once_with(True)

    @pytest.mark.asyncio
    async def test_load_markets_cached(self, exchange_adapter, mock_ccxt_exchange):
        """Test loading markets from cache"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = True

        result = await exchange_adapter.load_markets()

        assert result == mock_ccxt_exchange.markets
        mock_ccxt_exchange.load_markets.assert_not_called()

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_not_connected(self, exchange_adapter):
        """Test fetching OHLCV when not connected"""
        with pytest.raises(ExchangeError) as exc_info:
            await exchange_adapter.fetch_ohlcv("BTC/USDT", Timeframe.ONE_HOUR)
        assert "Exchange not connected" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_unsupported_timeframe(self, exchange_adapter, mock_ccxt_exchange):
        """Test fetching OHLCV with unsupported timeframe"""
        exchange_adapter.exchange = mock_ccxt_exchange
        mock_ccxt_exchange.timeframes = {"1h": "1h"}  # Only 1h supported

        with pytest.raises(ExchangeError) as exc_info:
            await exchange_adapter.fetch_ohlcv("BTC/USDT", Timeframe.ONE_MINUTE)
        assert "Timeframe 1m not supported" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_success(self, exchange_adapter, mock_ccxt_exchange):
        """Test successful OHLCV fetching"""
        exchange_adapter.exchange = mock_ccxt_exchange
        ohlcv_data = [
            [1609459200000, 100.0, 110.0, 90.0, 105.0, 1000.0],
            [1609459260000, 105.0, 115.0, 95.0, 110.0, 1100.0],
        ]
        mock_ccxt_exchange.fetch_ohlcv.return_value = ohlcv_data

        result = await exchange_adapter.fetch_ohlcv(
            "BTC/USDT", Timeframe.ONE_MINUTE, since=1609459200000, limit=2
        )

        assert len(result) == 2
        assert isinstance(result[0], Candle)
        assert result[0].timestamp == 1609459200000
        assert result[0].open == 100.0
        mock_ccxt_exchange.fetch_ohlcv.assert_called_once_with(
            "BTC/USDT", timeframe="1m", since=1609459200000, limit=2
        )

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_rate_limit_error(self, exchange_adapter, mock_ccxt_exchange):
        """Test OHLCV fetching with rate limit error"""
        exchange_adapter.exchange = mock_ccxt_exchange

        # Import ccxt exceptions
        import ccxt.async_support as ccxt

        mock_ccxt_exchange.fetch_ohlcv.side_effect = ccxt.RateLimitExceeded("Rate limit")

        with pytest.raises(RateLimitError) as exc_info:
            await exchange_adapter.fetch_ohlcv("BTC/USDT", Timeframe.ONE_HOUR)
        assert exc_info.value.retry_after == 0.05  # rateLimit / 1000

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_network_error(self, exchange_adapter, mock_ccxt_exchange):
        """Test OHLCV fetching with network error"""
        exchange_adapter.exchange = mock_ccxt_exchange

        import ccxt.async_support as ccxt

        mock_ccxt_exchange.fetch_ohlcv.side_effect = ccxt.NetworkError("Network down")

        with pytest.raises(NetworkError) as exc_info:
            await exchange_adapter.fetch_ohlcv("BTC/USDT", Timeframe.ONE_HOUR)
        assert "Network error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_ohlcv_exchange_error(self, exchange_adapter, mock_ccxt_exchange):
        """Test OHLCV fetching with exchange error"""
        exchange_adapter.exchange = mock_ccxt_exchange

        import ccxt.async_support as ccxt

        mock_ccxt_exchange.fetch_ohlcv.side_effect = ccxt.ExchangeError("Exchange error")

        with pytest.raises(ExchangeError) as exc_info:
            await exchange_adapter.fetch_ohlcv("BTC/USDT", Timeframe.ONE_HOUR)
        assert "Exchange error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_ticker_not_connected(self, exchange_adapter):
        """Test fetching ticker when not connected"""
        with pytest.raises(ExchangeError) as exc_info:
            await exchange_adapter.fetch_ticker("BTC/USDT")
        assert "Exchange not connected" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_fetch_ticker_success(self, exchange_adapter, mock_ccxt_exchange):
        """Test successful ticker fetching"""
        exchange_adapter.exchange = mock_ccxt_exchange
        ticker_data = {"symbol": "BTC/USDT", "last": 50000.0}
        mock_ccxt_exchange.fetch_ticker.return_value = ticker_data

        result = await exchange_adapter.fetch_ticker("BTC/USDT")

        assert result == ticker_data
        mock_ccxt_exchange.fetch_ticker.assert_called_once_with("BTC/USDT")

    @pytest.mark.asyncio
    async def test_fetch_ticker_error(self, exchange_adapter, mock_ccxt_exchange):
        """Test ticker fetching with error"""
        exchange_adapter.exchange = mock_ccxt_exchange

        import ccxt.async_support as ccxt

        mock_ccxt_exchange.fetch_ticker.side_effect = ccxt.BaseError("Ticker error")

        with pytest.raises(ExchangeError) as exc_info:
            await exchange_adapter.fetch_ticker("BTC/USDT")
        assert "Failed to fetch ticker" in str(exc_info.value)

    def test_has_timeframe_not_connected(self, exchange_adapter):
        """Test checking timeframe when not connected"""
        assert exchange_adapter.has_timeframe(Timeframe.ONE_HOUR) is False

    def test_has_timeframe_supported(self, exchange_adapter, mock_ccxt_exchange):
        """Test checking supported timeframe"""
        exchange_adapter.exchange = mock_ccxt_exchange
        assert exchange_adapter.has_timeframe(Timeframe.ONE_HOUR) is True

    def test_has_timeframe_not_supported(self, exchange_adapter, mock_ccxt_exchange):
        """Test checking unsupported timeframe"""
        exchange_adapter.exchange = mock_ccxt_exchange
        mock_ccxt_exchange.timeframes = {"1d": "1d"}  # Only daily
        assert exchange_adapter.has_timeframe(Timeframe.ONE_MINUTE) is False

    def test_get_supported_timeframes_not_connected(self, exchange_adapter):
        """Test getting supported timeframes when not connected"""
        assert exchange_adapter.get_supported_timeframes() == []

    def test_get_supported_timeframes_success(self, exchange_adapter, mock_ccxt_exchange):
        """Test getting supported timeframes"""
        exchange_adapter.exchange = mock_ccxt_exchange
        mock_ccxt_exchange.timeframes = {
            "1m": "1m",
            "1h": "1h",
            "1d": "1d",
            "invalid": "invalid",  # Should be skipped
        }

        result = exchange_adapter.get_supported_timeframes()

        assert len(result) == 3
        assert Timeframe.ONE_MINUTE in result
        assert Timeframe.ONE_HOUR in result
        assert Timeframe.ONE_DAY in result

    def test_has_symbol_not_connected(self, exchange_adapter):
        """Test checking symbol when not connected"""
        assert exchange_adapter.has_symbol("BTC/USDT") is False

    def test_has_symbol_markets_not_loaded(self, exchange_adapter, mock_ccxt_exchange):
        """Test checking symbol when markets not loaded"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = False
        assert exchange_adapter.has_symbol("BTC/USDT") is False

    def test_has_symbol_exists(self, exchange_adapter, mock_ccxt_exchange):
        """Test checking existing symbol"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = True
        assert exchange_adapter.has_symbol("BTC/USDT") is True

    def test_has_symbol_not_exists(self, exchange_adapter, mock_ccxt_exchange):
        """Test checking non-existing symbol"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = True
        assert exchange_adapter.has_symbol("INVALID/PAIR") is False

    def test_get_symbols_not_connected(self, exchange_adapter):
        """Test getting symbols when not connected"""
        assert exchange_adapter.get_symbols() == []

    def test_get_symbols_markets_not_loaded(self, exchange_adapter, mock_ccxt_exchange):
        """Test getting symbols when markets not loaded"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = False
        assert exchange_adapter.get_symbols() == []

    def test_get_symbols_success(self, exchange_adapter, mock_ccxt_exchange):
        """Test getting all symbols"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = True

        result = exchange_adapter.get_symbols()

        assert len(result) == 2
        assert "BTC/USDT" in result
        assert "ETH/USDT" in result

    def test_get_exchange_info_not_connected(self, exchange_adapter):
        """Test getting exchange info when not connected"""
        assert exchange_adapter.get_exchange_info() == {}

    def test_get_exchange_info_success(self, exchange_adapter, mock_ccxt_exchange):
        """Test getting exchange info"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = True

        info = exchange_adapter.get_exchange_info()

        assert info["id"] == "binance"
        assert info["name"] == "Binance"
        assert info["has_ohlcv"] is True
        assert info["has_ticker"] is True
        assert info["rate_limit"] == 50
        assert len(info["timeframes"]) == 4
        assert info["symbols_count"] == 2

    def test_get_exchange_info_markets_not_loaded(self, exchange_adapter, mock_ccxt_exchange):
        """Test getting exchange info when markets not loaded"""
        exchange_adapter.exchange = mock_ccxt_exchange
        exchange_adapter._markets_loaded = False

        info = exchange_adapter.get_exchange_info()

        assert info["symbols_count"] == 0

    def test_initialization_with_default_logger(self, exchange_config):
        """Test adapter initialization with default logger"""
        adapter = ExchangeAdapter(config=exchange_config)

        assert adapter.config == exchange_config
        assert adapter.logger is not None
        assert adapter.exchange is None
        assert adapter._markets_loaded is False

    def test_config_to_ccxt_config(self, exchange_config):
        """Test converting config to CCXT format"""
        ccxt_config = exchange_config.to_ccxt_config()

        assert ccxt_config["apiKey"] == "test_key"
        assert ccxt_config["secret"] == "test_secret"
        assert ccxt_config["enableRateLimit"] is True
        assert ccxt_config["options"] == {"test": "option"}
        assert "password" not in ccxt_config

    def test_config_to_ccxt_config_with_password(self):
        """Test converting config with password to CCXT format"""
        config = ExchangeConfig(exchange="okex", api_key="key", secret="secret", password="pass")

        ccxt_config = config.to_ccxt_config()

        assert ccxt_config["password"] == "pass"
