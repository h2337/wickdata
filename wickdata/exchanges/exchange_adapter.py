"""
Exchange adapter for CCXT integration
"""

from typing import Any, Dict, List, Optional

import ccxt.async_support as ccxt  # type: ignore[import-untyped]

from wickdata.core.errors import ExchangeError, NetworkError, RateLimitError
from wickdata.models.candle import Candle
from wickdata.models.config import ExchangeConfig
from wickdata.models.timeframe import Timeframe
from wickdata.utils.logger import Logger


class ExchangeAdapter:
    """Adapter for cryptocurrency exchange using CCXT"""

    def __init__(
        self,
        config: ExchangeConfig,
        logger: Optional[Logger] = None,
    ) -> None:
        """
        Initialize exchange adapter

        Args:
            config: Exchange configuration
            logger: Logger instance
        """
        self.config = config
        self.logger = logger or Logger(f"ExchangeAdapter.{config.exchange}")
        self.exchange: Optional[ccxt.Exchange] = None
        self._markets_loaded = False

    async def connect(self) -> None:
        """Connect to the exchange"""
        try:
            # Get exchange class
            exchange_class = getattr(ccxt, self.config.exchange)

            # Create exchange instance
            self.exchange = exchange_class(self.config.to_ccxt_config())

            # Load markets
            await self.load_markets()

            self.logger.info(f"Connected to {self.config.exchange}")

        except AttributeError:
            raise ExchangeError(
                f"Exchange '{self.config.exchange}' not supported by CCXT",
                exchange=self.config.exchange,
            )
        except Exception as e:
            raise ExchangeError(
                f"Failed to connect to {self.config.exchange}: {e}",
                exchange=self.config.exchange,
            )

    async def disconnect(self) -> None:
        """Disconnect from the exchange"""
        if self.exchange:
            await self.exchange.close()
            self.logger.info(f"Disconnected from {self.config.exchange}")

    async def load_markets(self, reload: bool = False) -> Dict[str, Any]:
        """
        Load market information

        Args:
            reload: Force reload markets

        Returns:
            Market information
        """
        if not self.exchange:
            raise ExchangeError("Exchange not connected", exchange=self.config.exchange)

        if not self._markets_loaded or reload:
            markets = await self.exchange.load_markets(reload)
            self._markets_loaded = True
            self.logger.debug(f"Loaded {len(markets)} markets")
            return markets  # type: ignore[no-any-return]

        return self.exchange.markets  # type: ignore[no-any-return]

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: Timeframe,
        since: Optional[int] = None,
        limit: Optional[int] = None,
    ) -> List[Candle]:
        """
        Fetch OHLCV candles from the exchange

        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe
            since: Start timestamp (milliseconds)
            limit: Maximum number of candles

        Returns:
            List of candles
        """
        if not self.exchange:
            raise ExchangeError("Exchange not connected", exchange=self.config.exchange)

        try:
            # Check if timeframe is supported
            if not self.has_timeframe(timeframe):
                raise ExchangeError(
                    f"Timeframe {timeframe} not supported by {self.config.exchange}",
                    exchange=self.config.exchange,
                )

            # Fetch OHLCV data
            ohlcv_data = await self.exchange.fetch_ohlcv(
                symbol,
                timeframe=str(timeframe),
                since=since,
                limit=limit,
            )

            # Convert to Candle objects
            candles = [Candle.from_ccxt(candle) for candle in ohlcv_data]

            self.logger.debug(
                f"Fetched {len(candles)} candles",
                symbol=symbol,
                timeframe=str(timeframe),
            )

            return candles

        except ccxt.RateLimitExceeded as e:
            raise RateLimitError(
                f"Rate limit exceeded: {e}",
                retry_after=self.exchange.rateLimit / 1000,
            )
        except ccxt.NetworkError as e:
            raise NetworkError(f"Network error: {e}")
        except ccxt.BaseError as e:
            raise ExchangeError(
                f"Exchange error: {e}",
                exchange=self.config.exchange,
            )

    async def fetch_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch ticker information

        Args:
            symbol: Trading pair symbol

        Returns:
            Ticker information
        """
        if not self.exchange:
            raise ExchangeError("Exchange not connected", exchange=self.config.exchange)

        try:
            ticker = await self.exchange.fetch_ticker(symbol)
            return ticker  # type: ignore[no-any-return]
        except ccxt.BaseError as e:
            raise ExchangeError(
                f"Failed to fetch ticker: {e}",
                exchange=self.config.exchange,
            )

    def has_timeframe(self, timeframe: Timeframe) -> bool:
        """
        Check if exchange supports a timeframe

        Args:
            timeframe: Timeframe to check

        Returns:
            True if supported, False otherwise
        """
        if not self.exchange:
            return False

        return str(timeframe) in self.exchange.timeframes

    def get_supported_timeframes(self) -> List[Timeframe]:
        """
        Get list of supported timeframes

        Returns:
            List of supported timeframes
        """
        if not self.exchange:
            return []

        supported = []
        for tf_str in self.exchange.timeframes:
            try:
                tf = Timeframe.from_string(tf_str)
                supported.append(tf)
            except ValueError:
                # Skip unsupported timeframes
                pass

        return supported

    def has_symbol(self, symbol: str) -> bool:
        """
        Check if exchange has a symbol

        Args:
            symbol: Trading pair symbol

        Returns:
            True if symbol exists, False otherwise
        """
        if not self.exchange or not self._markets_loaded:
            return False

        return symbol in self.exchange.markets

    def get_symbols(self) -> List[str]:
        """
        Get list of all symbols

        Returns:
            List of symbols
        """
        if not self.exchange or not self._markets_loaded:
            return []

        return list(self.exchange.markets.keys())

    def get_exchange_info(self) -> Dict[str, Any]:
        """
        Get exchange information

        Returns:
            Exchange information
        """
        if not self.exchange:
            return {}

        return {
            "id": self.exchange.id,
            "name": self.exchange.name,
            "has_ohlcv": self.exchange.has.get("fetchOHLCV", False),
            "has_ticker": self.exchange.has.get("fetchTicker", False),
            "rate_limit": self.exchange.rateLimit,
            "timeframes": list(self.exchange.timeframes.keys()),
            "symbols_count": len(self.exchange.markets) if self._markets_loaded else 0,
        }
