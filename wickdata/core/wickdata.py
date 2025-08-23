"""
Main WickData class - entry point for the library
"""

from typing import Any, Dict, Optional, Union

from wickdata.core.data_manager import DataManager
from wickdata.core.data_streamer import DataStreamer
from wickdata.database.base import Database
from wickdata.database.candle_repository import CandleRepository
from wickdata.database.database_factory import DatabaseFactory
from wickdata.exchanges.exchange_manager import ExchangeManager
from wickdata.models.config import ExchangeConfig, WickDataConfig
from wickdata.utils.logger import Logger


class WickData:
    """Main class for WickData library"""

    def __init__(
        self,
        config: Union[WickDataConfig, Dict[str, ExchangeConfig]],
        logger: Optional[Logger] = None,
    ) -> None:
        """
        Initialize WickData

        Args:
            config: WickData configuration or dictionary of exchange configs
            logger: Logger instance
        """
        # Handle config types
        if isinstance(config, dict):
            # Convert dict of exchange configs to WickDataConfig
            self.config = WickDataConfig(exchanges=config)
        else:
            self.config = config

        # Initialize logger
        self.logger = logger or Logger("WickData", level=self.config.log_level)

        # Initialize components
        self.database: Optional[Database] = None
        self.repository: Optional[CandleRepository] = None
        self.exchange_manager: Optional[ExchangeManager] = None
        self.data_manager: Optional[DataManager] = None
        self.data_streamer: Optional[DataStreamer] = None

        self._initialized = False

    async def initialize(self) -> None:
        """Initialize WickData components"""
        if self._initialized:
            return

        try:
            self.logger.info("Initializing WickData")

            # Create database
            if self.config.database is None:
                raise RuntimeError("Database configuration is required")
            self.database = DatabaseFactory.create(self.config.database, self.logger)
            await self.database.connect()
            await self.database.initialize()

            # Create repository
            self.repository = CandleRepository(self.database, self.logger)

            # Create exchange manager
            self.exchange_manager = ExchangeManager(self.logger)

            # Add configured exchanges
            for _name, exchange_config in self.config.exchanges.items():
                await self.exchange_manager.add_exchange(exchange_config)

            # Create data manager
            self.data_manager = DataManager(
                repository=self.repository,
                exchange_manager=self.exchange_manager,
                logger=self.logger,
            )

            # Create data streamer
            self.data_streamer = DataStreamer(
                repository=self.repository,
                logger=self.logger,
            )

            self._initialized = True
            self.logger.info("WickData initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize WickData: {e}")
            await self.close()
            raise

    def get_data_manager(self) -> DataManager:
        """
        Get the data manager instance

        Returns:
            DataManager instance

        Raises:
            RuntimeError: If not initialized
        """
        if not self._initialized:
            raise RuntimeError("WickData not initialized. Call initialize() first.")
        if self.data_manager is None:
            raise RuntimeError("Data manager not initialized")
        return self.data_manager

    def get_data_streamer(self) -> DataStreamer:
        """
        Get the data streamer instance

        Returns:
            DataStreamer instance

        Raises:
            RuntimeError: If not initialized
        """
        if not self._initialized:
            raise RuntimeError("WickData not initialized. Call initialize() first.")
        if self.data_streamer is None:
            raise RuntimeError("Data streamer not initialized")
        return self.data_streamer

    def get_repository(self) -> CandleRepository:
        """
        Get the repository instance

        Returns:
            CandleRepository instance

        Raises:
            RuntimeError: If not initialized
        """
        if not self._initialized:
            raise RuntimeError("WickData not initialized. Call initialize() first.")
        if self.repository is None:
            raise RuntimeError("Repository not initialized")
        return self.repository

    def get_exchange_manager(self) -> ExchangeManager:
        """
        Get the exchange manager instance

        Returns:
            ExchangeManager instance

        Raises:
            RuntimeError: If not initialized
        """
        if not self._initialized:
            raise RuntimeError("WickData not initialized. Call initialize() first.")
        if self.exchange_manager is None:
            raise RuntimeError("Exchange manager not initialized")
        return self.exchange_manager

    async def close(self) -> None:
        """Close all connections and clean up resources"""
        self.logger.info("Closing WickData")

        # Close exchange connections
        if self.exchange_manager:
            await self.exchange_manager.close_all()

        # Close database connection
        if self.database:
            await self.database.disconnect()

        self._initialized = False
        self.logger.info("WickData closed")

    async def __aenter__(self) -> "WickData":
        """Async context manager entry"""
        await self.initialize()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit"""
        await self.close()
