"""
Manager for multiple exchange connections
"""

from typing import Dict, List, Optional

from wickdata.core.errors import ConfigurationError
from wickdata.exchanges.exchange_adapter import ExchangeAdapter
from wickdata.models.config import ExchangeConfig
from wickdata.utils.logger import Logger


class ExchangeManager:
    """Manager for multiple exchange adapters"""

    def __init__(self, logger: Optional[Logger] = None) -> None:
        """
        Initialize exchange manager

        Args:
            logger: Logger instance
        """
        self.logger = logger or Logger("ExchangeManager")
        self.exchanges: Dict[str, ExchangeAdapter] = {}

    async def add_exchange(self, config: ExchangeConfig) -> ExchangeAdapter:
        """
        Add an exchange to the manager

        Args:
            config: Exchange configuration

        Returns:
            Exchange adapter
        """
        if config.exchange in self.exchanges:
            self.logger.warning(f"Exchange {config.exchange} already exists, replacing")
            await self.remove_exchange(config.exchange)

        adapter = ExchangeAdapter(config, self.logger)
        await adapter.connect()

        self.exchanges[config.exchange] = adapter
        self.logger.info(f"Added exchange: {config.exchange}")

        return adapter

    async def remove_exchange(self, name: str) -> None:
        """
        Remove an exchange from the manager

        Args:
            name: Exchange name
        """
        if name in self.exchanges:
            await self.exchanges[name].disconnect()
            del self.exchanges[name]
            self.logger.info(f"Removed exchange: {name}")

    def get_exchange(self, name: str) -> ExchangeAdapter:
        """
        Get an exchange adapter

        Args:
            name: Exchange name

        Returns:
            Exchange adapter

        Raises:
            ConfigurationError: If exchange not found
        """
        if name not in self.exchanges:
            raise ConfigurationError(
                f"Exchange '{name}' not configured",
                config_key=f"exchanges.{name}",
            )

        return self.exchanges[name]

    def has_exchange(self, name: str) -> bool:
        """
        Check if exchange exists

        Args:
            name: Exchange name

        Returns:
            True if exchange exists
        """
        return name in self.exchanges

    def list_exchanges(self) -> List[str]:
        """
        List all configured exchanges

        Returns:
            List of exchange names
        """
        return list(self.exchanges.keys())

    async def close_all(self) -> None:
        """Close all exchange connections"""
        for name in list(self.exchanges.keys()):
            await self.remove_exchange(name)
        self.logger.info("Closed all exchanges")
