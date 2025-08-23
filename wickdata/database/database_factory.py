"""
Database factory for creating database instances
"""

from typing import Optional

from wickdata.core.errors import ConfigurationError
from wickdata.database.base import Database
from wickdata.database.sqlite_database import SQLiteDatabase
from wickdata.models.config import DatabaseConfig
from wickdata.utils.logger import Logger


class DatabaseFactory:
    """Factory for creating database instances"""

    @staticmethod
    def create(config: DatabaseConfig, logger: Optional[Logger] = None) -> Database:
        """
        Create a database instance from configuration

        Args:
            config: Database configuration
            logger: Logger instance

        Returns:
            Database instance

        Raises:
            ConfigurationError: If provider is not supported
        """
        if config.provider == "sqlite":
            if config.url is None:
                raise ConfigurationError(
                    "Database URL is required",
                    config_key="database.url",
                )
            return SQLiteDatabase(config.url, logger)
        else:
            raise ConfigurationError(
                f"Unsupported database provider: {config.provider}",
                config_key="database.provider",
            )

    @staticmethod
    def create_from_url(url: str, logger: Optional[Logger] = None) -> Database:
        """
        Create a database instance from URL

        Args:
            url: Database URL
            logger: Logger instance

        Returns:
            Database instance

        Raises:
            ConfigurationError: If URL format is invalid
        """
        if url.startswith("sqlite://"):
            return SQLiteDatabase(url, logger)
        else:
            raise ConfigurationError(
                f"Unsupported database URL: {url}",
                config_key="database.url",
            )
