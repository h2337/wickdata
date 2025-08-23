"""
Base database interface for WickData
"""

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, List, Optional

from wickdata.models.candle import Candle
from wickdata.models.dataset_metadata import DatasetMetadata


class Database(ABC):
    """Abstract base class for database implementations"""

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the database"""
        pass

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the database"""
        pass

    @abstractmethod
    async def initialize(self) -> None:
        """Initialize database schema"""
        pass

    @abstractmethod
    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Any, None]:
        """Create a database transaction context"""
        pass

    @abstractmethod
    async def insert_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        candles: List[Candle],
    ) -> int:
        """
        Insert candles into the database

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            candles: List of candles to insert

        Returns:
            Number of candles inserted
        """
        pass

    @abstractmethod
    async def get_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: int,
        end_time: int,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Candle]:
        """
        Get candles from the database

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_time: Start timestamp (milliseconds)
            end_time: End timestamp (milliseconds)
            limit: Maximum number of candles to return
            offset: Number of candles to skip

        Returns:
            List of candles
        """
        pass

    @abstractmethod
    async def delete_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> int:
        """
        Delete candles from the database

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_time: Start timestamp (milliseconds)
            end_time: End timestamp (milliseconds)

        Returns:
            Number of candles deleted
        """
        pass

    @abstractmethod
    async def count_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> int:
        """
        Count candles in the database

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_time: Start timestamp (milliseconds)
            end_time: End timestamp (milliseconds)

        Returns:
            Number of candles
        """
        pass

    @abstractmethod
    async def get_metadata(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> Optional[DatasetMetadata]:
        """
        Get dataset metadata

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe

        Returns:
            Dataset metadata or None if not found
        """
        pass

    @abstractmethod
    async def update_metadata(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> None:
        """
        Update dataset metadata

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
        """
        pass

    @abstractmethod
    async def list_datasets(self) -> List[DatasetMetadata]:
        """
        List all datasets in the database

        Returns:
            List of dataset metadata
        """
        pass
