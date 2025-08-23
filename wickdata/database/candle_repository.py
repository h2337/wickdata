"""
Repository pattern for candle data access
"""

from typing import List, Optional

from wickdata.database.base import Database
from wickdata.models.candle import Candle
from wickdata.models.data_gap import DataGap
from wickdata.models.dataset_metadata import DatasetMetadata
from wickdata.models.timeframe import Timeframe
from wickdata.utils.logger import Logger
from wickdata.utils.timeframe_utils import TimeframeUtils


class CandleRepository:
    """Repository for candle data operations"""

    def __init__(self, database: Database, logger: Optional[Logger] = None) -> None:
        """
        Initialize candle repository

        Args:
            database: Database instance
            logger: Logger instance
        """
        self.database = database
        self.logger = logger or Logger("CandleRepository")

    async def insert_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
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
        if not candles:
            return 0

        # Sort candles by timestamp
        candles = sorted(candles, key=lambda c: c.timestamp)

        # Insert candles
        inserted = await self.database.insert_candles(exchange, symbol, str(timeframe), candles)

        # Update metadata
        if inserted > 0:
            await self.database.update_metadata(exchange, symbol, str(timeframe))

        self.logger.info(
            f"Inserted {inserted}/{len(candles)} candles",
            exchange=exchange,
            symbol=symbol,
            timeframe=str(timeframe),
        )

        return inserted

    async def get_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
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
        candles = await self.database.get_candles(
            exchange, symbol, str(timeframe), start_time, end_time, limit, offset
        )

        self.logger.debug(
            f"Retrieved {len(candles)} candles",
            exchange=exchange,
            symbol=symbol,
            timeframe=str(timeframe),
        )

        return candles

    async def delete_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
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
        deleted = await self.database.delete_candles(
            exchange, symbol, str(timeframe), start_time, end_time
        )

        # Update metadata
        if deleted > 0:
            await self.database.update_metadata(exchange, symbol, str(timeframe))

        self.logger.info(
            f"Deleted {deleted} candles",
            exchange=exchange,
            symbol=symbol,
            timeframe=str(timeframe),
        )

        return deleted

    async def count_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
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
        return await self.database.count_candles(
            exchange, symbol, str(timeframe), start_time, end_time
        )

    async def find_data_gaps(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_time: int,
        end_time: int,
    ) -> List[DataGap]:
        """
        Find gaps in the data

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_time: Start timestamp (milliseconds)
            end_time: End timestamp (milliseconds)

        Returns:
            List of data gaps
        """
        # Get all candles in the range
        candles = await self.get_candles(exchange, symbol, timeframe, start_time, end_time)

        if not candles:
            # Entire range is a gap
            candle_count = TimeframeUtils.get_candle_count(start_time, end_time, timeframe)
            return [DataGap(start_time, end_time, candle_count)]

        gaps = []
        timeframe_ms = timeframe.to_milliseconds()

        # Check gap at the beginning
        if candles[0].timestamp > start_time:
            gap_start = start_time
            gap_end = candles[0].timestamp - timeframe_ms
            if gap_end >= gap_start:
                candle_count = TimeframeUtils.get_candle_count(gap_start, gap_end, timeframe)
                gaps.append(DataGap(gap_start, gap_end, candle_count))

        # Check gaps between candles
        for i in range(len(candles) - 1):
            expected_next = candles[i].timestamp + timeframe_ms
            actual_next = candles[i + 1].timestamp

            if actual_next > expected_next:
                gap_start = expected_next
                gap_end = actual_next - timeframe_ms
                candle_count = TimeframeUtils.get_candle_count(gap_start, gap_end, timeframe)
                gaps.append(DataGap(gap_start, gap_end, candle_count))

        # Check gap at the end
        last_expected = candles[-1].timestamp + timeframe_ms
        if last_expected <= end_time:
            gap_start = last_expected
            gap_end = end_time
            candle_count = TimeframeUtils.get_candle_count(gap_start, gap_end, timeframe)
            gaps.append(DataGap(gap_start, gap_end, candle_count))

        self.logger.debug(
            f"Found {len(gaps)} gaps",
            exchange=exchange,
            symbol=symbol,
            timeframe=str(timeframe),
        )

        return gaps

    async def get_metadata(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
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
        return await self.database.get_metadata(exchange, symbol, str(timeframe))

    async def update_metadata(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
    ) -> None:
        """
        Update dataset metadata

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
        """
        await self.database.update_metadata(exchange, symbol, str(timeframe))

    async def list_datasets(self) -> List[DatasetMetadata]:
        """
        List all datasets in the database

        Returns:
            List of dataset metadata
        """
        return await self.database.list_datasets()
