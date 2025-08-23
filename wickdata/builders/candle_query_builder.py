"""
Builder pattern for querying candle data
"""

from datetime import datetime
from typing import List, Optional

from wickdata.core.errors import ValidationError
from wickdata.database.candle_repository import CandleRepository
from wickdata.models.candle import Candle
from wickdata.models.timeframe import Timeframe


class CandleQueryBuilder:
    """Builder for querying candle data"""

    def __init__(self, repository: CandleRepository) -> None:
        """
        Initialize the query builder

        Args:
            repository: Candle repository
        """
        self.repository = repository
        self._exchange: Optional[str] = None
        self._symbol: Optional[str] = None
        self._timeframe: Optional[Timeframe] = None
        self._start_time: Optional[int] = None
        self._end_time: Optional[int] = None
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._order_field: str = "timestamp"
        self._order_direction: str = "asc"

    def exchange(self, name: str) -> "CandleQueryBuilder":
        """
        Set the exchange filter

        Args:
            name: Exchange name

        Returns:
            Builder instance for chaining
        """
        self._exchange = name
        return self

    def symbol(self, pair: str) -> "CandleQueryBuilder":
        """
        Set the symbol filter

        Args:
            pair: Trading pair symbol

        Returns:
            Builder instance for chaining
        """
        self._symbol = pair
        return self

    def timeframe(self, tf: Timeframe) -> "CandleQueryBuilder":
        """
        Set the timeframe filter

        Args:
            tf: Timeframe

        Returns:
            Builder instance for chaining
        """
        self._timeframe = tf
        return self

    def date_range(self, start: datetime, end: datetime) -> "CandleQueryBuilder":
        """
        Set the date range filter

        Args:
            start: Start date
            end: End date

        Returns:
            Builder instance for chaining
        """
        self._start_time = int(start.timestamp() * 1000)
        self._end_time = int(end.timestamp() * 1000)
        return self

    def timestamp_range(self, start: int, end: int) -> "CandleQueryBuilder":
        """
        Set the timestamp range filter

        Args:
            start: Start timestamp (milliseconds)
            end: End timestamp (milliseconds)

        Returns:
            Builder instance for chaining
        """
        self._start_time = start
        self._end_time = end
        return self

    def limit(self, count: int) -> "CandleQueryBuilder":
        """
        Set the limit for results

        Args:
            count: Maximum number of results

        Returns:
            Builder instance for chaining
        """
        self._limit = count
        return self

    def offset(self, skip: int) -> "CandleQueryBuilder":
        """
        Set the offset for results

        Args:
            skip: Number of results to skip

        Returns:
            Builder instance for chaining
        """
        self._offset = skip
        return self

    def order_by(
        self,
        field: str = "timestamp",
        direction: str = "asc",
    ) -> "CandleQueryBuilder":
        """
        Set the ordering

        Args:
            field: Field to order by
            direction: Order direction (asc or desc)

        Returns:
            Builder instance for chaining
        """
        self._order_field = field
        self._order_direction = direction.lower()
        return self

    def _validate_query(self) -> None:
        """Validate query parameters"""
        if not self._exchange:
            raise ValidationError("Exchange is required for query", field="exchange")

        if not self._symbol:
            raise ValidationError("Symbol is required for query", field="symbol")

        if not self._timeframe:
            raise ValidationError("Timeframe is required for query", field="timeframe")

    async def execute(self) -> List[Candle]:
        """
        Execute the query

        Returns:
            List of candles matching the query
        """
        self._validate_query()

        # After validation, we know these are not None
        assert self._exchange is not None
        assert self._symbol is not None
        assert self._timeframe is not None

        # Default time range if not specified
        if self._start_time is None:
            self._start_time = 0
        if self._end_time is None:
            self._end_time = int(datetime.utcnow().timestamp() * 1000)

        candles = await self.repository.get_candles(
            exchange=self._exchange,
            symbol=self._symbol,
            timeframe=self._timeframe,
            start_time=self._start_time,
            end_time=self._end_time,
            limit=self._limit,
            offset=self._offset,
        )

        # Apply ordering (repository returns in ascending order by default)
        if self._order_direction == "desc":
            candles.reverse()

        return candles

    async def count(self) -> int:
        """
        Count matching candles

        Returns:
            Number of matching candles
        """
        self._validate_query()

        # After validation, we know these are not None
        assert self._exchange is not None
        assert self._symbol is not None
        assert self._timeframe is not None

        return await self.repository.count_candles(
            exchange=self._exchange,
            symbol=self._symbol,
            timeframe=self._timeframe,
            start_time=self._start_time,
            end_time=self._end_time,
        )

    async def exists(self) -> bool:
        """
        Check if any matching candles exist

        Returns:
            True if candles exist
        """
        count = await self.count()
        return count > 0

    async def stats(self) -> dict:
        """
        Get statistics for matching candles

        Returns:
            Statistics dictionary
        """
        self._validate_query()

        candles = await self.execute()

        if not candles:
            return {
                "count": 0,
                "first_timestamp": None,
                "last_timestamp": None,
                "min_price": None,
                "max_price": None,
                "total_volume": 0,
            }

        prices = []
        volumes = []

        for candle in candles:
            prices.extend([candle.low, candle.high])
            volumes.append(candle.volume)

        return {
            "count": len(candles),
            "first_timestamp": candles[0].timestamp,
            "last_timestamp": candles[-1].timestamp,
            "min_price": min(prices),
            "max_price": max(prices),
            "total_volume": sum(volumes),
        }
