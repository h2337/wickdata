"""
Builder pattern for creating data requests
"""

from datetime import datetime, timedelta, timezone
from typing import Optional, Union

from wickdata.core.errors import ValidationError
from wickdata.models.data_request import DataRequest
from wickdata.models.timeframe import Timeframe


class DataRequestBuilder:
    """Builder for creating DataRequest objects"""

    def __init__(self) -> None:
        """Initialize the builder"""
        self._exchange: Optional[str] = None
        self._symbol: Optional[str] = None
        self._timeframe: Optional[Timeframe] = None
        self._start_date: Optional[datetime] = None
        self._end_date: Optional[datetime] = None
        self._batch_size: int = 500
        self._concurrent_fetchers: int = 3
        self._rate_limit_delay: Optional[float] = None

    @classmethod
    def create(cls) -> "DataRequestBuilder":
        """Create a new builder instance"""
        return cls()

    def with_exchange(self, exchange: str) -> "DataRequestBuilder":
        """
        Set the exchange

        Args:
            exchange: Exchange name

        Returns:
            Builder instance for chaining
        """
        self._exchange = exchange
        return self

    def with_symbol(self, symbol: str) -> "DataRequestBuilder":
        """
        Set the trading pair symbol

        Args:
            symbol: Trading pair symbol

        Returns:
            Builder instance for chaining
        """
        self._symbol = symbol
        return self

    def with_timeframe(self, timeframe: Union[str, Timeframe]) -> "DataRequestBuilder":
        """
        Set the timeframe

        Args:
            timeframe: Timeframe string or enum

        Returns:
            Builder instance for chaining
        """
        if isinstance(timeframe, str):
            self._timeframe = Timeframe.from_string(timeframe)
        else:
            self._timeframe = timeframe
        return self

    def with_date_range(
        self,
        start: Union[datetime, str],
        end: Union[datetime, str],
    ) -> "DataRequestBuilder":
        """
        Set the date range

        Args:
            start: Start date
            end: End date

        Returns:
            Builder instance for chaining
        """
        if isinstance(start, str):
            self._start_date = datetime.fromisoformat(start)
        else:
            self._start_date = start

        if isinstance(end, str):
            self._end_date = datetime.fromisoformat(end)
        else:
            self._end_date = end

        return self

    def with_last_days(self, days: int) -> "DataRequestBuilder":
        """
        Set date range for the last N days

        Args:
            days: Number of days

        Returns:
            Builder instance for chaining
        """
        self._end_date = datetime.now(timezone.utc).replace(tzinfo=None)
        self._start_date = self._end_date - timedelta(days=days)
        return self

    def with_last_hours(self, hours: int) -> "DataRequestBuilder":
        """
        Set date range for the last N hours

        Args:
            hours: Number of hours

        Returns:
            Builder instance for chaining
        """
        self._end_date = datetime.now(timezone.utc).replace(tzinfo=None)
        self._start_date = self._end_date - timedelta(hours=hours)
        return self

    def with_last_weeks(self, weeks: int) -> "DataRequestBuilder":
        """
        Set date range for the last N weeks

        Args:
            weeks: Number of weeks

        Returns:
            Builder instance for chaining
        """
        self._end_date = datetime.now(timezone.utc).replace(tzinfo=None)
        self._start_date = self._end_date - timedelta(weeks=weeks)
        return self

    def with_month_to_date(self) -> "DataRequestBuilder":
        """
        Set date range from start of current month to now

        Returns:
            Builder instance for chaining
        """
        self._end_date = datetime.now(timezone.utc).replace(tzinfo=None)
        self._start_date = self._end_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return self

    def with_year_to_date(self) -> "DataRequestBuilder":
        """
        Set date range from start of current year to now

        Returns:
            Builder instance for chaining
        """
        self._end_date = datetime.now(timezone.utc).replace(tzinfo=None)
        self._start_date = self._end_date.replace(
            month=1, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        return self

    def with_batch_size(self, batch_size: int) -> "DataRequestBuilder":
        """
        Set the batch size for fetching

        Args:
            batch_size: Batch size

        Returns:
            Builder instance for chaining
        """
        self._batch_size = batch_size
        return self

    def with_concurrent_fetchers(self, concurrent_fetchers: int) -> "DataRequestBuilder":
        """
        Set the number of concurrent fetchers

        Args:
            concurrent_fetchers: Number of concurrent fetchers

        Returns:
            Builder instance for chaining
        """
        self._concurrent_fetchers = concurrent_fetchers
        return self

    def with_rate_limit_delay(self, delay: float) -> "DataRequestBuilder":
        """
        Set the rate limit delay

        Args:
            delay: Delay in seconds

        Returns:
            Builder instance for chaining
        """
        self._rate_limit_delay = delay
        return self

    def build(self) -> DataRequest:
        """
        Build the DataRequest object

        Returns:
            DataRequest object

        Raises:
            ValidationError: If required fields are missing
        """
        if not self._exchange:
            raise ValidationError("Exchange is required", field="exchange")

        if not self._symbol:
            raise ValidationError("Symbol is required", field="symbol")

        if not self._timeframe:
            raise ValidationError("Timeframe is required", field="timeframe")

        if not self._start_date:
            raise ValidationError("Start date is required", field="start_date")

        if not self._end_date:
            raise ValidationError("End date is required", field="end_date")

        return DataRequest(
            exchange=self._exchange,
            symbol=self._symbol,
            timeframe=self._timeframe,
            start_date=self._start_date,
            end_date=self._end_date,
            batch_size=self._batch_size,
            concurrent_fetchers=self._concurrent_fetchers,
            rate_limit_delay=self._rate_limit_delay,
        )
