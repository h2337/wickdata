"""
Data request model for fetching historical data
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from wickdata.models.timeframe import Timeframe


@dataclass
class DataRequest:
    """Request for fetching historical data"""

    exchange: str
    symbol: str
    timeframe: Timeframe
    start_date: datetime
    end_date: datetime
    batch_size: int = 500
    concurrent_fetchers: int = 3
    rate_limit_delay: Optional[float] = None

    def __post_init__(self) -> None:
        """Validate the data request"""
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be before end_date")

        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

        if self.concurrent_fetchers <= 0:
            raise ValueError("concurrent_fetchers must be positive")

        if isinstance(self.timeframe, str):
            self.timeframe = Timeframe.from_string(self.timeframe)

    def get_start_timestamp(self) -> int:
        """Get start timestamp in milliseconds"""
        return int(self.start_date.timestamp() * 1000)

    def get_end_timestamp(self) -> int:
        """Get end timestamp in milliseconds"""
        return int(self.end_date.timestamp() * 1000)

    def __repr__(self) -> str:
        return (
            f"DataRequest(exchange={self.exchange}, symbol={self.symbol}, "
            f"timeframe={self.timeframe}, start_date={self.start_date}, "
            f"end_date={self.end_date})"
        )
