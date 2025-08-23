"""
Dataset information model
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from wickdata.models.timeframe import Timeframe


@dataclass
class DatasetInfo:
    """Information about a stored dataset"""

    exchange: str
    symbol: str
    timeframe: Timeframe
    first_timestamp: Optional[int]  # Unix timestamp in milliseconds
    last_timestamp: Optional[int]  # Unix timestamp in milliseconds
    candle_count: int
    last_fetch_at: Optional[datetime]
    created_at: datetime

    def get_first_datetime(self) -> Optional[datetime]:
        """Get first timestamp as datetime"""
        if self.first_timestamp is None:
            return None
        return datetime.fromtimestamp(self.first_timestamp / 1000)

    def get_last_datetime(self) -> Optional[datetime]:
        """Get last timestamp as datetime"""
        if self.last_timestamp is None:
            return None
        return datetime.fromtimestamp(self.last_timestamp / 1000)

    def is_empty(self) -> bool:
        """Check if dataset is empty"""
        return self.candle_count == 0

    def __repr__(self) -> str:
        return (
            f"DatasetInfo(exchange={self.exchange}, symbol={self.symbol}, "
            f"timeframe={self.timeframe}, candles={self.candle_count})"
        )
