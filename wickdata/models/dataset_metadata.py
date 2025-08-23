"""
Dataset metadata model for database storage
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class DatasetMetadata:
    """Metadata for a stored dataset"""

    id: Optional[int]
    exchange: str
    symbol: str
    timeframe: str
    first_timestamp: Optional[int]
    last_timestamp: Optional[int]
    candle_count: int
    last_fetch_at: Optional[datetime]
    created_at: datetime
    updated_at: Optional[datetime] = None

    def __repr__(self) -> str:
        return (
            f"DatasetMetadata(exchange={self.exchange}, symbol={self.symbol}, "
            f"timeframe={self.timeframe}, candles={self.candle_count})"
        )
