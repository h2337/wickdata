"""
Data gap model for tracking missing data periods
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class DataGap:
    """Represents a gap in the data"""

    start_time: int  # Unix timestamp in milliseconds
    end_time: int  # Unix timestamp in milliseconds
    candle_count: int  # Number of missing candles

    def get_start_datetime(self) -> datetime:
        """Get start time as datetime"""
        return datetime.fromtimestamp(self.start_time / 1000)

    def get_end_datetime(self) -> datetime:
        """Get end time as datetime"""
        return datetime.fromtimestamp(self.end_time / 1000)

    def get_duration_seconds(self) -> float:
        """Get gap duration in seconds"""
        return (self.end_time - self.start_time) / 1000

    def __repr__(self) -> str:
        return (
            f"DataGap(start={self.get_start_datetime()}, "
            f"end={self.get_end_datetime()}, candles={self.candle_count})"
        )
