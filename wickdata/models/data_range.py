"""
Data range model for representing time ranges
"""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class DataRange:
    """Represents a time range for data"""

    start: datetime
    end: datetime

    def __post_init__(self) -> None:
        """Validate the data range"""
        if self.start > self.end:
            raise ValueError("start must be before or equal to end")

    def get_start_timestamp(self) -> int:
        """Get start timestamp in milliseconds"""
        return int(self.start.timestamp() * 1000)

    def get_end_timestamp(self) -> int:
        """Get end timestamp in milliseconds"""
        return int(self.end.timestamp() * 1000)

    def get_duration_seconds(self) -> float:
        """Get duration in seconds"""
        return (self.end - self.start).total_seconds()

    def contains(self, timestamp: int) -> bool:
        """Check if timestamp is within range"""
        return self.get_start_timestamp() <= timestamp <= self.get_end_timestamp()

    def overlaps(self, other: "DataRange") -> bool:
        """Check if this range overlaps with another"""
        return not (self.end < other.start or self.start > other.end)

    def __repr__(self) -> str:
        return f"DataRange(start={self.start}, end={self.end})"
