"""
Timeframe enum and utilities
"""

from enum import Enum


class Timeframe(str, Enum):
    """Supported timeframes for candle data"""

    ONE_MINUTE = "1m"
    THREE_MINUTES = "3m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    ONE_HOUR = "1h"
    TWO_HOURS = "2h"
    FOUR_HOURS = "4h"
    SIX_HOURS = "6h"
    EIGHT_HOURS = "8h"
    TWELVE_HOURS = "12h"
    ONE_DAY = "1d"
    THREE_DAYS = "3d"
    ONE_WEEK = "1w"
    ONE_MONTH = "1M"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def from_string(cls, value: str) -> "Timeframe":
        """Create Timeframe from string value"""
        for tf in cls:
            if tf.value == value:
                return tf
        raise ValueError(f"Invalid timeframe: {value}")

    def to_minutes(self) -> int:
        """Convert timeframe to minutes"""
        mapping = {
            Timeframe.ONE_MINUTE: 1,
            Timeframe.THREE_MINUTES: 3,
            Timeframe.FIVE_MINUTES: 5,
            Timeframe.FIFTEEN_MINUTES: 15,
            Timeframe.THIRTY_MINUTES: 30,
            Timeframe.ONE_HOUR: 60,
            Timeframe.TWO_HOURS: 120,
            Timeframe.FOUR_HOURS: 240,
            Timeframe.SIX_HOURS: 360,
            Timeframe.EIGHT_HOURS: 480,
            Timeframe.TWELVE_HOURS: 720,
            Timeframe.ONE_DAY: 1440,
            Timeframe.THREE_DAYS: 4320,
            Timeframe.ONE_WEEK: 10080,
            Timeframe.ONE_MONTH: 43200,  # 30 days
        }
        return mapping[self]

    def to_milliseconds(self) -> int:
        """Convert timeframe to milliseconds"""
        return self.to_minutes() * 60 * 1000

    def to_seconds(self) -> int:
        """Convert timeframe to seconds"""
        return self.to_minutes() * 60
