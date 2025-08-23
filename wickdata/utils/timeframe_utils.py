"""
Timeframe utilities for WickData
"""

from typing import Union

from wickdata.models.timeframe import Timeframe


class TimeframeUtils:
    """Utility functions for working with timeframes"""

    @staticmethod
    def to_milliseconds(timeframe: Union[str, Timeframe]) -> int:
        """Convert timeframe to milliseconds"""
        if isinstance(timeframe, str):
            timeframe = Timeframe.from_string(timeframe)
        return timeframe.to_milliseconds()

    @staticmethod
    def to_seconds(timeframe: Union[str, Timeframe]) -> int:
        """Convert timeframe to seconds"""
        if isinstance(timeframe, str):
            timeframe = Timeframe.from_string(timeframe)
        return timeframe.to_seconds()

    @staticmethod
    def to_minutes(timeframe: Union[str, Timeframe]) -> int:
        """Convert timeframe to minutes"""
        if isinstance(timeframe, str):
            timeframe = Timeframe.from_string(timeframe)
        return timeframe.to_minutes()

    @staticmethod
    def to_ccxt_timeframe(timeframe: Union[str, Timeframe]) -> str:
        """Convert timeframe to CCXT format"""
        if isinstance(timeframe, Timeframe):
            return timeframe.value
        return timeframe

    @staticmethod
    def from_ccxt_timeframe(ccxt_tf: str) -> Timeframe:
        """Convert CCXT timeframe to Timeframe enum"""
        return Timeframe.from_string(ccxt_tf)

    @staticmethod
    def get_candle_count(start_time: int, end_time: int, timeframe: Union[str, Timeframe]) -> int:
        """Calculate number of candles between two timestamps"""
        if start_time >= end_time:
            return 0

        timeframe_ms = TimeframeUtils.to_milliseconds(timeframe)
        duration_ms = end_time - start_time
        return int(duration_ms / timeframe_ms)

    @staticmethod
    def align_timestamp(timestamp: int, timeframe: Union[str, Timeframe]) -> int:
        """Align timestamp to timeframe boundary"""
        timeframe_ms = TimeframeUtils.to_milliseconds(timeframe)
        return (timestamp // timeframe_ms) * timeframe_ms

    @staticmethod
    def get_next_timestamp(timestamp: int, timeframe: Union[str, Timeframe]) -> int:
        """Get next timestamp for timeframe"""
        timeframe_ms = TimeframeUtils.to_milliseconds(timeframe)
        aligned = TimeframeUtils.align_timestamp(timestamp, timeframe)
        return aligned + timeframe_ms

    @staticmethod
    def get_previous_timestamp(timestamp: int, timeframe: Union[str, Timeframe]) -> int:
        """Get previous timestamp for timeframe"""
        timeframe_ms = TimeframeUtils.to_milliseconds(timeframe)
        aligned = TimeframeUtils.align_timestamp(timestamp, timeframe)
        if aligned == timestamp:
            return aligned - timeframe_ms
        return aligned

    @staticmethod
    def is_aligned(timestamp: int, timeframe: Union[str, Timeframe]) -> bool:
        """Check if timestamp is aligned to timeframe boundary"""
        return timestamp == TimeframeUtils.align_timestamp(timestamp, timeframe)
