"""
Historical data statistics model
"""

from dataclasses import dataclass
from datetime import datetime
from typing import List, Union

from wickdata.models.data_gap import DataGap
from wickdata.models.data_range import DataRange
from wickdata.models.timeframe import Timeframe


@dataclass
class HistoricalDataStats:
    """Statistics for historical data"""

    exchange: str
    symbol: str
    timeframe: Timeframe
    total_candles: int
    date_range: DataRange
    last_updated: datetime
    gaps: Union[List[DataGap], int]  # List of gaps or total gap count

    def get_gap_count(self) -> int:
        """Get the number of gaps"""
        if isinstance(self.gaps, list):
            return len(self.gaps)
        return self.gaps

    def get_coverage_percentage(self) -> float:
        """Calculate data coverage percentage"""
        if self.total_candles == 0:
            return 0.0

        expected_candles = self._calculate_expected_candles()
        if expected_candles == 0:
            return 100.0

        return min(100.0, (self.total_candles / expected_candles) * 100)

    def _calculate_expected_candles(self) -> int:
        """Calculate expected number of candles for the date range"""
        duration_ms = self.date_range.get_end_timestamp() - self.date_range.get_start_timestamp()
        timeframe_ms = self.timeframe.to_milliseconds()
        return int(duration_ms / timeframe_ms)

    def __repr__(self) -> str:
        return (
            f"HistoricalDataStats(exchange={self.exchange}, symbol={self.symbol}, "
            f"timeframe={self.timeframe}, candles={self.total_candles}, "
            f"coverage={self.get_coverage_percentage():.1f}%)"
        )
