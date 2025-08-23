"""
Gap coverage analysis model
"""

from dataclasses import dataclass
from typing import List, Optional

from wickdata.models.data_gap import DataGap


@dataclass
class GapCoverage:
    """Analysis of data gaps and coverage"""

    gaps: List[DataGap]
    total_gaps: int
    total_missing_candles: int
    coverage_percentage: float
    largest_gap: Optional[DataGap] = None

    def has_gaps(self) -> bool:
        """Check if there are any gaps"""
        return self.total_gaps > 0

    def is_complete(self) -> bool:
        """Check if coverage is complete"""
        return self.coverage_percentage >= 100.0

    def __repr__(self) -> str:
        return (
            f"GapCoverage(gaps={self.total_gaps}, "
            f"missing={self.total_missing_candles}, "
            f"coverage={self.coverage_percentage:.1f}%)"
        )
