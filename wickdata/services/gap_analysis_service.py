"""
Service for analyzing data gaps
"""

from typing import List, Optional

from wickdata.models.data_gap import DataGap
from wickdata.models.gap_coverage import GapCoverage
from wickdata.models.timeframe import Timeframe
from wickdata.utils.logger import Logger
from wickdata.utils.timeframe_utils import TimeframeUtils


class GapAnalysisService:
    """Service for analyzing gaps in historical data"""

    def __init__(self, logger: Optional[Logger] = None) -> None:
        """
        Initialize gap analysis service

        Args:
            logger: Logger instance
        """
        self.logger = logger or Logger("GapAnalysisService")

    def analyze_gap_coverage(
        self,
        gaps: List[DataGap],
        start_time: int,
        end_time: int,
        timeframe: Timeframe,
    ) -> GapCoverage:
        """
        Analyze gap coverage for a time range

        Args:
            gaps: List of data gaps
            start_time: Start timestamp (milliseconds)
            end_time: End timestamp (milliseconds)
            timeframe: Timeframe

        Returns:
            Gap coverage analysis
        """
        # Calculate total expected candles
        expected_candles = TimeframeUtils.get_candle_count(start_time, end_time, timeframe)

        # Calculate total missing candles
        total_missing = sum(gap.candle_count for gap in gaps)

        # Calculate coverage percentage
        if expected_candles > 0:
            coverage_percentage = ((expected_candles - total_missing) / expected_candles) * 100
        else:
            coverage_percentage = 100.0

        # Find largest gap
        largest_gap = None
        if gaps:
            largest_gap = max(gaps, key=lambda g: g.candle_count)

        coverage = GapCoverage(
            gaps=gaps,
            total_gaps=len(gaps),
            total_missing_candles=total_missing,
            coverage_percentage=coverage_percentage,
            largest_gap=largest_gap,
        )

        self.logger.debug(
            f"Gap analysis: {len(gaps)} gaps, {total_missing} missing candles, "
            f"{coverage_percentage:.1f}% coverage"
        )

        return coverage

    def merge_adjacent_gaps(
        self,
        gaps: List[DataGap],
        timeframe: Timeframe,
    ) -> List[DataGap]:
        """
        Merge adjacent gaps

        Args:
            gaps: List of data gaps
            timeframe: Timeframe

        Returns:
            List of merged gaps
        """
        if not gaps:
            return []

        # Sort gaps by start time
        sorted_gaps = sorted(gaps, key=lambda g: g.start_time)

        merged = []
        current_gap = sorted_gaps[0]
        timeframe_ms = timeframe.to_milliseconds()

        for gap in sorted_gaps[1:]:
            # Check if gaps are adjacent (within one timeframe)
            if gap.start_time <= current_gap.end_time + timeframe_ms:
                # Merge gaps
                current_gap = DataGap(
                    start_time=current_gap.start_time,
                    end_time=max(current_gap.end_time, gap.end_time),
                    candle_count=TimeframeUtils.get_candle_count(
                        current_gap.start_time,
                        max(current_gap.end_time, gap.end_time),
                        timeframe,
                    ),
                )
            else:
                merged.append(current_gap)
                current_gap = gap

        merged.append(current_gap)

        self.logger.debug(f"Merged {len(gaps)} gaps into {len(merged)} gaps")

        return merged

    def split_large_gaps(
        self,
        gaps: List[DataGap],
        max_candles_per_gap: int,
        timeframe: Timeframe,
    ) -> List[DataGap]:
        """
        Split large gaps into smaller chunks

        Args:
            gaps: List of data gaps
            max_candles_per_gap: Maximum candles per gap
            timeframe: Timeframe

        Returns:
            List of split gaps
        """
        if not gaps or max_candles_per_gap <= 0:
            return gaps

        split_gaps = []
        timeframe_ms = timeframe.to_milliseconds()

        for gap in gaps:
            if gap.candle_count <= max_candles_per_gap:
                split_gaps.append(gap)
            else:
                # Split the gap
                current_start = gap.start_time

                while current_start < gap.end_time:
                    chunk_size = min(
                        max_candles_per_gap,
                        TimeframeUtils.get_candle_count(current_start, gap.end_time, timeframe),
                    )

                    chunk_end = current_start + (chunk_size * timeframe_ms)
                    chunk_end = min(chunk_end, gap.end_time)

                    split_gaps.append(
                        DataGap(
                            start_time=current_start,
                            end_time=chunk_end,
                            candle_count=chunk_size,
                        )
                    )

                    current_start = chunk_end + timeframe_ms

        self.logger.debug(f"Split {len(gaps)} gaps into {len(split_gaps)} chunks")

        return split_gaps
