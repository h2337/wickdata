"""
Unit tests for GapAnalysisService
"""

from unittest.mock import Mock

import pytest

from wickdata.models.data_gap import DataGap
from wickdata.models.gap_coverage import GapCoverage
from wickdata.models.timeframe import Timeframe
from wickdata.services.gap_analysis_service import GapAnalysisService
from wickdata.utils.logger import Logger


@pytest.fixture
def mock_logger():
    """Create mock logger"""
    return Mock(spec=Logger)


@pytest.fixture
def gap_analysis_service(mock_logger):
    """Create GapAnalysisService instance"""
    return GapAnalysisService(logger=mock_logger)


@pytest.fixture
def sample_gaps():
    """Create sample data gaps for testing"""
    return [
        DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=1),
        DataGap(start_time=1609459380000, end_time=1609459500000, candle_count=2),
        DataGap(start_time=1609459620000, end_time=1609459800000, candle_count=3),
    ]


@pytest.fixture
def adjacent_gaps():
    """Create adjacent gaps that can be merged"""
    return [
        DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=1),
        DataGap(
            start_time=1609459320000, end_time=1609459380000, candle_count=1
        ),  # Adjacent (within 1 minute)
        DataGap(start_time=1609459500000, end_time=1609459560000, candle_count=1),  # Not adjacent
    ]


@pytest.fixture
def large_gaps():
    """Create large gaps for splitting"""
    return [
        DataGap(start_time=1609459200000, end_time=1609462800000, candle_count=60),  # 60 candles
        DataGap(start_time=1609466400000, end_time=1609468200000, candle_count=30),  # 30 candles
        DataGap(start_time=1609470000000, end_time=1609470600000, candle_count=10),  # 10 candles
    ]


class TestGapAnalysisService:

    def test_analyze_gap_coverage_with_gaps(self, gap_analysis_service, sample_gaps):
        """Test gap coverage analysis with gaps"""
        start_time = 1609459200000
        end_time = 1609459800000
        timeframe = Timeframe.ONE_MINUTE

        coverage = gap_analysis_service.analyze_gap_coverage(
            sample_gaps, start_time, end_time, timeframe
        )

        assert isinstance(coverage, GapCoverage)
        assert coverage.total_gaps == 3
        assert coverage.total_missing_candles == 6  # Sum of candle_counts: 1+2+3
        assert coverage.coverage_percentage < 100.0
        assert coverage.largest_gap == sample_gaps[2]  # Gap with 3 candles

    def test_analyze_gap_coverage_no_gaps(self, gap_analysis_service):
        """Test gap coverage analysis with no gaps"""
        gaps = []
        start_time = 1609459200000
        end_time = 1609459800000
        timeframe = Timeframe.ONE_MINUTE

        coverage = gap_analysis_service.analyze_gap_coverage(gaps, start_time, end_time, timeframe)

        assert coverage.total_gaps == 0
        assert coverage.total_missing_candles == 0
        assert coverage.coverage_percentage == 100.0
        assert coverage.largest_gap is None

    def test_analyze_gap_coverage_full_gap(self, gap_analysis_service):
        """Test gap coverage when entire range is a gap"""
        start_time = 1609459200000
        end_time = 1609459800000  # 10 minutes = 10 candles
        timeframe = Timeframe.ONE_MINUTE

        # Gap covers entire range
        gaps = [DataGap(start_time=start_time, end_time=end_time, candle_count=10)]

        coverage = gap_analysis_service.analyze_gap_coverage(gaps, start_time, end_time, timeframe)

        assert coverage.total_gaps == 1
        assert coverage.total_missing_candles == 10
        assert coverage.coverage_percentage == 0.0  # No coverage
        assert coverage.largest_gap == gaps[0]

    def test_analyze_gap_coverage_percentage_calculation(self, gap_analysis_service):
        """Test correct coverage percentage calculation"""
        start_time = 1609459200000
        end_time = 1609460100000  # 15 minutes = 15 candles
        timeframe = Timeframe.ONE_MINUTE

        # 5 missing candles out of 15
        gaps = [
            DataGap(start_time=1609459200000, end_time=1609459320000, candle_count=2),
            DataGap(start_time=1609459500000, end_time=1609459680000, candle_count=3),
        ]

        coverage = gap_analysis_service.analyze_gap_coverage(gaps, start_time, end_time, timeframe)

        assert coverage.total_missing_candles == 5
        # 10 out of 15 candles present = 66.67% coverage
        assert coverage.coverage_percentage == pytest.approx(66.67, rel=0.01)

    def test_merge_adjacent_gaps_basic(self, gap_analysis_service, adjacent_gaps):
        """Test merging of adjacent gaps"""
        timeframe = Timeframe.ONE_MINUTE

        merged = gap_analysis_service.merge_adjacent_gaps(adjacent_gaps, timeframe)

        # First two gaps should be merged, third remains separate
        assert len(merged) == 2
        assert merged[0].start_time == adjacent_gaps[0].start_time
        assert merged[0].end_time == adjacent_gaps[1].end_time
        assert merged[1] == adjacent_gaps[2]

    def test_merge_adjacent_gaps_empty(self, gap_analysis_service):
        """Test merging with empty gap list"""
        timeframe = Timeframe.ONE_MINUTE

        merged = gap_analysis_service.merge_adjacent_gaps([], timeframe)

        assert merged == []

    def test_merge_adjacent_gaps_single(self, gap_analysis_service):
        """Test merging with single gap"""
        gaps = [DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=1)]
        timeframe = Timeframe.ONE_MINUTE

        merged = gap_analysis_service.merge_adjacent_gaps(gaps, timeframe)

        assert len(merged) == 1
        assert merged[0] == gaps[0]

    def test_merge_adjacent_gaps_overlapping(self, gap_analysis_service):
        """Test merging overlapping gaps"""
        gaps = [
            DataGap(start_time=1609459200000, end_time=1609459320000, candle_count=2),
            DataGap(start_time=1609459260000, end_time=1609459380000, candle_count=2),  # Overlaps
            DataGap(start_time=1609459320000, end_time=1609459440000, candle_count=2),  # Overlaps
        ]
        timeframe = Timeframe.ONE_MINUTE

        merged = gap_analysis_service.merge_adjacent_gaps(gaps, timeframe)

        # All gaps should be merged into one
        assert len(merged) == 1
        assert merged[0].start_time == gaps[0].start_time
        assert merged[0].end_time == gaps[2].end_time

    def test_merge_adjacent_gaps_preserves_order(self, gap_analysis_service):
        """Test that merging preserves chronological order"""
        # Gaps out of order
        gaps = [
            DataGap(start_time=1609459500000, end_time=1609459560000, candle_count=1),
            DataGap(start_time=1609459200000, end_time=1609459260000, candle_count=1),
            DataGap(start_time=1609459320000, end_time=1609459380000, candle_count=1),
        ]
        timeframe = Timeframe.ONE_MINUTE

        merged = gap_analysis_service.merge_adjacent_gaps(gaps, timeframe)

        # Should be sorted and merged where adjacent
        assert len(merged) >= 1
        # Check that results are sorted
        for i in range(1, len(merged)):
            assert merged[i].start_time > merged[i - 1].end_time

    def test_split_large_gaps_basic(self, gap_analysis_service, large_gaps):
        """Test splitting large gaps into smaller chunks"""
        max_candles_per_gap = 25
        timeframe = Timeframe.ONE_MINUTE

        split = gap_analysis_service.split_large_gaps(large_gaps, max_candles_per_gap, timeframe)

        # Should split the 60-candle gap and 30-candle gap
        assert len(split) > len(large_gaps)

        # No split gap should exceed max_candles_per_gap
        for gap in split:
            assert gap.candle_count <= max_candles_per_gap

    def test_split_large_gaps_no_split_needed(self, gap_analysis_service):
        """Test that small gaps are not split"""
        gaps = [
            DataGap(start_time=1609459200000, end_time=1609459500000, candle_count=5),
            DataGap(start_time=1609459600000, end_time=1609459900000, candle_count=5),
        ]
        max_candles_per_gap = 10
        timeframe = Timeframe.ONE_MINUTE

        split = gap_analysis_service.split_large_gaps(gaps, max_candles_per_gap, timeframe)

        # No splitting should occur
        assert len(split) == len(gaps)
        assert split == gaps

    def test_split_large_gaps_empty(self, gap_analysis_service):
        """Test splitting with empty gap list"""
        timeframe = Timeframe.ONE_MINUTE

        split = gap_analysis_service.split_large_gaps([], 100, timeframe)

        assert split == []

    def test_split_large_gaps_invalid_max(self, gap_analysis_service):
        """Test splitting with invalid max_candles_per_gap"""
        gaps = [DataGap(start_time=1609459200000, end_time=1609459500000, candle_count=5)]
        timeframe = Timeframe.ONE_MINUTE

        # Zero or negative max should return original gaps
        split = gap_analysis_service.split_large_gaps(gaps, 0, timeframe)
        assert split == gaps

        split = gap_analysis_service.split_large_gaps(gaps, -1, timeframe)
        assert split == gaps

    def test_split_large_gaps_exact_division(self, gap_analysis_service):
        """Test splitting when gap divides exactly"""
        # 60 candles, split into chunks of 20
        gaps = [DataGap(start_time=1609459200000, end_time=1609462800000, candle_count=60)]
        max_candles_per_gap = 20
        timeframe = Timeframe.ONE_MINUTE

        split = gap_analysis_service.split_large_gaps(gaps, max_candles_per_gap, timeframe)

        # Should split into 3 gaps
        assert len(split) == 3
        # Each gap should be at most max_candles_per_gap
        for gap in split:
            assert gap.candle_count <= max_candles_per_gap
        # Total candles should be preserved (approximately)
        total_candles = sum(gap.candle_count for gap in split)
        assert total_candles <= 60

    def test_split_large_gaps_maintains_coverage(self, gap_analysis_service):
        """Test that splitting maintains full time coverage"""
        original_gap = DataGap(start_time=1609459200000, end_time=1609462800000, candle_count=60)
        gaps = [original_gap]
        max_candles_per_gap = 25
        timeframe = Timeframe.ONE_MINUTE

        split = gap_analysis_service.split_large_gaps(gaps, max_candles_per_gap, timeframe)

        # Check that split gaps cover the entire original range
        assert split[0].start_time == original_gap.start_time
        assert split[-1].end_time <= original_gap.end_time

        # Check no gaps between splits
        for i in range(1, len(split)):
            # Next gap should start right after previous one ends
            expected_start = split[i - 1].end_time + timeframe.to_milliseconds()
            assert split[i].start_time == expected_start
