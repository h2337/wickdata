"""
Progress tracking models
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ProgressStage(str, Enum):
    """Stages of progress for data operations"""

    INITIALIZING = "initializing"
    ANALYZING = "analyzing"
    DOWNLOADING = "downloading"
    PROCESSING = "processing"
    STORING = "storing"
    COMPLETE = "complete"
    ERROR = "error"


@dataclass
class ProgressInfo:
    """Information about operation progress"""

    stage: ProgressStage
    message: str
    current: int = 0
    total: int = 0
    percentage: float = 0.0
    candles_fetched: int = 0
    candles_total: int = 0
    current_operation: Optional[str] = None
    error: Optional[str] = None

    def update_percentage(self) -> None:
        """Update percentage based on current and total"""
        if self.total > 0:
            self.percentage = min(100.0, (self.current / self.total) * 100)
        else:
            self.percentage = 0.0

    def is_complete(self) -> bool:
        """Check if operation is complete"""
        return self.stage == ProgressStage.COMPLETE

    def has_error(self) -> bool:
        """Check if operation has error"""
        return self.stage == ProgressStage.ERROR

    def __repr__(self) -> str:
        return (
            f"ProgressInfo(stage={self.stage}, message={self.message}, "
            f"percentage={self.percentage:.1f}%)"
        )
