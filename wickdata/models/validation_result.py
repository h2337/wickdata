"""
Validation result model
"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class ValidationResult:
    """Result of data validation"""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    invalid_candles: List[int] = field(default_factory=list)  # Indices of invalid candles

    def add_error(self, error: str) -> None:
        """Add an error message"""
        self.errors.append(error)
        self.is_valid = False

    def add_warning(self, warning: str) -> None:
        """Add a warning message"""
        self.warnings.append(warning)

    def add_invalid_candle(self, index: int) -> None:
        """Add index of invalid candle"""
        self.invalid_candles.append(index)
        self.is_valid = False

    def get_error_count(self) -> int:
        """Get number of errors"""
        return len(self.errors) + len(self.invalid_candles)

    def __repr__(self) -> str:
        return (
            f"ValidationResult(valid={self.is_valid}, errors={len(self.errors)}, "
            f"warnings={len(self.warnings)}, invalid_candles={len(self.invalid_candles)})"
        )
