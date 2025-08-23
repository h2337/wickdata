"""
Service for validating candle data
"""

from typing import List, Optional

from wickdata.models.candle import Candle
from wickdata.models.data_request import DataRequest
from wickdata.models.validation_result import ValidationResult
from wickdata.utils.logger import Logger
from wickdata.utils.validation import validate_data_request


class DataValidationService:
    """Service for validating market data"""

    def __init__(self, logger: Optional[Logger] = None) -> None:
        """
        Initialize data validation service

        Args:
            logger: Logger instance
        """
        self.logger = logger or Logger("DataValidationService")

    def validate_candles(self, candles: List[Candle]) -> ValidationResult:
        """
        Validate a list of candles

        Args:
            candles: List of candles to validate

        Returns:
            Validation result
        """
        result = ValidationResult(is_valid=True)

        if not candles:
            result.add_warning("Empty candle list")
            return result

        # Check each candle
        for i, candle in enumerate(candles):
            # Check for invalid values
            if candle.timestamp <= 0:
                result.add_error(f"Invalid timestamp at index {i}: {candle.timestamp}")
                result.add_invalid_candle(i)

            if candle.open <= 0 or candle.high <= 0 or candle.low <= 0 or candle.close <= 0:
                result.add_error(f"Invalid price at index {i}")
                result.add_invalid_candle(i)

            if candle.volume < 0:
                result.add_error(f"Negative volume at index {i}: {candle.volume}")
                result.add_invalid_candle(i)

            # Check OHLC relationships
            if candle.high < candle.low:
                result.add_error(f"High < Low at index {i}")
                result.add_invalid_candle(i)

            if candle.high < candle.open or candle.high < candle.close:
                result.add_error(f"High price violation at index {i}")
                result.add_invalid_candle(i)

            if candle.low > candle.open or candle.low > candle.close:
                result.add_error(f"Low price violation at index {i}")
                result.add_invalid_candle(i)

        # Check for duplicate timestamps
        timestamps = [c.timestamp for c in candles]
        unique_timestamps = set(timestamps)
        if len(timestamps) != len(unique_timestamps):
            duplicates = len(timestamps) - len(unique_timestamps)
            result.add_error(f"Found {duplicates} duplicate timestamps")

        # Check for chronological order
        for i in range(1, len(candles)):
            if candles[i].timestamp <= candles[i - 1].timestamp:
                result.add_warning(f"Candles not in chronological order at index {i}")

        self.logger.debug(
            f"Validated {len(candles)} candles: "
            f"{result.get_error_count()} errors, {len(result.warnings)} warnings"
        )

        return result

    def validate_data_request(self, request: DataRequest) -> None:
        """
        Validate a data request

        Args:
            request: Data request to validate

        Raises:
            ValidationError: If request is invalid
        """
        validate_data_request(request)
        self.logger.debug(f"Validated data request: {request}")

    def sanitize_candles(self, candles: List[Candle]) -> List[Candle]:
        """
        Sanitize and clean candle data

        Args:
            candles: List of candles to sanitize

        Returns:
            List of sanitized candles
        """
        if not candles:
            return []

        # Remove invalid candles
        validation = self.validate_candles(candles)
        if validation.invalid_candles:
            valid_indices = set(range(len(candles))) - set(validation.invalid_candles)
            candles = [candles[i] for i in sorted(valid_indices)]

        # Remove duplicates (keep last occurrence)
        seen = {}
        for candle in candles:
            seen[candle.timestamp] = candle

        # Sort by timestamp
        sanitized = sorted(seen.values(), key=lambda c: c.timestamp)

        self.logger.debug(f"Sanitized {len(candles)} candles to {len(sanitized)} valid candles")

        return sanitized
