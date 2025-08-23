"""
Options for data streaming
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class StreamOptions:
    """Options for streaming candle data"""

    batch_size: int = 1000
    delay_ms: int = 0  # Delay between batches in milliseconds
    realtime: bool = False  # Replay at real-time speed
    max_size: Optional[int] = None  # Maximum number of candles to stream
    buffer_size: Optional[int] = None  # Buffer size for streaming

    def __post_init__(self) -> None:
        """Validate stream options"""
        if self.batch_size <= 0:
            raise ValueError("batch_size must be positive")

        if self.delay_ms < 0:
            raise ValueError("delay_ms cannot be negative")

        if self.max_size is not None and self.max_size <= 0:
            raise ValueError("max_size must be positive")

        if self.buffer_size is not None and self.buffer_size <= 0:
            raise ValueError("buffer_size must be positive")
