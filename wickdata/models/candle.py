"""
Candle data model
"""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class Candle:
    """OHLCV candle data"""

    timestamp: int  # Unix timestamp in milliseconds
    open: float
    high: float
    low: float
    close: float
    volume: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert candle to dictionary"""
        return {
            "timestamp": self.timestamp,
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Candle":
        """Create candle from dictionary"""
        return cls(
            timestamp=data["timestamp"],
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            volume=data["volume"],
        )

    @classmethod
    def from_ccxt(cls, ccxt_candle: list) -> "Candle":
        """Create candle from CCXT format [timestamp, open, high, low, close, volume]"""
        return cls(
            timestamp=int(ccxt_candle[0]),
            open=float(ccxt_candle[1]),
            high=float(ccxt_candle[2]),
            low=float(ccxt_candle[3]),
            close=float(ccxt_candle[4]),
            volume=float(ccxt_candle[5]) if len(ccxt_candle) > 5 else 0.0,
        )

    def to_ccxt(self) -> list:
        """Convert to CCXT format"""
        return [
            self.timestamp,
            self.open,
            self.high,
            self.low,
            self.close,
            self.volume,
        ]

    def __repr__(self) -> str:
        return (
            f"Candle(timestamp={self.timestamp}, open={self.open}, "
            f"high={self.high}, low={self.low}, close={self.close}, "
            f"volume={self.volume})"
        )
