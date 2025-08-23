"""
SQLAlchemy models for WickData database
"""

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from typing import Any

from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base: Any = declarative_base()


class CandleModel(Base):
    """SQLAlchemy model for candles table"""

    __tablename__ = "candles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(50), nullable=False)
    symbol = Column(String(50), nullable=False)
    timeframe = Column(String(10), nullable=False)
    timestamp = Column(BigInteger, nullable=False)
    open = Column(Float, nullable=False)
    high = Column(Float, nullable=False)
    low = Column(Float, nullable=False)
    close = Column(Float, nullable=False)
    volume = Column(Float, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "timeframe", "timestamp", name="uq_candle"),
        Index("idx_candles_lookup", "exchange", "symbol", "timeframe", "timestamp"),
        Index("idx_candles_timestamp", "timestamp"),
    )

    def __repr__(self) -> str:
        return (
            f"<Candle(exchange={self.exchange}, symbol={self.symbol}, "
            f"timeframe={self.timeframe}, timestamp={self.timestamp})>"
        )


class DatasetMetadataModel(Base):
    """SQLAlchemy model for dataset_metadata table"""

    __tablename__ = "dataset_metadata"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange = Column(String(50), nullable=False)
    symbol = Column(String(50), nullable=False)
    timeframe = Column(String(10), nullable=False)
    first_timestamp = Column(BigInteger, nullable=True)
    last_timestamp = Column(BigInteger, nullable=True)
    candle_count = Column(Integer, default=0)
    last_fetch_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("exchange", "symbol", "timeframe", name="uq_metadata"),
        Index("idx_metadata_lookup", "exchange", "symbol", "timeframe"),
    )

    def __repr__(self) -> str:
        return (
            f"<DatasetMetadata(exchange={self.exchange}, symbol={self.symbol}, "
            f"timeframe={self.timeframe}, candles={self.candle_count})>"
        )
