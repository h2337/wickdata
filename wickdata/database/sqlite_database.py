"""
SQLite database implementation for WickData
"""

from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator, List, Optional

from sqlalchemy import and_, delete, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from wickdata.core.errors import DatabaseError
from wickdata.database.base import Database
from wickdata.database.models import Base, CandleModel, DatasetMetadataModel
from wickdata.models.candle import Candle
from wickdata.models.dataset_metadata import DatasetMetadata
from wickdata.utils.logger import Logger


class SQLiteDatabase(Database):
    """SQLite database implementation"""

    def __init__(self, url: str, logger: Optional[Logger] = None) -> None:
        """
        Initialize SQLite database

        Args:
            url: Database URL (e.g., sqlite:///wickdata.db)
            logger: Logger instance
        """
        self.url = url
        self.logger = logger or Logger("SQLiteDatabase")
        self.engine: Optional[AsyncEngine] = None
        self.async_session: Optional[sessionmaker] = None

    async def connect(self) -> None:
        """Connect to the database"""
        try:
            # Convert URL for async
            if self.url.startswith("sqlite:///"):
                async_url = self.url.replace("sqlite:///", "sqlite+aiosqlite:///")
            else:
                async_url = self.url

            self.engine = create_async_engine(
                async_url,
                echo=False,
                future=True,
            )

            self.async_session = sessionmaker(
                self.engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )  # type: ignore[call-overload]

            self.logger.info("Connected to SQLite database", url=self.url)
        except Exception as e:
            raise DatabaseError(f"Failed to connect to database: {e}", operation="connect")

    async def disconnect(self) -> None:
        """Disconnect from the database"""
        if self.engine:
            await self.engine.dispose()
            self.logger.info("Disconnected from SQLite database")

    async def initialize(self) -> None:
        """Initialize database schema"""
        try:
            async with self.engine.begin() as conn:  # type: ignore[union-attr]
                await conn.run_sync(Base.metadata.create_all)
            self.logger.info("Database schema initialized")
        except Exception as e:
            raise DatabaseError(f"Failed to initialize database: {e}", operation="initialize")

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Any, None]:
        """Create a database transaction context"""
        async with self.async_session() as session, session.begin():  # type: ignore[misc]
            yield session

    async def insert_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        candles: List[Candle],
    ) -> int:
        """Insert candles into the database"""
        if not candles:
            return 0

        # Validate candles
        for candle in candles:
            if candle is None:
                raise ValueError("Cannot insert None candle")
            if not isinstance(candle, Candle):
                raise TypeError(f"Expected Candle object, got {type(candle)}")

        inserted_count = 0

        try:
            async with self.transaction() as session:
                for candle in candles:
                    # Check if candle already exists
                    stmt = select(CandleModel).where(
                        and_(
                            CandleModel.exchange == exchange,
                            CandleModel.symbol == symbol,
                            CandleModel.timeframe == timeframe,
                            CandleModel.timestamp == candle.timestamp,
                        )
                    )
                    result = await session.execute(stmt)
                    existing = result.scalar_one_or_none()

                    if not existing:
                        candle_model = CandleModel(
                            exchange=exchange,
                            symbol=symbol,
                            timeframe=timeframe,
                            timestamp=candle.timestamp,
                            open=candle.open,
                            high=candle.high,
                            low=candle.low,
                            close=candle.close,
                            volume=candle.volume,
                        )
                        session.add(candle_model)
                        inserted_count += 1

                await session.commit()

            self.logger.debug(
                f"Inserted {inserted_count} candles",
                exchange=exchange,
                symbol=symbol,
                timeframe=timeframe,
            )

            return inserted_count

        except Exception as e:
            raise DatabaseError(
                f"Failed to insert candles: {e}",
                operation="insert",
                table="candles",
            )

    async def get_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: int,
        end_time: int,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Candle]:
        """Get candles from the database"""
        try:
            async with self.async_session() as session:  # type: ignore[misc]
                stmt = (
                    select(CandleModel)
                    .where(
                        and_(
                            CandleModel.exchange == exchange,
                            CandleModel.symbol == symbol,
                            CandleModel.timeframe == timeframe,
                            CandleModel.timestamp >= start_time,
                            CandleModel.timestamp <= end_time,
                        )
                    )
                    .order_by(CandleModel.timestamp)
                )

                if offset:
                    stmt = stmt.offset(offset)
                if limit:
                    stmt = stmt.limit(limit)

                result = await session.execute(stmt)
                candle_models = result.scalars().all()

                candles = [
                    Candle(
                        timestamp=model.timestamp,
                        open=model.open,
                        high=model.high,
                        low=model.low,
                        close=model.close,
                        volume=model.volume,
                    )
                    for model in candle_models
                ]

                return candles

        except Exception as e:
            raise DatabaseError(
                f"Failed to get candles: {e}",
                operation="select",
                table="candles",
            )

    async def delete_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> int:
        """Delete candles from the database"""
        try:
            async with self.transaction() as session:
                conditions = [
                    CandleModel.exchange == exchange,
                    CandleModel.symbol == symbol,
                    CandleModel.timeframe == timeframe,
                ]

                if start_time is not None:
                    conditions.append(CandleModel.timestamp >= start_time)
                if end_time is not None:
                    conditions.append(CandleModel.timestamp <= end_time)

                stmt = delete(CandleModel).where(and_(*conditions))
                result = await session.execute(stmt)
                deleted_count = result.rowcount

                await session.commit()

                self.logger.debug(
                    f"Deleted {deleted_count} candles",
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                )

                return deleted_count  # type: ignore[no-any-return]

        except Exception as e:
            raise DatabaseError(
                f"Failed to delete candles: {e}",
                operation="delete",
                table="candles",
            )

    async def count_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> int:
        """Count candles in the database"""
        try:
            async with self.async_session() as session:  # type: ignore[misc]
                conditions = [
                    CandleModel.exchange == exchange,
                    CandleModel.symbol == symbol,
                    CandleModel.timeframe == timeframe,
                ]

                if start_time is not None:
                    conditions.append(CandleModel.timestamp >= start_time)
                if end_time is not None:
                    conditions.append(CandleModel.timestamp <= end_time)

                stmt = select(func.count(CandleModel.id)).where(and_(*conditions))
                result = await session.execute(stmt)
                count = result.scalar() or 0

                return count

        except Exception as e:
            raise DatabaseError(
                f"Failed to count candles: {e}",
                operation="count",
                table="candles",
            )

    async def get_metadata(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> Optional[DatasetMetadata]:
        """Get dataset metadata"""
        try:
            async with self.async_session() as session:  # type: ignore[misc]
                stmt = select(DatasetMetadataModel).where(
                    and_(
                        DatasetMetadataModel.exchange == exchange,
                        DatasetMetadataModel.symbol == symbol,
                        DatasetMetadataModel.timeframe == timeframe,
                    )
                )
                result = await session.execute(stmt)
                model = result.scalar_one_or_none()

                if model:
                    return DatasetMetadata(
                        id=model.id,
                        exchange=model.exchange,
                        symbol=model.symbol,
                        timeframe=model.timeframe,
                        first_timestamp=model.first_timestamp,
                        last_timestamp=model.last_timestamp,
                        candle_count=model.candle_count,
                        last_fetch_at=model.last_fetch_at,
                        created_at=model.created_at,
                        updated_at=model.updated_at,
                    )

                return None

        except Exception as e:
            raise DatabaseError(
                f"Failed to get metadata: {e}",
                operation="select",
                table="dataset_metadata",
            )

    async def update_metadata(
        self,
        exchange: str,
        symbol: str,
        timeframe: str,
    ) -> None:
        """Update dataset metadata"""
        try:
            async with self.transaction() as session:
                # Get first and last timestamps
                stmt = select(
                    func.min(CandleModel.timestamp),
                    func.max(CandleModel.timestamp),
                    func.count(CandleModel.id),
                ).where(
                    and_(
                        CandleModel.exchange == exchange,
                        CandleModel.symbol == symbol,
                        CandleModel.timeframe == timeframe,
                    )
                )
                result = await session.execute(stmt)
                first_ts, last_ts, count = result.one()

                # Check if metadata exists
                metadata_stmt = select(DatasetMetadataModel).where(
                    and_(
                        DatasetMetadataModel.exchange == exchange,
                        DatasetMetadataModel.symbol == symbol,
                        DatasetMetadataModel.timeframe == timeframe,
                    )
                )
                result = await session.execute(metadata_stmt)
                metadata = result.scalar_one_or_none()

                if metadata:
                    # Update existing metadata
                    metadata.first_timestamp = first_ts
                    metadata.last_timestamp = last_ts
                    metadata.candle_count = count
                    metadata.last_fetch_at = datetime.utcnow()
                    metadata.updated_at = datetime.utcnow()
                else:
                    # Create new metadata
                    metadata = DatasetMetadataModel(
                        exchange=exchange,
                        symbol=symbol,
                        timeframe=timeframe,
                        first_timestamp=first_ts,
                        last_timestamp=last_ts,
                        candle_count=count,
                        last_fetch_at=datetime.utcnow(),
                    )
                    session.add(metadata)

                await session.commit()

                self.logger.debug(
                    "Updated metadata",
                    exchange=exchange,
                    symbol=symbol,
                    timeframe=timeframe,
                )

        except Exception as e:
            raise DatabaseError(
                f"Failed to update metadata: {e}",
                operation="update",
                table="dataset_metadata",
            )

    async def list_datasets(self) -> List[DatasetMetadata]:
        """List all datasets in the database"""
        try:
            async with self.async_session() as session:  # type: ignore[misc]
                stmt = select(DatasetMetadataModel).order_by(
                    DatasetMetadataModel.exchange,
                    DatasetMetadataModel.symbol,
                    DatasetMetadataModel.timeframe,
                )
                result = await session.execute(stmt)
                models = result.scalars().all()

                datasets = [
                    DatasetMetadata(
                        id=model.id,
                        exchange=model.exchange,
                        symbol=model.symbol,
                        timeframe=model.timeframe,
                        first_timestamp=model.first_timestamp,
                        last_timestamp=model.last_timestamp,
                        candle_count=model.candle_count,
                        last_fetch_at=model.last_fetch_at,
                        created_at=model.created_at,
                        updated_at=model.updated_at,
                    )
                    for model in models
                ]

                return datasets

        except Exception as e:
            raise DatabaseError(
                f"Failed to list datasets: {e}",
                operation="select",
                table="dataset_metadata",
            )
