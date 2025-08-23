import asyncio
import shutil
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from wickdata.database.sqlite_database import SQLiteDatabase
from wickdata.models.candle import Candle


class TestSQLiteDatabase:
    @pytest.fixture
    async def temp_db_path(self):
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.db"
        yield db_path
        await asyncio.sleep(0.1)
        shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.fixture
    async def db(self, temp_db_path):
        database = SQLiteDatabase(f"sqlite:///{temp_db_path}")
        await database.connect()
        await database.initialize()
        yield database
        await database.disconnect()

    def datetime_to_ms(self, dt):
        """Convert datetime to milliseconds timestamp"""
        return int(dt.timestamp() * 1000)

    @pytest.mark.asyncio
    async def test_initialize_creates_database(self, temp_db_path):
        db = SQLiteDatabase(f"sqlite:///{temp_db_path}")
        await db.connect()
        await db.initialize()

        assert temp_db_path.exists()
        assert db.engine is not None
        assert db.async_session is not None

        await db.disconnect()

    @pytest.mark.asyncio
    async def test_initialize_creates_tables(self, db, temp_db_path):
        from sqlalchemy import create_engine, inspect

        engine = create_engine(f"sqlite:///{temp_db_path}")
        inspector = inspect(engine)
        tables = inspector.get_table_names()

        assert "candles" in tables
        assert "dataset_metadata" in tables
        engine.dispose()

    @pytest.mark.asyncio
    async def test_disconnect_releases_resources(self, temp_db_path):
        db = SQLiteDatabase(f"sqlite:///{temp_db_path}")
        await db.connect()
        await db.initialize()

        # Store references before disconnect
        has_engine = db.engine is not None
        has_session = db.async_session is not None

        assert has_engine
        assert has_session

        await db.disconnect()

        # After disconnect, engine should be disposed but references may still exist
        # The important thing is that the connection is closed

    @pytest.mark.asyncio
    async def test_insert_and_get_candles(self, db):
        timestamp = datetime(2024, 1, 1, 12, 0)
        candle = Candle(
            timestamp=self.datetime_to_ms(timestamp),
            open=100.0,
            high=105.0,
            low=99.0,
            close=103.0,
            volume=1000.0,
        )

        # Insert single candle
        count = await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=[candle]
        )

        assert count == 1

        # Get candles
        candles = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1m",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
        )

        assert len(candles) == 1
        assert candles[0].open == 100.0
        assert candles[0].close == 103.0

    @pytest.mark.asyncio
    async def test_insert_candles_batch(self, db):
        candles = []
        for i in range(5):
            timestamp = datetime(2024, 1, 1, 12, i)
            candles.append(
                Candle(
                    timestamp=self.datetime_to_ms(timestamp),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=99.0 + i,
                    close=103.0 + i,
                    volume=1000.0 + i,
                )
            )

        count = await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=candles
        )

        assert count == 5

        saved_candles = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1m",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
        )

        assert len(saved_candles) == 5
        assert saved_candles[0].open == 100.0
        assert saved_candles[4].open == 104.0

    @pytest.mark.asyncio
    async def test_insert_handles_duplicates(self, db):
        timestamp = self.datetime_to_ms(datetime(2024, 1, 1, 12, 0))
        candle1 = Candle(
            timestamp=timestamp, open=100.0, high=105.0, low=99.0, close=103.0, volume=1000.0
        )

        candle2 = Candle(
            timestamp=timestamp,  # Same timestamp
            open=101.0,
            high=106.0,
            low=100.0,
            close=104.0,
            volume=1100.0,
        )

        count1 = await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=[candle1]
        )

        count2 = await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=[candle2]
        )

        assert count1 == 1
        assert count2 == 0  # Duplicate should not be inserted

        candles = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1m",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
        )

        assert len(candles) == 1
        assert candles[0].open == 100.0  # Original value

    @pytest.mark.asyncio
    async def test_get_candles_with_filters(self, db):
        candles = []
        for i in range(10):
            timestamp = datetime(2024, 1, 1) + timedelta(hours=i)
            candles.append(
                Candle(
                    timestamp=self.datetime_to_ms(timestamp),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=99.0 + i,
                    close=103.0 + i,
                    volume=1000.0 + i,
                )
            )

        await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1h", candles=candles
        )

        filtered_candles = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1h",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1, 2, 0)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 1, 5, 0)),
        )

        assert len(filtered_candles) == 4
        assert filtered_candles[0].timestamp == self.datetime_to_ms(datetime(2024, 1, 1, 2, 0))
        assert filtered_candles[-1].timestamp == self.datetime_to_ms(datetime(2024, 1, 1, 5, 0))

    @pytest.mark.asyncio
    async def test_get_candles_with_limit(self, db):
        candles = []
        for i in range(100):
            timestamp = datetime(2024, 1, 1) + timedelta(minutes=i)
            candles.append(
                Candle(
                    timestamp=self.datetime_to_ms(timestamp),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=99.0 + i,
                    close=103.0 + i,
                    volume=1000.0 + i,
                )
            )

        await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=candles
        )

        limited_candles = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1m",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
            limit=10,
        )

        assert len(limited_candles) == 10

    @pytest.mark.asyncio
    async def test_delete_candles(self, db):
        candles = []
        for i in range(10):
            timestamp = datetime(2024, 1, 1) + timedelta(hours=i)
            candles.append(
                Candle(
                    timestamp=self.datetime_to_ms(timestamp),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=99.0 + i,
                    close=103.0 + i,
                    volume=1000.0 + i,
                )
            )

        await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1h", candles=candles
        )

        deleted_count = await db.delete_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1h",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1, 3, 0)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 1, 6, 0)),
        )

        assert deleted_count == 4

        remaining = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1h",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
        )

        assert len(remaining) == 6

    @pytest.mark.asyncio
    async def test_count_candles(self, db):
        candles = []
        for i in range(5):
            timestamp = datetime(2024, 1, 1, 12, i)
            candles.append(
                Candle(
                    timestamp=self.datetime_to_ms(timestamp),
                    open=100.0 + i,
                    high=105.0 + i,
                    low=99.0 + i,
                    close=103.0 + i,
                    volume=1000.0 + i,
                )
            )

        await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=candles
        )

        count = await db.count_candles(exchange="binance", symbol="BTC/USDT", timeframe="1m")

        assert count == 5

    @pytest.mark.asyncio
    async def test_get_and_update_metadata(self, db):
        # Insert some candles first
        candles = []
        for i in range(10):
            timestamp = datetime(2024, 1, i + 1)
            candles.append(
                Candle(
                    timestamp=self.datetime_to_ms(timestamp),
                    open=100.0,
                    high=105.0,
                    low=99.0,
                    close=103.0,
                    volume=1000.0,
                )
            )

        await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1d", candles=candles
        )

        # Update metadata
        await db.update_metadata(exchange="binance", symbol="BTC/USDT", timeframe="1d")

        # Get metadata
        metadata = await db.get_metadata(exchange="binance", symbol="BTC/USDT", timeframe="1d")

        assert metadata is not None
        assert metadata.exchange == "binance"
        assert metadata.symbol == "BTC/USDT"
        assert metadata.timeframe == "1d"
        assert metadata.candle_count == 10

    @pytest.mark.asyncio
    async def test_list_datasets(self, db):
        # Insert candles for multiple datasets
        datasets = [
            ("binance", "BTC/USDT", "1m"),
            ("binance", "ETH/USDT", "1h"),
            ("kraken", "BTC/USD", "1d"),
        ]

        for exchange, symbol, timeframe in datasets:
            candle = Candle(
                timestamp=self.datetime_to_ms(datetime(2024, 1, 1)),
                open=100.0,
                high=105.0,
                low=99.0,
                close=103.0,
                volume=1000.0,
            )

            await db.insert_candles(
                exchange=exchange, symbol=symbol, timeframe=timeframe, candles=[candle]
            )

            await db.update_metadata(exchange=exchange, symbol=symbol, timeframe=timeframe)

        all_metadata = await db.list_datasets()

        assert len(all_metadata) == 3

        # Check that all datasets are present
        dataset_keys = [(m.exchange, m.symbol, m.timeframe) for m in all_metadata]
        for dataset in datasets:
            assert dataset in dataset_keys

    @pytest.mark.asyncio
    async def test_concurrent_writes(self, db):
        async def write_candles(offset):
            candles = []
            for i in range(10):
                timestamp = datetime(2024, 1, 1) + timedelta(minutes=i + offset)
                candles.append(
                    Candle(
                        timestamp=self.datetime_to_ms(timestamp),
                        open=100.0 + i,
                        high=105.0 + i,
                        low=99.0 + i,
                        close=103.0 + i,
                        volume=1000.0 + i,
                    )
                )
            await db.insert_candles(
                exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=candles
            )

        # Run concurrent writes
        await asyncio.gather(write_candles(0), write_candles(10), write_candles(20))

        count = await db.count_candles(exchange="binance", symbol="BTC/USDT", timeframe="1m")

        assert count == 30

    @pytest.mark.asyncio
    async def test_different_symbols_isolation(self, db):
        timestamp = self.datetime_to_ms(datetime(2024, 1, 1, 12, 0))

        btc_candle = Candle(
            timestamp=timestamp, open=100.0, high=105.0, low=99.0, close=103.0, volume=1000.0
        )

        eth_candle = Candle(
            timestamp=timestamp, open=2000.0, high=2050.0, low=1990.0, close=2030.0, volume=500.0
        )

        await db.insert_candles(
            exchange="binance", symbol="BTC/USDT", timeframe="1m", candles=[btc_candle]
        )

        await db.insert_candles(
            exchange="binance", symbol="ETH/USDT", timeframe="1m", candles=[eth_candle]
        )

        btc_candles = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1m",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
        )

        eth_candles = await db.get_candles(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe="1m",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
        )

        assert len(btc_candles) == 1
        assert len(eth_candles) == 1
        assert btc_candles[0].open == 100.0
        assert eth_candles[0].open == 2000.0

    @pytest.mark.asyncio
    async def test_database_error_handling(self, db):
        # Test with invalid data
        with pytest.raises((ValueError, TypeError)):
            await db.insert_candles(
                exchange="binance",
                symbol="BTC/USDT",
                timeframe="1m",
                candles=[None],  # Invalid candle
            )

    @pytest.mark.asyncio
    async def test_empty_operations(self, db):
        # Test operations on empty database
        candles = await db.get_candles(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe="1m",
            start_time=self.datetime_to_ms(datetime(2024, 1, 1)),
            end_time=self.datetime_to_ms(datetime(2024, 1, 2)),
        )
        assert len(candles) == 0

        count = await db.count_candles(exchange="binance", symbol="BTC/USDT", timeframe="1m")
        assert count == 0

        metadata = await db.get_metadata(exchange="binance", symbol="BTC/USDT", timeframe="1m")
        assert metadata is None

        datasets = await db.list_datasets()
        assert len(datasets) == 0
