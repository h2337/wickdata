"""
Unit tests for DataStreamer component
"""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from wickdata.core.data_streamer import DataStreamer
from wickdata.models import Candle, StreamOptions, Timeframe


class TestDataStreamer:
    """Test cases for DataStreamer"""

    @pytest.fixture
    def mock_repository(self):
        """Create a mock repository"""
        return AsyncMock()

    @pytest.fixture
    def data_streamer(self, mock_repository):
        """Create a DataStreamer instance with mocks"""
        return DataStreamer(mock_repository)

    def create_test_candles(self, count=5):
        """Create test candles"""
        base_timestamp = 1704067200000
        candles = []
        for i in range(count):
            candles.append(
                Candle(
                    timestamp=base_timestamp + (i * 3600000),  # 1 hour apart
                    open=50000 + i * 100,
                    high=50100 + i * 100,
                    low=49900 + i * 100,
                    close=50050 + i * 100,
                    volume=1000 + i * 10,
                )
            )
        return candles

    @pytest.mark.asyncio
    async def test_stream_candles_basic(self, data_streamer, mock_repository):
        """Test basic candle streaming"""
        test_candles = self.create_test_candles(10)

        # Mock repository to return candles in batches
        mock_repository.get_candles.side_effect = [
            test_candles[:5],
            test_candles[5:],
            [],  # End of data
        ]

        # Stream candles
        all_candles = []
        async for batch in data_streamer.stream_candles(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            1704067200000,
            1704103200000,
            StreamOptions(batch_size=5),
        ):
            all_candles.extend(batch)

        # Verify
        assert len(all_candles) == 10
        assert data_streamer.is_active() is False
        assert mock_repository.get_candles.call_count == 3

    @pytest.mark.asyncio
    async def test_stream_candles_with_max_size(self, data_streamer, mock_repository):
        """Test streaming with max_size limit"""
        test_candles = self.create_test_candles(10)
        mock_repository.get_candles.return_value = test_candles

        # Stream with max_size=5
        all_candles = []
        async for batch in data_streamer.stream_candles(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            1704067200000,
            1704103200000,
            StreamOptions(batch_size=10, max_size=5),
        ):
            all_candles.extend(batch)

        # Should only get 5 candles
        assert len(all_candles) == 5

    @pytest.mark.asyncio
    async def test_stream_candles_with_delay(self, data_streamer, mock_repository):
        """Test streaming with delay between batches"""
        test_candles = self.create_test_candles(4)
        mock_repository.get_candles.side_effect = [
            test_candles[:2],
            test_candles[2:],
            [],
        ]

        start_time = asyncio.get_event_loop().time()

        async for _ in data_streamer.stream_candles(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            1704067200000,
            1704103200000,
            StreamOptions(batch_size=2, delay_ms=100),
        ):
            pass

        elapsed = asyncio.get_event_loop().time() - start_time
        # Should have at least 100ms delay between 2 batches
        assert elapsed >= 0.1

    @pytest.mark.asyncio
    async def test_stream_events(self, data_streamer, mock_repository):
        """Test event emission during streaming"""
        test_candles = self.create_test_candles(5)
        mock_repository.get_candles.side_effect = [test_candles, []]

        events = []

        def on_start(data):
            events.append(("start", data))

        def on_batch(candles):
            events.append(("batch", len(candles)))

        def on_complete(data):
            events.append(("complete", data))

        data_streamer.on("start", on_start)
        data_streamer.on("batch", on_batch)
        data_streamer.on("complete", on_complete)

        async for _ in data_streamer.stream_candles(
            "binance", "BTC/USDT", Timeframe.ONE_HOUR, 1704067200000, 1704103200000
        ):
            pass

        # Verify events
        assert len(events) == 3
        assert events[0][0] == "start"
        assert events[1][0] == "batch"
        assert events[1][1] == 5  # 5 candles in batch
        assert events[2][0] == "complete"

    @pytest.mark.asyncio
    async def test_stream_stop(self, data_streamer, mock_repository):
        """Test stopping stream"""
        test_candles = self.create_test_candles(10)
        mock_repository.get_candles.side_effect = [
            test_candles[:5],
            test_candles[5:],
            [],
        ]

        batch_count = 0
        async for _ in data_streamer.stream_candles(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            1704067200000,
            1704103200000,
            StreamOptions(batch_size=5),
        ):
            batch_count += 1
            if batch_count == 1:
                data_streamer.stop()

        # Should only get 1 batch before stopping
        assert batch_count == 1

    @pytest.mark.asyncio
    async def test_stream_to_callback(self, data_streamer, mock_repository):
        """Test streaming to callback function"""
        test_candles = self.create_test_candles(5)
        mock_repository.get_candles.side_effect = [test_candles, []]

        received_batches = []

        def callback(batch):
            received_batches.append(batch)

        await data_streamer.stream_to_callback(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            callback,
        )

        assert len(received_batches) == 1
        assert len(received_batches[0]) == 5

    @pytest.mark.asyncio
    async def test_stream_to_callback_async(self, data_streamer, mock_repository):
        """Test streaming to async callback function"""
        test_candles = self.create_test_candles(5)
        mock_repository.get_candles.side_effect = [test_candles, []]

        received_batches = []

        async def async_callback(batch):
            await asyncio.sleep(0.01)  # Simulate async work
            received_batches.append(batch)

        await data_streamer.stream_to_callback(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            async_callback,
        )

        assert len(received_batches) == 1

    @pytest.mark.asyncio
    async def test_stream_to_array(self, data_streamer, mock_repository):
        """Test streaming to array"""
        test_candles = self.create_test_candles(10)
        mock_repository.get_candles.side_effect = [
            test_candles[:5],
            test_candles[5:],
            [],
        ]

        candles = await data_streamer.stream_to_array(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            StreamOptions(batch_size=5),
        )

        assert len(candles) == 10
        assert candles == test_candles

    @pytest.mark.asyncio
    async def test_stream_to_buffer(self, data_streamer, mock_repository):
        """Test streaming to buffer with callback"""
        test_candles = self.create_test_candles(10)
        mock_repository.get_candles.side_effect = [
            test_candles[:6],
            test_candles[6:],
            [],
        ]

        buffer_calls = []

        def on_buffer(buffer):
            buffer_calls.append(len(buffer))

        await data_streamer.stream_to_buffer(
            "binance",
            "BTC/USDT",
            Timeframe.ONE_HOUR,
            datetime(2024, 1, 1),
            datetime(2024, 1, 2),
            buffer_size=4,
            on_buffer=on_buffer,
        )

        # Should have 3 buffer calls: 4, 4, 2 (remaining)
        assert len(buffer_calls) == 3
        assert buffer_calls[0] == 4
        assert buffer_calls[1] == 4
        assert buffer_calls[2] == 2

    @pytest.mark.asyncio
    async def test_stream_error_handling(self, data_streamer, mock_repository):
        """Test error handling during streaming"""
        mock_repository.get_candles.side_effect = Exception("Database error")

        error_event = None

        def on_error(error):
            nonlocal error_event
            error_event = error

        data_streamer.on("error", on_error)

        with pytest.raises(Exception, match="Database error"):
            async for _ in data_streamer.stream_candles(
                "binance", "BTC/USDT", Timeframe.ONE_HOUR, 1704067200000, 1704103200000
            ):
                pass

        # Error event should be emitted
        assert error_event is not None
        assert str(error_event) == "Database error"

    def test_is_active_initial_state(self, data_streamer):
        """Test initial active state"""
        assert data_streamer.is_active() is False
