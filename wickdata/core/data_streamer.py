"""
Data streamer component for streaming historical data
"""

import asyncio
from datetime import datetime
from typing import AsyncGenerator, Callable, List, Optional

from wickdata.core.event_emitter import EventEmitter
from wickdata.database.candle_repository import CandleRepository
from wickdata.models.candle import Candle
from wickdata.models.stream_options import StreamOptions
from wickdata.models.timeframe import Timeframe
from wickdata.utils.logger import Logger


class DataStreamer(EventEmitter):
    """Streamer for historical candle data"""

    def __init__(
        self,
        repository: CandleRepository,
        logger: Optional[Logger] = None,
    ) -> None:
        """
        Initialize data streamer

        Args:
            repository: Candle repository
            logger: Logger instance
        """
        super().__init__()
        self.repository = repository
        self.logger = logger or Logger("DataStreamer")
        self._is_active = False
        self._stop_requested = False

    async def stream_candles(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_time: int,
        end_time: int,
        options: Optional[StreamOptions] = None,
    ) -> AsyncGenerator[List[Candle], None]:
        """
        Stream candles as an async generator

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_time: Start timestamp (milliseconds)
            end_time: End timestamp (milliseconds)
            options: Stream options

        Yields:
            Batches of candles
        """
        options = options or StreamOptions()
        self._is_active = True
        self._stop_requested = False

        try:
            # Emit start event
            await self.emit(
                "start",
                {
                    "exchange": exchange,
                    "symbol": symbol,
                    "timeframe": str(timeframe),
                    "start_time": start_time,
                    "end_time": end_time,
                },
            )

            offset = 0
            total_streamed = 0

            while not self._stop_requested:
                # Fetch batch
                candles = await self.repository.get_candles(
                    exchange,
                    symbol,
                    timeframe,
                    start_time,
                    end_time,
                    limit=options.batch_size,
                    offset=offset,
                )

                if not candles:
                    break

                # Apply max_size limit
                if options.max_size and total_streamed + len(candles) > options.max_size:
                    candles = candles[: options.max_size - total_streamed]

                # Emit batch event
                await self.emit("batch", candles)

                # Yield batch
                yield candles

                total_streamed += len(candles)
                offset += len(candles)

                # Check if reached max_size
                if options.max_size and total_streamed >= options.max_size:
                    break

                # Apply delay
                if options.delay_ms > 0:
                    await asyncio.sleep(options.delay_ms / 1000)

                # Realtime simulation
                if options.realtime and len(candles) > 0:
                    # Calculate time between first and last candle
                    time_span = candles[-1].timestamp - candles[0].timestamp
                    if time_span > 0:
                        # Sleep for proportional time
                        sleep_time = time_span / 1000  # Convert to seconds
                        await asyncio.sleep(sleep_time)

            # Emit complete event
            await self.emit(
                "complete",
                {
                    "total_candles": total_streamed,
                },
            )

            self.logger.info(
                f"Streamed {total_streamed} candles",
                exchange=exchange,
                symbol=symbol,
                timeframe=str(timeframe),
            )

        except Exception as e:
            # Emit error event
            await self.emit("error", e)
            self.logger.error(f"Stream error: {e}")
            raise

        finally:
            self._is_active = False

    async def stream_to_callback(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
        callback: Callable[[List[Candle]], None],
        options: Optional[StreamOptions] = None,
    ) -> None:
        """
        Stream candles to a callback function

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_date: Start date
            end_date: End date
            callback: Callback function for each batch
            options: Stream options
        """
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        async for batch in self.stream_candles(
            exchange, symbol, timeframe, start_time, end_time, options
        ):
            if asyncio.iscoroutinefunction(callback):
                await callback(batch)
            else:
                callback(batch)

    async def stream_to_array(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
        options: Optional[StreamOptions] = None,
    ) -> List[Candle]:
        """
        Stream candles to an array

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_date: Start date
            end_date: End date
            options: Stream options

        Returns:
            List of all candles
        """
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        all_candles = []

        async for batch in self.stream_candles(
            exchange, symbol, timeframe, start_time, end_time, options
        ):
            all_candles.extend(batch)

        return all_candles

    async def stream_to_buffer(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
        buffer_size: int,
        on_buffer: Callable[[List[Candle]], None],
        options: Optional[StreamOptions] = None,
    ) -> None:
        """
        Stream candles to a buffer with callback when full

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_date: Start date
            end_date: End date
            buffer_size: Buffer size
            on_buffer: Callback when buffer is full
            options: Stream options
        """
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        buffer = []

        async for batch in self.stream_candles(
            exchange, symbol, timeframe, start_time, end_time, options
        ):
            buffer.extend(batch)

            # Check if buffer is full
            while len(buffer) >= buffer_size:
                # Extract full buffer
                full_buffer = buffer[:buffer_size]
                buffer = buffer[buffer_size:]

                # Call callback
                if asyncio.iscoroutinefunction(on_buffer):
                    await on_buffer(full_buffer)
                else:
                    on_buffer(full_buffer)

        # Process remaining buffer
        if buffer:
            if asyncio.iscoroutinefunction(on_buffer):
                await on_buffer(buffer)
            else:
                on_buffer(buffer)

    def stop(self) -> None:
        """Stop the active stream"""
        self._stop_requested = True
        self.logger.debug("Stream stop requested")

    def is_active(self) -> bool:
        """
        Check if streamer is active

        Returns:
            True if streaming
        """
        return self._is_active
