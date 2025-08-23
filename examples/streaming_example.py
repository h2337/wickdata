"""
Streaming example for WickData
"""

import asyncio
from datetime import datetime, timedelta, timezone

from wickdata import (
    StreamOptions,
    Timeframe,
    WickData,
    create_binance_config,
)


async def main():
    # Configure exchanges
    exchange_configs = {"binance": create_binance_config()}

    # Initialize WickData
    async with WickData(exchange_configs) as wickdata:
        # Get data manager and streamer
        data_manager = wickdata.get_data_manager()
        data_streamer = wickdata.get_data_streamer()

        # First, ensure we have some data to stream
        print("=== Fetching Initial Data ===")
        from wickdata import DataRequestBuilder

        request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("ETH/USDT")
            .with_timeframe("5m")
            .with_last_hours(24)
            .build()
        )

        await data_manager.fetch_historical_data(request)
        print("Data fetched successfully\n")

        # Example 1: Stream with async generator
        print("=== Example 1: Streaming with Async Generator ===")

        stream_options = StreamOptions(
            batch_size=100,
            delay_ms=500,  # 500ms delay between batches
        )

        start_date = datetime.now(timezone.utc) - timedelta(hours=2)
        end_date = datetime.now(timezone.utc)

        batch_count = 0
        candle_count = 0

        async for batch in data_streamer.stream_candles(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe=Timeframe.FIVE_MINUTES,
            start_time=int(start_date.timestamp() * 1000),
            end_time=int(end_date.timestamp() * 1000),
            options=stream_options,
        ):
            batch_count += 1
            candle_count += len(batch)
            print(f"Batch {batch_count}: {len(batch)} candles (total: {candle_count})")

            # Process first few batches only for demo
            if batch_count >= 3:
                data_streamer.stop()
                break

        print(f"Streamed {candle_count} candles in {batch_count} batches\n")

        # Example 2: Stream with callback
        print("=== Example 2: Streaming with Callback ===")

        processed_candles = []

        def process_batch(batch):
            processed_candles.extend(batch)
            print(f"Callback: Received {len(batch)} candles (total: {len(processed_candles)})")

        await data_streamer.stream_to_callback(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe=Timeframe.FIVE_MINUTES,
            start_date=start_date,
            end_date=end_date,
            callback=process_batch,
            options=StreamOptions(batch_size=50, max_size=200),
        )

        print(f"Processed {len(processed_candles)} candles via callback\n")

        # Example 3: Stream to array
        print("=== Example 3: Stream to Array ===")

        all_candles = await data_streamer.stream_to_array(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe=Timeframe.FIVE_MINUTES,
            start_date=start_date,
            end_date=end_date,
            options=StreamOptions(batch_size=100),
        )

        print(f"Collected {len(all_candles)} candles in array")
        if all_candles:
            print(f"First timestamp: {datetime.fromtimestamp(all_candles[0].timestamp / 1000)}")
            print(f"Last timestamp: {datetime.fromtimestamp(all_candles[-1].timestamp / 1000)}")

        # Example 4: Stream with buffer
        print("\n=== Example 4: Stream with Buffer ===")

        buffer_count = 0

        def process_buffer(buffer):
            nonlocal buffer_count
            buffer_count += 1
            print(f"Buffer {buffer_count}: {len(buffer)} candles")
            # Process the buffer here

        await data_streamer.stream_to_buffer(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe=Timeframe.FIVE_MINUTES,
            start_date=start_date,
            end_date=end_date,
            buffer_size=75,
            on_buffer=process_buffer,
            options=StreamOptions(batch_size=50),
        )

        print(f"Processed {buffer_count} buffers\n")

        # Example 5: Stream with events
        print("=== Example 5: Stream with Events ===")

        # Register event handlers
        def on_start(data):
            print(f"Stream started: {data['symbol']} {data['timeframe']}")

        def on_batch(candles):
            print(f"Batch event: {len(candles)} candles")

        def on_complete(data):
            print(f"Stream complete: {data['total_candles']} total candles")

        def on_error(error):
            print(f"Stream error: {error}")

        data_streamer.on("start", on_start)
        data_streamer.on("batch", on_batch)
        data_streamer.on("complete", on_complete)
        data_streamer.on("error", on_error)

        # Stream with events
        candle_count = 0
        async for batch in data_streamer.stream_candles(
            exchange="binance",
            symbol="ETH/USDT",
            timeframe=Timeframe.FIVE_MINUTES,
            start_time=int(start_date.timestamp() * 1000),
            end_time=int(end_date.timestamp() * 1000),
            options=StreamOptions(batch_size=50, max_size=150),
        ):
            candle_count += len(batch)

        print(f"Event-driven stream processed {candle_count} candles")

        # Clean up event listeners
        data_streamer.remove_all_listeners()


if __name__ == "__main__":
    asyncio.run(main())
