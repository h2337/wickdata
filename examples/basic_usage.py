"""
Basic usage example for WickData
"""

import asyncio
from datetime import datetime, timedelta, timezone

from wickdata import (
    DataRequestBuilder,
    Timeframe,
    WickData,
    create_binance_config,
)


async def main():
    # Configure exchanges
    exchange_configs = {
        "binance": create_binance_config(
            # api_key="your-api-key",  # Optional for public data
            # secret="your-secret"      # Optional for public data
        )
    }

    # Initialize WickData
    async with WickData(exchange_configs) as wickdata:
        # Get data manager
        data_manager = wickdata.get_data_manager()

        # Example 1: Fetch historical data with progress tracking
        print("\n=== Example 1: Fetching Historical Data ===")

        def progress_callback(info):
            print(f"{info.stage}: {info.message} - {info.percentage:.1f}%")

        # Build a data request for the last 7 days
        request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("BTC/USDT")
            .with_timeframe("1h")
            .with_last_days(7)
            .build()
        )

        # Fetch the data
        stats = await data_manager.fetch_historical_data(request, progress_callback)
        print(f"\nFetched {stats.total_candles} candles")
        print(f"Coverage: {stats.get_coverage_percentage():.1f}%")
        print(f"Gaps: {stats.get_gap_count()}")

        # Example 2: Query stored data
        print("\n=== Example 2: Querying Stored Data ===")

        # Get data for a specific date range
        start_date = datetime.now(timezone.utc) - timedelta(days=3)
        end_date = datetime.now(timezone.utc) - timedelta(days=2)

        candles = await data_manager.get_historical_data(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_date=start_date,
            end_date=end_date,
            limit=10,  # Get first 10 candles
        )

        print(f"Retrieved {len(candles)} candles")
        if candles:
            print(f"First candle: {candles[0]}")
            print(f"Last candle: {candles[-1]}")

        # Example 3: Update with latest data
        print("\n=== Example 3: Updating with Latest Data ===")

        new_candles = await data_manager.update_latest_data(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            lookback_candles=24,  # Get last 24 hours
        )

        print(f"Added {new_candles} new candles")

        # Example 4: Find missing data gaps
        print("\n=== Example 4: Finding Data Gaps ===")

        gaps = await data_manager.find_missing_data(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
            start_date=datetime.now(timezone.utc) - timedelta(days=7),
            end_date=datetime.now(timezone.utc),
        )

        print(f"Found {len(gaps)} gaps")
        for i, gap in enumerate(gaps[:3]):  # Show first 3 gaps
            print(f"  Gap {i+1}: {gap}")

        # Example 5: Get dataset statistics
        print("\n=== Example 5: Dataset Statistics ===")

        stats = await data_manager.get_data_stats(
            exchange="binance",
            symbol="BTC/USDT",
            timeframe=Timeframe.ONE_HOUR,
        )

        if stats:
            print(f"Exchange: {stats.exchange}")
            print(f"Symbol: {stats.symbol}")
            print(f"Timeframe: {stats.timeframe}")
            print(f"Total candles: {stats.total_candles}")
            print(f"Date range: {stats.date_range.start} to {stats.date_range.end}")
            print(f"Coverage: {stats.get_coverage_percentage():.1f}%")

        # Example 6: List available datasets
        print("\n=== Example 6: Available Datasets ===")

        datasets = await data_manager.get_available_datasets()
        print(f"Found {len(datasets)} datasets")

        for dataset in datasets:
            print(
                f"  - {dataset.exchange} {dataset.symbol} {dataset.timeframe}: "
                f"{dataset.candle_count} candles"
            )


if __name__ == "__main__":
    asyncio.run(main())
