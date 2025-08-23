"""
Query builder example for WickData
"""

import asyncio
from datetime import datetime, timedelta, timezone

from wickdata import (
    CandleQueryBuilder,
    DataRequestBuilder,
    Timeframe,
    WickData,
    create_binance_config,
)


async def main():
    # Configure exchanges
    exchange_configs = {"binance": create_binance_config()}

    # Initialize WickData
    async with WickData(exchange_configs) as wickdata:
        # Get components
        data_manager = wickdata.get_data_manager()
        repository = wickdata.get_repository()

        # First, ensure we have some data
        print("=== Fetching Initial Data ===")

        # Use DataRequestBuilder with various convenience methods
        requests = [
            # Last 24 hours of BTC 1h data
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("BTC/USDT")
            .with_timeframe("1h")
            .with_last_hours(24)
            .build(),
            # Last 7 days of ETH 4h data
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("ETH/USDT")
            .with_timeframe("4h")
            .with_last_days(7)
            .build(),
            # Month-to-date XRP daily data
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("XRP/USDT")
            .with_timeframe("1d")
            .with_month_to_date()
            .build(),
        ]

        for request in requests:
            stats = await data_manager.fetch_historical_data(request)
            print(f"Fetched {stats.total_candles} {request.symbol} {request.timeframe} candles")

        print("\n=== Using CandleQueryBuilder ===")

        # Create a query builder
        query = CandleQueryBuilder(repository)

        # Example 1: Simple query
        print("\nExample 1: Get BTC hourly data")
        candles = await (
            query.exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .date_range(
                datetime.now(timezone.utc) - timedelta(hours=12), datetime.now(timezone.utc)
            )
            .limit(10)
            .execute()
        )
        print(f"Retrieved {len(candles)} candles")

        # Example 2: Query with pagination
        print("\nExample 2: Paginated query")
        page_size = 5
        for page in range(3):
            candles = await (
                CandleQueryBuilder(repository)
                .exchange("binance")
                .symbol("ETH/USDT")
                .timeframe(Timeframe.FOUR_HOURS)
                .date_range(
                    datetime.now(timezone.utc) - timedelta(days=3), datetime.now(timezone.utc)
                )
                .limit(page_size)
                .offset(page * page_size)
                .execute()
            )
            print(f"Page {page + 1}: {len(candles)} candles")

        # Example 3: Count query
        print("\nExample 3: Count candles")
        count = await (
            CandleQueryBuilder(repository)
            .exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .date_range(
                datetime.now(timezone.utc) - timedelta(hours=24), datetime.now(timezone.utc)
            )
            .count()
        )
        print(f"Total candles in last 24h: {count}")

        # Example 4: Check existence
        print("\nExample 4: Check if data exists")
        exists = await (
            CandleQueryBuilder(repository)
            .exchange("binance")
            .symbol("XRP/USDT")
            .timeframe(Timeframe.ONE_DAY)
            .date_range(datetime.now(timezone.utc) - timedelta(days=7), datetime.now(timezone.utc))
            .exists()
        )
        print(f"XRP daily data exists: {exists}")

        # Example 5: Get statistics
        print("\nExample 5: Get candle statistics")
        stats = await (
            CandleQueryBuilder(repository)
            .exchange("binance")
            .symbol("ETH/USDT")
            .timeframe(Timeframe.FOUR_HOURS)
            .date_range(datetime.now(timezone.utc) - timedelta(days=2), datetime.now(timezone.utc))
            .stats()
        )

        if stats["count"] > 0:
            print(f"Candle count: {stats['count']}")
            print(f"Price range: ${stats['min_price']:.2f} - ${stats['max_price']:.2f}")
            print(f"Total volume: {stats['total_volume']:.2f}")
            print(f"First timestamp: {datetime.fromtimestamp(stats['first_timestamp'] / 1000)}")
            print(f"Last timestamp: {datetime.fromtimestamp(stats['last_timestamp'] / 1000)}")

        # Example 6: Descending order query
        print("\nExample 6: Get most recent candles (descending order)")
        recent_candles = await (
            CandleQueryBuilder(repository)
            .exchange("binance")
            .symbol("BTC/USDT")
            .timeframe(Timeframe.ONE_HOUR)
            .date_range(datetime.now(timezone.utc) - timedelta(days=1), datetime.now(timezone.utc))
            .order_by("timestamp", "desc")
            .limit(5)
            .execute()
        )

        print(f"Got {len(recent_candles)} most recent candles")
        for i, candle in enumerate(recent_candles):
            print(
                f"  {i+1}. {datetime.fromtimestamp(candle.timestamp / 1000)}: "
                f"${candle.close:.2f}"
            )

        # Example 7: Complex date range queries using DataRequestBuilder
        print("\n=== Advanced DataRequestBuilder Examples ===")

        # Year-to-date
        ytd_request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("BTC/USDT")
            .with_timeframe("1d")
            .with_year_to_date()
            .with_batch_size(1000)
            .with_concurrent_fetchers(5)
            .build()
        )
        print(f"YTD request: {ytd_request.start_date} to {ytd_request.end_date}")

        # Last 2 weeks with custom batch settings
        custom_request = (
            DataRequestBuilder.create()
            .with_exchange("binance")
            .with_symbol("ETH/USDT")
            .with_timeframe("1h")
            .with_last_weeks(2)
            .with_batch_size(200)
            .with_concurrent_fetchers(2)
            .with_rate_limit_delay(0.5)
            .build()
        )
        print(
            f"Custom request: Batch size={custom_request.batch_size}, "
            f"Fetchers={custom_request.concurrent_fetchers}"
        )


if __name__ == "__main__":
    asyncio.run(main())
