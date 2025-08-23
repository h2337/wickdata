"""
Quick integration test to verify the library works
"""

import asyncio
from datetime import datetime, timedelta

from wickdata import (
    DataRequestBuilder,
    WickData,
    create_binance_config,
)


async def test_basic_functionality():
    print("Testing WickData basic functionality...")

    # Configure exchanges
    exchange_configs = {
        "binance": create_binance_config()
    }

    # Initialize WickData
    try:
        async with WickData(exchange_configs) as wickdata:
            print("✓ WickData initialized successfully")

            # Get data manager
            _ = wickdata.get_data_manager()
            print("✓ DataManager retrieved")

            # Get data streamer
            _ = wickdata.get_data_streamer()
            print("✓ DataStreamer retrieved")

            # Build a simple request
            request = (
                DataRequestBuilder.create()
                .with_exchange("binance")
                .with_symbol("BTC/USDT")
                .with_timeframe("1h")
                .with_date_range(
                    datetime.now() - timedelta(days=1),
                    datetime.now()
                )
                .build()
            )
            print(f"✓ DataRequest built: {request.symbol} {request.timeframe}")

            # Test repository access
            _ = wickdata.get_repository()
            print("✓ Repository accessed")

            # Test exchange manager
            exchange_manager = wickdata.get_exchange_manager()
            exchanges = exchange_manager.list_exchanges()
            print(f"✓ Exchange manager has {len(exchanges)} exchange(s): {exchanges}")

            print("\nAll basic components working correctly!")
            return True

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = asyncio.run(test_basic_functionality())
    exit(0 if success else 1)
