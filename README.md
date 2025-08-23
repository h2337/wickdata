# wickdata

High-performance Python library for fetching, storing, and streaming historical cryptocurrency market data.

## Features

- 📊 **Historical Data Fetching** - Fetch OHLCV candle data from 100+ cryptocurrency exchanges
- 💾 **Efficient Storage** - SQLite database with automatic deduplication and indexing
- 🚀 **Streaming Capabilities** - Memory-efficient streaming for large datasets
- 🔍 **Intelligent Gap Detection** - Automatically identify and fill missing data periods
- 📈 **Progress Tracking** - Real-time progress updates for long-running operations
- 🔄 **Retry Logic** - Automatic retries with exponential backoff for failed requests
- 🏗️ **Builder Patterns** - Intuitive APIs for constructing queries and requests
- ⚡ **Async/Await** - Full async support for high-performance operations
- 🔌 **Exchange Support** - Built on CCXT for compatibility with 100+ exchanges
- ✅ **Comprehensive Testing** - 90%+ test coverage ensuring reliability and stability

## Installation

```bash
pip install wickdata
```

Or install from source:

```bash
git clone https://github.com/h2337/wickdata.git
cd wickdata
pip install -e .
```

## Quick Start

```python
import asyncio
from wickdata import WickData, DataRequestBuilder, create_binance_config

async def main():
    # Configure exchanges
    exchange_configs = {
        'binance': create_binance_config()  # API keys optional for public data
    }
    
    # Initialize WickData
    async with WickData(exchange_configs) as wickdata:
        # Get data manager
        data_manager = wickdata.get_data_manager()
        
        # Build request for last 7 days of BTC/USDT hourly data
        request = (DataRequestBuilder.create()
            .with_exchange('binance')
            .with_symbol('BTC/USDT')
            .with_timeframe('1h')
            .with_last_days(7)
            .build())
        
        # Fetch with progress tracking
        def on_progress(info):
            print(f"{info.stage}: {info.percentage:.1f}%")
        
        stats = await data_manager.fetch_historical_data(request, on_progress)
        print(f"Fetched {stats.total_candles} candles")

asyncio.run(main())
```

Check examples/ directory for more examples.

## Core Components

### WickData
Main entry point for the library. Manages database, exchanges, and provides access to data operations.

### DataManager
Handles fetching, storing, and querying historical data with intelligent gap detection.

### DataStreamer
Provides memory-efficient streaming of large datasets with various output options.

### Builder Patterns
- **DataRequestBuilder** - Build data fetch requests with convenient methods
- **CandleQueryBuilder** - Construct database queries with fluent interface

## Supported Timeframes

- 1m, 3m, 5m, 15m, 30m (minutes)
- 1h, 2h, 4h, 6h, 8h, 12h (hours)
- 1d, 3d (days)
- 1w (week)
- 1M (month)

## Configuration

### Exchange Configuration

```python
from wickdata import create_binance_config, create_coinbase_config

# Binance
binance_config = create_binance_config(
    api_key='your-api-key',  # Optional for public data
    secret='your-secret',     # Optional for public data
    testnet=False
)

# Coinbase
coinbase_config = create_coinbase_config(
    api_key='your-api-key',
    secret='your-secret',
    passphrase='your-passphrase',
    sandbox=False
)
```

### Database Configuration

```python
from wickdata.models.config import DatabaseConfig, WickDataConfig

config = WickDataConfig(
    exchanges={'binance': binance_config},
    database=DatabaseConfig(
        provider='sqlite',
        url='sqlite:///my_data.db'
    ),
    log_level='INFO'
)
```

## Examples

### Fetching Historical Data

```python
# Using convenience methods
request = (DataRequestBuilder.create()
    .with_exchange('binance')
    .with_symbol('ETH/USDT')
    .with_timeframe('4h')
    .with_last_weeks(2)  # Last 2 weeks
    .build())

# Or specific date range
request = (DataRequestBuilder.create()
    .with_exchange('binance')
    .with_symbol('BTC/USDT')
    .with_timeframe('1d')
    .with_date_range('2024-01-01', '2024-01-31')
    .build())
```

### Querying Stored Data

```python
# Create query builder
query = CandleQueryBuilder(repository)

# Get recent data with pagination
candles = await (query
    .exchange('binance')
    .symbol('BTC/USDT')
    .timeframe(Timeframe.ONE_HOUR)
    .date_range(start_date, end_date)
    .limit(100)
    .offset(0)
    .execute())

# Get statistics
stats = await query.stats()
```

### Streaming Data

```python
# Stream with async generator
async for batch in data_streamer.stream_candles(
    exchange='binance',
    symbol='ETH/USDT',
    timeframe=Timeframe.FIVE_MINUTES,
    start_time=start_timestamp,
    end_time=end_timestamp,
    options=StreamOptions(batch_size=1000, delay_ms=100)
):
    process_batch(batch)

# Stream to callback
await data_streamer.stream_to_callback(
    exchange='binance',
    symbol='BTC/USDT',
    timeframe=Timeframe.ONE_HOUR,
    start_date=start_date,
    end_date=end_date,
    callback=process_candles,
    options=StreamOptions(batch_size=500)
)
```

### Gap Detection and Analysis

```python
# Find missing data
gaps = await data_manager.find_missing_data(
    exchange='binance',
    symbol='BTC/USDT',
    timeframe=Timeframe.ONE_HOUR,
    start_date=start_date,
    end_date=end_date
)

print(f"Found {len(gaps)} gaps")
for gap in gaps:
    print(f"  Gap: {gap.get_start_datetime()} to {gap.get_end_datetime()}")
    print(f"  Missing candles: {gap.candle_count}")
```

## Error Handling

WickData provides comprehensive error handling with specific exception types:

```python
from wickdata import (
    WickDataError,      # Base error class
    ExchangeError,      # Exchange-specific errors
    ValidationError,    # Input validation errors
    RateLimitError,     # Rate limiting errors
    NetworkError,       # Network connectivity issues
    DatabaseError,      # Database operation errors
    ConfigurationError, # Configuration problems
    DataGapError       # Gap-related errors
)

try:
    await data_manager.fetch_historical_data(request)
except RateLimitError as e:
    print(f"Rate limited. Retry after {e.retry_after}s")
except ExchangeError as e:
    print(f"Exchange error: {e.message}")
```

## Performance

- **Batch Processing**: Insert 10,000+ candles per second (SQLite)
- **Concurrent Fetching**: Multiple concurrent fetchers per exchange
- **Memory Efficient**: Streaming prevents memory overflow for large datasets
- **Smart Caching**: Automatic deduplication and gap detection
- **Connection Pooling**: Efficient database connection management

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/h2337/wickdata.git
cd wickdata

# Install in development mode with dev dependencies
pip install -e ".[dev]"
```

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=wickdata

# Run specific test file
pytest tests/test_data_manager.py
```

### Code Quality

```bash
# Format code
black wickdata

# Lint code
ruff check wickdata

# Type checking
mypy wickdata
```

## Architecture

WickData follows a modular architecture with clear separation of concerns:

- **Core Layer**: Main WickData class, DataManager, DataStreamer
- **Database Layer**: Repository pattern with SQLite implementation
- **Exchange Layer**: CCXT integration with adapter pattern
- **Service Layer**: Gap analysis, retry logic, validation
- **Models**: Data models with validation
- **Builders**: Fluent interfaces for complex object construction

## Contributing

Contributions are welcome!

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
