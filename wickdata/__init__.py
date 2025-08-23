"""
WickData - High-performance library for cryptocurrency market data
"""

__version__ = "0.1.0"

from wickdata.builders.candle_query_builder import CandleQueryBuilder
from wickdata.builders.data_request_builder import DataRequestBuilder
from wickdata.core.data_manager import DataManager
from wickdata.core.data_streamer import DataStreamer
from wickdata.core.errors import (
    ConfigurationError,
    DatabaseError,
    DataGapError,
    ExchangeError,
    NetworkError,
    RateLimitError,
    ValidationError,
    WickDataError,
)
from wickdata.core.wickdata import WickData
from wickdata.models.candle import Candle
from wickdata.models.config import (
    DatabaseConfig,
    ExchangeConfig,
    PoolConfig,
    WickDataConfig,
)
from wickdata.models.data_gap import DataGap
from wickdata.models.data_request import DataRequest
from wickdata.models.historical_data_stats import HistoricalDataStats
from wickdata.models.stream_options import StreamOptions
from wickdata.models.timeframe import Timeframe
from wickdata.utils.config_helpers import (
    create_binance_config,
    create_bybit_config,
    create_coinbase_config,
    create_kraken_config,
)

__all__ = [
    "WickData",
    "DataManager",
    "DataStreamer",
    "DataRequestBuilder",
    "CandleQueryBuilder",
    "Candle",
    "DataRequest",
    "Timeframe",
    "DataGap",
    "HistoricalDataStats",
    "StreamOptions",
    "ExchangeConfig",
    "DatabaseConfig",
    "PoolConfig",
    "WickDataConfig",
    "create_binance_config",
    "create_coinbase_config",
    "create_kraken_config",
    "create_bybit_config",
    "WickDataError",
    "ExchangeError",
    "ValidationError",
    "RateLimitError",
    "NetworkError",
    "DatabaseError",
    "ConfigurationError",
    "DataGapError",
]
