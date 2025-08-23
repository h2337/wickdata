"""
Configuration models for WickData
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class PoolConfig:
    """Database connection pool configuration"""

    min_connections: int = 2
    max_connections: int = 10
    timeout: float = 10.0
    idle_timeout: float = 60.0
    enable_query_logging: bool = False
    slow_query_threshold_ms: int = 1000


@dataclass
class DatabaseConfig:
    """Database configuration"""

    provider: str = "sqlite"
    url: Optional[str] = None
    pool_config: Optional[PoolConfig] = None

    def __post_init__(self) -> None:
        """Set default URL if not provided"""
        if self.url is None:
            if self.provider == "sqlite":
                self.url = "sqlite:///wickdata.db"
            else:
                raise ValueError(f"URL required for provider: {self.provider}")

        if self.pool_config is None:
            self.pool_config = PoolConfig()


@dataclass
class ExchangeConfig:
    """Configuration for a cryptocurrency exchange"""

    exchange: str
    api_key: Optional[str] = None
    secret: Optional[str] = None
    password: Optional[str] = None  # Some exchanges require password
    enable_rate_limit: bool = True
    options: Dict[str, Any] = field(default_factory=dict)

    def to_ccxt_config(self) -> Dict[str, Any]:
        """Convert to CCXT exchange configuration"""
        config = {
            "apiKey": self.api_key,
            "secret": self.secret,
            "enableRateLimit": self.enable_rate_limit,
            "options": self.options,
        }

        if self.password:
            config["password"] = self.password

        # Remove None values
        return {k: v for k, v in config.items() if v is not None}


@dataclass
class RetryConfig:
    """Configuration for retry behavior"""

    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


@dataclass
class FetchConfig:
    """Configuration for data fetching"""

    batch_size: int = 500
    concurrent_fetchers: int = 3
    rate_limit_delay: Optional[float] = None
    retry_config: Optional[RetryConfig] = None

    def __post_init__(self) -> None:
        """Initialize retry config if not provided"""
        if self.retry_config is None:
            self.retry_config = RetryConfig()


@dataclass
class WickDataConfig:
    """Main configuration for WickData"""

    exchanges: Dict[str, ExchangeConfig] = field(default_factory=dict)
    database: Optional[DatabaseConfig] = None
    fetch_config: Optional[FetchConfig] = None
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        """Initialize default configurations"""
        if self.database is None:
            self.database = DatabaseConfig()

        if self.fetch_config is None:
            self.fetch_config = FetchConfig()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WickDataConfig":
        """Create config from dictionary"""
        exchanges = {}
        if "exchanges" in data:
            for name, exchange_data in data["exchanges"].items():
                if isinstance(exchange_data, ExchangeConfig):
                    exchanges[name] = exchange_data
                else:
                    exchanges[name] = ExchangeConfig(**exchange_data)

        database = None
        if "database" in data:
            if isinstance(data["database"], DatabaseConfig):
                database = data["database"]
            else:
                database = DatabaseConfig(**data["database"])

        fetch_config = None
        if "fetch_config" in data:
            if isinstance(data["fetch_config"], FetchConfig):
                fetch_config = data["fetch_config"]
            else:
                fetch_config = FetchConfig(**data["fetch_config"])

        return cls(
            exchanges=exchanges,
            database=database,
            fetch_config=fetch_config,
            log_level=data.get("log_level", "INFO"),
        )

    def add_exchange(self, config: ExchangeConfig) -> None:
        """Add an exchange configuration"""
        self.exchanges[config.exchange] = config

    def get_exchange(self, name: str) -> Optional[ExchangeConfig]:
        """Get exchange configuration by name"""
        return self.exchanges.get(name)
