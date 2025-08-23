"""
Configuration helper functions for common exchanges
"""

import time
from typing import Any, Optional

from wickdata.models.config import ExchangeConfig


def create_binance_config(
    api_key: Optional[str] = None,
    secret: Optional[str] = None,
    testnet: bool = False,
    **options: Any,
) -> ExchangeConfig:
    """
    Create configuration for Binance exchange

    Args:
        api_key: API key
        secret: API secret
        testnet: Use testnet
        **options: Additional options

    Returns:
        ExchangeConfig for Binance
    """
    config_options: dict[str, Any] = {
        "adjustForTimeDifference": True,
        "recvWindow": 60000,
    }

    if testnet:
        config_options["test"] = True
        config_options["urls"] = {
            "api": {
                "public": "https://testnet.binance.vision/api",
                "private": "https://testnet.binance.vision/api",
            }
        }

    config_options.update(options)

    return ExchangeConfig(
        exchange="binance",
        api_key=api_key,
        secret=secret,
        enable_rate_limit=True,
        options=config_options,
    )


def create_coinbase_config(
    api_key: Optional[str] = None,
    secret: Optional[str] = None,
    passphrase: Optional[str] = None,
    sandbox: bool = False,
    **options: Any,
) -> ExchangeConfig:
    """
    Create configuration for Coinbase exchange

    Args:
        api_key: API key
        secret: API secret
        passphrase: API passphrase
        sandbox: Use sandbox environment
        **options: Additional options

    Returns:
        ExchangeConfig for Coinbase
    """
    config_options: dict[str, Any] = {}

    if sandbox:
        config_options["urls"] = {
            "api": "https://api-public.sandbox.exchange.coinbase.com",
        }

    config_options.update(options)

    return ExchangeConfig(
        exchange="coinbase",
        api_key=api_key,
        secret=secret,
        password=passphrase,
        enable_rate_limit=True,
        options=config_options,
    )


def create_kraken_config(
    api_key: Optional[str] = None, secret: Optional[str] = None, **options: Any
) -> ExchangeConfig:
    """
    Create configuration for Kraken exchange

    Args:
        api_key: API key
        secret: API secret
        **options: Additional options

    Returns:
        ExchangeConfig for Kraken
    """
    config_options: dict[str, Any] = {
        "nonce": lambda: int(1000 * time.time()),
    }
    config_options.update(options)

    return ExchangeConfig(
        exchange="kraken",
        api_key=api_key,
        secret=secret,
        enable_rate_limit=True,
        options=config_options,
    )


def create_bybit_config(
    api_key: Optional[str] = None,
    secret: Optional[str] = None,
    testnet: bool = False,
    **options: Any,
) -> ExchangeConfig:
    """
    Create configuration for Bybit exchange

    Args:
        api_key: API key
        secret: API secret
        testnet: Use testnet
        **options: Additional options

    Returns:
        ExchangeConfig for Bybit
    """
    config_options: dict[str, Any] = {
        "adjustForTimeDifference": True,
    }

    if testnet:
        config_options["urls"] = {
            "api": {
                "public": "https://api-testnet.bybit.com",
                "private": "https://api-testnet.bybit.com",
            }
        }

    config_options.update(options)

    return ExchangeConfig(
        exchange="bybit",
        api_key=api_key,
        secret=secret,
        enable_rate_limit=True,
        options=config_options,
    )
