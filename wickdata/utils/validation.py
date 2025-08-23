"""
Validation utilities for WickData
"""

import re
from typing import Union

from wickdata.core.errors import ValidationError
from wickdata.models.data_request import DataRequest
from wickdata.models.timeframe import Timeframe

# Valid symbol pattern (e.g., BTC/USDT, ETH-USD, etc.)
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+[/-][A-Z0-9]+$")

# Valid exchange name pattern
EXCHANGE_PATTERN = re.compile(r"^[a-z][a-z0-9_]*$")

# List of common valid timeframes
VALID_TIMEFRAMES = {
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "8h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
}


def validate_symbol(symbol: str) -> str:
    """
    Validate and normalize a trading pair symbol

    Args:
        symbol: Trading pair symbol (e.g., BTC/USDT)

    Returns:
        Normalized symbol

    Raises:
        ValidationError: If symbol is invalid
    """
    if not symbol:
        raise ValidationError("Symbol cannot be empty", field="symbol")

    # Convert to uppercase
    symbol = symbol.upper()

    # Replace common separators with /
    symbol = symbol.replace("-", "/").replace("_", "/")

    if not SYMBOL_PATTERN.match(symbol):
        raise ValidationError(
            f"Invalid symbol format: {symbol}. Expected format: BASE/QUOTE (e.g., BTC/USDT)",
            field="symbol",
            value=symbol,
        )

    return symbol


def validate_exchange_name(exchange: str) -> str:
    """
    Validate exchange name

    Args:
        exchange: Exchange name

    Returns:
        Normalized exchange name

    Raises:
        ValidationError: If exchange name is invalid
    """
    if not exchange:
        raise ValidationError("Exchange name cannot be empty", field="exchange")

    # Convert to lowercase
    exchange = exchange.lower()

    if not EXCHANGE_PATTERN.match(exchange):
        raise ValidationError(
            f"Invalid exchange name: {exchange}. Must start with letter and contain only lowercase letters, numbers, and underscores",
            field="exchange",
            value=exchange,
        )

    return exchange


def validate_timeframe(timeframe: Union[str, Timeframe]) -> Timeframe:
    """
    Validate timeframe

    Args:
        timeframe: Timeframe string or enum

    Returns:
        Timeframe enum

    Raises:
        ValidationError: If timeframe is invalid
    """
    if isinstance(timeframe, Timeframe):
        return timeframe

    if not timeframe:
        raise ValidationError("Timeframe cannot be empty", field="timeframe")

    if timeframe not in VALID_TIMEFRAMES:
        raise ValidationError(
            f"Invalid timeframe: {timeframe}. Valid options: {', '.join(sorted(VALID_TIMEFRAMES))}",
            field="timeframe",
            value=timeframe,
        )

    try:
        return Timeframe.from_string(timeframe)
    except ValueError as e:
        raise ValidationError(str(e), field="timeframe", value=timeframe)


def validate_data_request(request: DataRequest) -> None:
    """
    Validate a data request

    Args:
        request: Data request to validate

    Raises:
        ValidationError: If request is invalid
    """
    # Validate exchange
    request.exchange = validate_exchange_name(request.exchange)

    # Validate symbol
    request.symbol = validate_symbol(request.symbol)

    # Validate timeframe
    request.timeframe = validate_timeframe(request.timeframe)

    # Validate dates
    if request.start_date >= request.end_date:
        raise ValidationError(
            "Start date must be before end date",
            field="date_range",
            value=f"{request.start_date} to {request.end_date}",
        )

    # Validate batch size
    if request.batch_size <= 0:
        raise ValidationError(
            "Batch size must be positive", field="batch_size", value=request.batch_size
        )

    # Validate concurrent fetchers
    if request.concurrent_fetchers <= 0:
        raise ValidationError(
            "Concurrent fetchers must be positive",
            field="concurrent_fetchers",
            value=request.concurrent_fetchers,
        )


def sanitize_symbol(symbol: str) -> str:
    """
    Sanitize a symbol for safe use

    Args:
        symbol: Symbol to sanitize

    Returns:
        Sanitized symbol
    """
    if not symbol:
        return ""

    # Convert to uppercase first
    sanitized = symbol.upper()

    # Replace common separators with /
    sanitized = sanitized.replace("-", "/").replace("_", "/")

    # Remove any non-alphanumeric characters except /
    sanitized = re.sub(r"[^A-Z0-9/]", "", sanitized)

    return sanitized


def is_valid_symbol(symbol: str) -> bool:
    """
    Check if a symbol is valid

    Args:
        symbol: Symbol to check

    Returns:
        True if valid, False otherwise
    """
    try:
        validate_symbol(symbol)
        return True
    except ValidationError:
        return False


def is_valid_exchange(exchange: str) -> bool:
    """
    Check if an exchange name is valid

    Args:
        exchange: Exchange name to check

    Returns:
        True if valid, False otherwise
    """
    try:
        validate_exchange_name(exchange)
        return True
    except ValidationError:
        return False


def is_valid_timeframe(timeframe: str) -> bool:
    """
    Check if a timeframe is valid

    Args:
        timeframe: Timeframe to check

    Returns:
        True if valid, False otherwise
    """
    return timeframe in VALID_TIMEFRAMES
