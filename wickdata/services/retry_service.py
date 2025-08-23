"""
Service for retry logic with exponential backoff
"""

import asyncio
import random
from typing import Any, Awaitable, Callable, Optional, TypeVar

from wickdata.core.errors import NetworkError, RateLimitError, WickDataError
from wickdata.models.config import RetryConfig
from wickdata.utils.logger import Logger

T = TypeVar("T")


class RetryService:
    """Service for executing functions with retry logic"""

    def __init__(
        self, config: Optional[RetryConfig] = None, logger: Optional[Logger] = None
    ) -> None:
        """
        Initialize retry service

        Args:
            config: Retry configuration
            logger: Logger instance
        """
        self.config = config or RetryConfig()
        self.logger = logger or Logger("RetryService")

    async def execute(
        self,
        func: Callable[..., Awaitable[T]],
        operation_name: str,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """
        Execute a function with retry logic

        Args:
            func: Async function to execute
            operation_name: Name of the operation for logging
            *args: Function arguments
            **kwargs: Function keyword arguments

        Returns:
            Function result

        Raises:
            Last exception if all retries failed
        """
        last_exception = None
        delay = self.config.initial_delay

        for attempt in range(self.config.max_retries + 1):
            try:
                result = await func(*args, **kwargs)

                if attempt > 0:
                    self.logger.info(
                        f"Operation '{operation_name}' succeeded after {attempt} retries"
                    )

                return result

            except (RateLimitError, NetworkError) as e:
                # These are retryable errors
                last_exception = e

                if isinstance(e, RateLimitError) and e.retry_after:
                    delay = e.retry_after

                if attempt < self.config.max_retries:
                    # Add jitter if configured
                    actual_delay = delay
                    if self.config.jitter:
                        actual_delay = delay * (0.5 + random.random())

                    self.logger.warning(
                        f"Operation '{operation_name}' failed (attempt {attempt + 1}/{self.config.max_retries + 1}), "
                        f"retrying in {actual_delay:.2f}s: {e}"
                    )

                    await asyncio.sleep(actual_delay)

                    # Exponential backoff for next attempt
                    delay = min(
                        delay * self.config.exponential_base,
                        self.config.max_delay,
                    )

            except Exception as e:
                # Non-retryable error
                self.logger.error(
                    f"Operation '{operation_name}' failed with non-retryable error: {e}"
                )
                raise

        # All retries exhausted
        self.logger.error(
            f"Operation '{operation_name}' failed after {self.config.max_retries + 1} attempts"
        )

        if last_exception:
            raise last_exception
        else:
            raise WickDataError(f"Operation '{operation_name}' failed after all retries")

    def with_config(self, config: RetryConfig) -> "RetryService":
        """
        Create a new retry service with different configuration

        Args:
            config: New retry configuration

        Returns:
            New retry service instance
        """
        return RetryService(config, self.logger)
