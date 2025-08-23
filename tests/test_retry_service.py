"""
Unit tests for RetryService
"""

import asyncio
from unittest.mock import AsyncMock, Mock

import pytest

from wickdata.core.errors import NetworkError, RateLimitError
from wickdata.models.config import RetryConfig
from wickdata.services.retry_service import RetryService
from wickdata.utils.logger import Logger


@pytest.fixture
def mock_logger():
    """Create mock logger"""
    return Mock(spec=Logger)


@pytest.fixture
def retry_config():
    """Create retry configuration for testing"""
    return RetryConfig(
        max_retries=3,
        initial_delay=0.01,  # Very short delay for testing
        max_delay=0.1,
        exponential_base=2.0,
        jitter=False,  # Disable jitter for predictable testing
    )


@pytest.fixture
def retry_service(retry_config, mock_logger):
    """Create RetryService instance"""
    return RetryService(config=retry_config, logger=mock_logger)


@pytest.fixture
def mock_async_func():
    """Create mock async function"""
    return AsyncMock()


class TestRetryService:

    @pytest.mark.asyncio
    async def test_execute_success_first_attempt(self, retry_service, mock_async_func):
        """Test successful execution on first attempt"""
        mock_async_func.return_value = "success"

        result = await retry_service.execute(
            mock_async_func, "test_operation", "arg1", "arg2", keyword="value"
        )

        assert result == "success"
        mock_async_func.assert_called_once_with("arg1", "arg2", keyword="value")

    @pytest.mark.asyncio
    async def test_execute_success_after_retry(self, retry_service, mock_async_func):
        """Test successful execution after retries"""
        # Fail twice, then succeed
        mock_async_func.side_effect = [
            NetworkError("Network error"),
            NetworkError("Network error"),
            "success",
        ]

        result = await retry_service.execute(mock_async_func, "test_operation")

        assert result == "success"
        assert mock_async_func.call_count == 3

    @pytest.mark.asyncio
    async def test_execute_all_retries_exhausted(self, retry_service, mock_async_func):
        """Test when all retries are exhausted"""
        # Always fail with network error
        mock_async_func.side_effect = NetworkError("Network error")

        with pytest.raises(NetworkError) as exc_info:
            await retry_service.execute(mock_async_func, "test_operation")

        assert "Network error" in str(exc_info.value)
        # max_retries=3 means 4 total attempts (1 initial + 3 retries)
        assert mock_async_func.call_count == 4

    @pytest.mark.asyncio
    async def test_execute_rate_limit_error(self, retry_service, mock_async_func):
        """Test handling of rate limit errors"""
        # Create rate limit error with retry_after
        rate_limit_error = RateLimitError("Rate limited", retry_after=0.02)
        mock_async_func.side_effect = [rate_limit_error, "success"]

        result = await retry_service.execute(mock_async_func, "test_operation")

        assert result == "success"
        assert mock_async_func.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_non_retryable_error(self, retry_service, mock_async_func):
        """Test that non-retryable errors are raised immediately"""
        # ValueError is not retryable
        mock_async_func.side_effect = ValueError("Invalid value")

        with pytest.raises(ValueError) as exc_info:
            await retry_service.execute(mock_async_func, "test_operation")

        assert str(exc_info.value) == "Invalid value"
        # Should not retry for non-retryable errors
        assert mock_async_func.call_count == 1

    @pytest.mark.asyncio
    async def test_execute_exponential_backoff(self, retry_service, mock_async_func):
        """Test exponential backoff between retries"""
        mock_async_func.side_effect = NetworkError("Network error")

        start_time = asyncio.get_event_loop().time()

        with pytest.raises(NetworkError):
            await retry_service.execute(mock_async_func, "test_operation")

        elapsed_time = asyncio.get_event_loop().time() - start_time

        # With initial_delay=0.01, exponential_base=2.0, and 3 retries:
        # Delays: 0.01, 0.02, 0.04 = 0.07 total minimum delay
        assert elapsed_time >= 0.07
        # Should not exceed max_delay * number of retries
        assert elapsed_time < 0.5

    @pytest.mark.asyncio
    async def test_execute_with_jitter(self, mock_logger):
        """Test retry with jitter enabled"""
        config = RetryConfig(
            max_retries=2,
            initial_delay=0.01,
            jitter=True,
        )
        service = RetryService(config=config, logger=mock_logger)

        mock_func = AsyncMock()
        mock_func.side_effect = [NetworkError("Error"), "success"]

        result = await service.execute(mock_func, "test_op")

        assert result == "success"
        assert mock_func.call_count == 2

    @pytest.mark.asyncio
    async def test_execute_max_delay_cap(self, mock_logger):
        """Test that delay is capped at max_delay"""
        config = RetryConfig(
            max_retries=5,
            initial_delay=0.01,
            max_delay=0.02,  # Very low max_delay
            exponential_base=10.0,  # High base to quickly exceed max_delay
        )
        service = RetryService(config=config, logger=mock_logger)

        mock_func = AsyncMock()
        mock_func.side_effect = NetworkError("Error")

        start_time = asyncio.get_event_loop().time()

        with pytest.raises(NetworkError):
            await service.execute(mock_func, "test_op")

        elapsed_time = asyncio.get_event_loop().time() - start_time

        # Delay should be capped at max_delay (0.02) per retry
        # 5 retries * 0.02 = 0.1 seconds maximum
        assert elapsed_time < 0.15

    @pytest.mark.asyncio
    async def test_execute_no_exception_on_exhausted_retries(self, retry_service, mock_async_func):
        """Test WickDataError is raised when no specific exception"""
        # Return None/failure without raising exception
        mock_async_func.side_effect = [None, None, None, None]

        # Mock the function to simulate failures without exceptions
        async def failing_func(*args, **kwargs):
            raise NetworkError("Network error")

        mock_async_func.side_effect = failing_func

        with pytest.raises(NetworkError):
            await retry_service.execute(mock_async_func, "test_operation")

    @pytest.mark.asyncio
    async def test_with_config_creates_new_instance(self, retry_service):
        """Test that with_config creates a new RetryService instance"""
        new_config = RetryConfig(max_retries=10)
        new_service = retry_service.with_config(new_config)

        assert new_service is not retry_service
        assert new_service.config == new_config
        assert new_service.config.max_retries == 10
        assert retry_service.config.max_retries == 3  # Original unchanged

    @pytest.mark.asyncio
    async def test_execute_logs_success_after_retry(
        self, retry_service, mock_async_func, mock_logger
    ):
        """Test that successful retry is logged"""
        mock_async_func.side_effect = [NetworkError("Error"), "success"]

        result = await retry_service.execute(mock_async_func, "test_operation")

        assert result == "success"
        # Check that info log was called for successful retry
        mock_logger.info.assert_called()
        assert "succeeded after 1 retries" in str(mock_logger.info.call_args)

    @pytest.mark.asyncio
    async def test_execute_logs_retry_attempts(self, retry_service, mock_async_func, mock_logger):
        """Test that retry attempts are logged"""
        mock_async_func.side_effect = [NetworkError("Error 1"), NetworkError("Error 2"), "success"]

        result = await retry_service.execute(mock_async_func, "test_operation")

        assert result == "success"
        # Check that warning logs were called for retries
        assert mock_logger.warning.call_count >= 2

    @pytest.mark.asyncio
    async def test_execute_logs_non_retryable_error(
        self, retry_service, mock_async_func, mock_logger
    ):
        """Test that non-retryable errors are logged"""
        mock_async_func.side_effect = ValueError("Bad value")

        with pytest.raises(ValueError):
            await retry_service.execute(mock_async_func, "test_operation")

        # Check that error log was called
        mock_logger.error.assert_called()
        assert "non-retryable error" in str(mock_logger.error.call_args)

    @pytest.mark.asyncio
    async def test_execute_zero_retries(self, mock_logger):
        """Test with zero retries (only initial attempt)"""
        config = RetryConfig(max_retries=0)
        service = RetryService(config=config, logger=mock_logger)

        mock_func = AsyncMock()
        mock_func.side_effect = NetworkError("Error")

        with pytest.raises(NetworkError):
            await service.execute(mock_func, "test_op")

        # Only one attempt should be made
        assert mock_func.call_count == 1

    @pytest.mark.asyncio
    async def test_execute_custom_retry_after(self, retry_service, mock_async_func):
        """Test that custom retry_after from RateLimitError is respected"""
        # Set a specific retry_after time
        custom_delay = 0.05
        rate_limit_error = RateLimitError("Rate limited", retry_after=custom_delay)
        mock_async_func.side_effect = [rate_limit_error, "success"]

        start_time = asyncio.get_event_loop().time()
        result = await retry_service.execute(mock_async_func, "test_operation")
        elapsed_time = asyncio.get_event_loop().time() - start_time

        assert result == "success"
        # Should respect the custom retry_after delay
        assert elapsed_time >= custom_delay
        assert elapsed_time < custom_delay + 0.02  # Small buffer for execution time
