"""
Service for fetching data from exchanges with intelligent gap filling
"""

import asyncio
from datetime import datetime
from typing import Callable, List, Optional

from wickdata.exchanges.exchange_adapter import ExchangeAdapter
from wickdata.models.candle import Candle
from wickdata.models.data_gap import DataGap
from wickdata.models.progress_info import ProgressInfo, ProgressStage
from wickdata.models.timeframe import Timeframe
from wickdata.services.data_validation_service import DataValidationService
from wickdata.services.retry_service import RetryService
from wickdata.utils.logger import Logger

ProgressCallback = Callable[[ProgressInfo], None]


class DataFetcherService:
    """Service for fetching historical data from exchanges"""

    def __init__(
        self,
        exchange: ExchangeAdapter,
        retry_service: Optional[RetryService] = None,
        validation_service: Optional[DataValidationService] = None,
        logger: Optional[Logger] = None,
    ) -> None:
        """
        Initialize data fetcher service

        Args:
            exchange: Exchange adapter
            retry_service: Retry service
            validation_service: Data validation service
            logger: Logger instance
        """
        self.exchange = exchange
        self.retry_service = retry_service or RetryService()
        self.validation_service = validation_service or DataValidationService()
        self.logger = logger or Logger("DataFetcherService")

    async def fetch_gap(
        self,
        symbol: str,
        timeframe: Timeframe,
        gap: DataGap,
        batch_size: int = 500,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> List[Candle]:
        """
        Fetch data for a specific gap

        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe
            gap: Data gap to fill
            batch_size: Batch size for fetching
            progress_callback: Progress callback

        Returns:
            List of fetched candles
        """
        all_candles = []
        current_start = gap.start_time
        timeframe_ms = timeframe.to_milliseconds()

        while current_start <= gap.end_time:
            # Calculate batch end time
            max_batch_end = current_start + (batch_size * timeframe_ms)
            batch_end = min(max_batch_end, gap.end_time)

            # Fetch batch with retry
            try:
                candles = await self.retry_service.execute(
                    self.exchange.fetch_ohlcv,
                    f"fetch_ohlcv_{symbol}_{timeframe}",
                    symbol,
                    timeframe,
                    current_start,
                    batch_size,
                )

                # Validate and sanitize candles
                candles = self.validation_service.sanitize_candles(candles)

                # Filter candles within the gap range
                candles = [c for c in candles if gap.start_time <= c.timestamp <= gap.end_time]

                all_candles.extend(candles)

                # Update progress
                if progress_callback:
                    fetched = len(all_candles)
                    progress = ProgressInfo(
                        stage=ProgressStage.DOWNLOADING,
                        message=f"Fetching {symbol} {timeframe}",
                        current=fetched,
                        total=gap.candle_count,
                        candles_fetched=fetched,
                        candles_total=gap.candle_count,
                    )
                    progress.update_percentage()
                    progress_callback(progress)

                # Move to next batch
                if candles:
                    last_timestamp = candles[-1].timestamp
                    current_start = last_timestamp + timeframe_ms
                else:
                    # No more data available
                    break

            except Exception as e:
                self.logger.error(
                    f"Failed to fetch batch for {symbol} {timeframe}: {e}",
                    start=current_start,
                    end=batch_end,
                )
                # Move to next batch even on error
                current_start = batch_end + timeframe_ms

        self.logger.info(
            f"Fetched {len(all_candles)} candles for gap",
            symbol=symbol,
            timeframe=str(timeframe),
            gap_start=datetime.fromtimestamp(gap.start_time / 1000),
            gap_end=datetime.fromtimestamp(gap.end_time / 1000),
        )

        return all_candles

    async def fetch_multiple_gaps(
        self,
        symbol: str,
        timeframe: Timeframe,
        gaps: List[DataGap],
        batch_size: int = 500,
        concurrent_fetchers: int = 3,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> List[Candle]:
        """
        Fetch data for multiple gaps concurrently

        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe
            gaps: List of data gaps to fill
            batch_size: Batch size for fetching
            concurrent_fetchers: Number of concurrent fetchers
            progress_callback: Progress callback

        Returns:
            List of fetched candles
        """
        if not gaps:
            return []

        all_candles = []
        total_candles = sum(gap.candle_count for gap in gaps)
        fetched_count = 0

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(concurrent_fetchers)

        async def fetch_with_semaphore(gap: DataGap) -> List[Candle]:
            async with semaphore:
                candles = await self.fetch_gap(symbol, timeframe, gap, batch_size)

                nonlocal fetched_count
                fetched_count += len(candles)

                if progress_callback:
                    progress = ProgressInfo(
                        stage=ProgressStage.DOWNLOADING,
                        message=f"Fetching {symbol} {timeframe}",
                        current=fetched_count,
                        total=total_candles,
                        candles_fetched=fetched_count,
                        candles_total=total_candles,
                        current_operation=f"Gap {gaps.index(gap) + 1}/{len(gaps)}",
                    )
                    progress.update_percentage()
                    progress_callback(progress)

                return candles

        # Fetch all gaps concurrently
        tasks = [fetch_with_semaphore(gap) for gap in gaps]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect successful results
        for result in results:
            if isinstance(result, list):
                all_candles.extend(result)
            elif isinstance(result, Exception):
                self.logger.error(f"Gap fetch failed: {result}")

        # Sort and deduplicate
        all_candles = self.validation_service.sanitize_candles(all_candles)

        self.logger.info(
            f"Fetched {len(all_candles)} total candles from {len(gaps)} gaps",
            symbol=symbol,
            timeframe=str(timeframe),
        )

        return all_candles

    async def fetch_latest(
        self,
        symbol: str,
        timeframe: Timeframe,
        lookback_candles: int = 100,
    ) -> List[Candle]:
        """
        Fetch the latest candles

        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe
            lookback_candles: Number of recent candles to fetch

        Returns:
            List of latest candles
        """
        try:
            candles = await self.retry_service.execute(
                self.exchange.fetch_ohlcv,
                f"fetch_latest_{symbol}_{timeframe}",
                symbol,
                timeframe,
                None,  # No since parameter for latest
                lookback_candles,
            )

            # Validate and sanitize
            candles = self.validation_service.sanitize_candles(candles)

            self.logger.info(
                f"Fetched {len(candles)} latest candles",
                symbol=symbol,
                timeframe=str(timeframe),
            )

            return candles

        except Exception as e:
            self.logger.error(
                f"Failed to fetch latest candles: {e}",
                symbol=symbol,
                timeframe=str(timeframe),
            )
            raise
