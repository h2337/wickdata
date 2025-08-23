"""
Data manager component for fetching and managing historical data
"""

from datetime import datetime
from typing import Callable, List, Optional

from wickdata.database.candle_repository import CandleRepository
from wickdata.exchanges.exchange_manager import ExchangeManager
from wickdata.models.candle import Candle
from wickdata.models.data_gap import DataGap
from wickdata.models.data_range import DataRange
from wickdata.models.data_request import DataRequest
from wickdata.models.dataset_info import DatasetInfo
from wickdata.models.historical_data_stats import HistoricalDataStats
from wickdata.models.progress_info import ProgressInfo, ProgressStage
from wickdata.models.timeframe import Timeframe
from wickdata.services.data_fetcher_service import DataFetcherService
from wickdata.services.data_validation_service import DataValidationService
from wickdata.services.gap_analysis_service import GapAnalysisService
from wickdata.utils.logger import Logger

ProgressCallback = Callable[[ProgressInfo], None]


class DataManager:
    """Manager for historical data operations"""

    def __init__(
        self,
        repository: CandleRepository,
        exchange_manager: ExchangeManager,
        logger: Optional[Logger] = None,
    ) -> None:
        """
        Initialize data manager

        Args:
            repository: Candle repository
            exchange_manager: Exchange manager
            logger: Logger instance
        """
        self.repository = repository
        self.exchange_manager = exchange_manager
        self.logger = logger or Logger("DataManager")
        self.gap_service = GapAnalysisService(logger)
        self.validation_service = DataValidationService(logger)

    async def fetch_historical_data(
        self,
        request: DataRequest,
        progress_callback: Optional[ProgressCallback] = None,
    ) -> HistoricalDataStats:
        """
        Fetch historical data based on request

        Args:
            request: Data request
            progress_callback: Progress callback function

        Returns:
            Historical data statistics
        """
        # Validate request
        self.validation_service.validate_data_request(request)

        # Get exchange adapter
        exchange = self.exchange_manager.get_exchange(request.exchange)

        # Create fetcher service
        fetcher = DataFetcherService(
            exchange=exchange,
            logger=self.logger,
        )

        # Report initialization
        if progress_callback:
            progress_callback(
                ProgressInfo(
                    stage=ProgressStage.INITIALIZING,
                    message=f"Starting fetch for {request.symbol} on {request.exchange}",
                )
            )

        # Find existing gaps
        if progress_callback:
            progress_callback(
                ProgressInfo(
                    stage=ProgressStage.ANALYZING,
                    message="Analyzing existing data and gaps",
                )
            )

        gaps = await self.repository.find_data_gaps(
            request.exchange,
            request.symbol,
            request.timeframe,
            request.get_start_timestamp(),
            request.get_end_timestamp(),
        )

        self.logger.info(
            f"Found {len(gaps)} gaps to fill",
            exchange=request.exchange,
            symbol=request.symbol,
            timeframe=str(request.timeframe),
        )

        # Split large gaps if needed
        gaps = self.gap_service.split_large_gaps(
            gaps,
            request.batch_size * 2,  # Allow gaps up to 2x batch size
            request.timeframe,
        )

        # Fetch data for gaps
        if gaps:
            candles = await fetcher.fetch_multiple_gaps(
                request.symbol,
                request.timeframe,
                gaps,
                request.batch_size,
                request.concurrent_fetchers,
                progress_callback,
            )

            # Store fetched candles
            if progress_callback:
                progress_callback(
                    ProgressInfo(
                        stage=ProgressStage.STORING,
                        message="Storing fetched data",
                    )
                )

            inserted = await self.repository.insert_candles(
                request.exchange,
                request.symbol,
                request.timeframe,
                candles,
            )

            self.logger.info(
                f"Stored {inserted} new candles",
                exchange=request.exchange,
                symbol=request.symbol,
                timeframe=str(request.timeframe),
            )

        # Get final statistics
        total_candles = await self.repository.count_candles(
            request.exchange,
            request.symbol,
            request.timeframe,
            request.get_start_timestamp(),
            request.get_end_timestamp(),
        )

        # Get updated gaps
        final_gaps = await self.repository.find_data_gaps(
            request.exchange,
            request.symbol,
            request.timeframe,
            request.get_start_timestamp(),
            request.get_end_timestamp(),
        )

        stats = HistoricalDataStats(
            exchange=request.exchange,
            symbol=request.symbol,
            timeframe=request.timeframe,
            total_candles=total_candles,
            date_range=DataRange(request.start_date, request.end_date),
            last_updated=datetime.utcnow(),
            gaps=final_gaps,
        )

        # Report completion
        if progress_callback:
            progress_callback(
                ProgressInfo(
                    stage=ProgressStage.COMPLETE,
                    message=f"Completed: {total_candles} candles, {len(final_gaps)} remaining gaps",
                    percentage=100.0,
                )
            )

        return stats

    async def update_latest_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        lookback_candles: int = 100,
    ) -> int:
        """
        Update with the latest data

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            lookback_candles: Number of recent candles to fetch

        Returns:
            Number of new candles added
        """
        # Get exchange adapter
        exchange_adapter = self.exchange_manager.get_exchange(exchange)

        # Create fetcher service
        fetcher = DataFetcherService(
            exchange=exchange_adapter,
            logger=self.logger,
        )

        # Fetch latest candles
        candles = await fetcher.fetch_latest(symbol, timeframe, lookback_candles)

        # Store candles
        inserted = await self.repository.insert_candles(exchange, symbol, timeframe, candles)

        self.logger.info(
            f"Updated with {inserted} new candles",
            exchange=exchange,
            symbol=symbol,
            timeframe=str(timeframe),
        )

        return inserted

    async def get_historical_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
    ) -> List[Candle]:
        """
        Get historical data from the database

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_date: Start date
            end_date: End date
            limit: Maximum number of candles
            offset: Number of candles to skip

        Returns:
            List of candles
        """
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        return await self.repository.get_candles(
            exchange, symbol, timeframe, start_time, end_time, limit, offset
        )

    async def find_missing_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_date: datetime,
        end_date: datetime,
    ) -> List[DataGap]:
        """
        Find missing data gaps

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_date: Start date
            end_date: End date

        Returns:
            List of data gaps
        """
        start_time = int(start_date.timestamp() * 1000)
        end_time = int(end_date.timestamp() * 1000)

        return await self.repository.find_data_gaps(
            exchange, symbol, timeframe, start_time, end_time
        )

    async def get_data_stats(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
    ) -> Optional[HistoricalDataStats]:
        """
        Get data statistics

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe

        Returns:
            Historical data statistics or None
        """
        metadata = await self.repository.get_metadata(exchange, symbol, timeframe)

        if not metadata or metadata.candle_count == 0:
            return None

        # Get gaps count
        gaps = await self.repository.find_data_gaps(
            exchange,
            symbol,
            timeframe,
            metadata.first_timestamp or 0,
            metadata.last_timestamp or 0,
        )

        date_range = DataRange(
            datetime.fromtimestamp((metadata.first_timestamp or 0) / 1000),
            datetime.fromtimestamp((metadata.last_timestamp or 0) / 1000),
        )

        return HistoricalDataStats(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            total_candles=metadata.candle_count,
            date_range=date_range,
            last_updated=metadata.last_fetch_at or metadata.created_at,
            gaps=len(gaps),  # Just count for stats
        )

    async def get_available_datasets(self) -> List[DatasetInfo]:
        """
        Get list of available datasets

        Returns:
            List of dataset information
        """
        metadata_list = await self.repository.list_datasets()

        datasets = []
        for metadata in metadata_list:
            try:
                timeframe = Timeframe.from_string(metadata.timeframe)
                datasets.append(
                    DatasetInfo(
                        exchange=metadata.exchange,
                        symbol=metadata.symbol,
                        timeframe=timeframe,
                        first_timestamp=metadata.first_timestamp,
                        last_timestamp=metadata.last_timestamp,
                        candle_count=metadata.candle_count,
                        last_fetch_at=metadata.last_fetch_at,
                        created_at=metadata.created_at,
                    )
                )
            except ValueError:
                # Skip invalid timeframes
                pass

        return datasets

    async def delete_data(
        self,
        exchange: str,
        symbol: str,
        timeframe: Timeframe,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> int:
        """
        Delete historical data

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            timeframe: Timeframe
            start_date: Start date (optional)
            end_date: End date (optional)

        Returns:
            Number of candles deleted
        """
        start_time = int(start_date.timestamp() * 1000) if start_date else None
        end_time = int(end_date.timestamp() * 1000) if end_date else None

        deleted = await self.repository.delete_candles(
            exchange, symbol, timeframe, start_time, end_time
        )

        self.logger.info(
            f"Deleted {deleted} candles",
            exchange=exchange,
            symbol=symbol,
            timeframe=str(timeframe),
        )

        return deleted
