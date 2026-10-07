from __future__ import annotations
from datetime import timedelta, datetime
from pyspark.sql import DataFrame

from melius.libraries.Logger import Logger
from melius.session.sources.AdlsGen2Source import AdlsGen2Source
from melius.session.JobRunSession import JobRunSession
from melius.session.Watermark import Watermark
from melius.libraries.Constants import FileTypes


class TimePartitionedParquetSource(AdlsGen2Source):
    """
    TimePartitionedParquetSource handles loading data from time-partitioned Parquet storage sources with watermark management.
    """

    def __init__(
        self,
        session: JobRunSession,
        dataName: str,
        dataVersion: str,
        storageConfig: str,
        parquetRootPath: str,
        parquetPathPattern: str,
        watermarkPath: str,
        lookBackDelta: timedelta,
        backFillStartDateTime: datetime,
        watermarkStorageConfig: str = None
    ) -> None:
        """
        Initialize the DeltaLakeSource.

        Args:
            session: JobRunSession object
            dataName: Name of the source data
            dataVersion: Version of the source data
            storageConfig: Storage configuration name for the source
            parquetRootPath: relative root path to the Parquet data
            parquetPathPattern: pattern for the Parquet file paths
            watermarkPath: relative path to the watermark file
            lookBackDelta: Time delta to look back for new data
            backFillStartDateTime: start datetime for backfilling data
            watermarkStorageConfig: Storage configuration name for the watermark (optional). if not provided, uses storageConfig.
        """
        super().__init__(
            session=session,
            dataName=dataName,
            dataVersion=dataVersion,
            storageConfig=storageConfig,
            dataRootPath=parquetRootPath,
            lookBackDelta=lookBackDelta,
            backFillStartDateTime=backFillStartDateTime
        )

        self._parquetPathPattern = parquetPathPattern

        self._watermark = Watermark(
            dataName=dataName,
            dataVersion=dataVersion,
            storageConfig=watermarkStorageConfig if watermarkStorageConfig is not None else storageConfig,
            watermarkPath=watermarkPath
        )

    # -------------------------
    # Data processing methods for Base Model data.
    # -------------------------
    def loadLatestParquet(self) -> tuple[DataFrame, str]:
        """
        Load the latest parquet file based on the previous watermark.

        Returns:
            A tuple containing the DataFrame of the latest parquet and an error message if any.

        Raises:
            ValueError: If watermark is not set
            FileNotFoundError: If no base data files are found
        """
        watermark: Watermark = self.session.watermark
        filePath = f"{self._dataRootPath}/{watermark.previousWatermarkDateTime.strftime(self._parquetPathPattern)}"

        return self.loadParquet(filePath)

    def loadAndValidateLatestParquet(self, required_columns: list[str] = None) -> tuple[DataFrame, str]:
        """
        Load the latest parquet file based on the previous watermark and validate required columns.

        Args:
            required_columns: List of required column names to validate, None means no validation.

        Returns:
            DataFrame containing the validated base model data and error message if any.

        Raises:
            ValueError: If validation fails
            FileNotFoundError: If no base data files are found
        """
        dfBase, msg = self.loadLatestParquet()

        if dfBase is None and msg:
            return None, msg

        if required_columns:
            missing_columns = [col for col in required_columns if col not in dfBase.columns]
            if missing_columns:
                raise ValueError(f"Required columns missing from base model data: {missing_columns}")

        return dfBase, None

    def loadLatestParquetBetween(self, startTime: datetime, endTime: datetime) -> tuple[DataFrame, str]:
        """
        Load the latest parquet file between specified start and end times.

        Returns:
            A tuple containing the DataFrame with the dataframe and the source path and error message if any.

        Raises:
            ValueError: If watermark is not set
            FileNotFoundError: If no base data files are found
        """

        Logger.getInstance().writeInfo(f"Searching for the latest parquet file between {startTime} and {endTime} in path: '{self.dataRootPath}'")
        connector = self.getStorageConnector()
        includedDataFile, error = connector.findLatestOrOldestFileInRange(
            self.dataRootPath,
            start_dt=startTime,
            end_dt=endTime,
            prefix=self.dataName,
            fileType=FileTypes.Parquet,
            latest=True
        )

        if includedDataFile is None or error:
            raise FileNotFoundError(f"No parquet file found between {startTime} and {endTime}. Error: {error}")

        Logger.getInstance().writeInfo(f"Found the latest parquet file: '{includedDataFile}' between {startTime} and {endTime}")

        return self.loadParquet(includedDataFile)

    @property
    def parquetPathPattern(self) -> str:
        return self._parquetPathPattern

    @property
    def parquetRootPath(self) -> str:
        return self._dataRootPath