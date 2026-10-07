from __future__ import annotations
from datetime import timedelta, datetime
import json
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel

from melius.connectors.BlobStorageConnector import BlobStorageConnector
from melius.session.sources.BaseSource import BaseSource
from melius.session.JobRunSession import JobRunSession
from melius.libraries.Logger import Logger
from melius.libraries.Constants import KnownStrings
from melius.libraries.WatermarkHelper import WatermarkHelper


class AdlsGen2Source(BaseSource):
    """
    AdlsGen2Source handles loading data from ADLS Gen2 storage sources with watermark management.
    """

    def __init__(
        self,
        session: JobRunSession,
        dataName: str,
        dataVersion: str,
        storageConfig: str,
        dataRootPath: str,
        lookBackDelta: timedelta,
        backFillStartDateTime: datetime
    ) -> None:
        """
        Initialize the DeltaLakeSource.

        Args:
            session: JobRunSession object
            dataName: Name of the source data
            dataVersion: Version of the source data
            storageConfig: Storage configuration for the source
            dataRootPath: Root path for the source data
            lookBackDelta: Time delta to look back for new data
            backFillStartDateTime: Start datetime for backfilling data
        """
        super().__init__(
            session=session,
            name=f"DeltaLakeSource_{dataName}_{dataVersion}",
            lookBackDelta=lookBackDelta,
            backFillStartDateTime=backFillStartDateTime
        )
        self._storageConfig = storageConfig
        Logger.getInstance().writeInfo(f"Initialized AdlsGen2Source for data: {dataName}, version: {dataVersion}, dataRootPath: {dataRootPath}")
        Logger.getInstance().writeInfo(f"Storage Config: \n{json.dumps(storageConfig, indent=2)}")

        self._dataName = dataName
        self._dataVersion = dataVersion

        self._dataRootPath = dataRootPath

    def getStorageConnector(self) -> BlobStorageConnector:
        """
        Get the BlobStorageConnector for the source data.
        """
        return BlobStorageConnector.getConnector(self._storageConfig)

    def getDataStorageAbsolutePath(self) -> str:
        """
        Get the base storage absolute path for the source data.
        """
        return self.getStorageConnector().getAbsolutePath(self._dataRootPath)

    def loadFromDeltaTable(self, filter: str, autoRepartition: bool = True) -> DataFrame:
        """
        Load data from the Delta table.
        """
        connector = self.getStorageConnector()
        return connector.loadDeltaTable(self.dataRootPath, filter=filter, autoRepartition=autoRepartition)

    def loadParquet(self, filePath: str) -> DataFrame:
        """
        Load a parquet file from the specified path.

        Args:
            filePath: Path to the parquet file.
        Returns:
            DataFrame containing the loaded parquet data. error message if any.
        """
        Logger.getInstance().writeInfo(f"Loading a parquet file from path: '{filePath}'")
        connector = self.getStorageConnector()
        dfBase = connector.loadFrom([filePath])
        if dfBase is None or dfBase.rdd.isEmpty():
            return None, f"Parquet file '{filePath}' is empty"

        Logger.getInstance().writeInfo(f"Successfully loaded a parquet file '{filePath}'")
        return dfBase, None

    def loadDataBetweenStartDTAndEndDT(
        self,
        startTime: datetime,
        endTime: datetime,
        filter: str = None,
        autoRepartition: bool = True,
        persist: bool = False,
        timeColumn: str = KnownStrings.SourceIngestionTime
    ) -> tuple[DataFrame, str]:
        """
        Internal method to load source data between specified start time (exclusive) and end time (inclusive).
        Watemark management is handled outside this method. please make sure that caller handles watermark updates.

        Args:
            startTime: Start time for data loading (exclusive)
            endTime: End time for data loading (inclusive)
            filter: SQL filter to apply when loading data
            autoRepartition: Whether to auto repartition the dataframe
            persist: Whether to persist the loaded dataframe to disk
            timeColumn: The column name representing time in the source data for filtering. default is KnownStrings.SourceIngestionTime.

        Returns:
            Tuple of (DataFrame, error_message). DataFrame is None if no data or error
        """
        if startTime >= endTime:
            error_msg = f"Invalid time range: startTime ({startTime}) must be less than endTime ({endTime})"
            Logger.getInstance().writeError(error_msg)
            return None, error_msg

        timefilter = f"{timeColumn} > '{startTime.strftime(WatermarkHelper.DateTimeFormat)}' AND {timeColumn} <= '{endTime.strftime(WatermarkHelper.DateTimeFormat)}'"

        if filter is None or filter == "":
            filter = timefilter
        else:
            filter = f"({timefilter}) AND ({filter})"

        Logger.getInstance().writeInfo(f"Loading data between {startTime} and {endTime} with filter: {filter}")
        dfDelta = self.loadFromDeltaTable(filter, autoRepartition)

        if dfDelta is None or dfDelta.rdd.isEmpty():
            Logger.getInstance().writeInfo(f"No new data after filtering: {filter}")
            return None, f"No new data after filtering: {filter}"

        if persist:
            Logger.getInstance().writeInfo("Persisting new source data to disk.")
            dfDelta.persist(StorageLevel.DISK_ONLY)

        return dfDelta, None

    @property
    def dataName(self) -> str:
        return self._dataName

    @property
    def dataVersion(self) -> str:
        return self._dataVersion

    @property
    def storageConfig(self) -> str:
        return self._storageConfig

    @property
    def dataRootPath(self) -> str:
        return self._dataRootPath