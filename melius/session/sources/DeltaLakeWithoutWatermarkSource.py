from __future__ import annotations
from datetime import timedelta, datetime
from pyspark.sql import DataFrame

from melius.libraries.WatermarkHelper import WatermarkHelper
from melius.libraries.Constants import KnownStrings
from melius.libraries.Logger import Logger
from melius.session.sources.AdlsGen2Source import AdlsGen2Source
from melius.session.JobRunSession import JobRunSession
from melius.session.Watermark import Watermark


class DeltaLakeWithoutWatermarkSource(AdlsGen2Source):
    """
    DeltaLakeWithoutWatermarkSource handles loading data from Delta Lake storage sources without watermark management.
    """

    def __init__(
        self,
        session: JobRunSession,
        dataName: str,
        dataVersion: str,
        storageConfig: str,
        deltaLakePath: str,
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
            deltaLakePath: relative path to the Delta Lake data
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
            dataRootPath=deltaLakePath,
            lookBackDelta=lookBackDelta,
            backFillStartDateTime=backFillStartDateTime
        )

        self._watermark = Watermark(
            dataName=dataName,
            dataVersion=dataVersion,
            storageConfig=watermarkStorageConfig if watermarkStorageConfig is not None else storageConfig,
            watermarkPath=watermarkPath
        )

    def loadNewData(
        self,
        filter: str,
        previousWatermarkDt: datetime | None,
        backFillStartDateTime: datetime,
        lookBackDelta: timedelta,
        autoRepartition: bool = True,
        persist: bool = True,
        updateCurrentWatermark: bool = False,
        minDataAge: timedelta = None,
        timeColumn: str = KnownStrings.SourceIngestionTime
    ) -> tuple[DataFrame, str]:
        """
        Load new data from the source based on watermarks (previous -destination- and current -source-).
        if there is no new data, return None for dataframe and appropriate message.

        Args:
            filter: SQL filter to apply when loading data
            previousWatermarkDt: Previous watermark datetime
            backFillStartDateTime: Backfill start datetime if no watermark exists
            lookBackDelta: Time delta to look back for new data
            autoRepartition: Whether to auto repartition the dataframe
            persist: Whether to persist the loaded dataframe to disk
            updateCurrentWatermark: Whether to update the current watermark after loading data
            minDataAge: Minimum new data age to consider. if None, don't consider minimum new data.
            timeColumn: The column name representing ingestion time in the source data to filter on.

        Returns:
            Tuple of (DataFrame, error_message). DataFrame is None if no data or error
        """
        watermarkDest = previousWatermarkDt

        if self.getStorageConnector().existsPath(self.dataRootPath) is False:
            Logger.getInstance().writeInfo(f"Delta table path {self.dataRootPath} does not exist. No data to load.")
            return None, f"Delta table path {self.dataRootPath} does not exist. No data to load."

        if watermarkDest is None:  # first run or backfill
            startTime = backFillStartDateTime + timedelta(hours=1)  # add 1hour to allow base model available in backfill.
        else:  # normal incremental run: use last watermark as start time
            startTime = watermarkDest
        # always set endTime as startTime + lookBackDelta no matter first run or incremental run
        endTime = startTime + lookBackDelta

        loadedData, errMsg = self.loadDataBetweenStartDTAndEndDT(startTime, endTime, filter, autoRepartition, persist, timeColumn=timeColumn)

        if loadedData is not None and not loadedData.rdd.isEmpty():  # normal case: continous incremental data
            # set the watermarkDateTime to the max ingestion time of the loaded data.
            # watermarkDateTime can be less than endTime if source ingestion has not populated data up to endTime yet.
            watermarkDateTime = loadedData.agg({KnownStrings.SourceIngestionTime: "max"}).collect()[0][0]
            Logger.getInstance().writeInfo(f"Loaded new source data from {startTime} to {watermarkDateTime}.")
        else:  # no new data between startTime and endTime. This can happen in two cases. Need to further check if there is new data beyond startTime.
            smallestTime = self.getStorageConnector().getFirstIngestionDateTimeOfDeltaTableSince(self._sourceDataDeltaTablePath, timeColumn, startTime.strftime(WatermarkHelper.DateTimeFormat))
            if smallestTime is not None:  # Case 1: This means there is no data between startTime and endTime but there is new data beyond endTime. This can happen in 3 subcases:
                # subcase 1: the backfill startTime is set to too old and even earlier than the olded source data ingestion time.
                # subcase 2: Two contiguous time ranges of data are separated by a missing-data interval. This is unlikely to happen unless source ingestion is paused for a while.
                # subcase 3: health model run reached the end of the source ingestion at the first query, then source ingestion added new data before the second query.
                # no matter which subcase it is, we just load data from startTime to the smallestTime found, and set watermark to smallestTime.
                Logger.getInstance().writeInfo(f"Source startTime since {startTime} is found: {smallestTime}.")
                loadedData, errMsg = self.loadDataBetweenStartDTAndEndDT(startTime, smallestTime, filter, autoRepartition, persist, timeColumn=timeColumn)
                if loadedData is not None:
                    Logger.getInstance().writeInfo(f"Loaded new source data from {startTime} to {smallestTime}.")
                    watermarkDateTime = smallestTime
                else:
                    Logger.getInstance().writeInfo("No new data found in source after rechecking.")
                    watermarkDateTime = None
            else:
                # health model run reached the end of the source ingestion and there is no new data beyond endTime.
                Logger.getInstance().writeInfo("No new data found in source.")
                loadedData = None
                errMsg = f"No new data since last run. Last watermarkDest: {watermarkDest}"
                watermarkDateTime = None

        if watermarkDateTime is not None and updateCurrentWatermark:
            Logger.getInstance().writeInfo(f"Updating current watermark to {watermarkDateTime}.")
            self.setCurrentWatermark(watermarkDateTime)
        else:
            Logger.getInstance().writeInfo("Not updating current watermark as per parameter setting. or no new data to update.")

        return loadedData, errMsg

    def loadNewDataWithWindowSize(
        self,
        filter: str,
        window_size: timedelta,
        previousWatermarkDt: datetime | None,
        backFillStartDateTime: datetime,
        lookBackDelta: timedelta,
        minDataAge: timedelta | None = None,
        autoRepartition: bool = True,
        persist: bool = True,
        updateCurrentWatermark: bool = False,
        timeColumn: str = KnownStrings.SourceIngestionTime
    ) -> tuple[DataFrame, str]:
        """
        Enhanced version of loadNewSourceData that supports custom window sizes.

        Args:
            filter: SQL filter for the data
            window_size: How much historical data to include
            previousWatermarkDt: Previous watermark datetime
            backFillStartDateTime: Backfill start datetime if no watermark exists
            lookBackDelta: Time delta to look back for new data
            minDataAge: Minimum new data age required to trigger loading. if None, load whatever is available.
            autoRepartition: Whether to auto repartition the dataframe
            persist: Whether to persist the loaded dataframe to disk
            updateCurrentWatermark: Whether to update the current watermark after loading data

        Returns:
            Tuple of (DataFrame, error_message). DataFrame is None if no data or error.
        """
        watermarkDest = previousWatermarkDt
        if self.getStorageConnector().existsPath(self.dataRootPath) is False:
            Logger.getInstance().writeInfo(f"Delta table path {self.dataRootPath} does not exist. No data to load.")
            return None, f"Delta table path {self.dataRootPath} does not exist. No data to load."

        if watermarkDest is None:  # first run or backfill
            endTime = backFillStartDateTime
        else:
            endTime = watermarkDest + lookBackDelta

        watermarkSrc = self.getStorageConnector().getLastIngestionDateTimeOfDeltaTable(self._sourceDataDeltaTablePath, timeColumn)

        startTime = endTime - window_size

        loadedData, errMsg = self.loadDataBetweenStartDTAndEndDT(startTime, endTime, filter, autoRepartition, persist, timeColumn=timeColumn)

        if watermarkDest is None:
            endTime = backFillStartDateTime
            startTime = endTime - window_size
            if startTime > watermarkSrc:
                Logger.getInstance().writeInfo(f"Source is in catching up mode. Adjusting backfill start time to the current source Watermark {watermarkSrc}.")
                startTime = watermarkSrc - window_size
                endTime = watermarkSrc
            Logger.getInstance().writeInfo(f"Backfill with {window_size} window from {startTime} to {endTime}")
        elif minDataAge is None:
            endTime = watermarkSrc
            startTime = endTime - window_size
            Logger.getInstance().writeInfo("minDataAge is None. use watermarkSource as endTime.")
            Logger.getInstance().writeInfo(f"Processing new data with {window_size} window from {startTime} to {endTime}")
        elif watermarkSrc >= (watermarkDest + minDataAge):
            endTime = watermarkDest + minDataAge
            startTime = endTime - window_size
            Logger.getInstance().writeInfo(f"New data of at least {minDataAge} old found since last run. use watermarkDest + minDataAge as endTime.")
            Logger.getInstance().writeInfo(f"Processing new data with {window_size} window from {startTime} to {endTime}")
        else:
            return None, f"No new data or not enough new data since last run. Last watermarkDest: {watermarkDest} / Last watermarkSrc: {watermarkSrc} / window_size: {window_size}"

        loadedData, errMsg = self.loadDataBetweenStartDTAndEndDT(startTime, endTime, filter, autoRepartition, persist)

        if updateCurrentWatermark:
            Logger.getInstance().writeInfo(f"Updating current watermark to {endTime}.")
            self.setCurrentWatermark(endTime)
        else:
            Logger.getInstance().writeInfo("Not updating current watermark as per parameter setting.")
        return loadedData, errMsg

    def loadLatestData(
        self,
        filter: str,
        window: timedelta,
        autoRepartition: bool = True,
        persist: bool = True,
        updateCurrentWatermark: bool = False
    ) -> tuple[DataFrame, str]:
        """
        Load latest data from the source without checking for new watermarks.

        Args:
            filter: SQL filter to apply when loading data
            window: Time window to include historical data
            setWatermarkCallback: Callback function to set watermark
            loadFromDeltaTableCallback: Callback function to load from delta table
            autoRepartition: Whether to auto repartition the dataframe
            persist: Whether to persist the loaded dataframe to disk. default is True
            updateCurrentWatermark: Whether to update the current watermark after loading data

        Returns:
            Tuple of (DataFrame, error_message). DataFrame is None if no data or error
        """
        watermarkSrc = self._watermark.previousWatermarkDateTime

        if window is None or window.total_seconds() <= 0:
            return None, "Invalid window size."

        if watermarkSrc is None:
            return None, "No watermark found in source."

        endTime = watermarkSrc
        startTime = endTime - window
        Logger.getInstance().writeInfo(f"Loading latest data from source table from {startTime} to {endTime} with filter: {filter}.")

        loadedData, errMsg = self.loadDataBetweenStartDTAndEndDT(startTime, endTime, filter, autoRepartition, persist)

        if updateCurrentWatermark:
            Logger.getInstance().writeInfo(f"Updating current watermark to {endTime}.")
            self.setCurrentWatermark(endTime)
        else:
            Logger.getInstance().writeInfo("Not updating current watermark as per parameter setting.")

        return loadedData, errMsg

    @property
    def dataDeltaTablePath(self) -> str:
        return self._dataDeltaTablePath

    @property
    def dataWatermarkPath(self) -> str:
        return self._dataWatermarkPath