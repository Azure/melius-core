from __future__ import annotations
from datetime import timedelta, datetime
from pyspark.sql import DataFrame

from melius.libraries.Constants import KnownStrings
from melius.libraries.Logger import Logger
from melius.session.sources.AdlsGen2Source import AdlsGen2Source
from melius.session.JobRunSession import JobRunSession
from melius.session.Watermark import Watermark


class DeltaLakeSource(AdlsGen2Source):
    """
    DeltaLakeSource handles loading data from Delta Lake storage sources with watermark management.
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
        minDataAge: timedelta | None = None
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
            minDataAge: Minimum new data age to process. If None, don't consider minimum new data.

        Returns:
            Tuple of (DataFrame, error_message). DataFrame is None if no data or error
        """
        if minDataAge is not None and minDataAge < lookBackDelta:
            return None, f"minDataAge({minDataAge}) should be greater than or equal to lookBackDelta ({lookBackDelta})."

        watermarkDest = previousWatermarkDt
        watermarkSrc = self._watermark.previousWatermarkDateTime

        if watermarkSrc is None:
            Logger.getInstance().writeInfo("No watermark found in source. Source may not exist yet.")
            return None, "No watermark found in source. Source may not exist yet."

        startTime = None
        endTime = None

        if watermarkDest is None:
            endTime = backFillStartDateTime + timedelta(hours=1)
            Logger.getInstance().writeInfo(f"Backfill from the source table since {backFillStartDateTime}.")
        elif (minDataAge is None and watermarkSrc > watermarkDest) or (minDataAge is not None and (watermarkSrc - watermarkDest) >= minDataAge):
            startTime = watermarkDest
            endTime = startTime + lookBackDelta
        else:
            Logger.getInstance().writeInfo("No new data found in source.")
            return None, f"No new data since last run. Last watermarkDest: {watermarkDest} / Last watermarkSrc: {watermarkSrc}"

        if endTime > watermarkSrc:
            Logger.getInstance().writeInfo(f"Adjust the initial endTime({endTime}) since it is later than watermarkSrc({watermarkSrc}).")
            endTime = watermarkSrc

        if startTime is None:
            Logger.getInstance().writeInfo(f"Setting startTime based on endTime({endTime}) and lookBackDelta({lookBackDelta}).")
            startTime = endTime - lookBackDelta

        Logger.getInstance().writeInfo(f"Loading new data from source table from {startTime} to {endTime} with filter: {filter}.")

        dfDelta, msg = self.loadDataBetweenStartDTAndEndDT(startTime, endTime, filter, autoRepartition, persist)

        if updateCurrentWatermark:
            Logger.getInstance().writeInfo(f"Updating current watermark to {endTime}.")
            self.setCurrentWatermark(endTime)
        else:
            Logger.getInstance().writeInfo("Not updating current watermark as per parameter setting.")

        return dfDelta, msg

    def loadLatestData(
        self,
        filter: str,
        window: timedelta,
        autoRepartition: bool = True,
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

        dfDelta, msg = self.loadDataBetweenStartDTAndEndDT(startTime, endTime, filter, autoRepartition)

        if updateCurrentWatermark:
            Logger.getInstance().writeInfo(f"Updating current watermark to {endTime}.")
            self.setCurrentWatermark(endTime)
        else:
            Logger.getInstance().writeInfo("Not updating current watermark as per parameter setting.")

        return dfDelta, msg

    def loadNewDataWithWindowSize(
        self,
        filter: str,
        window_size: timedelta,
        previousWatermarkDt: datetime,
        backFillStartDateTime: datetime,
        lookBackDelta: timedelta,
        timeColumn: str = KnownStrings.SourceIngestionTime,
        minDataAge: timedelta | None = None,
        autoRepartition: bool = True,
        persist: bool = True,
        updateCurrentWatermark: bool = False
    ) -> tuple[DataFrame, str]:
        """
        Enhanced version of loadNewSourceData that supports custom window sizes.

        Args:
            filter: SQL filter for the data
            window_size: How much historical data to include
            previousWatermarkDt: Previous watermark datetime
            backFillStartDateTime: Backfill start datetime if no watermark exists
            lookBackDelta: Time delta to look back for new data.
            timeColumn: The column name representing time in the source data for filtering. default is KnownStrings.SourceIngestionTime.
            minDataAge: Minimum new data age required to trigger loading. if None, load whatever is available.
            autoRepartition: Whether to auto repartition the dataframe
            persist: Whether to persist the loaded dataframe to disk
            updateCurrentWatermark: Whether to update the current watermark after loading data

        Returns:
            Tuple of (DataFrame, error_message). DataFrame is None if no data or error.
        """

        if minDataAge is not None and minDataAge < lookBackDelta:
            return None, f"minDataAge({minDataAge}) should be greater than or equal to lookBackDelta ({lookBackDelta})."

        watermarkDest = previousWatermarkDt
        watermarkSrc = self._watermark.previousWatermarkDateTime

        if watermarkSrc is None:
            return None, "No watermark found in source."

        if watermarkDest is None:
            Logger.getInstance().writeInfo("No previous watermark found in destination. Starting backfill.")
            endTime = backFillStartDateTime
        elif watermarkSrc > watermarkDest:
            if minDataAge is None or watermarkSrc >= (watermarkDest + minDataAge):
                Logger.getInstance().writeInfo("Enough new data found in source.")
                endTime = watermarkDest + lookBackDelta
            else:
                return None, f"No new data or not enough new data based on minDataAge({minDataAge}). Last watermarkDest: {watermarkDest} / Last watermarkSrc: {watermarkSrc} / window_size: {window_size}"
            startTime = endTime - window_size
            if minDataAge is None:
                Logger.getInstance().writeInfo("minDataAge is None. Using watermarkSource as endTime.")
            Logger.getInstance().writeInfo(f"Processing new data with {window_size} window from {startTime} to {endTime}")
        else:
            Logger.getInstance().writeInfo("No new data found in source.")
            return None, f"No new data since last run. Last watermarkDest: {watermarkDest} / Last watermarkSrc: {watermarkSrc}"

        if endTime > watermarkSrc:
            Logger.getInstance().writeInfo(f"The initial endTime({endTime}) is later than watermarkSrc({watermarkSrc})."
                                           f"Use watermarkSource as endTime.")
            endTime = watermarkSrc
        startTime = endTime - window_size
        Logger.getInstance().writeInfo(f"Processing new data with {window_size} window from {startTime} to {endTime}")

        dfDelta, msg = self.loadDataBetweenStartDTAndEndDT(
            startTime,
            endTime,
            filter,
            autoRepartition,
            persist,
            timeColumn=timeColumn
        )

        if updateCurrentWatermark:
            Logger.getInstance().writeInfo(f"Updating current watermark to {endTime}.")
            self.setCurrentWatermark(endTime)
        else:
            Logger.getInstance().writeInfo("Not updating current watermark as per parameter setting.")

        return dfDelta, msg

    def getMinValueOfTimeColumn(
        self,
        timeColumn: str
    ) -> datetime | None:
        """
        Get the minimum value of the specified time column from the Delta Lake source.

        Args:
            timeColumn: The column name representing time in the source data.

        Returns:
            The minimum value of the specified time column, or None if no data is found.
        """
        Logger.getInstance().writeInfo(f"Getting minimum value of column {timeColumn} from Delta Lake source.")
        dfDelta = self.loadFromDeltaTable(filter=None, autoRepartition=False)

        if dfDelta is None or dfDelta.rdd.isEmpty():
            Logger.getInstance().writeInfo("No data found in Delta Lake source to get minimum time value.")
            return None

        minTimeRow = dfDelta.agg({timeColumn: "min"}).collect()[0]
        minTimeValue = minTimeRow[f"min({timeColumn})"]

        Logger.getInstance().writeInfo(f"Minimum value of column {timeColumn} is {minTimeValue}.")

        return minTimeValue

    def getMaxValueOfTimeColumn(
        self,
        timeColumn: str,
        afterDateTime: datetime | None = None,
        excludeFutureDateTime: bool = True
    ) -> datetime | None:
        """
        Get the maximum value of the specified time column from the Delta Lake source.

        Args:
            timeColumn: The column name representing time in the source data.
            afterDateTime: Optional datetime to filter records after this datetime.
            excludeFutureDateTime: Whether to exclude future datetime values from max calculation.

        Returns:
            The maximum value of the specified time column, or None if no data is found.
        """
        Logger.getInstance().writeInfo(f"Getting maximum value of column {timeColumn} from Delta Lake source.")
        if afterDateTime is not None:
            filter = f"{timeColumn} > '{afterDateTime.strftime('%Y-%m-%d %H:%M:%S')}'"
            Logger.getInstance().writeInfo(f"Applying filter to get records after {afterDateTime}: {filter}.")
        else:
            filter = None
            Logger.getInstance().writeInfo("No afterDateTime filter applied.")
        dfDelta = self.loadFromDeltaTable(filter=filter, autoRepartition=False)

        if dfDelta is None or dfDelta.rdd.isEmpty():
            Logger.getInstance().writeInfo("No data found in Delta Lake source to get maximum time value.")
            return None

        if excludeFutureDateTime:
            currentDt = datetime.utcnow()
            Logger.getInstance().writeInfo(f"Excluding future datetime values greater than current datetime {currentDt}.")
            dfDelta = dfDelta.filter(dfDelta[timeColumn] <= currentDt)

            if dfDelta.rdd.isEmpty():
                Logger.getInstance().writeInfo("No data found in Delta Lake source after excluding future datetime values.")
                return None

        maxTimeRow = dfDelta.agg({timeColumn: "max"}).collect()[0]
        maxTimeValue = maxTimeRow[f"max({timeColumn})"]

        Logger.getInstance().writeInfo(f"Maximum value of column {timeColumn} is {maxTimeValue}.")

        return maxTimeValue

    def getMinMaxValuesOfTimeColumn(
        self,
        timeColumn: str,
        excludeFutureDateTime: bool = True,
        df: DataFrame = None
    ) -> tuple[datetime | None, datetime | None]:
        """
        Get the minimum and maximum values of the specified time column in a single pass.

        Args:
            timeColumn: The column name representing time in the source data.
            excludeFutureDateTime: Whether to exclude future datetime values from the calculation.
            df: Optional pre-loaded/filtered DataFrame to compute min/max over. When not
                provided, the whole Delta Lake source is loaded. Passing an already-loaded
                DataFrame avoids re-scanning the source and lets callers scope the min/max to
                a subset (e.g. an already-filtered, persisted DataFrame).

        Returns:
            A tuple containing the minimum and maximum values of the specified time column, or (None, None) if no data is found.
        """
        Logger.getInstance().writeInfo(f"Getting min and max values of column {timeColumn}.")
        if df is None:
            df = self.loadFromDeltaTable(filter=None, autoRepartition=False)

        if df is None:
            Logger.getInstance().writeInfo("No data found to get minimum and maximum time values.")
            return None, None

        if excludeFutureDateTime:
            currentDt = datetime.utcnow()
            Logger.getInstance().writeInfo(f"Excluding future datetime values greater than current datetime {currentDt}.")
            df = df.filter(df[timeColumn] <= currentDt)

        # Single action computes both min and max. An empty (or fully future-dated) input
        # yields nulls here, so no extra emptiness-check action is required.
        row = df.selectExpr(
            f"min({timeColumn}) as minTimeValue",
            f"max({timeColumn}) as maxTimeValue",
        ).collect()[0]

        minTimeValue = row["minTimeValue"]
        maxTimeValue = row["maxTimeValue"]

        Logger.getInstance().writeInfo(
            f"Min/Max values of column {timeColumn}: {minTimeValue} / {maxTimeValue}."
        )

        return minTimeValue, maxTimeValue

    @property
    def dataDeltaTablePath(self) -> str:
        return self._dataDeltaTablePath

    @property
    def dataWatermarkPath(self) -> str:
        return self._dataWatermarkPath