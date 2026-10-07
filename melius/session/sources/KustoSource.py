from __future__ import annotations
from datetime import timedelta, datetime
from pyspark.sql import DataFrame
from pyspark import StorageLevel

from melius.libraries.WatermarkHelper import WatermarkHelper
from melius.libraries.Constants import KnownStrings
from melius.libraries.Logger import Logger
from melius.connectors.KustoConnector import KustoConnector
from melius.session.sources.BaseSource import BaseSource
from melius.session.JobRunSession import JobRunSession


class KustoSource(BaseSource):
    """
    KustoSource handles loading data from Kusto sources
    """

    def __init__(
        self,
        session: JobRunSession,
        kustoCluster: str,
        kustoTable: str,
        kustoDatabase: str,
        query: str,
        watermarkQuery: str,
        parallelism: int,
        lookBackDelta: timedelta,
        backFillStartDateTime: datetime,
        ingestionTimeMoniker: str = KnownStrings.IngestionTimeMoniker
    ) -> None:
        """
        Initialize the KustoSource.

        Args:
            session: VMWatchSession instance for context and utilities
            kustoCluster: Name of the Kusto cluster
            kustoTable: Name of the Kusto table
            kustoDatabase: Name of the Kusto database
            query: Kusto query to execute
            watermarkQuery: Kusto query to get the watermark
            parallelism: Degree of parallelism for Kusto queries
            lookBackDelta: Time delta to look back for new data if no watermark exists
            backFillStartDateTime: Start datetime for backfilling data
            ingestionTimeMoniker: Column name for ingestion time
        """
        super().__init__(
            session=session,
            name=f"KustoSource_{kustoDatabase}_{kustoTable}",
            lookBackDelta=lookBackDelta,
            backFillStartDateTime=backFillStartDateTime
        )

        self._kustoDatabase = kustoDatabase
        self._kustoTable = kustoTable
        self._kustoCluster = kustoCluster
        self._query = query
        self._watermarkQuery = watermarkQuery
        self._parallelism = parallelism
        self._ingestionTimeMoniker = ingestionTimeMoniker
        self._kustoConfig = {
            f"{KustoConnector.Cluster}": self._kustoCluster,
            f"{KustoConnector.Database}": self._kustoDatabase,
            f"{KustoConnector.Table}": self._kustoTable,
            f"{KustoConnector.IngestionTimeMoniker}": self._ingestionTimeMoniker
        }
        Logger.getInstance().writeInfo(f"Initialized KustoSource for database: {kustoDatabase}, table: {kustoTable}, cluster: {kustoCluster}")

    def getKustoConnector(self) -> KustoConnector:
        """
        Get the KustoConnector for the source data.
        """
        return KustoConnector.getConnector(self._kustoConfig)

    def getLatestDataDateTime(self, forceRefresh: bool = False) -> datetime | None:
        """
        Get the latest data datetime from Kusto
        Args:
            forceRefresh: Whether to force refresh the latest data datetime from Kusto. default is False.
        """
        if forceRefresh or not hasattr(self, '_currentWatermarkDateTime') or self._currentWatermarkDateTime is None:
            Logger.getInstance().writeInfo("Forcing refresh of latest data datetime from Kusto.")
            if self._watermarkQuery is None:
                Logger.getInstance().writeInfo("No watermark query provided, using last ingestion datetime from Kusto connector.")
                self._currentWatermarkDateTime = self.getKustoConnector().getLastIngestionDateTime()
            else:
                Logger.getInstance().writeInfo("Fetching latest data datetime using provided watermark query.")
                self._currentWatermarkDateTime = self.getKustoConnector().getFirstValueFromQuery(self._watermarkQuery)

        Logger.getInstance().writeInfo(f"Current latest data datetime from Kusto: {self._currentWatermarkDateTime}")
        return self._currentWatermarkDateTime

    def loadNewData(
            self,
            updateCurrentWatermark: bool = False,
            persist: bool=False,
            repartitionCount: int=None,
            partitionSizeInMB: int=None,
            minDataAge: timedelta | None = None) -> tuple[DataFrame, str]:
        """
        Load new data from Kusto source
        Args:
            updateCurrentWatermark: Whether to update the current watermark after loading data. default is False.
            persist: Whether to persist the resulting DataFrame to disk. default is False.
            repartitionCount: Optional number of partitions to repartition the DataFrame to. If None, no repartitioning is done.
            partitionSizeInMB: Optional target partition size in MB. If provided, will calculate repartitionCount based on DataFrame size.
            minDataAge: Minimum new data age to consider. if None, don't consider minimum new data.
        Returns:
            A tuple containing the new DataFrame and an error message (if any).
        """
        if partitionSizeInMB is not None and repartitionCount is not None:
            raise ValueError("you can't specify both partitionSizeInMB and repartitionCount.")

        if minDataAge is not None and minDataAge < self.session.lookBackDelta:
            return None, f"minDataAge({minDataAge}) should be greater than or equal to lookBackDelta ({self.session.lookBackDelta})."

        previousWatermarkDt = self._session.watermark.previousWatermarkDateTime

        if previousWatermarkDt:
            startTime = previousWatermarkDt
            Logger.getInstance().writeInfo(f"Last watermark at: {previousWatermarkDt}")
        else:
            startTime = self.backFillStartDateTime
            Logger.getInstance().writeInfo(f"No Ingestion yet. Backfill since: {self.backFillStartDateTime}")

        latestSourceDateTime = self.getLatestDataDateTime()
        Logger.getInstance().writeInfo(f"Latest Source data datetime at: {latestSourceDateTime if latestSourceDateTime else 'No ingestion time found in source Kusto.'}")

        startTimeStr = None
        endTimeStr = None

        if latestSourceDateTime is None:
            return None, "No data in source Kusto."
        elif latestSourceDateTime <= startTime:
            return None, f"No new data in source Kusto. Last ingestion time: {startTime} >= Source Ingestion time: {latestSourceDateTime}"
        elif (minDataAge is None and latestSourceDateTime > startTime) or (minDataAge is not None and latestSourceDateTime >= startTime + minDataAge):
            if latestSourceDateTime - startTime < self.session.lookBackDelta:
                endTime = latestSourceDateTime
                Logger.getInstance().writeInfo("New data is less than lookBackDelta. use latestSourceDateTime as endTime.")
            else:
                endTime = startTime + self.session.lookBackDelta
                Logger.getInstance().writeInfo("New data is more than lookBackDelta. use startDateTime + lookBackDelta as endTime.")
            Logger.getInstance().writeInfo(f"Processing new data from source table from {startTime} to {endTime}.")
        else:
            return None, f"No new data in source Kusto. Last ingestion time: {startTime} / Source Ingestion time: {latestSourceDateTime} : minDataAge for {minDataAge}"

        startTimeStr = WatermarkHelper.getUTCStringForKusto(startTime)
        endTimeStr = WatermarkHelper.getUTCStringForKusto(endTime)
        Logger.getInstance().writeInfo(f"Reading new data : > {startTimeStr} and <= {endTimeStr}")

        query = self._query
        if startTimeStr is not None:
            query = query.replace(KnownStrings.IngestionStartDT, startTimeStr)
        if endTimeStr is not None:
            query = query.replace(KnownStrings.IngestionEndDT, endTimeStr)

        dfDelta = self.getKustoConnector().loadDataFrameViaConnector(query, forceDistributed=True, parallelism=self._parallelism)

        if updateCurrentWatermark:
            self.setCurrentWatermark(endTime)
        else:
            Logger.getInstance().writeInfo("Not updating current watermark as per parameter setting.")

        if dfDelta is None or dfDelta.rdd.isEmpty():
            return None, "No new Data from source Kusto."

        if partitionSizeInMB is not None:
            # Estimate the number of partitions based on DataFrame size and target partition size
            dfSizeInBytes = dfDelta.rdd.map(lambda row: len(str(row).encode('utf-8'))).sum()
            targetPartitionSizeInBytes = partitionSizeInMB * 1024 * 1024
            repartitionCount = max(1, int(dfSizeInBytes / targetPartitionSizeInBytes))
            Logger.getInstance().writeInfo(f"Estimated DataFrame size: {dfSizeInBytes} bytes. Target partition size: {targetPartitionSizeInBytes} bytes. Calculated repartition count: {repartitionCount}.")
        else:
            Logger.getInstance().writeInfo("No partition size or repartition count specified, skipping repartitioning.")

        # repartition the dataframe if needed
        if repartitionCount is None:
            Logger.getInstance().writeInfo("Using default repartition count based on partition cadence.")
        else:
            currentPartitions = dfDelta.rdd.getNumPartitions()
            if repartitionCount == currentPartitions:
                Logger.getInstance().writeInfo(f"DataFrame already has {currentPartitions} partitions. No repartition needed.")
            elif repartitionCount < currentPartitions:
                Logger.getInstance().writeInfo(f"Repartitioning DataFrame from {currentPartitions} to {repartitionCount} partitions (coalesce).")
                dfDelta = dfDelta.coalesce(repartitionCount)
            else:
                Logger.getInstance().writeInfo(f"Repartitioning DataFrame from {currentPartitions} to {repartitionCount} partitions (repartition).")
                dfDelta = dfDelta.repartition(repartitionCount)

        if persist:
            Logger.getInstance().writeInfo("Persisting new source data to disk.")
            dfDelta = self.session.persistDataframe(dfDelta, StorageLevel.DISK_ONLY)

        return dfDelta, None

    def setStartDateTime(self, startDateTime: datetime) -> None:
        """
        Set the start date time for data extraction.

        Args:
            startDateTime: The start date time to set.
        """
        self._startDateTime = startDateTime

    @property
    def startDateTime(self) -> datetime:
        """
        Get the start date time for data extraction.

        Returns:
            The start date time.
        """
        return self._startDateTime