from __future__ import annotations
from pyspark.sql import DataFrame
from typing import Dict, List

from melius.libraries.Logger import Logger
from melius.session.sinks.AdlsGen2Sink import AdlsGen2Sink


class DeltaLakeSink(AdlsGen2Sink):
    """
    DeltaLakeSink handles writing data to Delta Lake storage.
    """

    def __init__(
        self,
        dataVersion: str,
        dataName: str,
        storageConfig: str,
        deltaLakePath: str
    ) -> None:
        """
        Initialize the DeltaLake Sink object.

        Args:
            dataVersion: Version of the data (e.g., "1.0.0")
            dataName: Name of the data
            storageConfigName: Name of the storage configuration to use
            dataRegion: Target region for the data deployment
        """
        super().__init__(
            dataVersion=dataVersion,
            dataName=dataName,
            storageConfig=storageConfig,
            storageRootPath=deltaLakePath
        )

    def appendToDeltaLake(
        self,
        dataFrame: DataFrame,
        partitionBy: List[str] = None,
        snappyCompression: bool = True,
        mergeSchema: bool = False
    ) -> None:
        """
        Append a DataFrame to the health model Delta table.

        Args:
            dataFrame: DataFrame to append
            partitionBy: List of columns to partition the delta table by
            snappyCompression: Whether to use snappy compression
            mergeSchema: Whether to merge schema when writing
        """
        if dataFrame is None or dataFrame.rdd.isEmpty():
            Logger.getInstance().writeInfo("The input dataframe is empty. Skipping append to Delta Lake.")
            return

        Logger.getInstance().writeInfo(f"Appending dataframe to Delta Lake table at path: {self.deltaLakePath}")
        connector = self.getStorageConnector()
        connector.appendDeltaLake(
            dataFrame=dataFrame,
            path=self.deltaLakePath,
            partitionBy=partitionBy,
            snappyCompression=snappyCompression,
            mergeSchema=mergeSchema
        )
        Logger.getInstance().writeInfo("Successfully appended a dataframe to the Delta Lake table")

    def vacuumDeltaLake(
        self,
        compact: bool = True,
        retentionDays: int = 45,
        time_col: str = "SourceIngestionTime",
        zorder_cols: List[str] = None
    ) -> None:
        """
        Vacuum the Delta Lake to clean up old data files.

        Args:
            compact: whether to compact the delta table before vacuuming, default is True.
            retentionDays: The retention days for the delta table vacuum, default is 45 days. It should be greater than equal to 7 days.
            time_col: The time column to use for compaction, default is "SourceIngestionTime".
            zorder_cols: columns to Z-ORDER BY during compaction, default is None.
        """
        connector = self.getStorageConnector()
        connector.vacuumDeltaLake(
            path=self.deltaLakePath,
            compact=compact,
            retentionDays=retentionDays,
            time_col=time_col,
            zorder_cols=zorder_cols
        )

        Logger.getInstance().writeInfo(f"Successfully vacuumed the Delta Lake table at path: {self.deltaLakePath}")

    def mergeDeltaTable(
        self,
        dataFrame: DataFrame,
        pkCols: List[str],
        condCols: List[str] = None
    ) -> None:
        """
        Merge a DataFrame into the Delta Lake table.

        Args:
            dataFrame: DataFrame to merge
            deltaTablePath: Path to the delta table
            pkCols: List of primary key columns for matching records
            condCols: List of condition columns for determining updates
        """
        connector = self.getStorageConnector()
        connector.mergeDeltaTable(
            dataFrame=dataFrame,
            pathDeltaTable=self.deltaLakePath,
            pkCols=pkCols,
            condCols=condCols
        )
        Logger.getInstance().writeInfo(f"Successfully merged a dataframe into the Delta Lake table at path: {self.deltaLakePath}")

    def getOnlyNewData(
        self,
        dataFrame: DataFrame,
        pkCols: List[str],
        timeCol: str = "EventTime"
    ) -> DataFrame:
        """
        Get only new data from a DataFrame against the Delta Lake table.

        Args:
            dataFrame: DataFrame to merge
            deltaTablePath: Path to the delta table
            pkCols: List of primary key columns for matching records
            timeCol: Time column to determine new records, default is "EventTime"
        Returns:
            DataFrame containing only new records that do not exist in the Delta Lake table based on the primary key columns.
        """
        connector = self.getStorageConnector()
        return connector.getNewDataOnlyAgainstDeltaLake(
            dataFrame=dataFrame,
            pathDeltaTable=self.deltaLakePath,
            pkCols=pkCols,
            timeCol=timeCol
        )

    @property
    def deltaLakePath(self) -> str:
        """
        Get the Delta Lake path.
        """
        return self._storageRootPath

    def overwriteDeltaLake(
        self,
        dataFrame: DataFrame,
        partitionBy: List[str] = None,
        overwriteSchema: bool = False
    ) -> None:
        """
        Overwrite the Delta Lake table with a DataFrame.

        Args:
            dataFrame: DataFrame to write
            partitionBy: List of columns to partition the delta table by
            overwriteSchema: Whether to overwrite the schema (adds/removes columns)
        """
        if dataFrame is None or dataFrame.rdd.isEmpty():
            Logger.getInstance().writeInfo("The input dataframe is empty. Skipping overwrite to Delta Lake.")
            return

        Logger.getInstance().writeInfo(f"Overwriting Delta Lake table at path: {self.deltaLakePath}")
        connector = self.getStorageConnector()
        writer = dataFrame.write.format("delta").mode("overwrite")
        if overwriteSchema:
            writer = writer.option("overwriteSchema", "true")
        if partitionBy and len(partitionBy) > 0:
            writer = writer.partitionBy(*partitionBy)
        writer.save(connector.getAbsolutePath(self.deltaLakePath))
        Logger.getInstance().writeInfo("Successfully overwrote the Delta Lake table")

    def mergeDeltaTableWithUpdateCols(
        self,
        dataFrame: DataFrame,
        pkCols: List[str],
        updateCols: List[str]
    ) -> None:
        """
        Merge a DataFrame into the Delta Lake table, updating only the specified columns on match.

        Args:
            dataFrame: DataFrame to merge
            pkCols: List of primary key columns for matching records
            updateCols: List of columns to update when a match is found
        """
        connector = self.getStorageConnector()
        connector.mergeDeltaTableWithUpdateCols(
            dataFrame=dataFrame,
            pathDeltaTable=self.deltaLakePath,
            pkCols=pkCols,
            updateCols=updateCols
        )
        Logger.getInstance().writeInfo(f"Successfully merged with custom update columns into Delta Lake table at path: {self.deltaLakePath}")

    def isDeltaTable(self) -> bool:
        """
        Check if the configured path is a valid Delta table.

        Returns:
            True if the path is a Delta table, False otherwise.
        """
        connector = self.getStorageConnector()
        return connector.isDeltaTable(self.deltaLakePath)

    def deleteByPredicate(self, condition: str) -> None:
        """
        Delete rows from the Delta Lake table matching the given condition.

        Args:
            condition: SQL predicate string (e.g. "BucketTime < '2024-01-01'")
        """
        connector = self.getStorageConnector()
        connector.deleteRowsFromDeltaTable(
            path=self.deltaLakePath,
            condition=condition
        )
        Logger.getInstance().writeInfo(f"Successfully deleted rows from Delta Lake table at path: {self.deltaLakePath}")

    def getLastModifiedTime(self):
        """
        Get the last modification timestamp of the Delta Lake table.
        This is an O(1) metadata read from the transaction log.

        Returns:
            datetime or None: The timestamp of the last write, or None if unavailable.
        """
        connector = self.getStorageConnector()
        return connector.getLastModifiedTime(self.deltaLakePath)

    def addColumns(self, columns: Dict[str, str]) -> None:
        """
        Add columns to the Delta Lake table via ALTER TABLE ADD COLUMNS.

        Args:
            columns: Dictionary of column_name -> spark_type_string
                     (e.g. {"new_col": "STRING", "score": "DOUBLE"})
        """
        connector = self.getStorageConnector()
        connector.addColumnsToTable(
            path=self.deltaLakePath,
            columns=columns
        )
        Logger.getInstance().writeInfo(f"Successfully added columns to Delta Lake table at path: {self.deltaLakePath}")
