from __future__ import annotations
from datetime import datetime
from pyspark.sql import DataFrame

from melius.libraries.Logger import Logger
from melius.libraries.Constants import FileTypes
from melius.session.sinks.AdlsGen2Sink import AdlsGen2Sink


class TimePartitionedParquetSink(AdlsGen2Sink):
    """
    TimePartitionedParquetSink handles writing data to time-partitioned parquet files.
    """

    def __init__(
        self,
        dataName: str,
        dataVersion: str,
        storageConfig: str,
        parquetRootPath: str,
        parquetPathPattern: str
    ) -> None:
        """
        Initialize the TimePartitionedParquetSink.

        Args:
            dataName: Name of the data
            dataVersion: Version of the data (e.g., "1.0.0")
            storageConfig: Name of the storage configuration to use
            parquetRootPath: Root path for storing parquet files
            parquetPathPattern: Pattern for naming parquet files. Example: "/%Y/%m/%d/dataname_%Y%m%d%H%M.parquet"
        """
        super().__init__(
            dataName=dataName,
            dataVersion=dataVersion,
            storageConfig=storageConfig,
            storageRootPath=parquetRootPath
        )

        self._parquetPathPattern = parquetPathPattern

    def saveToTimePartitionedParquet(
        self,
        df: DataFrame,
        watermarkDateTime: datetime
    ) -> str:
        """
        Save a DataFrame to a model parquet file in the standard model path structure.

        Args:
            df: DataFrame to save
            watermarkDateTime: Current watermark datetime

        Returns:
            The path where the file was saved
        """
        parquet_path = f"{self.parquetRootPath}/{watermarkDateTime.strftime(self.parquetPathPattern)}"
        Logger.getInstance().writeInfo(f"parquetRootPath: {self.parquetRootPath}")
        Logger.getInstance().writeInfo(f"Saving DataFrame to parquet path: {parquet_path}")

        self.saveToParquet(df, parquet_path=parquet_path)
        return parquet_path

    def saveToParquet(self, df: DataFrame, parquet_path: str) -> str:
        """
        Save a DataFrame to a parquet file.

        Args:
            df: DataFrame to save
            parquet_path: Path to save the parquet file

        Returns:
            The path where the file was saved
        """
        if parquet_path is None:
            raise ValueError("parquet_path should not be None")

        connector = self.getStorageConnector()
        connector.saveTo(df, parquet_path, FileTypes.Parquet)
        Logger.getInstance().writeInfo(f"Successfully saved model data to {parquet_path}")
        return parquet_path

    @property
    def parquetRootPath(self) -> str:
        return self.storageRootPath

    @property
    def parquetPathPattern(self) -> str:
        return self._parquetPathPattern