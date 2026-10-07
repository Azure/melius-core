# Databricks notebook source
from abc import ABC
from datetime import datetime
from melius.libraries.Logger import Logger
from melius.libraries.Utility import Utils
from melius.connectors.BlobStorageConnector import BlobStorageConnector
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import min as spark_min
from pyspark.sql import Row
from pyspark.sql import functions as F


class WatermarkHelper(ABC):
    """WaterMark Helper"""

    KeyName = "key"
    MaxIng = "maxIng"

    # The format of the watermark datetime string
    DateTimeFormat = "%Y-%m-%dT%H:%M:%S.%fZ"

    # The old schema for our watermark store: key, maxIng
    SchemaJson = StructType([
        StructField(KeyName, StringType(), True),
        StructField(MaxIng, TimestampType(), True)
    ])

    # The schema for our watermark store in delta table: JobName, WatermarkValue
    SchemaDelta = StructType([
        StructField("JobName", StringType(), False),
        StructField("WatermarkValue", TimestampType(), True),
        StructField("PreciseTimestamp", TimestampType(), True)
    ])

    @staticmethod
    def getModelFolderName(model: str, version: str = '0.0.1') -> str:
        """
        Generate a folder name for the model based on its name and version.
        """
        if model is None or model == "":
            raise ValueError("Model name cannot be None or empty.")

        folderName = None

        if version is None or version == '0.0.1' or version == "":
            Logger.getInstance().writeInfo("Default version '0.0.1' detected, using model name as folder name.")
            folderName = model
        else:
            folderName = f'{model}_{version}'
            Logger.getInstance().writeInfo(f"Using model name and version '{version}' as folder name.")

        Logger.getInstance().writeInfo(f"Model folder name: {folderName}")

        return folderName

    @staticmethod
    def getUTCStringForKusto(dt: datetime) -> str:
        """
        Convert a datetime object to a UTC string in the format used by Kusto. (up to milliseconds)
        """
        if dt is None:
            return None
        return dt.strftime(WatermarkHelper.DateTimeFormat)[:-4] + "Z"

    @staticmethod
    def getWatermarkDateTime(connector: BlobStorageConnector, watermarkPath: str, modelName: str, modelVersion: str='0.0.1') -> datetime:
        """
        Get the watermark datetime from the specified path in ADLS Gen2 storage.
        1. Check if the watermark path exists. If it does not exist, return None.
        2. If it exists, first try to read from a Delta table. If that fails, fall back to reading from a JSON file.
        3. If both methods fail, return None.

        args:
            connector: BlobStorageConnector instance to interact with ADLS Gen2 storage.
            modelName: Optional model name to filter the watermark.
            modelVersion: Optional model version to filter the watermark.

        returns: datetime or None
        """

        if connector.existsPath(watermarkPath):
            Logger.getInstance().writeInfo(f"Watermark path exists: {watermarkPath}")

            JOB = f"{modelName}_{modelVersion}"

            Logger.getInstance().writeInfo(f"Loading existing watermark table from delta table: {watermarkPath}")
            df = connector.ensureDeltaTableForRead(watermarkPath)

            if df is None or df.count() == 0:
                Logger.getInstance().writeInfo("No existing watermark table found in delta table. returning None")
                return None

            Logger.getInstance().writeInfo("searching for watermark for job: " + JOB)
            df = df.filter(F.col("JobName") == F.lit(JOB))

            # print the df for logging.
            df.show(truncate=False)

            df = (df.select(F.col("WatermarkValue").alias("wm"))
                    .limit(1))

            row = df.first()
            return row["wm"] if row and row["wm"] is not None else None

        Logger.getInstance().writeInfo(f"Watermark path does not exist: {watermarkPath}, returning None")
        return None

    @staticmethod
    def saveWatermark(connector: BlobStorageConnector, watermarkPath: str, modelName: str, modelVersion: str, watermarkDt: datetime) -> None:
        """
        Save the watermark DataFrame to the specified path in ADLS Gen2 storage.
        """
        spark = Utils.getSparkSession()
        dt = connector.ensureDeltaTableForWrite(watermarkPath, WatermarkHelper.SchemaDelta)

        update_df = spark.createDataFrame(
            [Row(JobName=f"{modelName}_{modelVersion}", WatermarkValue=watermarkDt, PreciseTimestamp=datetime.utcnow())]
        )

        Logger.getInstance().writeInfo(f"Saving watermark for job: {modelName}_{modelVersion}, Watermark: {watermarkDt}")

        (dt.alias("t")
         .merge(update_df.alias("u"), "t.JobName = u.JobName")
         .whenMatchedUpdate(set={"WatermarkValue": "u.WatermarkValue", "PreciseTimestamp": "u.PreciseTimestamp"})
         .whenNotMatchedInsert(values={"JobName": "u.JobName", "WatermarkValue": "u.WatermarkValue", "PreciseTimestamp": "u.PreciseTimestamp"})
         .execute())

    @staticmethod
    def getMinValueOfColumnFromDeltaTable(connector: BlobStorageConnector, sourcePath: str, column: str) -> datetime:
        """
        Get the minimum value of a column from a Delta table.
        """
        try:
            df = connector.loadDeltaTable(sourcePath)
        except Exception as e:
            Logger.getInstance().writeInfo(f"Delta table not found with exception '{e}'. return None")
            return None

        result = df.select(spark_min(column).alias("min_value")).first()
        return result["min_value"] if result and result["min_value"] else None

    @staticmethod
    def getKeyName(clusterName: str, databaseName: str, tableName: str) -> str:
        """
        Generate a unique key for the watermark dictionary based on cluster, database, and table names.
        """
        return f"{clusterName}.{databaseName}.{tableName}"

    @staticmethod
    def isVacuumNeeded(previous: datetime, current: datetime) -> bool:
        """
        Check if vacuuming is needed based on the previous and current watermark values.
        Vacuuming is needed if the previous watermark date and current watermark date are different.
        """
        if previous is None:
            Logger.getInstance().writeInfo("Previous watermark is None, vacuum is not needed.")
        elif current is None:
            Logger.getInstance().writeInfo("Current watermark is None, vacuum is not needed.")
        elif previous.date() != current.date():
            Logger.getInstance().writeInfo("Previous and current watermark dates are different, vacuum is needed.")
            return True
        else:
            Logger.getInstance().writeInfo("Previous and current watermark dates are the same, vacuum is not needed.")

        # Vacuum is needed if the day has changed between previous and current
        return False