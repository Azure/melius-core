from __future__ import annotations
from datetime import timedelta, datetime
from pyspark.sql.types import StructType, StructField, StringType as SparkStringType
from pyspark.sql.streaming import DataStreamReader
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from melius.session.sources.AdlsGen2Source import AdlsGen2Source
from melius.session.JobRunSession import JobRunSession
from melius.libraries.Logger import Logger
from melius.libraries.Utility import Utils


class CsvSource(AdlsGen2Source):
    """Incremental CSV file processing from ADLS Gen2 via Databricks Auto Loader.

    Use this source when upstream data lands in ADLS Gen2 as CSV files (e.g. exports
    from external systems, partner feeds, or ETL pipelines) and you need to process
    only the new or changed files since the last run.

    Internally it uses Databricks Auto Loader (cloudFiles) to track which files have
    already been ingested, so each run picks up only the unprocessed files. An optional
    ``targetSchema`` can be supplied to cast and align columns before writing to
    downstream sinks (Delta Lake, Kusto, etc.).

    Typical workflow::

        source = CsvSource(session, dataName, dataVersion, storageConfig,
                           csvRootPath="/raw/partner-feed/v1", ...)
        df = source.loadCsvWithAutoLoader(schemaLocation="/checkpoints/schema")
        df = source.applyTargetSchema(df)
        # write df to a Delta Lake or Kusto sink
    """

    def __init__(
        self,
        session: JobRunSession,
        dataName: str,
        dataVersion: str,
        storageConfig: str,
        csvRootPath: str,
        lookBackDelta: timedelta,
        backFillStartDateTime: datetime,
        schema: StructType = None,
        header: bool = True,
        delimiter: str = ",",
        inferSchema: bool = True,
        multiLine: bool = False,
        encoding: str = "UTF-8",
        nullValue: str = "",
        dateFormat: str = "yyyy-MM-dd",
        timestampFormat: str = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
        targetSchema: StructType = None
    ) -> None:
        """Initialize CsvSource with CSV parsing options and storage config."""
        super().__init__(
            session=session,
            dataName=dataName,
            dataVersion=dataVersion,
            storageConfig=storageConfig,
            dataRootPath=csvRootPath,
            lookBackDelta=lookBackDelta,
            backFillStartDateTime=backFillStartDateTime
        )

        self._schema = schema
        self._targetSchema = targetSchema
        self._header = header
        self._delimiter = delimiter
        self._inferSchema = inferSchema
        self._multiLine = multiLine
        self._encoding = encoding
        self._nullValue = nullValue
        self._dateFormat = dateFormat
        self._timestampFormat = timestampFormat

        Logger.getInstance().writeInfo(
            f"Initialized CsvSource for data: {dataName}, version: {dataVersion}, "
            f"csvRootPath: {csvRootPath}, header: {header}, delimiter: '{delimiter}'"
        )

    def loadCsvWithAutoLoader(
        self,
        schemaLocation: str,
        filePattern: str = "*.csv",
        maxFilesPerTrigger: int = 1000,
        maxBytesPerTrigger: str = None,
        includeExistingFiles: bool = True,
        schemaEvolutionMode: str = "none",
        schemaLocationConnector=None
    ) -> DataStreamReader:
        """Create an Auto Loader (cloudFiles) streaming reader for incremental CSV processing.

        Returns a streaming DataFrame. Use writeStream to process the data.

        Args:
            schemaLocation: Relative path for Auto Loader schema tracking.
            schemaLocationConnector: Optional connector to resolve schemaLocation.
                If not provided, uses the source storage connector.
        """
        spark = Utils.getSparkSession()
        connector = self.getStorageConnector()

        sourcePath = connector.getAbsolutePath(self._dataRootPath)
        schemaConnector = schemaLocationConnector or connector
        absoluteSchemaLocation = schemaConnector.getAbsolutePath(schemaLocation)

        Logger.getInstance().writeInfo(
            f"Creating Auto Loader for CSV files at: {sourcePath}, "
            f"schemaLocation: {absoluteSchemaLocation}, pattern: {filePattern}"
        )

        reader = spark.readStream.format("cloudFiles") \
            .option("cloudFiles.format", "csv") \
            .option("cloudFiles.schemaLocation", absoluteSchemaLocation) \
            .option("cloudFiles.includeExistingFiles", str(includeExistingFiles).lower()) \
            .option("cloudFiles.schemaEvolutionMode", schemaEvolutionMode) \
            .option("cloudFiles.inferColumnTypes", str(self._inferSchema).lower()) \
            .option("pathGlobFilter", filePattern) \
            .option("header", str(self._header).lower()) \
            .option("delimiter", self._delimiter) \
            .option("multiLine", str(self._multiLine).lower()) \
            .option("encoding", self._encoding) \
            .option("nullValue", self._nullValue) \
            .option("dateFormat", self._dateFormat) \
            .option("timestampFormat", self._timestampFormat)

        if maxFilesPerTrigger:
            reader = reader.option("cloudFiles.maxFilesPerTrigger", str(maxFilesPerTrigger))

        if maxBytesPerTrigger:
            reader = reader.option("cloudFiles.maxBytesPerTrigger", maxBytesPerTrigger)

        reader = reader.option("ignoreMissingFiles", "true")
        reader = reader.option("ignoreCorruptFiles", "true")

        if self._schema:
            Logger.getInstance().writeInfo("Using provided read schema for Auto Loader")
            reader = reader.schema(self._schema)
        elif self._targetSchema:
            # Derive read schema from targetSchema - all columns as StringType.
            # This ensures Auto Loader always reads all target columns from CSV,
            # overriding the stale _schemas/ log when new columns are added.
            # applyTargetSchema handles type casting later.
            read_schema = StructType([
                StructField(f.name, SparkStringType(), True)
                for f in self._targetSchema.fields
            ])
            Logger.getInstance().writeInfo(
                f"Derived read schema from targetSchema "
                f"({len(read_schema.fields)} fields as StringType). "
                f"applyTargetSchema will cast to correct types."
            )
            reader = reader.schema(read_schema)
        else:
            Logger.getInstance().writeInfo(
                "No read schema provided; Auto Loader will read all columns. "
                "applyTargetSchema will select and cast later."
            )

        return reader.load(sourcePath)

    def applyTargetSchema(self, df: DataFrame) -> DataFrame:
        """Select and cast columns to match targetSchema. Extra columns are dropped.
        Missing columns are filled with null. Invalid cast values become null (using try_cast)."""
        if not self._targetSchema:
            return df

        columns = []
        for field in self._targetSchema.fields:
            if field.name in df.columns:
                # Use try_cast to safely handle malformed values (returns null instead of throwing)
                columns.append(
                    F.expr(f"try_cast(`{field.name}` as {field.dataType.simpleString()})").alias(field.name)
                )
            else:
                columns.append(F.lit(None).cast(field.dataType).alias(field.name))

        Logger.getInstance().writeInfo(
            f"Applied target schema with try_cast: {len(self._targetSchema.fields)} fields. "
            f"Malformed values will be set to null."
        )
        return df.select(columns)

    @property
    def targetSchema(self) -> StructType:
        """Get the target schema for Delta writes."""
        return self._targetSchema

    @targetSchema.setter
    def targetSchema(self, value: StructType) -> None:
        """Set the target schema for Delta writes."""
        self._targetSchema = value

    @property
    def csvRootPath(self) -> str:
        """Get the CSV root path."""
        return self._dataRootPath

    @property
    def schema(self) -> StructType:
        """Get the schema."""
        return self._schema

    @schema.setter
    def schema(self, value: StructType) -> None:
        """Set the schema."""
        self._schema = value
