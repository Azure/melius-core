import abc
import math
from pyspark.sql.types import StructType
from functools import reduce
from pyspark.sql import DataFrame

from melius.libraries.Logger import Logger


class DatabricksHelper(abc.ABC):
    """
    A helper class for Databricks operations in Melius framework.
    """

    # DataBricks dbUtils
    _dbutils = None

    # sparkSession
    _sparkSession = None

    # sqlContext
    _sqlContext = None

    @staticmethod
    def setDBUtils(dbutils):
        DatabricksHelper._dbutils = dbutils

    @staticmethod
    def getDBUtils():
        return DatabricksHelper._dbutils

    @staticmethod
    def setSparkSession(spark):
        '''
        Set the Spark session instance (spark).
        '''
        DatabricksHelper._sparkSession = spark

    @staticmethod
    def getSparkSession():
        '''
        Get the Spark session instance (spark).
        '''
        return DatabricksHelper._sparkSession

    @staticmethod
    def setSqlContext(context):
        '''
        Set the SQL context instance (sqlContext).
        '''
        DatabricksHelper._sqlContext = context

    @staticmethod
    def getSqlContext():
        '''
        Get the SQL context instance (sqlContext).
        '''
        return DatabricksHelper._sqlContext

    @staticmethod
    def selectColumns(df, expected_schema: StructType):
        """
        Select specific columns in the schema from a DataFrame.
        it makes sure that data is in the right format and has the expected columns (in order).

        :param df: The DataFrame to select from.
        :param expected_schema: The expected schema to align with.
        :return: A new DataFrame with only the selected columns.
        """
        expected_columns = [field.name for field in expected_schema]
        missing_columns = [col for col in expected_columns if col not in df.columns]

        if missing_columns:
            raise ValueError(f"The following expected columns are missing from the DataFrame: {missing_columns}")
        # Optional: return the aligned DataFrame if needed
        return DatabricksHelper.getSparkSession().createDataFrame(
            df.select(*expected_columns).rdd,
            schema=expected_schema
        )

    @staticmethod
    def checkDataFrameColumns(df, expected_columns: set) -> bool:
        """
        Check if the DataFrame contains the expected columns.

        :param df: The DataFrame to check.
        :param expected_columns: A set of expected column names.
        """
        missing_columns = expected_columns - set(df.columns)

        return not missing_columns

    @staticmethod
    def optimizeDataFrameForKustoWrite(dataFrame: DataFrame, target_size_mb: int) -> DataFrame:
        """
        Optimizes partition count using Spark Catalyst stats (Metadata) for Kusto writes.
        instead of triggering a physical data scan.

        Args:
            dataFrame (DataFrame): The DataFrame to optimize.
            target_size_mb (int): The target size of each partition in megabytes.
        Returns:
            DataFrame: The optimized DataFrame with adjusted partition count.
        """
        try:
            Logger.getInstance().writeInfo("Optimizing partitions based on estimated data size from logical plan.")
            # Calculate ideal partition count
            ideal_partitions = DatabricksHelper.getOptimalPartitionCountBySize(dataFrame, target_size_mb)

            # Current partition count
            current_partitions = dataFrame.rdd.getNumPartitions()

            # LOGIC: Only mess with partitions if we are shrinking (Coalesce).
            # Increasing partitions (Repartition) causes a full shuffle, which is expensive.
            # Kusto prefers larger files (500MB - 1GB), so we usually want to shrink.
            if ideal_partitions < current_partitions:
                Logger.getInstance().writeInfo(f"Coalescing from {current_partitions} to {ideal_partitions} partitions for Kusto optimization.")
                return dataFrame.coalesce(ideal_partitions)
            else:
                Logger.getInstance().writeInfo(f"No partition optimization needed. Current: {current_partitions}, Ideal: {ideal_partitions}.")

            return dataFrame

        except Exception as e:
            # Fallback if plan stats are unavailable (rare)
            Logger.getInstance().writeInfo(f"Could not estimate size from plan, skipping optimization. Error: {e}")
            return dataFrame

    @staticmethod
    def getOptimalPartitionCountBySize(dataFrame: DataFrame, target_size_mb: float, min_partitions: int = 1, max_partitions: int | None = None) -> int:
        """
        Get the optimal number of partitions for a DataFrame based on its size only.
        Args:
            dataFrame (DataFrame): The DataFrame for which to determine the optimal partition count.
            target_size_mb (float): The target size of each partition in megabytes.
            min_partitions (int): Minimum number of partitions to return. Default is 1.
            max_partitions (int | None): Maximum number of partitions to return. Default is None (no limit).
        Returns:
            int: The optimal number of partitions for the DataFrame.
        """
        if target_size_mb <= 0:
            raise ValueError("target_size_mb must be greater than zero.")

        # Access the optimized logical plan to get estimated size in bytes
        # This is a Metadata operation (Instant)
        plan_stats = dataFrame._jdf.queryExecution().optimizedPlan().stats()
        estimated_size_bytes = int(plan_stats.sizeInBytes())
        Logger.getInstance().writeInfo(f"Estimated data size from plan: {estimated_size_bytes} bytes.")

        # Handle missing / suspicious stats
        if estimated_size_bytes <= 0 or estimated_size_bytes == 1:
            current_partitions = dataFrame.rdd.getNumPartitions()
            Logger.getInstance().writeInfo(
                f"Statistics unavailable or not meaningful. "
                f"Using existing partition count: {current_partitions}"
            )
            return max(min_partitions, current_partitions)

        # Convert target MB to Bytes
        target_size_bytes = target_size_mb * 1024 * 1024

        # Calculate ideal partition count
        partitionCount = math.ceil(estimated_size_bytes / target_size_bytes)
        partitionCount = max(min_partitions, partitionCount)

        if max_partitions is not None:
            partitionCount = min(partitionCount, max_partitions)
        Logger.getInstance().writeInfo(f"Calculated optimal partition count based on size ({target_size_mb} MB): {partitionCount}")

        return partitionCount

    @staticmethod
    def optimizeDataFramePartitions(dataFrame: DataFrame, partitionSizeInMB: int = 128, parallelFactor: int = 3, dataOnly: bool = False) -> DataFrame:
        """
        Optimize the number of partitions for a DataFrame based on its size and available cores.
        This method adjusts the partition count to improve performance during data processing.
        Args:
            dataFrame (DataFrame): The DataFrame for which to determine the optimal partition count.
            partitionSizeInMB (int): The target size of each partition in megabytes. Default is 128 MB.
            parallelFactor (int): A factor to adjust the partition count for parallel processing. Default is 3.
        Returns:
            DataFrame: The optimal number of partitions for the DataFrame.
        """
        if dataFrame is None:
            raise ValueError("DataFrame is None.")

        try:
            partitionsBySize = DatabricksHelper.getOptimalPartitionCountBySize(dataFrame, partitionSizeInMB)
            Logger.getInstance().writeInfo(f"Optimal partition count for DataFrame based on datasize: {partitionsBySize}")

            # Get the number of cores available in the Spark session
            # and calculate the optimal partition count based on cores
            from pyspark import SparkContext

            sc = SparkContext.getOrCreate()
            partitionsByCores = sc.defaultParallelism * parallelFactor
            Logger.getInstance().writeInfo(f"Optimal partition count for DataFrame based on cores: {partitionsByCores}")

            if dataOnly:
                Logger.getInstance().writeInfo("Data only mode: using size-based partitioning.")
                recommendedPartitionCount = partitionsBySize
            else:
                Logger.getInstance().writeInfo("Consider Data and Cores mode: using the smaller one.")
                recommendedPartitionCount = min(partitionsBySize, partitionsByCores)

            Logger.getInstance().writeInfo(f"Recommended partition count for DataFrame: {recommendedPartitionCount}")

            currentPartitions = dataFrame.rdd.getNumPartitions()

            if recommendedPartitionCount < currentPartitions:
                Logger.getInstance().writeInfo(f"Coalescing from {currentPartitions} to {recommendedPartitionCount} partitions.")
                dataFrame = dataFrame.coalesce(recommendedPartitionCount)
            elif recommendedPartitionCount > currentPartitions:
                Logger.getInstance().writeInfo(f"Repartitioning from {currentPartitions} to {recommendedPartitionCount} partitions.")
                dataFrame = dataFrame.repartition(recommendedPartitionCount)
            else:
                Logger.getInstance().writeInfo(f"No partition adjustment needed since the current partition count({currentPartitions}) is optimal ({recommendedPartitionCount}).")

        except Exception as e:
            Logger.getInstance().writeError(f"Error determining optimal partition count: {e}. Using default partition count.")

        return dataFrame

    @staticmethod
    def _logAndCollectNonEmpty(name: str, df: DataFrame | None):
        '''
        Log information about the DataFrame and return it if non-empty.
        :param name: Name of the log source.
        :param df: The DataFrame to check.
        :return: The DataFrame if non-empty, else None.
        '''

        if df is None:
            Logger.getInstance().writeInfo(f'No dataframe for {name} log (None).')
            return None

        # One action per df - necessary if we really want "No new data" logs
        if df.rdd.isEmpty():
            Logger.getInstance().writeInfo(f'No new data from {name} log')
            return None

        Logger.getInstance().writeInfo(f'Processing new data from {name} log')
        return df

    @staticmethod
    def unionNonEmptyDataFrames(dataFrames: list[DataFrame | None]) -> DataFrame | None:
        '''
        Union a list of DataFrames, ignoring any that are None or empty.
        :param dataFrames: List of DataFrames to union.
        :return: The unioned DataFrame, or None if all were empty or None.
        '''
        if not dataFrames:
            Logger.getInstance().writeInfo('No DataFrames provided for union.')
            return None

        validatedDfs = [DatabricksHelper._logAndCollectNonEmpty(f"DataFrame_{i}", df) for i, df in enumerate(dataFrames)]
        nonEmptyDfs = [df for df in validatedDfs if df is not None]

        if not nonEmptyDfs:
            Logger.getInstance().writeInfo('All provided DataFrames are empty or None.')
            return None
        else:
            Logger.getInstance().writeInfo(f'Unioning {len(nonEmptyDfs)} non-empty DataFrames.')

        return reduce(lambda a, b: a.union(b), nonEmptyDfs)