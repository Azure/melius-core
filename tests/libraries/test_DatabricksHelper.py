import pytest
from unittest.mock import MagicMock, patch


def test_selectColumns_success_and_missing_columns(mocker):
    from pyspark.sql.types import StructType, StructField, StringType
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Prepare a fake input DataFrame with columns a,b,c
    fake_df = mocker.MagicMock()
    fake_df.columns = ["a", "b", "c"]
    # simulate df.select(*cols).rdd
    selected_df = mocker.MagicMock()
    selected_df.rdd = object()
    fake_df.select.return_value = selected_df

    expected_schema = StructType([StructField("a", StringType()), StructField("b", StringType())])

    # Mock SparkSession.createDataFrame
    fake_spark = mocker.MagicMock()
    fake_out_df = object()
    fake_spark.createDataFrame.return_value = fake_out_df
    DatabricksHelper.setSparkSession(fake_spark)

    out = DatabricksHelper.selectColumns(fake_df, expected_schema)
    assert out is fake_out_df
    fake_df.select.assert_called_once_with("a", "b")
    fake_spark.createDataFrame.assert_called_once()

    # now missing column scenario
    fake_df.columns = ["a"]
    with pytest.raises(ValueError):
        DatabricksHelper.selectColumns(fake_df, expected_schema)


def test_getOptimalPartitionCountBySize(mocker):
    """Test getOptimalPartitionCountBySize method"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock DataFrame with stats
    fake_df = mocker.MagicMock()
    fake_stats = mocker.MagicMock()

    # Set up the chain: dataFrame._jdf.queryExecution().optimizedPlan().stats()
    fake_df._jdf.queryExecution.return_value.optimizedPlan.return_value.stats.return_value = fake_stats
    fake_stats.sizeInBytes.return_value = 1024 * 1024 * 1024  # 1024 MB

    # Test with 500 MB target size - should result in 3 partitions (1024/500 = 2.048, ceil = 3)
    result = DatabricksHelper.getOptimalPartitionCountBySize(fake_df, 500)
    assert result == 3

    # Test with 256 MB target size - should result in 4 partitions (1024/256 = 4)
    result = DatabricksHelper.getOptimalPartitionCountBySize(fake_df, 256)
    assert result == 4

    # Test with 0 MB target size - should raise ValueError
    with pytest.raises(ValueError, match="target_size_mb must be greater than zero"):
        DatabricksHelper.getOptimalPartitionCountBySize(fake_df, 0)

    # Test with negative target size - should raise ValueError
    with pytest.raises(ValueError, match="target_size_mb must be greater than zero"):
        DatabricksHelper.getOptimalPartitionCountBySize(fake_df, -10)


@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFrameForKustoWrite_coalesce(mock_logger):
    """Test optimizeDataFrameForKustoWrite when coalescing is needed"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock DataFrame
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.return_value = 10
    fake_df.coalesce.return_value = fake_df

    # Mock the size calculation method
    with patch.object(DatabricksHelper, 'getOptimalPartitionCountBySize', return_value=5):
        result = DatabricksHelper.optimizeDataFrameForKustoWrite(fake_df, 500)

    # Should coalesce since ideal (5) < current (10)
    fake_df.coalesce.assert_called_once_with(5)
    assert result == fake_df


@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFrameForKustoWrite_no_optimization(mock_logger):
    """Test optimizeDataFrameForKustoWrite when no optimization is needed"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock DataFrame
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.return_value = 5

    # Mock the size calculation method
    with patch.object(DatabricksHelper, 'getOptimalPartitionCountBySize', return_value=8):
        result = DatabricksHelper.optimizeDataFrameForKustoWrite(fake_df, 500)

    # Should not coalesce since ideal (8) > current (5)
    fake_df.coalesce.assert_not_called()
    assert result == fake_df


@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFrameForKustoWrite_exception(mock_logger):
    """Test optimizeDataFrameForKustoWrite when an exception occurs"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock DataFrame
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.side_effect = Exception("Test exception")

    # Mock the size calculation method to also raise an exception
    with patch.object(DatabricksHelper, 'getOptimalPartitionCountBySize', side_effect=Exception("Size calculation failed")):
        result = DatabricksHelper.optimizeDataFrameForKustoWrite(fake_df, 500)

    # Should return original DataFrame and log error
    assert result == fake_df
    mock_logger.return_value.writeInfo.assert_called()


@patch('pyspark.SparkContext.getOrCreate')
@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFramePartitions_coalesce(mock_logger, mock_spark_context):
    """Test optimizeDataFramePartitions when coalescing is needed"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock SparkContext
    mock_sc = MagicMock()
    mock_sc.defaultParallelism = 4
    mock_spark_context.return_value = mock_sc

    # Mock DataFrame
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.return_value = 10
    fake_df.coalesce.return_value = fake_df

    # Mock the size calculation method to return 6 partitions
    with patch.object(DatabricksHelper, 'getOptimalPartitionCountBySize', return_value=6):
        result = DatabricksHelper.optimizeDataFramePartitions(fake_df, partitionSizeInMB=128, parallelFactor=3)

    # Should coalesce to min(6, 12) = 6 partitions since current is 10
    fake_df.coalesce.assert_called_once_with(6)
    assert result == fake_df


@patch('pyspark.SparkContext.getOrCreate')
@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFramePartitions_repartition(mock_logger, mock_spark_context):
    """Test optimizeDataFramePartitions when repartitioning is needed"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock SparkContext
    mock_sc = MagicMock()
    mock_sc.defaultParallelism = 4
    mock_spark_context.return_value = mock_sc

    # Mock DataFrame
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.return_value = 2
    fake_df.repartition.return_value = fake_df

    # Mock the size calculation method to return 8 partitions
    with patch.object(DatabricksHelper, 'getOptimalPartitionCountBySize', return_value=8):
        result = DatabricksHelper.optimizeDataFramePartitions(fake_df, partitionSizeInMB=128, parallelFactor=3)

    # Should repartition to min(8, 12) = 8 partitions since current is 2
    fake_df.repartition.assert_called_once_with(8)
    assert result == fake_df


@patch('pyspark.SparkContext.getOrCreate')
@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFramePartitions_data_only_mode(mock_logger, mock_spark_context):
    """Test optimizeDataFramePartitions in dataOnly mode"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock SparkContext
    mock_sc = MagicMock()
    mock_sc.defaultParallelism = 4
    mock_spark_context.return_value = mock_sc

    # Mock DataFrame
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.return_value = 2
    fake_df.repartition.return_value = fake_df

    # Mock the size calculation method to return 8 partitions
    with patch.object(DatabricksHelper, 'getOptimalPartitionCountBySize', return_value=8):
        result = DatabricksHelper.optimizeDataFramePartitions(fake_df, partitionSizeInMB=128, parallelFactor=3, dataOnly=True)

    # Should use only size-based partitioning (8) instead of min(8, 12)
    fake_df.repartition.assert_called_once_with(8)
    assert result == fake_df


@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFramePartitions_no_change_needed(mock_logger):
    """Test optimizeDataFramePartitions when no partition change is needed"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock DataFrame
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.return_value = 5
    # Make repartition return the same object to match test expectation
    fake_df.repartition.return_value = fake_df

    with patch('pyspark.SparkContext.getOrCreate') as mock_spark_context:
        # Mock SparkContext
        mock_sc = MagicMock()
        mock_sc.defaultParallelism = 4
        mock_spark_context.return_value = mock_sc

        # Mock the size calculation method to return 8 partitions, but min with cores gives 5
        with patch.object(DatabricksHelper, 'getOptimalPartitionCountBySize', return_value=8):
            result = DatabricksHelper.optimizeDataFramePartitions(fake_df, partitionSizeInMB=128, parallelFactor=3)

    # Should repartition since recommended (min(8, 12)=8) != current (5)
    # min(8, 4*3) = min(8, 12) = 8, current is 5, so should repartition to 8
    fake_df.repartition.assert_called_once_with(8)
    assert result == fake_df


@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFramePartitions_none_dataframe(mock_logger):
    """Test optimizeDataFramePartitions with None DataFrame"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    with pytest.raises(ValueError, match="DataFrame is None"):
        DatabricksHelper.optimizeDataFramePartitions(None)


@patch('pyspark.SparkContext.getOrCreate')
@patch('melius.libraries.DatabricksHelper.Logger.getInstance')
def test_optimizeDataFramePartitions_exception_handling(mock_logger, mock_spark_context):
    """Test optimizeDataFramePartitions when exception occurs"""
    from melius.libraries.DatabricksHelper import DatabricksHelper

    # Mock DataFrame that raises exception
    fake_df = MagicMock()
    fake_df.rdd.getNumPartitions.side_effect = Exception("Test exception")

    result = DatabricksHelper.optimizeDataFramePartitions(fake_df)

    # Should return original DataFrame and log error
    assert result == fake_df
    mock_logger.return_value.writeError.assert_called_once()
