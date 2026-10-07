import unittest.mock as mock
from melius.libraries.Utility import Utils
from melius.libraries.SparkHelper import SparkHelper


def test_repartitionByShuffleCount(mocker):
    dataFrame = mock.Mock()
    sqlContext = mock.Mock()
    mocker.patch.object(sqlContext, 'getConf').return_value = "11"
    Utils.setSqlContext(sqlContext)

    SparkHelper.repartitionByShuffleCount(dataFrame)
    dataFrame.repartition.assert_called_once_with(11)


def test_changeShufflePartitionCount_ChangeOnlyShuffleCount(mocker):
    sqlContext = mock.Mock()
    Utils.setSqlContext(sqlContext)
    mockCount = mocker.patch('melius.libraries.SparkHelper.SparkHelper.countRows')

    SparkHelper.changeShufflePartitionCount(1010)
    mockCount.assert_not_called()
    sqlContext.setConf.assert_called_once_with('spark.sql.shuffle.partitions', '1010')


def test_changeShufflePartitionCount_CountAndChangeShuffleCount(mocker):
    sqlContext = mock.Mock()
    dataFrame = mock.Mock()
    Utils.setSqlContext(sqlContext)
    mockCount = mocker.patch('melius.libraries.SparkHelper.SparkHelper.countRows')

    SparkHelper.changeShufflePartitionCount(1010, 'vwTest', dataFrame)
    mockCount.assert_called_with(dataFrame)
    sqlContext.setConf.assert_called_once_with('spark.sql.shuffle.partitions', '1010')


def test_countRows():
    dataFrame = mock.Mock()

    SparkHelper.countRows(dataFrame)
    dataFrame.count.assert_called()


def test_createViewAndDataFrame(mocker):
    sqlContext = mock.Mock()
    sparkSession = mock.Mock()
    dataFrame = mock.Mock()
    mocker.patch.object(sparkSession, 'sql').return_value = dataFrame
    Utils.setSparkSession(sparkSession)
    Utils.setSqlContext(sqlContext)
    mockRepartition = mocker.patch('melius.libraries.SparkHelper.SparkHelper.repartitionByShuffleCount')
    mockRowCount = mocker.patch('melius.libraries.SparkHelper.SparkHelper.countRows')

    query = 'query here'
    vwName = 'vwTest'
    SparkHelper.createViewAndDataFrame(vwName, query)

    sparkSession.sql.assert_called_once()
    dataFrame.createOrReplaceTempView.assert_called_once_with(vwName)
    mockRepartition.assert_not_called()
    mockRowCount.assert_not_called()
    dataFrame.cache.assert_not_called()
    dataFrame.checkpoint.assert_not_called()

    SparkHelper.createViewAndDataFrame(vwName, query, True)
    mockRepartition.assert_not_called()
    mockRowCount.assert_called()
    dataFrame.cache.assert_called_once()
    dataFrame.checkpoint.assert_not_called()