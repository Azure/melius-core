import pytest
import pathlib
from unittest.mock import patch, Mock, call
from datetime import timedelta, datetime
from io import UnsupportedOperation
from melius.connectors.BlobStorageConnector import BlobStorageConnector
from melius.libraries.Constants import FileTypes, KnownStrings
from tests.mockclasses.dbutilsmock import dbutilsmock


@pytest.fixture
@patch.object(BlobStorageConnector, '_connectToBlobStorage', return_value=None)
def blobStorageConnectorInstance(mock_connectToBlobStorage, rootPath):
    """
    Return BlobStorageConnector object
    """
    config = {
        'storageAccount': 'vmwchmpoc',  # Example storage account name
        'container': 'container',       # Example container name
        'RootPath': rootPath
        }
    return BlobStorageConnector(config)


@pytest.fixture
def datasetName():
    return "ClustersWithCapacitySummary.msu.common"


@pytest.fixture
def rootPath():
    return "/"


@pytest.fixture
def currentDayParquetFiles(startDate):
    """ Returns parquet of startDate

    Args:
        startDate ([type]): Date for which parquet files will be generated

    Returns:
        [type]: Array of parquet files
    """
    return [
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{startDate.strftime("%Y/%m/%d")}/{startDate.strftime("%Y%m%d")}010000_ClustersWithCapacitySummary_v0.parquet',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{startDate.strftime("%Y/%m/%d")}/{startDate.strftime("%Y%m%d")}020000_ClustersWithCapacitySummary_v0.parquet',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{startDate.strftime("%Y/%m/%d")}/{startDate.strftime("%Y%m%d")}030000_ClustersWithCapacitySummary_v0.parquet'
    ]


@pytest.fixture
def currentDayCSVFiles(startDate):
    """ Returns CSV files of startDate

    Args:
        startDate ([type]): Date for which CSV files will be generated

    Returns:
        [type]: Array of CSV files
    """
    return [
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{startDate.strftime("%Y/%m/%d")}/{startDate.strftime("%Y%m%d")}010000_ClustersWithCapacitySummary_v0.csv',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{startDate.strftime("%Y/%m/%d")}/{startDate.strftime("%Y%m%d")}020000_ClustersWithCapacitySummary_v0.csv',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{startDate.strftime("%Y/%m/%d")}/{startDate.strftime("%Y%m%d")}030000_ClustersWithCapacitySummary_v0.csv'
    ]


@pytest.fixture
def oldParquetFiles(startDate):
    """ Returns parquet of older than startDate

    Args:
        startDate ([type]): Parquets files will be generated older than startDate

    Returns:
        [type]: Array of parquet files
    """
    oneDayOldDate = startDate + timedelta(days=-1)
    return [
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{oneDayOldDate.strftime("%Y/%m/%d")}/{oneDayOldDate.strftime("%Y%m%d")}010000_ClustersWithCapacitySummary_v0.parquet',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{oneDayOldDate.strftime("%Y/%m/%d")}/{oneDayOldDate.strftime("%Y%m%d")}020000_ClustersWithCapacitySummary_v0.parquet',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{oneDayOldDate.strftime("%Y/%m/%d")}/{oneDayOldDate.strftime("%Y%m%d")}030000_ClustersWithCapacitySummary_v0.parquet'
    ]


@pytest.fixture
def newerParquetFiles(endDate):
    """ Returns parquet of greater than endDate

    Args:
        endDate ([type]): Parquets files will be generated newer than endDate

    Returns:
        [type]: Array of parquet files
    """
    nextDate = endDate + timedelta(days=1)
    return [
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{nextDate.strftime("%Y/%m/%d")}/{nextDate.strftime("%Y%m%d")}010000_ClustersWithCapacitySummary_v0.parquet',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{nextDate.strftime("%Y/%m/%d")}/{nextDate.strftime("%Y%m%d")}020000_ClustersWithCapacitySummary_v0.parquet',
        f'/dbfs/mnt/cdetest/common/cleansed/msu/ClustersWithCapacitySummary/{nextDate.strftime("%Y/%m/%d")}/{nextDate.strftime("%Y%m%d")}030000_ClustersWithCapacitySummary_v0.parquet'
    ]


@pytest.fixture
def startDate():
    return datetime(2023, 1, 1)


@pytest.fixture
def endDate():
    return datetime(2023, 1, 2)


@pytest.fixture
def relativePath():
    return '/relative/path'


def mockPath(file):
    return pathlib.PurePosixPath(file)


def removeDbfs(parquetFiles):
    result = []
    for item in parquetFiles:
        result.append(item.replace("/dbfs", ""))
    return result


def test_isParquetReadyForUse_NonPartitioned(blobStorageConnectorInstance, monkeypatch, mocker):
    ''' Test isParquetReadyForUse method for a single partition file
    '''
    with monkeypatch.context():
        mocker.patch('melius.libraries.Utility.Utils.getDBUtils', return_value=dbutilsmock)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isDirectory', return_value=False)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.hasSuccessFile', return_value=True)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isFile', return_value=True)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.hasContentInFile', return_value=True)
        assert blobStorageConnectorInstance.isParquetReadyForUse('/mnt/test/mock.parquet')


def test_isParquetReadyForUse_Partitioned_WithSUCCESS(blobStorageConnectorInstance, monkeypatch, mocker):
    ''' Test isParquetReadyForUse method with a partitioned file with _SUCCESS
    '''
    with monkeypatch.context():
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isDirectory', return_value=True)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.hasSuccessFile', return_value=True)
        assert blobStorageConnectorInstance.isParquetReadyForUse('/mnt/test/mock.parquet')


def test_isParquetReadyForUse_Partitioned_WithOutSUCCESS(blobStorageConnectorInstance, monkeypatch, mocker):
    ''' Test isParquetReadyForUse method with a partitioned file without _SUCCESS
    '''
    with monkeypatch.context():
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isDirectory', return_value=True)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.hasSuccessFile', return_value=False)
        assert blobStorageConnectorInstance.isParquetReadyForUse('/mnt/test/mock.parquet') is False


def test_isParquetReadyForUse_PathDoesNotExist(blobStorageConnectorInstance, monkeypatch, mocker):
    ''' Test isParquetReadyForUse method when path does not exist
    '''
    with monkeypatch.context():
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isDirectory', return_value=False)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isFile', return_value=False)
        assert blobStorageConnectorInstance.isParquetReadyForUse('/mnt/test/nonexistent.parquet') is False


def test_isParquetReadyForUse_FileWithoutContent(blobStorageConnectorInstance, monkeypatch, mocker):
    ''' Test isParquetReadyForUse method for a file without content
    '''
    with monkeypatch.context():
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isDirectory', return_value=False)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.isFile', return_value=True)
        mocker.patch('melius.connectors.BlobStorageConnector.BlobStorageConnector.hasContentInFile', return_value=False)
        assert blobStorageConnectorInstance.isParquetReadyForUse('/mnt/test/empty.parquet') is False


class TestBlobStorageConnectorClassMethods:
    ''' Test class methods of BlobStorageConnector '''

    def test_getConnector_NewInstance(self, mocker):
        ''' Test getConnector creates new instance '''
        config = {'storageAccount': 'test', 'container': 'test'}

        # Clear existing connectors
        BlobStorageConnector.clearConnectors()

        with patch.object(BlobStorageConnector, '_connectToBlobStorage', return_value='/test/path'):
            connector = BlobStorageConnector.getConnector(config)
            assert isinstance(connector, BlobStorageConnector)
            assert 'test/test' in BlobStorageConnector._existingConnectors

    def test_getConnector_ExistingInstance(self, mocker):
        ''' Test getConnector returns existing instance '''
        config = {'storageAccount': 'test', 'container': 'test'}

        # Clear existing connectors
        BlobStorageConnector.clearConnectors()

        with patch.object(BlobStorageConnector, '_connectToBlobStorage', return_value='/test/path'):
            connector1 = BlobStorageConnector.getConnector(config)
            connector2 = BlobStorageConnector.getConnector(config)
            assert connector1 is connector2

    def test_getConnector_WithCustomName(self, mocker):
        ''' Test getConnector with custom name '''
        config = {'storageAccount': 'test', 'container': 'test', 'Name': 'custom-name'}

        # Clear existing connectors
        BlobStorageConnector.clearConnectors()

        with patch.object(BlobStorageConnector, '_connectToBlobStorage', return_value='/test/path'):
            BlobStorageConnector.getConnector(config)
            assert 'custom-name' in BlobStorageConnector._existingConnectors

    def test_getConnectorName_WithName(self):
        ''' Test _getConnectorName with Name in config '''
        config = {'Name': 'custom-name', 'storageAccount': 'test', 'container': 'test'}
        name = BlobStorageConnector._getConnectorName(config)
        assert name == 'custom-name'

    def test_getConnectorName_WithoutName(self):
        ''' Test _getConnectorName without Name in config '''
        config = {'storageAccount': 'TestAccount', 'container': 'test-container'}
        name = BlobStorageConnector._getConnectorName(config)
        assert name == 'testaccount/test-container'

    def test_clearConnectors(self):
        ''' Test clearConnectors method '''
        config = {'storageAccount': 'test', 'container': 'test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage', return_value='/test/path'):
            BlobStorageConnector.getConnector(config)
            assert len(BlobStorageConnector._existingConnectors) > 0

            BlobStorageConnector.clearConnectors()
            assert len(BlobStorageConnector._existingConnectors) == 0


class TestBlobStorageConnectorInitAndConnection:
    ''' Test initialization and connection methods '''

    @pytest.fixture(autouse=True)
    def _reset_auth_mode(self):
        """Reset BaseConnector auth mode to default before each test."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        original = BaseConnector.getAuthMode()
        BaseConnector.setAuthMode(ConnectorAuthModes.ManagedIdentiy)
        yield
        BaseConnector.setAuthMode(original)

    def test_init_with_rootpath(self):
        ''' Test initialization with RootPath in config '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/custom/root'}
        connector = BlobStorageConnector(config)
        assert connector._rootPath == '/custom/root'

    def test_init_without_rootpath(self):
        ''' Test initialization without RootPath in config '''
        config = {'storageAccount': 'test', 'container': 'test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage', return_value='/auto/root'):
            connector = BlobStorageConnector(config)
            assert connector._rootPath == '/auto/root'

    def test_getStorageAccountUrl(self):
        ''' Test getStorageAccountUrl method '''
        config = {'storageAccount': 'testaccount', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)
        url = connector.getStorageAccountUrl()
        assert url == 'https://testaccount.dfs.core.windows.net'

    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_connectToBlobStorage_MSI(self, mock_access_token):
        ''' Test _connectToBlobStorage with MSI '''
        config = {'storageAccount': 'test', 'container': 'testcontainer'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=None), \
             patch.object(BlobStorageConnector, '_connectWithMSI') as mock_msi:

            BlobStorageConnector(config)

            # The _connectToBlobStorage is called in __init__, so _connectWithMSI should have been called
            mock_msi.assert_called_once_with('test')

    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_connectToBlobStorage_AppId(self, mock_access_token):
        ''' Test _connectToBlobStorage with AppId '''
        config = {'storageAccount': 'test', 'container': 'testcontainer'}
        access_config = {KnownStrings.AccessType: KnownStrings.AccessType_AppId}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=access_config), \
             patch.object(BlobStorageConnector, '_connectWithAppIdAndCert') as mock_appid:

            BlobStorageConnector(config)

            # The _connectToBlobStorage is called in __init__, so _connectWithAppIdAndCert should have been called
            mock_appid.assert_called_once_with(access_config, 'test', 'testcontainer')

    def test_connectToBlobStorage_UnsupportedAccessType(self):
        ''' Test _connectToBlobStorage with unsupported access type '''
        config = {'storageAccount': 'test', 'container': 'testcontainer'}
        access_config = {KnownStrings.AccessType: 'unsupported'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=access_config):
            with pytest.raises(UnsupportedOperation, match="Unsupported access type"):
                BlobStorageConnector(config)

    def test_getAccessConfig_WithConfig(self):
        ''' Test _getAccessConfig with AccessConfig in config '''
        access_config = {'type': 'test'}
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test', KnownStrings.AccessConfig: access_config}
        BlobStorageConnector(config)

        result = BlobStorageConnector(config)._getAccessConfig()
        assert result == access_config

    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_getAccessConfig_WithoutConfig(self, mock_access_token):
        ''' Test _getAccessConfig without AccessConfig in config '''
        mock_access_token.global_config = {'global': 'config'}
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        result = connector._getAccessConfig()
        assert result == {'global': 'config'}

    def test_connectWithAppIdAndCert_NotImplemented(self):
        ''' Test _connectWithAppIdAndCert raises NotImplementedError '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        with pytest.raises(UnsupportedOperation, match="not implemented yet"):
            connector._connectWithAppIdAndCert({}, 'test', 'test')

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_connectWithMSI(self, mock_utils):
        ''' Test _connectWithMSI method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_spark = Mock()
        mock_utils.getSparkSession.return_value = mock_spark

        with patch.object(connector, '_getAccessConfig', return_value={}), \
             patch.object(BlobStorageConnector, '_resolveMsiIdentity', return_value=('tenant-id', 'client-id')):
            connector._connectWithMSI('testaccount')

            # Verify spark configuration calls
            expected_calls = [
                call('fs.azure.account.auth.type.testaccount.dfs.core.windows.net', 'OAuth'),
                call('fs.azure.account.oauth.provider.type.testaccount.dfs.core.windows.net', 'org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider'),
                call('fs.azure.account.oauth2.msi.tenant.testaccount.dfs.core.windows.net', 'tenant-id'),
                call('fs.azure.account.oauth2.client.id.testaccount.dfs.core.windows.net', 'client-id')
            ]
            mock_spark.conf.set.assert_has_calls(expected_calls)


class TestBlobStorageConnectorDataOperations:
    ''' Test data operation methods '''

    @patch('melius.connectors.BlobStorageConnector.Utils')
    @patch('pyspark.sql.functions.min')
    @patch('pyspark.sql.functions.col')
    def test_getFirstIngestionDateTimeOfDeltaTable(self, mock_col, mock_min, mock_utils):
        ''' Test getFirstIngestionDateTimeOfDeltaTable method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        # Mock DataFrame and aggregation result
        mock_df = Mock()
        mock_agg = Mock()

        mock_df.agg.return_value = mock_agg
        mock_agg.collect.return_value = [[datetime(2023, 1, 1)]]

        # Mock PySpark functions
        mock_column = Mock()
        mock_col.return_value = mock_column
        mock_min_column = Mock()
        mock_min.return_value = mock_min_column

        with patch.object(connector, 'loadDeltaTable', return_value=mock_df):
            result = connector.getFirstIngestionDateTimeOfDeltaTable('path', 'timestamp_col')

            assert result == datetime(2023, 1, 1)
            mock_col.assert_called_with('timestamp_col')
            mock_min.assert_called_with(mock_column)

    @patch('melius.libraries.Utility.Utils')
    @patch('melius.connectors.BlobStorageConnector.DatabricksHelper')
    def test_loadDeltaTable(self, mock_databricks_helper, mock_utils):
        ''' Test loadDeltaTable method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_read = Mock()
        mock_format = Mock()

        mock_sql_context.read = mock_read
        mock_read.format.return_value = mock_format

        mock_utils.getSqlContext.return_value = mock_sql_context

        mock_df = Mock()
        mock_df.rdd.getNumPartitions.return_value = 5
        mock_df.repartition.return_value = mock_df
        mock_format.load.return_value = mock_df
        mock_databricks_helper.optimizeDataFramePartitions.return_value = mock_df

        result = connector.loadDeltaTable('path')

        assert result == mock_df
        mock_read.format.assert_called_with('delta')
        mock_format.load.assert_called_with('/test/path')

    @patch('melius.libraries.Utility.Utils')
    @patch('melius.connectors.BlobStorageConnector.DatabricksHelper')
    def test_loadDeltaTable_WithFilter(self, mock_databricks_helper, mock_utils):
        ''' Test loadDeltaTable method with filter '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_read = Mock()
        mock_format = Mock()

        mock_sql_context.read = mock_read
        mock_read.format.return_value = mock_format

        mock_utils.getSqlContext.return_value = mock_sql_context

        mock_df = Mock()
        mock_df.rdd.getNumPartitions.return_value = 5
        mock_df.repartition.return_value = mock_df
        mock_df.filter.return_value = mock_df  # Make filter return the same mock object
        mock_format.load.return_value = mock_df
        mock_databricks_helper.optimizeDataFramePartitions.return_value = mock_df

        result = connector.loadDeltaTable('path', "timestamp > '2023-01-01'")

        assert result == mock_df
        mock_df.filter.assert_called_once_with("timestamp > '2023-01-01'")
        mock_format.load.assert_called_with('/test/path')

    @patch('melius.libraries.Utility.Utils')
    @patch('melius.connectors.BlobStorageConnector.DatabricksHelper')
    def test_loadDeltaTable_autoRepartition_disabled_by_default(self, mock_databricks_helper, mock_utils):
        ''' Test loadDeltaTable method with autoRepartition disabled by default '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_read = Mock()
        mock_format = Mock()

        mock_sql_context.read = mock_read
        mock_read.format.return_value = mock_format

        mock_utils.getSqlContext.return_value = mock_sql_context

        mock_df = Mock()
        mock_format.load.return_value = mock_df

        result = connector.loadDeltaTable('path')  # Not passing autoRepartition, should default to False

        assert result == mock_df
        # Should not call optimizeDataFramePartitions when autoRepartition is False (default)
        mock_databricks_helper.optimizeDataFramePartitions.assert_not_called()

    @patch('melius.libraries.Utility.Utils')
    @patch('melius.connectors.BlobStorageConnector.DatabricksHelper')
    def test_loadDeltaTable_autoRepartition_enabled_explicitly(self, mock_databricks_helper, mock_utils):
        ''' Test loadDeltaTable method with autoRepartition explicitly enabled '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_read = Mock()
        mock_format = Mock()

        mock_sql_context.read = mock_read
        mock_read.format.return_value = mock_format

        mock_utils.getSqlContext.return_value = mock_sql_context

        mock_df = Mock()
        mock_format.load.return_value = mock_df
        mock_databricks_helper.optimizeDataFramePartitions.return_value = mock_df

        result = connector.loadDeltaTable('path', autoRepartition=True)

        assert result == mock_df
        # Should call optimizeDataFramePartitions when autoRepartition is True
        mock_databricks_helper.optimizeDataFramePartitions.assert_called_once_with(mock_df)

    @patch('melius.libraries.Utility.Utils')
    def test_loadJson(self, mock_utils):
        ''' Test loadJson method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_read = Mock()
        mock_format = Mock()
        mock_load = Mock()

        mock_sql_context.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.load.return_value = mock_load

        mock_utils.getSqlContext.return_value = mock_sql_context

        connector.loadJson('file.json')

        mock_read.format.assert_called_with('json')
        mock_format.load.assert_called_with('/test/file.json')

    @patch('melius.libraries.Utility.Utils')
    def test_loadJson_WithSchema(self, mock_utils):
        ''' Test loadJson method with schema '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_read = Mock()
        mock_format = Mock()
        mock_schema = Mock()
        mock_load = Mock()

        mock_sql_context.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.schema.return_value = mock_schema
        mock_schema.load.return_value = mock_load

        mock_utils.getSqlContext.return_value = mock_sql_context

        schema = Mock()
        connector.loadJson('file.json', schema)

        mock_read.format.assert_called_with('json')
        mock_format.schema.assert_called_with(schema)
        mock_schema.load.assert_called_with('/test/file.json')

    @patch('melius.connectors.BlobStorageConnector.Utils')
    @patch('delta.tables.DeltaTable')
    def test_deleteRowsFromDeltaTable(self, mock_delta_table, mock_utils):
        ''' Test deleteRowsFromDeltaTable method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}

        with patch.object(BlobStorageConnector, '_connectToBlobStorage'):
            connector = BlobStorageConnector(config)

        mock_spark = Mock()
        mock_utils.getSparkSession.return_value = mock_spark

        mock_dt = Mock()
        mock_delta_table.forPath.return_value = mock_dt

        connector.deleteRowsFromDeltaTable('path', 'id = 1')

        mock_delta_table.forPath.assert_called_with(mock_spark, '/test/path')
        mock_dt.delete.assert_called_with('id = 1')


class TestBlobStorageConnectorFileOperations:
    ''' Test file operation methods '''

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_getListOfFilesUnder(self, mock_access_token, mock_data_lake_client):
        ''' Test getListOfFilesUnder method '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_path_client = Mock()
        mock_fs_client.get_directory_client.return_value = mock_path_client

        file1 = Mock()
        file1.name = 'file1.txt'
        file2 = Mock()
        file2.name = 'file2.txt'
        mock_paths = [file1, file2]
        mock_path_client.get_paths.return_value = mock_paths

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.getListOfFilesUnder('/test/path')

            assert result == ['file1.txt', 'file2.txt']

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_hasContentInFile_True(self, mock_access_token, mock_data_lake_client):
        ''' Test hasContentInFile method returns True '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_file_client = Mock()
        mock_fs_client.get_file_client.return_value = mock_file_client
        mock_file_client.exists.return_value = True

        mock_properties = Mock()
        mock_properties.size = 1024
        mock_file_client.get_file_properties.return_value = mock_properties

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.hasContentInFile('/test/file.txt')

            assert result is True

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_hasContentInFile_False_EmptyFile(self, mock_access_token, mock_data_lake_client):
        ''' Test hasContentInFile method returns False for empty file '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_file_client = Mock()
        mock_fs_client.get_file_client.return_value = mock_file_client
        mock_file_client.exists.return_value = True

        mock_properties = Mock()
        mock_properties.size = 0
        mock_file_client.get_file_properties.return_value = mock_properties

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.hasContentInFile('/test/file.txt')

            assert result is False

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_hasContentInFile_False_FileNotExists(self, mock_access_token, mock_data_lake_client):
        ''' Test hasContentInFile method returns False for non-existent file '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_file_client = Mock()
        mock_fs_client.get_file_client.return_value = mock_file_client
        mock_file_client.exists.return_value = False

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.hasContentInFile('/test/file.txt')

            assert result is False

    def test_existsPath_True(self):
        ''' Test existsPath returns True '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        with patch.object(connector, 'getPathType', return_value='File'):
            result = connector.existsPath('/test/path')
            assert result is True

    def test_existsPath_False(self):
        ''' Test existsPath returns False '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        with patch.object(connector, 'getPathType', return_value='Unknown'):
            result = connector.existsPath('/test/path')
            assert result is False

    def test_isDirectory_True(self):
        ''' Test isDirectory returns True '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        with patch.object(connector, 'getPathType', return_value='Directory'):
            result = connector.isDirectory('/test/path')
            assert result is True

    def test_isFile_True(self):
        ''' Test isFile returns True '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        with patch.object(connector, 'getPathType', return_value='File'):
            result = connector.isFile('/test/path')
            assert result is True

    @patch('melius.connectors.BlobStorageConnector.Utils')
    @patch('melius.connectors.BlobStorageConnector.DeltaTable')
    def test_isDeltaTable_True(self, mock_delta_table, mock_utils):
        ''' Test isDeltaTable returns True '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_spark = Mock()
        mock_utils.getSparkSession.return_value = mock_spark
        mock_delta_table.isDeltaTable.return_value = True

        result = connector.isDeltaTable('/test/path')

        assert result is True
        mock_delta_table.isDeltaTable.assert_called_with(mock_spark, '/test/path')

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_hasSuccessFile_True(self, mock_utils, mocker):
        ''' Test hasSuccessFile returns True '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_dbutils = Mock()
        mock_utils.getDBUtils.return_value = mock_dbutils

        with patch.object(connector, 'existsPath', return_value=True):
            result = connector.hasSuccessFile('/test/path')
            assert result is True

    def test_getAbsolutePath_RelativePath(self):
        ''' Test getAbsolutePath with relative path '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/root'}
        connector = BlobStorageConnector(config)

        result = connector.getAbsolutePath('relative/path')
        assert result == '/root/relative/path'

    def test_getAbsolutePath_AbsolutePath(self):
        ''' Test getAbsolutePath with absolute path '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/root'}
        connector = BlobStorageConnector(config)

        result = connector.getAbsolutePath('/root/absolute/path')
        assert result == '/root/absolute/path'

    def test_getAbsolutePath_WithLeadingSlash(self):
        ''' Test getAbsolutePath with leading slash '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/root'}
        connector = BlobStorageConnector(config)

        result = connector.getAbsolutePath('/relative/path')
        assert result == '/root/relative/path'

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_deletePath_Exists(self, mock_utils):
        ''' Test deletePath when path exists '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_dbutils = Mock()
        mock_utils.getDBUtils.return_value = mock_dbutils

        with patch.object(connector, 'existsPath', return_value=True):
            connector.deletePath('/test/file.txt')
            mock_dbutils.fs.rm.assert_called_once_with('/test/file.txt', True)

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_deletePath_NotExists(self, mock_utils):
        ''' Test deletePath when path does not exist '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_dbutils = Mock()
        mock_utils.getDBUtils.return_value = mock_dbutils

        with patch.object(connector, 'existsPath', return_value=False):
            connector.deletePath('/test/file.txt')
            mock_dbutils.fs.rm.assert_not_called()


class TestBlobStorageConnectorSaveOperations:
    ''' Test save operation methods '''

    def test_saveTo_InvalidDataFrame(self):
        ''' Test saveTo with invalid dataframe '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        with pytest.raises(ValueError, match="cannot be None or empty"):
            connector.saveTo(None, '/test/path', FileTypes.Parquet)

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_saveTo_Parquet(self, mock_utils):
        ''' Test saveTo with Parquet format '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_df.count.return_value = 100

        with patch.object(connector, 'deletePath'), \
             patch.object(connector, '_saveParquet') as mock_save_parquet:

            result = connector.saveTo(mock_df, '/test/file.parquet', FileTypes.Parquet)

            mock_save_parquet.assert_called_once()
            # Since the input path already starts with root path, it should remain the same
            assert result == '/test/file.parquet'

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_saveTo_CSV(self, mock_utils):
        ''' Test saveTo with CSV format '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_df.count.return_value = 100

        with patch.object(connector, 'deletePath'), \
             patch.object(connector, '_saveCsv') as mock_save_csv:

            result = connector.saveTo(mock_df, '/test/file.csv', FileTypes.CSV)

            mock_save_csv.assert_called_once()
            # Since the input path already starts with root path, it should remain the same
            assert result == '/test/file.csv'

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_saveTo_JSON(self, mock_utils):
        ''' Test saveTo with JSON format '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_df.count.return_value = 100

        with patch.object(connector, 'deletePath'), \
             patch.object(connector, 'saveJson') as mock_save_json:

            result = connector.saveTo(mock_df, '/test/file.json', FileTypes.JSON)

            mock_save_json.assert_called_once()
            # Since the input path already starts with root path, it should remain the same
            assert result == '/test/file.json'

    def test_saveTo_UnsupportedFormat(self):
        ''' Test saveTo with unsupported format '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_df.count.return_value = 100

        # Mock an unsupported file type
        unsupported_type = Mock()
        unsupported_type.__str__ = lambda self: "UnsupportedType"

        with patch.object(connector, 'deletePath'), \
             pytest.raises(UnsupportedOperation, match="not supported for saving"):

            connector.saveTo(mock_df, '/test/file.unknown', unsupported_type)

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_saveParquet(self, mock_utils):
        ''' Test _saveParquet method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.option.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.partitionBy.return_value = mock_write

        mock_utils.adjustPartition.return_value = mock_df

        connector._saveParquet('/test/output', mock_df, 4, 'overwrite', ['col1'])

        mock_write.option.assert_called_with("parquet.block.size", 256 * 1024*1024)
        mock_write.mode.assert_called_with('overwrite')
        mock_write.partitionBy.assert_called_with('col1')
        mock_write.parquet.assert_called_with('/test/output')

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_saveCsv(self, mock_utils):
        ''' Test _saveCsv method (no pre-existing target path) '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_repartitioned = Mock()
        mock_write = Mock()

        mock_df.repartition.return_value = mock_repartitioned
        mock_repartitioned.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.options.return_value = mock_write
        mock_write.csv = Mock()

        mock_dbutils = Mock()
        mock_dbutils.fs.rm = Mock()
        mock_dbutils.fs.ls = Mock(return_value=[Mock(path='/test/output/part-00000.csv')])
        mock_utils.getDBUtils.return_value = mock_dbutils

        # _saveCsv checks connector.existsPath(outputPath) to decide whether to rm
        with patch.object(connector, 'existsPath', return_value=False):
            connector._saveCsv('/test/output.csv', mock_df, FileTypes.CSV)

        mock_dbutils.fs.rm.assert_not_called()
        mock_df.repartition.assert_called_once_with(1)
        mock_write.mode.assert_called_once_with('overwrite')
        mock_write.options.assert_called_once_with(delimiter=',', header=True)
        mock_write.csv.assert_called_once_with('/test/output.csv')

    def test_saveCsv_WrongExtension(self):
        ''' Test _saveCsv with wrong file extension '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()

        with pytest.raises(ValueError, match="File extension is not matched"):
            connector._saveCsv('/test/output.txt', mock_df, FileTypes.CSV)

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_saveJson(self, mock_utils):
        ''' Test saveJson method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_repartitioned = Mock()
        mock_write = Mock()
        mock_df.repartition.return_value = mock_repartitioned
        mock_repartitioned.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.format.return_value = mock_write

        mock_dbutils = Mock()
        mock_utils.getDBUtils.return_value = mock_dbutils

        mock_file = Mock()
        mock_file.path = '/test/output/part-00000.json'
        mock_dbutils.fs.ls.return_value = [mock_file]

        connector.saveJson('/test/output.json', mock_df)

        mock_df.repartition.assert_called_with(1)
        mock_write.mode.assert_called_with('overwrite')
        mock_write.format.assert_called_with('json')
        mock_write.save.assert_called_with('/test/output.json')


class TestBlobStorageConnectorDeltaOperations:
    ''' Test Delta table operation methods '''

    @patch('melius.connectors.BlobStorageConnector.DatabricksHelper')
    def test_saveDeltaTable_EmptyDataFrame(self, mock_databricks_helper):
        ''' Test _saveDeltaTable with empty dataframe '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = True

        connector._saveDeltaTable(mock_df, '/test/path', 'append')

        # Should return early without any write operations
        mock_databricks_helper.optimizeDataFramePartitions.assert_not_called()

    @patch('melius.connectors.BlobStorageConnector.DatabricksHelper')
    def test_saveDeltaTable_WithRepartition(self, mock_databricks_helper):
        ''' Test _saveDeltaTable with repartitioning '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.rdd.getNumPartitions.return_value = 5
        mock_df.repartition.return_value = mock_df

        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write
        mock_write.partitionBy.return_value = mock_write

        mock_databricks_helper.optimizeDataFramePartitions.return_value = mock_df

        connector._saveDeltaTable(mock_df, '/test/path', 'append', ['col1'], True, True)

        mock_write.format.assert_called_with('delta')
        mock_write.mode.assert_called_with('append')
        mock_write.option.assert_any_call('compression', 'snappy')
        mock_write.option.assert_any_call('mergeSchema', 'true')
        mock_write.partitionBy.assert_called_with('col1')
        mock_write.save.assert_called_with('/test/path')

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_appendDeltaTable_NewTable(self, mock_utils):
        ''' Test appendDeltaTable for new table '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()

        with patch.object(connector, 'existsPath', return_value=False), \
             patch.object(connector, '_saveDeltaTable') as mock_save:

            connector.appendDeltaLake(mock_df, '/test/path')

            # Since the input path already starts with root path, it should remain the same
            mock_save.assert_called_once_with(mock_df, '/test/path', 'overwrite', None, True, False)

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_appendDeltaTable_ExistingTable(self, mock_utils):
        ''' Test appendDeltaTable for existing table '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()

        with patch.object(connector, 'existsPath', return_value=True), \
             patch.object(connector, '_saveDeltaTable') as mock_save:

            connector.appendDeltaLake(mock_df, '/test/path')

            # Since the input path already starts with root path, it should remain the same
            mock_save.assert_called_once_with(mock_df, '/test/path', 'append', None, True, False)

    def test_overwriteDeltaTable(self):
        ''' Test overwriteDeltaTable method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()

        with patch.object(connector, '_saveDeltaTable') as mock_save:
            connector.overwriteDeltaTable(mock_df, '/test/path', ['col1'])

            # Since the input path already starts with root path, it should remain the same
            mock_save.assert_called_once_with(mock_df, '/test/path', 'overwrite', ['col1'])

    def test_mergeDeltaTable_NewTable(self):
        ''' Test mergeDeltaTable with new table '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_df.schema = 'schema'
        mock_df.rdd.isEmpty.return_value = False

        with patch.object(connector, 'existsPath', return_value=False), \
             patch.object(connector, 'ensureDeltaTableForWrite') as mock_overwrite:

            connector.mergeDeltaTable(mock_df, '/test/path', ['id'], ['name'])

            # Since the input path already starts with root path, it should remain the same
            mock_overwrite.assert_called_once_with('/test/path', 'schema')


class TestBlobStorageConnectorLoadOperations:
    ''' Test load operation methods '''

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_loadFrom_Parquet(self, mock_utils):
        ''' Test loadFrom with Parquet files '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_utils.getSqlContext.return_value = mock_sql_context

        files = ['/test/file1.parquet', '/test/file2.parquet']

        with patch.object(connector, 'isParquetReadyForUse', return_value=True):
            connector.loadFrom(files)

            mock_sql_context.read.options.assert_called_with(mergeSchema=True)
            mock_sql_context.read.options.return_value.parquet.assert_called_with(*files)

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_loadFrom_CSV(self, mock_utils):
        ''' Test loadFrom with CSV files '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_utils.getSqlContext.return_value = mock_sql_context

        mock_df1 = Mock()
        mock_df2 = Mock()
        mock_df1.union.return_value = mock_df2

        mock_read = Mock()
        mock_sql_context.read.options.return_value.csv.return_value = mock_df1
        mock_sql_context.read = mock_read
        mock_read.options.return_value = mock_read

        files = ['/test/file1.csv', '/test/file2.csv']

        with patch.object(connector, 'isParquetReadyForUse', return_value=True):
            connector.loadFrom(files)

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_loadFrom_JSON(self, mock_utils):
        ''' Test loadFrom with JSON files '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_sql_context = Mock()
        mock_utils.getSqlContext.return_value = mock_sql_context

        mock_df1 = Mock()
        mock_df2 = Mock()
        mock_df1.union.return_value = mock_df2

        mock_read = Mock()
        mock_sql_context.read.option.return_value.json.return_value = mock_df1
        mock_sql_context.read = mock_read
        mock_read.option.return_value = mock_read

        files = ['/test/file1.json', '/test/file2.json']

        with patch.object(connector, 'isParquetReadyForUse', return_value=True):
            connector.loadFrom(files)

    def test_loadFrom_UnsupportedFileType(self):
        ''' Test loadFrom with unsupported file type '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        files = ['/test/file1.unknown']

        with patch.object(connector, 'isParquetReadyForUse', return_value=True), \
             pytest.raises(UnsupportedOperation, match="not a supported fileType"):

            connector.loadFrom(files)

    def test_loadLatestFile(self):
        ''' Test loadLatestFile method '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)

        with patch.object(connector, 'findLatestOrOldestFileInRange', return_value=('/test/latest.parquet', None)) as mock_find, \
             patch.object(connector, 'loadFrom', return_value=Mock()) as mock_load:

            connector.loadLatestFile('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)

            mock_find.assert_called_once_with('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)
            mock_load.assert_called_once_with(['/test/latest.parquet'])

    def test_findLatestFileInRange_InvalidRange(self):
        ''' Test findLatestFileInRange with invalid date range '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        start_date = datetime(2023, 1, 2)
        end_date = datetime(2023, 1, 1)  # end before start

        with pytest.raises(ValueError, match="end_dt must be >= start_dt"):
            connector.findLatestOrOldestFileInRange('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)

    def test_findLatestFileInRange_NoFilesFound(self):
        ''' Test findLatestFileInRange when no files found '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 2)

        with patch.object(connector, 'getListOfFilesUnder', return_value=[]), \
             patch.object(connector, 'getPathType', return_value='Unknown'):

            result = connector.findLatestOrOldestFileInRange('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)
            assert result == (None, "No prefix_YYYYMMDDHHmm.parquet files found between 2023-01-01T00:00:00 and 2023-01-02T00:00:00.")

    def test_findLatestFileInRange_ValidFile(self):
        ''' Test findLatestFileInRange with valid files '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        start_date = datetime(2023, 1, 1, 10, 0)
        end_date = datetime(2023, 1, 1, 12, 0)

        # Mock file that matches pattern and is in range
        mock_files = [
            'prefix_202301011100.parquet',  # 11:00 - in range
            'prefix_202301011300.parquet'   # 13:00 - out of range
        ]

        with patch.object(connector, 'getListOfFilesUnder', return_value=mock_files), \
             patch.object(connector, 'isParquetReadyForUse', return_value=True):

            with patch.object(connector, 'getPathType', return_value='File'):
                result = connector.findLatestOrOldestFileInRange('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)

            assert result == ('prefix_202301011100.parquet', None)


class TestBlobStorageConnectorUtilityMethods:
    ''' Test utility methods '''


class TestBlobStorageConnectorPathOperations:
    ''' Test path operation methods '''

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_getPathType_Directory(self, mock_access_token, mock_data_lake_client):
        ''' Test getPathType returns Directory '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_dir_client = Mock()
        mock_fs_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.exists.return_value = True

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.getPathType('/test/path')

            assert result == "Directory"

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_getPathType_File(self, mock_access_token, mock_data_lake_client):
        ''' Test getPathType returns File '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_dir_client = Mock()
        mock_fs_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.exists.return_value = False

        mock_file_client = Mock()
        mock_fs_client.get_file_client.return_value = mock_file_client
        mock_file_client.exists.return_value = True

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.getPathType('/test/file.txt')

            assert result == "File"

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_getPathType_Unknown(self, mock_access_token, mock_data_lake_client):
        ''' Test getPathType returns Unknown '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_dir_client = Mock()
        mock_fs_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.exists.return_value = False

        mock_file_client = Mock()
        mock_fs_client.get_file_client.return_value = mock_file_client
        mock_file_client.exists.return_value = False

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.getPathType('/test/nonexistent')

            assert result == "Unknown"

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_getPathType_WithTrailingSlash(self, mock_access_token, mock_data_lake_client):
        ''' Test getPathType with trailing slash normalization '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_dir_client = Mock()
        mock_fs_client.get_directory_client.return_value = mock_dir_client
        mock_dir_client.exists.return_value = True

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.getPathType('/test/path/')

            # Should call with normalized path (no trailing slash)
            mock_fs_client.get_directory_client.assert_called_with('/test/path')
            assert result == "Directory"

    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    def test_getListOfFilesUnder_WithTrailingSlash(self, mock_access_token, mock_data_lake_client):
        ''' Test getListOfFilesUnder with trailing slash normalization '''
        config = {'storageAccount': 'test', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        mock_fs_client = Mock()
        mock_client.get_file_system_client.return_value = mock_fs_client

        mock_path_client = Mock()
        mock_fs_client.get_directory_client.return_value = mock_path_client

        file1 = Mock()
        file1.name = 'file1.txt'
        file2 = Mock()
        file2.name = 'file2.txt'
        mock_paths = [file1, file2]
        mock_path_client.get_paths.return_value = mock_paths

        with patch.object(connector, '_getAccessConfig', return_value={}):
            result = connector.getListOfFilesUnder('/test/path/')

            # Should call with normalized path (no trailing slash)
            mock_fs_client.get_directory_client.assert_called_with('/test/path')
            assert result == ['file1.txt', 'file2.txt']


class TestBlobStorageConnectorFicRouting:
    ''' Test FIC routing in _connectToBlobStorage '''

    @pytest.fixture(autouse=True)
    def _reset_auth_mode(self):
        """Reset BaseConnector auth mode before each test."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        original = BaseConnector.getAuthMode()
        BaseConnector.setAuthMode(ConnectorAuthModes.ManagedIdentiy)
        yield
        BaseConnector.setAuthMode(original)

    def test_sni_mode_routes_to_access_token(self):
        ''' When auth mode is Sni, _connectWithAccessToken is called '''
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.Sni)

        config = {'storageAccount': 'test', 'container': 'testcontainer'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=None), \
             patch.object(BlobStorageConnector, '_connectWithAccessToken') as mock_at:

            BlobStorageConnector(config)
            mock_at.assert_called_once_with(None, 'test', 'testcontainer')

    def test_non_sni_mode_routes_to_msi(self):
        ''' When auth mode is not Sni, _connectWithMSI is called '''
        config = {'storageAccount': 'test', 'container': 'testcontainer'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=None), \
             patch.object(BlobStorageConnector, '_connectWithMSI') as mock_msi:

            BlobStorageConnector(config)
            mock_msi.assert_called_once_with('test')

    def test_sni_mode_with_access_config_passes_config(self):
        ''' Sni mode passes access config to _connectWithAccessToken '''
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.Sni)

        config = {'storageAccount': 'acct', 'container': 'cont'}
        access_cfg = {'accessType': 'FIC', 'clientId': 'c', 'tenantId': 't'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=access_cfg), \
             patch.object(BlobStorageConnector, '_connectWithAccessToken') as mock_at:

            BlobStorageConnector(config)
            mock_at.assert_called_once_with(access_cfg, 'acct', 'cont')

    def test_sni_mode_returns_abfss_path(self):
        ''' _connectToBlobStorage returns correct abfss path under Sni '''
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.Sni)

        config = {'storageAccount': 'myacct', 'container': 'mycont'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=None), \
             patch.object(BlobStorageConnector, '_connectWithAccessToken'):

            connector = BlobStorageConnector(config)
            assert connector._rootPath == 'abfss://mycont@myacct.dfs.core.windows.net'


class TestBlobStorageConnectorResolveMsiIdentity:
    ''' Test _resolveMsiIdentity static method '''

    def test_resolve_with_client_id_in_config(self):
        ''' _resolveMsiIdentity reads clientId from config and tenantId from JWT '''
        import jwt as jwt_lib

        fake_token = jwt_lib.encode(
            {"tid": "jwt-tenant", "azp": "jwt-client"},
            "secret",
            algorithm="HS256"
        )

        mock_mi = Mock()
        mock_mi.get_token.return_value = Mock(token=fake_token)

        config = {KnownStrings.Config_ClientId: "config-client"}

        with patch('azure.identity.ManagedIdentityCredential', return_value=mock_mi):
            tenant_id, client_id = BlobStorageConnector._resolveMsiIdentity(config)

        assert tenant_id == "jwt-tenant"
        assert client_id == "config-client"

    def test_resolve_without_client_id_extracts_from_jwt(self):
        ''' _resolveMsiIdentity extracts clientId from JWT when not in config '''
        import jwt as jwt_lib

        fake_token = jwt_lib.encode(
            {"tid": "jwt-tenant", "azp": "jwt-client-azp"},
            "secret",
            algorithm="HS256"
        )

        mock_mi = Mock()
        mock_mi.get_token.return_value = Mock(token=fake_token)

        with patch('azure.identity.ManagedIdentityCredential', return_value=mock_mi):
            tenant_id, client_id = BlobStorageConnector._resolveMsiIdentity(None)

        assert tenant_id == "jwt-tenant"
        assert client_id == "jwt-client-azp"

    def test_resolve_uses_appid_fallback(self):
        ''' _resolveMsiIdentity falls back to appid claim when azp is absent '''
        import jwt as jwt_lib

        fake_token = jwt_lib.encode(
            {"tid": "t1", "appid": "appid-client"},
            "secret",
            algorithm="HS256"
        )

        mock_mi = Mock()
        mock_mi.get_token.return_value = Mock(token=fake_token)

        with patch('azure.identity.ManagedIdentityCredential', return_value=mock_mi):
            tenant_id, client_id = BlobStorageConnector._resolveMsiIdentity({})

        assert tenant_id == "t1"
        assert client_id == "appid-client"


class TestBlobStorageConnectorAccessTokenConnection:
    ''' Test _connectWithAccessToken method '''

    @patch('melius.connectors.BlobStorageConnector.Utils')
    @patch('melius.connectors.BlobStorageConnector.DataLakeServiceClient')
    @patch('melius.connectors.BlobStorageConnector.AccessToken')
    @patch('melius.connectors.BlobStorageConnector.generate_file_system_sas')
    def test_connectWithAccessToken(self, mock_generate_sas, mock_access_token, mock_data_lake_client, mock_utils):
        ''' Test _connectWithAccessToken method '''
        config = {'storageAccount': 'testaccount', 'container': 'testcontainer', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        # Mock AccessToken
        mock_credential = Mock()
        mock_access_token.return_value.getCredential.return_value = mock_credential

        # Mock DataLakeServiceClient
        mock_client = Mock()
        mock_data_lake_client.return_value = mock_client

        # Mock user delegation key
        mock_user_delegation_key = Mock()
        mock_client.get_user_delegation_key.return_value = mock_user_delegation_key

        # Mock SAS token generation
        mock_sas_token = 'mock_sas_token'
        mock_generate_sas.return_value = mock_sas_token

        # Mock Spark session
        mock_spark = Mock()
        mock_utils.getSparkSession.return_value = mock_spark

        access_config = {'test': 'config'}
        connector._connectWithAccessToken(access_config, 'testaccount', 'testcontainer')

        # Verify DataLakeServiceClient was created with correct parameters
        mock_data_lake_client.assert_called_once_with(
            account_url='https://testaccount.dfs.core.windows.net',
            credential=mock_credential
        )

        # Verify user delegation key was requested
        mock_client.get_user_delegation_key.assert_called_once()

        # Verify SAS token was generated
        mock_generate_sas.assert_called_once()

        # Verify Spark configuration was set
        expected_calls = [
            call('fs.azure.account.auth.type.testaccount.dfs.core.windows.net', 'SAS'),
            call('fs.azure.sas.token.provider.type.testaccount.dfs.core.windows.net', 'org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider'),
            call('fs.azure.sas.fixed.token.testaccount.dfs.core.windows.net', mock_sas_token)
        ]
        mock_spark.conf.set.assert_has_calls(expected_calls)


class TestBlobStorageConnectorBackwardCompatibility:
    ''' Explicit backward compatibility tests to verify default MSI behavior '''

    @pytest.fixture(autouse=True)
    def _reset_auth_mode(self):
        """Reset BaseConnector auth mode to Msi before each test."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        original = BaseConnector.getAuthMode()
        BaseConnector.setAuthMode(ConnectorAuthModes.ManagedIdentiy)
        yield
        BaseConnector.setAuthMode(original)

    def test_default_auth_uses_msi_path(self):
        ''' By default (no FIC), BlobStorageConnector should use MSI path '''
        config = {'storageAccount': 'test', 'container': 'testcontainer'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=None), \
             patch.object(BlobStorageConnector, '_connectWithMSI') as mock_msi, \
             patch.object(BlobStorageConnector, '_connectWithAccessToken') as mock_at:

            BlobStorageConnector(config)

            # MSI should be called, not access token
            mock_msi.assert_called_once_with('test')
            mock_at.assert_not_called()

    def test_msi_config_uses_msi_path(self):
        ''' Explicit MSI accessConfig should use MSI path '''
        config = {'storageAccount': 'test', 'container': 'testcontainer'}
        access_cfg = {'accessType': 'MSI'}

        with patch.object(BlobStorageConnector, '_getAccessConfig', return_value=access_cfg), \
             patch.object(BlobStorageConnector, '_connectWithMSI') as mock_msi, \
             patch.object(BlobStorageConnector, '_connectWithAccessToken') as mock_at:

            BlobStorageConnector(config)

            # MSI should be called
            mock_msi.assert_called_once_with('test')
            mock_at.assert_not_called()

    def test_get_connector_returns_instance_without_fic(self):
        ''' getConnector() should work correctly without FIC '''
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes

        # Ensure we're in Msi mode
        BaseConnector.setAuthMode(ConnectorAuthModes.ManagedIdentiy)

        config = {'storageAccount': 'backcompat', 'container': 'test'}

        BlobStorageConnector.clearConnectors()

        with patch.object(BlobStorageConnector, '_connectToBlobStorage', return_value='/test/path'):
            connector = BlobStorageConnector.getConnector(config)

            assert connector is not None
            assert isinstance(connector, BlobStorageConnector)
            # Should be using default auth mode (Msi)
            assert BaseConnector.getAuthMode() == ConnectorAuthModes.ManagedIdentiy


class TestBlobStorageConnectorEdgeCases:
    ''' Test edge cases and error conditions '''

    def test_getAbsolutePath_BackslashNormalization(self):
        ''' Test getAbsolutePath normalizes backslashes '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/root'}
        connector = BlobStorageConnector(config)

        result = connector.getAbsolutePath('relative\\path\\with\\backslashes')
        assert result == '/root/relative/path/with/backslashes'

    @patch('melius.connectors.BlobStorageConnector.Utils')
    def test_saveCsv_FileAlreadyExists(self, mock_utils):
        ''' Test _saveCsv when target path already exists (should remove it first) '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_df = Mock()
        mock_repartitioned = Mock()
        mock_write = Mock()

        mock_df.repartition.return_value = mock_repartitioned
        mock_repartitioned.write = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.options.return_value = mock_write
        mock_write.csv = Mock()

        # _saveCsv checks connector.existsPath(outputPath)
        with patch.object(connector, 'existsPath', return_value=True):
            mock_dbutils = Mock()
            mock_dbutils.fs.rm = Mock()
            mock_dbutils.fs.ls = Mock(return_value=[Mock(path='/test/output/part-00000.csv')])
            mock_utils.getDBUtils.return_value = mock_dbutils

            connector._saveCsv('/test/output.csv', mock_df, FileTypes.CSV)

            mock_dbutils.fs.rm.assert_called_once_with('/test/output.csv', True)
            mock_df.repartition.assert_called_once_with(1)
            mock_write.mode.assert_called_once_with('overwrite')
            mock_write.options.assert_called_once_with(delimiter=',', header=True)
            mock_write.csv.assert_called_once_with('/test/output.csv')

    def test_findLatestFileInRange_BadTimestamp(self):
        ''' Test findLatestFileInRange with invalid timestamp in filename '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        start_date = datetime(2023, 1, 1, 10, 0)
        end_date = datetime(2023, 1, 1, 12, 0)

        # Mock file with invalid timestamp
        mock_files = [
            'prefix_invalid_timestamp.parquet',
            'prefix_202301991100.parquet'  # Invalid date
        ]

        with patch.object(connector, 'getListOfFilesUnder', return_value=mock_files), \
             patch.object(connector, 'getPathType', return_value='File'):

            result = connector.findLatestOrOldestFileInRange('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)

            assert result == (None, "No prefix_YYYYMMDDHHmm.parquet files found between 2023-01-01T10:00:00 and 2023-01-01T12:00:00.")

    def test_findLatestFileInRange_NonParquetNotReady(self):
        ''' Test findLatestFileInRange with non-parquet files that are not ready '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        start_date = datetime(2023, 1, 1, 10, 0)
        end_date = datetime(2023, 1, 1, 12, 0)

        mock_files = ['prefix_202301011100.parquet']

        with patch.object(connector, 'getListOfFilesUnder', return_value=mock_files), \
             patch.object(connector, 'isParquetReadyForUse', return_value=False), \
             patch.object(connector, 'getPathType', return_value='Unknown'):

            result = connector.findLatestOrOldestFileInRange('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)

            assert result == (None, "No prefix_YYYYMMDDHHmm.parquet files found between 2023-01-01T10:00:00 and 2023-01-01T12:00:00.")

    def test_findLatestFileInRange_MultipleValidFiles(self):
        ''' Test findLatestFileInRange returns newest file when multiple valid files exist '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        start_date = datetime(2023, 1, 1, 10, 0)
        end_date = datetime(2023, 1, 1, 12, 0)

        # Mock files with different timestamps in range
        mock_files = [
            'prefix_202301011030.parquet',  # 10:30
            'prefix_202301011100.parquet',  # 11:00 - newer
            'prefix_202301011130.parquet'   # 11:30 - newest
        ]

        with patch.object(connector, 'getListOfFilesUnder', return_value=mock_files), \
             patch.object(connector, 'getPathType', return_value='File'), \
             patch.object(connector, 'isParquetReadyForUse', return_value=True):

            result = connector.findLatestOrOldestFileInRange('/test/root', start_date, end_date, 'prefix', FileTypes.Parquet)

            # Should return the newest file (11:30)
            assert result == ('prefix_202301011130.parquet', None)

    def test_loadFrom_FilterNonReadyFiles(self):
        ''' Test loadFrom filters out non-ready files '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_sql_context = Mock()

        with patch('melius.connectors.BlobStorageConnector.Utils') as mock_utils:
            mock_utils.getSqlContext.return_value = mock_sql_context

            files = ['/test/ready.parquet', '/test/notready.parquet']
            ready_files = ['/test/ready.parquet']

            def mock_is_ready(file):
                return file in ready_files

            with patch.object(connector, 'isParquetReadyForUse', side_effect=mock_is_ready):
                connector.loadFrom(files)

                # Should only load the ready file
                mock_sql_context.read.options.assert_called_with(mergeSchema=True)
                mock_sql_context.read.options.return_value.parquet.assert_called_with('/test/ready.parquet')

    def test_loadFrom_TrailingSlashRemoval(self):
        ''' Test loadFrom removes trailing slashes from file paths '''
        config = {'storageAccount': 'test', 'container': 'test', 'RootPath': '/test'}
        connector = BlobStorageConnector(config)

        mock_sql_context = Mock()

        with patch('melius.connectors.BlobStorageConnector.Utils') as mock_utils:
            mock_utils.getSqlContext.return_value = mock_sql_context

            files = ['/test/file1.parquet/', '/test/file2.parquet']

            with patch.object(connector, 'isParquetReadyForUse', return_value=True), \
                 patch.object(connector, 'getPathType', return_value='File'):
                connector.loadFrom(files)

                # Should call with normalized paths (no trailing slash)
                expected_files = ['/test/file1.parquet', '/test/file2.parquet']
                mock_sql_context.read.options.return_value.parquet.assert_called_with(*expected_files)


class TestFindLatestOrOldestFileInRange:
    """Test class for findLatestOrOldestFileInRange method"""

    @pytest.fixture
    def test_start_date(self):
        """Fixture for test start date - timezone naive"""
        return datetime(2023, 5, 15, 10, 0)  # 2023-05-15 10:00

    @pytest.fixture
    def test_end_date(self):
        """Fixture for test end date - timezone naive"""
        return datetime(2023, 5, 17, 14, 0)  # 2023-05-17 14:00

    @pytest.fixture
    def test_start_date_tz(self):
        """Fixture for test start date - timezone aware"""
        from datetime import timezone
        return datetime(2023, 5, 15, 10, 0, tzinfo=timezone.utc)

    @pytest.fixture
    def test_end_date_tz(self):
        """Fixture for test end date - timezone aware"""
        from datetime import timezone
        return datetime(2023, 5, 17, 14, 0, tzinfo=timezone.utc)

    @pytest.fixture
    def test_files_in_range(self):
        """Fixture for files within the date range"""
        return [
            '/test/2023/05/15/TestPrefix_202305151030.parquet',  # 2023-05-15 10:30
            '/test/2023/05/16/TestPrefix_202305161200.parquet',  # 2023-05-16 12:00
            '/test/2023/05/17/TestPrefix_202305171300.parquet',  # 2023-05-17 13:00
        ]

    @pytest.fixture
    def test_files_out_of_range(self):
        """Fixture for files outside the date range"""
        return [
            '/test/2023/05/14/TestPrefix_202305141500.parquet',  # Before range
            '/test/2023/05/18/TestPrefix_202305181000.parquet',  # After range
        ]

    @pytest.fixture
    def test_csv_files_in_range(self):
        """Fixture for CSV files within the date range"""
        return [
            '/test/2023/05/15/TestPrefix_202305151030.csv',  # 2023-05-15 10:30
            '/test/2023/05/16/TestPrefix_202305161200.csv',  # 2023-05-16 12:00
            '/test/2023/05/17/TestPrefix_202305171300.csv',  # 2023-05-17 13:00
        ]

    @pytest.fixture
    def mixed_files(self):
        """Fixture for mixed files with different patterns"""
        return [
            '/test/2023/05/15/TestPrefix_202305151030.parquet',  # Valid
            '/test/2023/05/15/WrongPrefix_202305151100.parquet',  # Wrong prefix
            '/test/2023/05/15/TestPrefix_20230515103.parquet',   # Wrong timestamp format
            '/test/2023/05/15/TestPrefix_202305151200.txt',      # Wrong extension
            '/test/2023/05/15/TestPrefix_202305151300.parquet',  # Valid
        ]

    def test_find_latest_file_success(self, blobStorageConnectorInstance, test_start_date, test_end_date, test_files_in_range):
        """Test finding the latest file in range successfully"""
        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder') as mock_get_files, \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            # Mock file listings for different days
            def mock_file_listing(path):
                if '2023/05/15' in path:
                    return ['/test/2023/05/15/TestPrefix_202305151030.parquet']
                elif '2023/05/16' in path:
                    return ['/test/2023/05/16/TestPrefix_202305161200.parquet']
                elif '2023/05/17' in path:
                    return ['/test/2023/05/17/TestPrefix_202305171300.parquet']
                return []

            mock_get_files.side_effect = mock_file_listing

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet,
                latest=True
            )

            # Should return the latest file (2023-05-17 13:00)
            assert result == ('/test/2023/05/17/TestPrefix_202305171300.parquet', None)

    def test_find_oldest_file_success(self, blobStorageConnectorInstance, test_start_date, test_end_date, test_files_in_range):
        """Test finding the oldest file in range successfully"""
        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder') as mock_get_files, \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            # Mock file listings for different days
            def mock_file_listing(path):
                if '2023/05/15' in path:
                    return ['/test/2023/05/15/TestPrefix_202305151030.parquet']
                elif '2023/05/16' in path:
                    return ['/test/2023/05/16/TestPrefix_202305161200.parquet']
                elif '2023/05/17' in path:
                    return ['/test/2023/05/17/TestPrefix_202305171300.parquet']
                return []

            mock_get_files.side_effect = mock_file_listing

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet,
                latest=False
            )

            # Should return the oldest file (2023-05-15 10:30)
            assert result == ('/test/2023/05/15/TestPrefix_202305151030.parquet', None)

    def test_find_file_csv_type(self, blobStorageConnectorInstance, test_start_date, test_end_date):
        """Test finding files with CSV file type"""
        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder') as mock_get_files:

            def mock_file_listing(path):
                if '2023/05/15' in path:
                    return ['/test/2023/05/15/TestPrefix_202305151030.csv']
                return []

            mock_get_files.side_effect = mock_file_listing

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.CSV,
                latest=True
            )

            assert result == ('/test/2023/05/15/TestPrefix_202305151030.csv', None)

    def test_invalid_date_range(self, blobStorageConnectorInstance):
        """Test with invalid date range (end_dt < start_dt)"""
        start_date = datetime(2023, 5, 17, 10, 0)
        end_date = datetime(2023, 5, 15, 10, 0)  # Earlier than start

        with pytest.raises(ValueError, match="end_dt must be >= start_dt"):
            blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=start_date,
                end_dt=end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

    def test_inconsistent_timezone_awareness(self, blobStorageConnectorInstance):
        """Test with inconsistent timezone awareness between start and end dates"""
        from datetime import timezone
        start_date = datetime(2023, 5, 15, 10, 0)  # Naive
        end_date = datetime(2023, 5, 17, 10, 0, tzinfo=timezone.utc)  # Aware

        with pytest.raises(TypeError, match="can't compare offset-naive and offset-aware datetimes"):
            blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=start_date,
                end_dt=end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

    def test_no_files_found(self, blobStorageConnectorInstance, test_start_date, test_end_date):
        """Test when no files are found in the range"""
        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=[]):

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                    rootPath='/test',
                    start_dt=test_start_date,
                    end_dt=test_end_date,
                    prefix='TestPrefix',
                    fileType=FileTypes.Parquet
                )
            assert result == (None, "No TestPrefix_YYYYMMDDHHmm.parquet files found between 2023-05-15T10:00:00 and 2023-05-17T14:00:00.")

    def test_directory_does_not_exist(self, blobStorageConnectorInstance, test_start_date, test_end_date):
        """Test when directories don't exist for the date range"""
        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=False):

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

            assert result == (None, "No TestPrefix_YYYYMMDDHHmm.parquet files found between 2023-05-15T10:00:00 and 2023-05-17T14:00:00.")

    def test_filename_pattern_filtering(self, blobStorageConnectorInstance, test_start_date, test_end_date, mixed_files):
        """Test that only files matching the exact pattern are considered"""
        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=mixed_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet,
                latest=True
            )

            # Should return the latest valid file (13:00), ignoring invalid patterns
            assert result == ('/test/2023/05/15/TestPrefix_202305151300.parquet', None)

    def test_parquet_readiness_check(self, blobStorageConnectorInstance, test_start_date, test_end_date):
        """Test that parquet files are checked for readiness"""
        test_files = ['/test/2023/05/15/TestPrefix_202305151030.parquet']

        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=test_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=False):

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

            assert result == (None, "No TestPrefix_YYYYMMDDHHmm.parquet files found between 2023-05-15T10:00:00 and 2023-05-17T14:00:00.")

    def test_non_parquet_files_skip_readiness_check(self, blobStorageConnectorInstance, test_start_date, test_end_date):
        """Test that non-parquet files skip the readiness check"""
        test_files = ['/test/2023/05/15/TestPrefix_202305151030.csv']

        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=test_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse') as mock_readiness:

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.CSV
            )

            # Should not call readiness check for CSV files
            mock_readiness.assert_not_called()
            assert result == ('/test/2023/05/15/TestPrefix_202305151030.csv', None)

    def test_timezone_aware_dates(self, blobStorageConnectorInstance, test_start_date_tz, test_end_date_tz):
        """Test with timezone-aware dates"""
        test_files = ['/test/2023/05/15/TestPrefix_202305151030.parquet']

        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=test_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date_tz,
                end_dt=test_end_date_tz,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

            assert result == ('/test/2023/05/15/TestPrefix_202305151030.parquet', None)

    def test_files_outside_time_range_filtered(self, blobStorageConnectorInstance):
        """Test that files outside the time range are filtered out"""
        # Range: 2023-05-15 10:00 to 2023-05-15 12:00
        start_date = datetime(2023, 5, 15, 10, 0)
        end_date = datetime(2023, 5, 15, 12, 0)

        test_files = [
            '/test/2023/05/15/TestPrefix_202305150900.parquet',  # Before range (9:00)
            '/test/2023/05/15/TestPrefix_202305151100.parquet',  # In range (11:00)
            '/test/2023/05/15/TestPrefix_202305151300.parquet',  # After range (13:00)
        ]

        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=test_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=start_date,
                end_dt=end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

            # Should only find the file in range (11:00)
            assert result == ('/test/2023/05/15/TestPrefix_202305151100.parquet', None)

    def test_malformed_timestamp_ignored(self, blobStorageConnectorInstance, test_start_date, test_end_date):
        """Test that files with malformed timestamps are ignored"""
        test_files = [
            '/test/2023/05/15/TestPrefix_202305151030.parquet',  # Valid
            '/test/2023/05/15/TestPrefix_202305159999.parquet',  # Invalid hour (99)
            '/test/2023/05/15/TestPrefix_202313011030.parquet',  # Invalid month (13)
        ]

        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=test_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            result = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

            # Should only find the valid file
            assert result == ('/test/2023/05/15/TestPrefix_202305151030.parquet', None)

    def test_path_construction_with_trailing_slash(self, blobStorageConnectorInstance, test_start_date, test_end_date):
        """Test that root path with trailing slash is handled correctly"""
        test_files = ['/test/2023/05/15/TestPrefix_202305151030.parquet']

        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True) as mock_exists, \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=test_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test/',  # With trailing slash
                start_dt=test_start_date,
                end_dt=test_end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet
            )

            # Should construct path correctly without double slashes
            expected_call = '/test/2023/05/17'
            mock_exists.assert_called_with(expected_call)

    def test_multiple_files_same_day_latest_selected(self, blobStorageConnectorInstance):
        """Test that when multiple files exist on the same day, the correct one is selected"""
        start_date = datetime(2023, 5, 15, 8, 0)
        end_date = datetime(2023, 5, 15, 16, 0)

        test_files = [
            '/test/2023/05/15/TestPrefix_202305151000.parquet',  # 10:00
            '/test/2023/05/15/TestPrefix_202305151200.parquet',  # 12:00 - Latest
            '/test/2023/05/15/TestPrefix_202305151400.parquet',  # 14:00
        ]

        with patch.object(blobStorageConnectorInstance, 'existsPath', return_value=True), \
             patch.object(blobStorageConnectorInstance, 'getListOfFilesUnder', return_value=test_files), \
             patch.object(blobStorageConnectorInstance, 'isParquetReadyForUse', return_value=True):

            # Test latest
            result_latest = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=start_date,
                end_dt=end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet,
                latest=True
            )

            # Test oldest
            result_oldest = blobStorageConnectorInstance.findLatestOrOldestFileInRange(
                rootPath='/test',
                start_dt=start_date,
                end_dt=end_date,
                prefix='TestPrefix',
                fileType=FileTypes.Parquet,
                latest=False
            )

            assert result_latest == ('/test/2023/05/15/TestPrefix_202305151400.parquet', None)
            assert result_oldest == ('/test/2023/05/15/TestPrefix_202305151000.parquet', None)