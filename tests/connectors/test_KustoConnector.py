import pytest
import unittest.mock as mock
from melius.connectors.KustoConnector import KustoConnector
from melius.libraries.Constants import FileTypes, TableState
from melius.libraries.Utility import Utils


@pytest.fixture(scope="class")
def kustoConnectorInstance():
    """
    Return KustoConnector object
    """
    Utils.setSparkSession(mock.Mock())
    config = {
        KustoConnector.Cluster: "mycluster123",
        KustoConnector.Database: "mydatabase123"
    }
    return KustoConnector(config)


@pytest.fixture
def kustoConnectorInstanceWithDates(kustoConnectorInstance, startDate, endDate):
    kustoConnectorInstance._startDt = startDate
    kustoConnectorInstance._endDt = endDate
    return kustoConnectorInstance


@pytest.fixture
def datasetFiles():
    return ['RICfrLoad.api.common']


@pytest.fixture
def datasetName():
    return "ClustersWithCapacitySummary/common"


@pytest.fixture(params=[TableState.ExtentExists, TableState.ExtentNotExists, TableState.TableNotExists])
def tableState(request):
    return request.param


@pytest.fixture(params=['', 'testcol'])
def extendColumn(request):
    return request.param


@pytest.fixture(params=[True, False])
def overWrite(request):
    return request.param


@pytest.fixture
def tableName():
    return "test-table"


@pytest.fixture
def tag():
    return "test-tag"


def test_KustoConnectorInstanceCreated(kustoConnectorInstance):
    assert isinstance(kustoConnectorInstance, KustoConnector)


def test__updateKustoColumns(kustoConnectorInstance, tableName, mocker):
    expectedQuery = f'{tableName} | limit 0'
    expectedCommand = '.alter-merge table test-table (col3:string)'

    kustoDf = mock.Mock()
    kustoDf.columns = ["col1","col2"]
    df = mock.Mock()
    df.columns = ["col1", "col2", "col3"]
    stringType = mock.Mock()
    stringType.dataType = 'StringType'
    df.schema = {
        "col1": stringType,
        "col2": stringType,
        "col3": stringType
    }

    # mock executeQuery
    mockedExecuteQuery = mocker.patch.object(kustoConnectorInstance, 'loadDataFrameViaKustoClient')
    mockedExecuteQuery.return_value = kustoDf

    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')

    # actual call to the function under test
    kustoConnectorInstance._updateKustoColumns(df, tableName)

    # assertions
    mockedExecuteQuery.assert_called_with(expectedQuery)
    mockKustoApiCall.assert_called_with(expectedCommand)


def test__dropExtentFromTable(kustoConnectorInstance, tableName, tag, mocker):
    expectedCommand = f".drop extents <|.show table {tableName} extents where tags has '{tag}'"
    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')
    kustoConnectorInstance._dropExtentFromTable(tableName, tag)
    mockKustoApiCall.assert_called_with(expectedCommand)


def test__checkCycleByExtent(kustoConnectorInstance, tableName, tag, mocker):
    expectedCommand = f".show table {tableName} extents where tags has '{tag}'"

    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')
    mockKustoApiCall.return_value = {'Tables': [{'Rows': [1,2,3,4,5]}]}
    actualTableState = kustoConnectorInstance._checkCycleByExtent(tableName, tag)

    # assertions
    assert actualTableState == TableState.ExtentExists
    mockKustoApiCall.assert_called_with(expectedCommand)


def test__checkCycleByExtentWithNoRows(kustoConnectorInstance, tableName, tag, mocker):
    expectedCommand = f".show table {tableName} extents where tags has '{tag}'"

    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')
    mockKustoApiCall.return_value = {'Tables': [{'Rows': []}]}
    actualTableState = kustoConnectorInstance._checkCycleByExtent(tableName, tag)

    # assertions
    assert actualTableState == TableState.ExtentNotExists
    mockKustoApiCall.assert_called_with(expectedCommand)


def test__checkCycleByExtentWithEntityNotFoundException(kustoConnectorInstance, tableName, tag, mocker):
    expectedCommand = f".show table {tableName} extents where tags has '{tag}'"

    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')
    mockKustoApiCall.side_effect = Exception('BadRequest_EntityNotFound')
    actualTableState = kustoConnectorInstance._checkCycleByExtent(tableName, tag)

    # assertions
    assert actualTableState == TableState.TableNotExists
    mockKustoApiCall.assert_called_with(expectedCommand)


def test__checkCycleByExtentRaiseException(kustoConnectorInstance, tableName, tag, mocker):

    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')
    mockKustoApiCall.side_effect = Exception('Test_Exception')

    # assertions
    with pytest.raises(Exception):
        kustoConnectorInstance._checkCycleByExtent(tableName, tag)


def test__moveFromTempToMaster(kustoConnectorInstance, tableName, mocker):
    tempTableName = "temp-test-table"
    expectedCommand = f".move extents all from table {tempTableName} to table {tableName}"

    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')
    kustoConnectorInstance._moveFromTempToMaster(tempTableName, tableName)

    # assertions
    mockKustoApiCall.assert_called_with(expectedCommand)


def test__alterTag(kustoConnectorInstance, tableName, tag, mocker):
    expectedCommand = f".alter extent tags ('{tag}') <|.show table {tableName} extents"

    # mock kustoApiCall
    mockKustoApiCall = mocker.patch.object(kustoConnectorInstance._kustApiClient, 'postManagementApi')
    kustoConnectorInstance._alterTag(tableName, tag)

    # assertions
    mockKustoApiCall.assert_called_with(expectedCommand)


def test_getDefaultFileType(kustoConnectorInstance):
    assert kustoConnectorInstance.getDefaultFileType() == FileTypes.Unknown


@mock.patch('melius.connectors.KustoConnector.DatabricksHelper')
@mock.patch('melius.libraries.Utility.Utils')
@mock.patch('melius.libraries.Logger.Logger.getInstance')
def test_publishDataframeViaConnector_default_parameters(mock_logger, mock_utils, mock_databricks_helper, kustoConnectorInstance, mocker):
    """Test publishDataframeViaConnector with default parameters"""
    # Mock AccessToken to avoid authentication issues
    mock_access_token = mocker.patch.object(kustoConnectorInstance, '_accessToken')
    mock_access_token.getAccessToken.return_value = "fake_token"

    # Mock DataFrame
    mock_df = mock.Mock()

    # Mock schema validation
    mock_utils.validateSchema.return_value = True

    # Mock DatabricksHelper optimization
    optimized_df = mock.Mock()
    mock_databricks_helper.optimizeDataFrameForKustoWrite.return_value = optimized_df

    # Mock dataframe write operation
    mock_write = mock.Mock()
    optimized_df.write = mock_write
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.option.return_value = mock_write

    # Call the method
    kustoConnectorInstance.publishDataframeViaConnector(mock_df, "test_table")

    # Verify DatabricksHelper was called with default target_size_mb=500
    mock_databricks_helper.optimizeDataFrameForKustoWrite.assert_called_once_with(mock_df, target_size_mb=500)

    # Verify write operations
    mock_write.format.assert_called_with("com.microsoft.kusto.spark.datasource")
    mock_write.mode.assert_called_with("append")


@mock.patch('melius.connectors.KustoConnector.DatabricksHelper')
@mock.patch('melius.libraries.Utility.Utils')
@mock.patch('melius.libraries.Logger.Logger.getInstance')
def test_publishDataframeViaConnector_custom_target_size(mock_logger, mock_utils, mock_databricks_helper, kustoConnectorInstance, mocker):
    """Test publishDataframeViaConnector with custom target_size_mb parameter"""
    # Mock AccessToken to avoid authentication issues
    mock_access_token = mocker.patch.object(kustoConnectorInstance, '_accessToken')
    mock_access_token.getAccessToken.return_value = "fake_token"

    # Mock DataFrame
    mock_df = mock.Mock()

    # Mock schema validation
    mock_utils.validateSchema.return_value = True

    # Mock DatabricksHelper optimization
    optimized_df = mock.Mock()
    mock_databricks_helper.optimizeDataFrameForKustoWrite.return_value = optimized_df

    # Mock dataframe write operation
    mock_write = mock.Mock()
    optimized_df.write = mock_write
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.option.return_value = mock_write

    # Call the method with custom target_size_mb
    kustoConnectorInstance.publishDataframeViaConnector(mock_df, "test_table", target_size_mb=1000)

    # Verify DatabricksHelper was called with custom target_size_mb=1000
    mock_databricks_helper.optimizeDataFrameForKustoWrite.assert_called_once_with(mock_df, target_size_mb=1000)


@mock.patch('melius.connectors.KustoConnector.DatabricksHelper')
@mock.patch('melius.libraries.Logger.Logger.getInstance')
def test_publishDataframeViaConnector_with_schema_validation(mock_logger, mock_databricks_helper, kustoConnectorInstance, mocker):
    """Test publishDataframeViaConnector with schema validation"""
    from pyspark.sql.types import StructType, StructField, StringType

    # Mock AccessToken to avoid authentication issues
    mock_access_token = mocker.patch.object(kustoConnectorInstance, '_accessToken')
    mock_access_token.getAccessToken.return_value = "fake_token"

    # Mock DataFrame
    mock_df = mock.Mock()

    # Mock schema
    test_schema = StructType([StructField("col1", StringType(), True)])

    # Mock DatabricksHelper optimization
    optimized_df = mock.Mock()
    mock_databricks_helper.optimizeDataFrameForKustoWrite.return_value = optimized_df

    # Mock dataframe write operation
    mock_write = mock.Mock()
    optimized_df.write = mock_write
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.option.return_value = mock_write

    # Mock schema validation to return True
    mock_validate = mocker.patch('melius.connectors.KustoConnector.Utils.validateSchema', return_value=True)

    # Call the method with schema
    kustoConnectorInstance.publishDataframeViaConnector(mock_df, "test_table", schema=test_schema)

    # Verify schema validation was called
    mock_validate.assert_called_once_with(mock_df, test_schema)


@mock.patch('melius.connectors.KustoConnector.DatabricksHelper')
@mock.patch('melius.libraries.Logger.Logger.getInstance')
def test_publishDataframeViaConnector_schema_validation_fails(mock_logger, mock_databricks_helper, kustoConnectorInstance, mocker):
    """Test publishDataframeViaConnector when schema validation fails"""
    from pyspark.sql.types import StructType, StructField, StringType

    # Mock AccessToken to avoid authentication issues
    mock_access_token = mocker.patch.object(kustoConnectorInstance, '_accessToken')
    mock_access_token.getAccessToken.return_value = "fake_token"

    # Mock DataFrame
    mock_df = mock.Mock()

    # Mock schema
    test_schema = StructType([StructField("col1", StringType(), True)])

    # Mock schema validation to return False
    mock_validate = mocker.patch('melius.connectors.KustoConnector.Utils.validateSchema', return_value=False)

    # Call the method with schema and expect ValueError
    with pytest.raises(ValueError, match="The provided dataframe does not match the expected schema for table test_table"):
        kustoConnectorInstance.publishDataframeViaConnector(mock_df, "test_table", schema=test_schema)

    # Verify schema validation was called
    mock_validate.assert_called_once_with(mock_df, test_schema)

    # Verify optimization was not called since validation failed
    mock_databricks_helper.optimizeDataFrameForKustoWrite.assert_not_called()


# ============================================================
# Config-driven FIC/AppId auth routing tests
# ============================================================
class TestKustoConnectorConfigDrivenAuth:
    """Tests for _getKustoClientStringBuilder config-driven routing."""

    @pytest.fixture
    def fic_kusto_connector(self):
        """KustoConnector with accessConfig.accessType=FIC."""
        Utils.setSparkSession(mock.Mock())
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
            "accessConfig": {
                "accessType": "FIC",
                "tenantId": "fic-tenant",
                "clientId": "fic-client",
            }
        }
        return KustoConnector(config)

    @pytest.fixture
    def appid_kusto_connector(self):
        """KustoConnector with accessConfig.accessType=AppId."""
        Utils.setSparkSession(mock.Mock())
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
            "accessConfig": {
                "accessType": "AppId",
                "tenantId": "appid-tenant",
                "clientId": "appid-client",
                "scope": "myscope",
                "cert": "mycert",
            }
        }
        return KustoConnector(config)

    @pytest.fixture
    def msi_kusto_connector(self):
        """KustoConnector with accessConfig.accessType=MSI (should fallback to global auth)."""
        Utils.setSparkSession(mock.Mock())
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
            "accessConfig": {
                "accessType": "MSI",
                "clientId": "msi-client",
            }
        }
        return KustoConnector(config)

    @pytest.fixture
    def no_accessconfig_connector(self):
        """KustoConnector without accessConfig (pure legacy fallback)."""
        Utils.setSparkSession(mock.Mock())
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
        }
        return KustoConnector(config)

    def test_fic_config_uses_token_credential(self, fic_kusto_connector, mocker):
        """accessType=FIC routes to with_azure_token_credential."""
        fake_cred = mock.Mock(name="fic_credential")
        mocker.patch.object(fic_kusto_connector._accessToken, 'getCredential', return_value=fake_cred)
        mock_kcsb = mocker.patch('melius.connectors.KustoConnector.KustoConnectionStringBuilder')

        fic_kusto_connector._getKustoClientStringBuilder()

        mock_kcsb.with_azure_token_credential.assert_called_once_with(
            "https://mycluster.kusto.windows.net/",
            fake_cred
        )
        mock_kcsb.with_aad_managed_service_identity_authentication.assert_not_called()

    def test_fic_config_for_ingestion(self, fic_kusto_connector, mocker):
        """accessType=FIC with forIngestion=True uses ingest- URL."""
        fake_cred = mock.Mock(name="fic_credential")
        mocker.patch.object(fic_kusto_connector._accessToken, 'getCredential', return_value=fake_cred)
        mock_kcsb = mocker.patch('melius.connectors.KustoConnector.KustoConnectionStringBuilder')

        fic_kusto_connector._getKustoClientStringBuilder(forIngestion=True)

        mock_kcsb.with_azure_token_credential.assert_called_once_with(
            "https://ingest-mycluster.kusto.windows.net/",
            fake_cred
        )

    def test_appid_config_uses_token_credential(self, appid_kusto_connector, mocker):
        """accessType=AppId routes to with_azure_token_credential."""
        fake_cred = mock.Mock(name="appid_credential")
        mocker.patch.object(appid_kusto_connector._accessToken, 'getCredential', return_value=fake_cred)
        mock_kcsb = mocker.patch('melius.connectors.KustoConnector.KustoConnectionStringBuilder')

        appid_kusto_connector._getKustoClientStringBuilder()

        mock_kcsb.with_azure_token_credential.assert_called_once_with(
            "https://mycluster.kusto.windows.net/",
            fake_cred
        )

    def test_msi_config_falls_back_to_global_auth_mode(self, msi_kusto_connector, mocker):
        """accessType=MSI does NOT match FIC/AppId, so falls through to global auth mode."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.ManagedIdentiy)

        mock_kcsb = mocker.patch('melius.connectors.KustoConnector.KustoConnectionStringBuilder')

        msi_kusto_connector._getKustoClientStringBuilder()

        mock_kcsb.with_aad_managed_service_identity_authentication.assert_called_once_with(
            "https://mycluster.kusto.windows.net/"
        )
        mock_kcsb.with_azure_token_credential.assert_not_called()

    def test_no_accessconfig_uses_global_msi(self, no_accessconfig_connector, mocker):
        """No accessConfig at all falls through to global MSI auth mode."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.ManagedIdentiy)

        mock_kcsb = mocker.patch('melius.connectors.KustoConnector.KustoConnectionStringBuilder')

        no_accessconfig_connector._getKustoClientStringBuilder()

        mock_kcsb.with_aad_managed_service_identity_authentication.assert_called_once()

    def test_no_accessconfig_global_sni_uses_token_credential(self, no_accessconfig_connector, mocker):
        """No accessConfig but global Sni mode -> with_azure_token_credential (legacy compat)."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.Sni)

        fake_cred = mock.Mock(name="legacy_sni_credential")
        mocker.patch.object(no_accessconfig_connector._accessToken, 'getCredential', return_value=fake_cred)
        mock_kcsb = mocker.patch('melius.connectors.KustoConnector.KustoConnectionStringBuilder')

        no_accessconfig_connector._getKustoClientStringBuilder()

        mock_kcsb.with_azure_token_credential.assert_called_once_with(
            "https://mycluster.kusto.windows.net/",
            fake_cred
        )


@mock.patch('melius.connectors.KustoConnector.DatabricksHelper')
@mock.patch('melius.libraries.Utility.Utils')
@mock.patch('melius.libraries.Logger.Logger.getInstance')
def test_publishDataframeViaConnector_different_write_modes(mock_logger, mock_utils, mock_databricks_helper, kustoConnectorInstance, mocker):
    """Test publishDataframeViaConnector with different write modes"""
    # Mock AccessToken to avoid authentication issues
    mock_access_token = mocker.patch.object(kustoConnectorInstance, '_accessToken')
    mock_access_token.getAccessToken.return_value = "fake_token"

    # Mock DataFrame
    mock_df = mock.Mock()

    # Mock schema validation
    mock_utils.validateSchema.return_value = True

    # Mock DatabricksHelper optimization
    optimized_df = mock.Mock()
    mock_databricks_helper.optimizeDataFrameForKustoWrite.return_value = optimized_df

    # Mock dataframe write operation
    mock_write = mock.Mock()
    optimized_df.write = mock_write
    mock_write.format.return_value = mock_write
    mock_write.mode.return_value = mock_write
    mock_write.option.return_value = mock_write

    # Test transactional write mode
    mock_write.reset_mock()
    kustoConnectorInstance.publishDataframeViaConnector(mock_df, "test_table", writeMode="Transactional")

    # Verify transactional mode sets specific options
    mock_write.option.assert_any_call("writeMode", "Transactional")
    mock_write.option.assert_any_call("writeEnableAsync", False)
    mock_write.option.assert_any_call("pollingOnDriver", True)
    mock_write.option.assert_any_call("clientBatchingLimit", 1024)

    # Test queued write mode
    mock_write.reset_mock()
    kustoConnectorInstance.publishDataframeViaConnector(mock_df, "test_table", writeMode="Queued")

    # Verify queued mode sets specific options
    mock_write.option.assert_any_call("writeMode", "Queued")
    mock_write.option.assert_any_call("writeEnableAsync", False)
    mock_write.option.assert_any_call("pollingOnDriver", True)
    mock_write.option.assert_any_call("clientBatchingLimit", 1024)


# ============================================================
# Tests for accessConfig pass-through to KustoApiClient
# ============================================================
class TestKustoConnectorApiClientConfigPassthrough:
    """Verify KustoConnector passes accessConfig to KustoApiClient."""

    def test_accessconfig_passed_to_kusto_api_client(self, mocker):
        """When accessConfig is present, it is forwarded to KustoApiClient."""
        Utils.setSparkSession(mock.Mock())
        mock_api = mocker.patch('melius.connectors.KustoConnector.KustoApiClient')
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
            "accessConfig": {
                "accessType": "FIC",
                "tenantId": "t1",
                "clientId": "c1",
            }
        }
        KustoConnector(config)
        mock_api.assert_called_once_with(
            "https://mycluster.kusto.windows.net/",
            "mydb",
            config=config["accessConfig"],
        )

    def test_no_accessconfig_passes_none_to_kusto_api_client(self, mocker):
        """When accessConfig is absent, config=None is passed to KustoApiClient."""
        Utils.setSparkSession(mock.Mock())
        mock_api = mocker.patch('melius.connectors.KustoConnector.KustoApiClient')
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
        }
        KustoConnector(config)
        mock_api.assert_called_once_with(
            "https://mycluster.kusto.windows.net/",
            "mydb",
            config=None,
        )


# ============================================================
# Tests for customizedSinkBlobAccessToken in publishDataframeViaConnector
# ============================================================
class TestCustomizedSinkBlobAccessToken:
    """Verify customizedSinkBlobAccessToken is set during non-interactive write."""

    @mock.patch('melius.connectors.KustoConnector.DatabricksHelper')
    @mock.patch('melius.libraries.Utility.Utils')
    @mock.patch('melius.libraries.Logger.Logger.getInstance')
    def test_customized_sink_blob_token_set(self, mock_logger, mock_utils, mock_dbhelper):
        """In non-interactive mode, customizedSinkBlobAccessToken should be set to the same token."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.ManagedIdentiy)

        Utils.setSparkSession(mock.Mock())
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
        }
        connector = KustoConnector(config)

        mock_df = mock.Mock()
        mock_utils.validateSchema.return_value = True
        optimized_df = mock.Mock()
        mock_dbhelper.optimizeDataFrameForKustoWrite.return_value = optimized_df

        mock_write = mock.Mock()
        optimized_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write

        # Mock access token
        connector._accessToken = mock.Mock()
        connector._accessToken.getAccessToken.return_value = "my-fake-token"

        connector.publishDataframeViaConnector(mock_df, "test_table")

        # Verify both accessToken and customizedSinkBlobAccessToken are set
        mock_write.option.assert_any_call("accessToken", "my-fake-token")
        mock_write.option.assert_any_call("customizedSinkBlobAccessToken", "my-fake-token")

    @mock.patch('melius.connectors.KustoConnector.DatabricksHelper')
    @mock.patch('melius.libraries.Utility.Utils')
    @mock.patch('melius.libraries.Logger.Logger.getInstance')
    def test_interactive_mode_does_not_set_sink_blob_token(self, mock_logger, mock_utils, mock_dbhelper):
        """In interactive (DeviceCode) mode, customizedSinkBlobAccessToken should NOT be set."""
        from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
        BaseConnector.setAuthMode(ConnectorAuthModes.InterActive)

        Utils.setSparkSession(mock.Mock())
        config = {
            KustoConnector.Cluster: "mycluster",
            KustoConnector.Database: "mydb",
        }
        connector = KustoConnector(config)

        mock_df = mock.Mock()
        mock_utils.validateSchema.return_value = True
        optimized_df = mock.Mock()
        mock_dbhelper.optimizeDataFrameForKustoWrite.return_value = optimized_df

        mock_write = mock.Mock()
        optimized_df.write = mock_write
        mock_write.format.return_value = mock_write
        mock_write.mode.return_value = mock_write
        mock_write.option.return_value = mock_write

        connector.publishDataframeViaConnector(mock_df, "test_table")

        # Verify DeviceCode auth is set
        mock_write.option.assert_any_call("auth", "DeviceCode")
        # Verify customizedSinkBlobAccessToken is NOT set
        for call_args in mock_write.option.call_args_list:
            assert call_args[0][0] != "customizedSinkBlobAccessToken", \
                "customizedSinkBlobAccessToken should not be set in interactive mode"
