import csv
from datetime import datetime
import json
from melius.libraries.Utility import Utils
from melius.libraries.Logger import Logger
from melius.connectors.BaseConnector import BaseConnector, ConnectorAuthModes
from melius.restapis.KustoApiClient import KustoApiClient
from melius.libraries.Constants import TableState, KnownStrings
from melius.libraries.AccessToken import AccessToken
from melius.libraries.DatabricksHelper import DatabricksHelper

from typing import Dict, Any
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.data_format import DataFormat
from azure.kusto.ingest import (
    IngestionProperties,
    QueuedIngestClient,
    ReportLevel
)
from azure.kusto.data.helpers import dataframe_from_result_table
import io
from pyspark.sql.types import StructType


class KustoConnector(BaseConnector):
    ''' Connector to Kustor based on spark connector for Azure Data Explorer
    you can find more info on https://github.com/Azure/azure-kusto-spark/blob/master/samples/src/main/python/pyKusto.py
    '''
    ''' static members '''
    # name of format for Kusto
    FORMAT_STRING = "com.microsoft.kusto.spark.datasource"
    Cluster = "kustoCluster"
    Database = "kustoDatabase"
    Table = "kustoTable"
    Query = "kustoQuery"
    TableCreation = "tableCreateOptions"
    IngestionPropertyJson = "sparkIngestionPropertiesJson"
    WriteEnableAsync = "writeEnableAsync"
    ClientBatchingLimit = "clientBatchingLimit"
    PollingOnDriver = "pollingOnDriver"
    WriteMode = "writeMode"
    IngestionTimeMoniker = KnownStrings.IngestionTimeMoniker

    # Mapping: https://github.com/Azure/azure-kusto-spark/blob/master/docs/Spark-Kusto%20DataTypes%20mapping.md
    _sparkToKustoTypeMapping = {
        'StringType': 'string',
        'IntegerType': 'int',
        'LongType': 'long',
        'BooleanType': 'bool',
        'ShortType': 'int',
        'DoubleType': 'real',
        'ByteType': 'int',
        'FloatType': 'real',
        'DecimalType': 'decimal',
        'TimestampType': 'datetime',
        'DateType': 'datetime',
        'StructType': 'dynamic',
        'MapType': 'dynamic',
        'ArrayType': 'dynamic'
    }

    _existingConnectors: Dict[str, Any] = {}

    @classmethod
    def getConnector(cls, config) -> Any:
        name = cls.getConnectorName(config)

        if name not in cls._existingConnectors:
            Logger.getInstance().writeInfo("create a new instance of KustoConnector for: " + name)
            cls._existingConnectors[name] = KustoConnector(config)
        else:
            Logger.getInstance().writeInfo("returning an existing instance of KustoConnector for: " + name)

        return cls._existingConnectors[name]

    @classmethod
    def getConnectorName(cls, config) -> str:
        ''' Get the name of the connector based on its configuration.
            This is used to uniquely identify the connector instance.
        '''
        if "Name" in config and config["Name"]:
            return config["Name"]
        else:
            return f"{config[KustoConnector.Cluster].lower()}.{config[KustoConnector.Database]}"

    @classmethod
    def clearConnectors(cls):
        '''
        Clear all existing connectors.
        '''
        cls._existingConnectors.clear()
        Logger.getInstance().writeInfo("Cleared all existing BlobStorageConnectors.")

    ''' class members '''
    def __init__(self, config):
        ''' constructor
            config: Kusto connection option
        '''
        self._config = config
        self._kustoCluster = config[KustoConnector.Cluster]
        self._kustoDatabase = config[KustoConnector.Database]
        self._kustApiClient = KustoApiClient(self._getKustoClusterUrl(), self._kustoDatabase, config=self._config.get(KnownStrings.AccessConfig))
        self._pyKusto = Utils.getSparkSession().builder.appName("kustoPySpark").getOrCreate()
        self._accessToken = AccessToken(scope=f"https://{self._kustoCluster}.kusto.windows.net/.default", config=self._config.get(KnownStrings.AccessConfig))
        self._startDt = None
        self._endDt = None

    def getLastIngestionDateTime(self, tableName: str=None) -> datetime:  # pragma: no cover
        ''' Get the last ingestion date time from the Kusto table '''

        if tableName is None or tableName == "":
            tableName = self._config[KustoConnector.Table]

        ingestionTimeMoniker = self._config.get(KustoConnector.IngestionTimeMoniker, KnownStrings.IngestionTimeMoniker)

        Logger.getInstance().writeInfo(f'Getting the last ingestion date time from the {tableName} table with IngestionTimeMoniker {ingestionTimeMoniker}')

        query = f"""
        {tableName}
        """

        return self.getLastIngestionDateTimeFromQuery(query)

    def getLastIngestionDateTimeFromQuery(self, query: str) -> datetime:  # pragma: no cover
        ''' Get the last ingestion date time from the Kusto query '''

        if query is None or query == "":
            raise ValueError("Query cannot be None or empty.")

        ingestionTimeMoniker = self._config.get(KustoConnector.IngestionTimeMoniker, KnownStrings.IngestionTimeMoniker)

        Logger.getInstance().writeInfo(f'Getting the last ingestion date time from the {query}')

        query = f"""
        {query} | summarize max({ingestionTimeMoniker})
        """
        return self.getFirstValueFromQuery(query)

    def getFirstValueFromQuery(self, query: str) -> Any:  # pragma: no cover
        ''' Get the First value from the Kusto query '''

        if query is None or query == "":
            raise ValueError("Query cannot be None or empty.")

        df = self.loadDataFrameViaKustoClient(query)

        if df is None or df.rdd.isEmpty():
            Logger.getInstance().writeInfo("No data returned from Kusto query.")
            return None

        Logger.getInstance().writeInfo(f'first row first column: {df.first()[0]}')
        return df.first()[0]

    def getFirstIngestionDateTime(self, tableName: str=None) -> datetime:  # pragma: no cover
        ''' Get the last ingestion date time from the Kusto table '''

        ingestionTimeMoniker = self._config.get(KustoConnector.IngestionTimeMoniker, KnownStrings.IngestionTimeMoniker)

        Logger.getInstance().writeInfo(f'Getting the First ingestion date time from the {tableName} table with IngestionTimeMoniker {ingestionTimeMoniker}')

        query = f"""
        {tableName}
        | summarize min({ingestionTimeMoniker})
        """

        df = self.loadDataFrameViaKustoClient(query)

        if df is None or df.rdd.isEmpty():
            Logger.getInstance().writeInfo("No data returned from Kusto query.")
            return None

        Logger.getInstance().writeInfo(f'First ingestion date time: {df.first()[0]}')
        return df.first()[0]

    def hasData(self, tableName: str=None) -> datetime:  # pragma: no cover
        ''' check whether the Kusto table has data or not '''

        if tableName is None or tableName == "":
            tableName = self._config[KustoConnector.Table]

        ingestionTimeMoniker = self._config.get(KustoConnector.IngestionTimeMoniker, KnownStrings.IngestionTimeMoniker)

        Logger.getInstance().writeInfo(f'Check whether {tableName} table has data or not using IngestionTimeMoniker {ingestionTimeMoniker}')

        return self.getLastIngestionDateTime(tableName) is not None

    def executeCommand(self, commandToExecute):
        ''' Execute command in the current connection context '''
        Logger.getInstance().writeInfo(f'Executing a Kusto Command: {commandToExecute}')
        return self._kustApiClient.postManagementApi(commandToExecute)

    def loadDataFrameViaKustoClient(self, query):
        ''' Get a dataframe from Kusto using the KustoClient '''
        Logger.getInstance().writeInfo(f'Executing Kusto query: {query}')
        kcsb = self._getKustoClientStringBuilder()
        client = KustoClient(kcsb)
        response = client.execute(self._kustoDatabase, query)
        df = Utils.getSparkSession().createDataFrame(dataframe_from_result_table(response.primary_results[0]))

        if df is None or df.rdd.isEmpty():
            Logger.getInstance().writeInfo("No data returned from Kusto query.")

        return df

    def loadJsonViaKustoApi(self, query):
        ''' Get a dataframe from Kusto using the KustoClient '''
        Logger.getInstance().writeInfo(f'Executing Kusto query via API: {query}')
        return self._kustApiClient.postQueryApi(query)

    def publishDataframeViaQueedIngest(self, dataFrame, tableName: str, schema: StructType = None):
        '''
        Publish data frame to a Kusto Table via QueuedIngestClient
        this sample was used. https://github.com/Azure/azure-kusto-python/blob/master/azure-kusto-ingest/tests/sample.py

        Arguments:
            dataFrame {dataframe} -- dataframe to publish
        '''
        if schema and not Utils.validateSchema(dataFrame, schema):
            raise ValueError(f"The provided dataframe does not match the expected schema for table {tableName}.")

        kcsb = self._getKustoClientStringBuilder()
        # Define your Kusto ingest URI (update with your cluster details)
        ingest_client = QueuedIngestClient(kcsb)

        # Configure the ingestion properties
        ingestion_props = IngestionProperties(
            database=self._kustoDatabase,  # Replace with your database name
            table=tableName,               # Replace with your table name
            data_format=DataFormat.CSV,    # Ensure your format matches the DataFrame conversion
            report_level=ReportLevel.FailuresAndSuccesses
        )

        # Convert the DataFrame to CSV using an in-memory buffer
        csv_buffer = io.StringIO()
        dataFrame.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)  # Reset the buffer's position to the beginning

        # Ingest the CSV data from the in-memory stream
        ingest_client.ingest_from_stream(csv_buffer, ingestion_properties=ingestion_props)
        Logger.getInstance().writeInfo(f'Published data to the {tableName} table via QueuedIngestClient.')

    def publishSingleRecordViaQueuedIngest(self, record, tableName: str = None):
        '''
        Publish a single record to Kusto using the QueuedIngestClient
        '''
        kcsb = self._getKustoClientStringBuilder(forIngestion=True)
        ingest_client = QueuedIngestClient(kcsb)
        tableName = self._config[KustoConnector.Table] if tableName is None else tableName

        # Configure the ingestion properties
        ingestion_props = IngestionProperties(
            database=self._kustoDatabase,
            table=tableName,
            data_format=DataFormat.CSV,
            report_level=ReportLevel.FailuresAndSuccesses,
            additional_properties={"ignoreFirstRecord": "true"},
        )

        # Convert the record to CSV using an in-memory buffer
        csv_buffer = io.StringIO()
        csv_writer = csv.writer(csv_buffer)
        csv_writer.writerow(record.keys())
        csv_writer.writerow(record.values())
        csv_buffer.seek(0)

        # Ingest the CSV data from the in-memory stream
        ingest_client.ingest_from_stream(csv_buffer, ingestion_properties=ingestion_props)

    def loadDataFrameViaConnector(self, query, forceDistributed: bool = False, parallelism: int = 4):
        '''
        Get a dataframe from Kusto using KustoConnector
        Arguments:
            query {str} -- Kusto query to execute
            forceDistributed {bool} -- whether to force distributed read mode, default is False
            parallelism {int} -- number of parallel tasks to use for distributed read, default is 4
        '''

        Logger.getInstance().writeInfo(f'Executing Kusto query:\n{query}')

        obj = self._pyKusto.read.format("com.microsoft.kusto.spark.datasource")

        if (BaseConnector.getAuthMode() == ConnectorAuthModes.InterActive):
            Logger.getInstance().writeInfo('Device code authentication is used for Kusto query execution.')
            obj = obj.option("auth", "DeviceCode")
        else:
            Logger.getInstance().writeInfo('Executing Kusto query with access token authentication.')
            obj = obj.option("accessToken", self._accessToken.getAccessToken())

        if forceDistributed:
            Logger.getInstance().writeInfo('Force distributed read mode is enabled for Kusto query execution.')
            obj = obj.option("readMode", "ForceDistributedMode")

        obj = obj \
            .option(KustoConnector.Cluster, self._kustoCluster) \
            .option(KustoConnector.Database, self._kustoDatabase) \
            .option(KustoConnector.Query, query) \
            .option("queryFilterPushDown", "true")  # Enable filter pushdown for better performance

        return obj.load()

    def publishDataframeViaConnector(self, dataFrame, tableName: str,
                                     tableCreation: str = 'CreateIfNotExist',
                                     writeMode: str = "Queued",
                                     schema: StructType = None,
                                     target_size_mb: int = 500,
                                     ingestionProperties: Dict[str, Any] = {},
                                     debug: bool = False):
        '''
        Publish data frame to a Kusto Table via KustoConnector
        This method will also update the Kusto table schema if necessary.

        Arguments:
            dataFrame {dataframe} -- dataframe to publish

            tableName {str} -- name of the Kusto table to publish data to

            tableCreation {str} -- table creation option, default is 'CreateIfNotExist' ( can be 'CreateOrReplace' or 'CreateIfNotExist' )

            writeMode {str} -- write mode option, default is "Queued" ( can be "Queued" or "Transactional" or "KustoStreaming" )

            schema {StructType} -- expected schema of the dataframe, default is None

            target_size_mb {int} -- target size in MB for optimizing dataframe partitions to upload data to Kusto more effectively, default is 500 MB (100 MB to 1 GB is recommended)

            ingestionProperties {Dict[str, Any]} -- additional ingestion properties to include in the write operation, default is empty dict

            debug {bool} -- whether to enable debug mode, default is False
        '''
        Logger.getInstance().writeInfo(f'Publishing data to the {tableName} table in {writeMode} mode with table creation option {tableCreation}.')

        if schema and not Utils.validateSchema(dataFrame, schema):
            raise ValueError(f"The provided dataframe does not match the expected schema for table {tableName}.")

        dataFrame = DatabricksHelper.optimizeDataFrameForKustoWrite(dataFrame, target_size_mb=target_size_mb)

        # writeEnableAsync: When writing to Kusto, allows the driver to complete operation asynchronously.
        # clientBatchingLimit: A limit indicating the size in MB of the aggregated data before ingested to Kusto, default is '300'. Note that this is done for each partition. For production purposes we suggest to set the value to '1024'
        # pollingOnDriver: When set to true, the driver will poll for the ingestion status and return it to the user. Default is false.
        obj = dataFrame.write \
            .format(KustoConnector.FORMAT_STRING) \
            .option(KustoConnector.Cluster, self._kustoCluster) \
            .option(KustoConnector.Database, self._kustoDatabase) \
            .option(KustoConnector.TableCreation, tableCreation) \
            .option(KustoConnector.Table, tableName) \
            .option(KustoConnector.ClientBatchingLimit, 1024) \
            .option(KustoConnector.WriteMode, writeMode) \
            .option(KustoConnector.IngestionPropertyJson, json.dumps(ingestionProperties)) \
            .mode("append") \
            .option(KustoConnector.WriteEnableAsync, False) \
            .option(KustoConnector.PollingOnDriver, True)

        if (BaseConnector.getAuthMode() == ConnectorAuthModes.InterActive):
            obj = obj.option("auth", "DeviceCode")
        else:
            token = self._accessToken.getAccessToken()
            obj = obj.option("accessToken", token)
            # Provide the same token for transient blob staging auth to prevent
            # the Spark Kusto connector from falling back to DefaultAzureCredential,
            # which may pick up unrelated MSI credentials from Hadoop/Spark config.
            obj = obj.option("customizedSinkBlobAccessToken", token)

        obj.save()

        if debug:
            Logger.getInstance().writeInfo(f'Published data to the {tableName} table with {dataFrame.count()} rows.')
        else:
            Logger.getInstance().writeInfo(f'Published data to the {tableName} table.')

    def _getKustoClusterFullName(self):
        ''' Get the full name of the Kusto cluster. '''
        if self._kustoCluster:
            return f"{self._kustoCluster}.kusto.windows.net"
        else:
            raise ValueError("Kusto cluster name is not provided in the configuration.")

    def _getKustoClusterUrl(self, forIngestion: bool = False) -> str:
        ''' Get the Kusto cluster URL from the configuration. '''
        if not self._kustoCluster:
            raise ValueError("Kusto cluster name is not provided in the configuration.")

        if forIngestion:
            return f"https://ingest-{self._kustoCluster}.kusto.windows.net/"
        else:
            return f"https://{self._kustoCluster}.kusto.windows.net/"

    def _getKustoClientStringBuilder(self, forIngestion: bool = False):
        '''Get the Kusto client string builder.

        Routes authentication based on the accessConfig first (config-driven),
        then falls back to the global BaseConnector auth mode (legacy).

        Config-driven routing:
          - accessType "FIC"   -> with_azure_token_credential (FIC credential)
          - accessType "AppId" -> with_azure_token_credential (certificate credential)
          - accessType "MSI"   -> with_aad_managed_service_identity_authentication

        Global auth mode fallback (legacy / backward compat):
          - Sni         -> with_azure_token_credential
          - InterActive -> with_aad_device_authentication
          - ManagedIdentiy -> with_aad_managed_service_identity_authentication
        '''
        cluster_url = self._getKustoClusterUrl(forIngestion)

        # --- Config-driven routing (preferred) ---
        access_config = self._config.get(KnownStrings.AccessConfig)
        if access_config and isinstance(access_config, dict):
            access_type = access_config.get(KnownStrings.AccessType)
            if access_type in (KnownStrings.AccessType_FIC, KnownStrings.AccessType_AppId):
                Logger.getInstance().writeInfo(f'Kusto auth: using credential-based auth (accessType={access_type})')
                return KustoConnectionStringBuilder.with_azure_token_credential(
                    cluster_url,
                    self._accessToken.getCredential()
                )
            elif access_type == KnownStrings.AccessType_MSI:
                Logger.getInstance().writeInfo('Kusto auth: using managed service identity authentication (accessType=MSI)')
                return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_url)

        # --- Global auth mode fallback (legacy) ---
        authType = BaseConnector.getAuthMode()

        if authType == ConnectorAuthModes.ManagedIdentiy:
            return KustoConnectionStringBuilder.with_aad_managed_service_identity_authentication(cluster_url)
        if authType == ConnectorAuthModes.InterActive:
            return KustoConnectionStringBuilder.with_aad_device_authentication(cluster_url)
        if authType == ConnectorAuthModes.Sni:
            return KustoConnectionStringBuilder.with_azure_token_credential(
                cluster_url,
                self._accessToken.getCredential()
            )

        raise ValueError("Invalid authentication type.")

    def _updateKustoColumns(self, dataFrame, tableName: str):
        '''
          Update schema of a Kusto table to match the schema of the provided Spark dataframe.
          Currently only supports appending columns to the existing Kusto table.

          Arguments:
            dataFrame {dataframe} -- Spark dataframe
            tableName {str} -- name of the Kusto table whose schema to update
        '''
        kustoDf = self.loadDataFrameViaKustoClient(f'{tableName} | limit 0')
        Logger.getInstance().writeInfo(f'number of columns of dataFrame: {len(dataFrame.columns)}')
        Logger.getInstance().writeInfo(f'number of columns of kusto table: {len(kustoDf.columns)}')

        colsToAdd = set(dataFrame.columns) - set(kustoDf.columns)
        Logger.getInstance().writeInfo(f'number of columns to add: {len(colsToAdd)}')

        if len(colsToAdd) > 0:
            Logger.getInstance().writeInfo(f'New columns ({", ".join(colsToAdd)}) detected in dataframe. Adding these columns to {tableName} Kusto table.')
            sparkType = lambda colName: str(dataFrame.schema[colName].dataType)
            # We need to use .split('(')[0] in the next line because the Spark type could be qualified
            # (e.g. DecimalType(38, 3)). We only want the generic spark type (e.g. DecimalType) for looking
            # up the Kusto type in the mapping.
            kustoType = lambda colName: KustoConnector._sparkToKustoTypeMapping[sparkType(colName).split('(')[0]]
            kustoCols = [f'{colName}:{kustoType(colName)}' for colName in colsToAdd]
            command = f'.alter-merge table {tableName} ({", ".join(kustoCols)})'
            Logger.getInstance().writeInfo(f'Executing Kusto command: {command}')
            self._kustApiClient.postManagementApi(command)  # Update Kusto table schema

    def _checkCycleByExtent(self, tableName: str, tag: str):
        ''' check whether the cycle exists in the Kusto table or not '''

        command = f".show table {tableName} extents where tags has '{tag}'"

        Logger.getInstance().writeInfo(f'Check whether an extent with a tag:{tag} exists in the {tableName} table')

        try:
            content = self._kustApiClient.postManagementApi(command)

            return TableState.ExtentExists if len(content['Tables'][0]['Rows']) != 0 else TableState.ExtentNotExists
        except Exception as e:
            if not ('BadRequest_EntityNotFound' in str(e)):
                # it is a different issue than the unexisting table.
                Logger.getInstance().writeInfo(e)
                raise e

        Logger.getInstance().writeInfo(f'{tableName} do not exist.')
        return TableState.TableNotExists

    def _dropExtentFromTable(self, tableName: str, tag: str):
        ''' drop a set of data which is with a cycle tag '''

        Logger.getInstance().writeInfo(f'Drop an extent with a tag:{tag} from the {tableName} table')

        command = f".drop extents <|.show table {tableName} extents where tags has '{tag}'"

        _ = self._kustApiClient.postManagementApi(command)

    def _moveFromTempToMaster(self, tempTableName: str, tableName: str):
        ''' move data from temp table to master table '''

        Logger.getInstance().writeInfo(f'Move data from the {tempTableName} to {tableName}')

        command = f".move extents all from table {tempTableName} to table {tableName}"

        _ = self._kustApiClient.postManagementApi(command)

    def _alterTag(self, tableName: str, tag: str):
        ''' alter a tag of extents '''

        Logger.getInstance().writeInfo(f'Alter an extent with a tag {tag} in the {tableName} table')

        command = f".alter extent tags ('{tag}') <|.show table {tableName} extents"

        _ = self._kustApiClient.postManagementApi(command)