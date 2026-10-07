from datetime import datetime, timedelta
from os import path
import re
from io import UnsupportedOperation
from melius.libraries.Constants import FileTypes, KnownStrings
from melius.libraries.Logger import Logger
from melius.libraries.Utility import Utils
from melius.libraries.AccessToken import AccessToken
from melius.connectors.BaseConnector import BaseConnector
from melius.libraries.DatabricksHelper import DatabricksHelper
from typing import Dict, Any, List
from azure.storage.filedatalake import (
    DataLakeServiceClient,
    FileSystemSasPermissions,
    generate_file_system_sas,
)
from functools import reduce
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from pyspark.sql.types import StructType
from pyspark.sql import functions as F, types as T


class BlobStorageConnector(BaseConnector):
    '''
    DatasetLoader to load files from storage account.
    '''

    ''' static members '''
    StorageAccount = "storageAccount"  # The key in the config for the storage account name
    Container = "container"  # The key in the config for the container name
    StorageScope = "https://storage.azure.com/.default"  # The scope for the storage account access token

    _existingConnectors: Dict[str, Any] = {}

    @classmethod
    def getConnector(cls, config) -> Any:
        name = cls._getConnectorName(config)

        if name not in cls._existingConnectors:
            Logger.getInstance().writeInfo("create a new instance of BlobStorageConnector for: " + name)
            cls._existingConnectors[name] = BlobStorageConnector(config)

        Logger.getInstance().writeInfo("returning an existing instance of BlobStorageConnector for: " + name)
        return cls._existingConnectors[name]

    @classmethod
    def _getConnectorName(cls, config) -> str:
        ''' Get the name of the connector based on its configuration.
            This is used to uniquely identify the connector instance.
        '''
        if "Name" in config and config["Name"]:
            return config["Name"]
        else:
            return config[BlobStorageConnector.StorageAccount].lower() + '/' + config[BlobStorageConnector.Container]  # Unique name based on storage account and container

    @classmethod
    def clearConnectors(cls):
        '''
        Clear all existing connectors.
        '''
        cls._existingConnectors.clear()
        Logger.getInstance().writeInfo("Cleared all existing BlobStorageConnectors.")

    ''' class members'''
    def __init__(self, config):
        '''
        Constructor
        '''
        self._config = config
        self._rootPath = config["RootPath"] if "RootPath" in config else self._connectToBlobStorage()  # Ensure connection is established

    def getStorageAccountUrl(self) -> str:
        '''
        Get the storage account URL.
        '''
        return f"https://{self._config[BlobStorageConnector.StorageAccount]}.dfs.core.windows.net"

    def _connectToBlobStorage(self) -> str:
        '''
        Establish connection to the Blob Storage using the provided configuration.

        Routes to the appropriate auth method based on the accessConfig:
          - accessType "FIC" -> _connectWithAccessToken (User Delegation SAS via FIC credential)
          - accessType "MSI" or None -> _connectWithMSI (Spark MsiTokenProvider)
          - accessType "AppId" -> _connectWithAppIdAndCert (certificate-based)
          - Global auth mode Sni (legacy) -> _connectWithAccessToken

        This is fully backward compatible -- non-FIC paths are unchanged.
        '''
        from melius.connectors.BaseConnector import ConnectorAuthModes

        config = self._getAccessConfig()

        accountName = self._config[BlobStorageConnector.StorageAccount]
        containerName = self._config[BlobStorageConnector.Container]

        access_type = config.get(KnownStrings.AccessType) if config and isinstance(config, dict) else None

        if access_type == KnownStrings.AccessType_FIC:
            # FIC: use User Delegation SAS generated via FIC credential for Spark
            Logger.getInstance().writeInfo("FIC config detected: using SAS token approach for Spark ABFS access")
            self._connectWithAccessToken(config, accountName, containerName)
        elif BaseConnector.getAuthMode() == ConnectorAuthModes.Sni:
            # Legacy Sni mode (global flag) -- backward compat with previous monkey-patch approach
            Logger.getInstance().writeInfo("Sni auth mode: using SAS token approach for Spark ABFS access")
            self._connectWithAccessToken(config, accountName, containerName)
        elif access_type is None or access_type == KnownStrings.AccessType_MSI:
            self._connectWithMSI(accountName)
        elif access_type == KnownStrings.AccessType_AppId:
            self._connectWithAppIdAndCert(config, accountName, containerName)
        else:
            raise UnsupportedOperation(f"Unsupported access type '{access_type}' for Blob Storage Connector.")

        return f"abfss://{containerName}@{accountName}.dfs.core.windows.net"

    def _getAccessConfig(self):
        '''
        Get the access configuration for the Blob Storage Connector.
        This is used to determine how to connect to the Blob Storage.
        it considers the global configuration if it is not provided in the blobstorage config.
        '''
        config = self._config[KnownStrings.AccessConfig] if KnownStrings.AccessConfig in self._config else AccessToken.global_config
        return config

    def _connectWithAppIdAndCert(self, config, accountName, containerName):
        '''
        Place holder for connecting to Blob Storage using App ID and Certificate.
        '''
        raise UnsupportedOperation("Connecting to Blob Storage using App ID and Certificate is not implemented yet.")

    def _connectWithMSI(self, accountName):
        '''
        Connect to Blob Storage using Managed Identity.

        Configures Spark's ABFS driver to authenticate via MsiTokenProvider,
        which needs the MI's own tenantId and clientId.

        param accountName: The name of the storage account.
        '''
        Logger.getInstance().writeInfo("Using Managed Identity for Blob Storage access.")

        config = self._getAccessConfig()
        tenantId, clientId = self._resolveMsiIdentity(config)

        Logger.getInstance().writeInfo(f"MI identity: tenantId={tenantId}, clientId={clientId}")

        Utils.getSparkSession().conf.set(f"fs.azure.account.auth.type.{accountName}.dfs.core.windows.net", "OAuth")
        Utils.getSparkSession().conf.set(f"fs.azure.account.oauth.provider.type.{accountName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider")
        Utils.getSparkSession().conf.set(f"fs.azure.account.oauth2.msi.tenant.{accountName}.dfs.core.windows.net", tenantId)
        # only if you attached a **user-assigned** MI to the cluster
        Utils.getSparkSession().conf.set(f"fs.azure.account.oauth2.client.id.{accountName}.dfs.core.windows.net", clientId)

    @staticmethod
    def _resolveMsiIdentity(config) -> tuple:
        """Get the Managed Identity's tenantId and clientId.

        Uses ManagedIdentityCredential directly (rather than AccessToken) so
        the result is always the MI's own identity -- unaffected by any
        credential patches applied elsewhere.

        Returns:
            tuple: (tenantId, clientId)
        """
        import jwt
        from azure.identity import ManagedIdentityCredential

        # Read clientId from config if available
        mi_client_id = None
        if config and isinstance(config, dict):
            mi_client_id = config.get(KnownStrings.Config_ClientId)

        # Build credential for the cluster's Managed Identity
        mi_credential = (
            ManagedIdentityCredential(client_id=mi_client_id)
            if mi_client_id
            else ManagedIdentityCredential()
        )

        # Decode MI token to extract tenantId (and clientId as fallback)
        token = mi_credential.get_token(BlobStorageConnector.StorageScope).token
        claims = jwt.decode(token, options={"verify_signature": False})
        tenant_id = claims["tid"]

        if not mi_client_id:
            mi_client_id = claims.get("azp") or claims.get("appid")

        return tenant_id, mi_client_id

    def _connectWithAccessToken(self, config, accountName, containerName):
        '''
        Connect to Blob Storage using Managed Identity or Access Token.
        This method uses the Azure Data Lake Storage Gen2 SDK to create a connection.

        param accountName: The name of the storage account.
        param containerName: The name of the container.
        param config: The configuration dictionary.
        '''
        Logger.getInstance().writeInfo("Using Access Token from Managed Identity for Blob Storage access.")
        credential = AccessToken(config=config, scope=BlobStorageConnector.StorageScope)

        # Create the BlobServiceClient using AAD credentials
        account_url = self.getStorageAccountUrl()

        Logger.getInstance().writeInfo(f'Storage account url: {account_url}')
        dataLakeClient = DataLakeServiceClient(account_url=account_url, credential=credential.getCredential())

        startTime = datetime.utcnow() - timedelta(minutes=15)  # Start time for the user delegation key
        endTime = startTime + timedelta(hours=7)
        # Get the user delegation key
        user_delegation_key = dataLakeClient.get_user_delegation_key(
            key_start_time=startTime,
            key_expiry_time=endTime
        )

        # Read, Write, Delete, List, Add, Create, Execute, Move, Manage Ownership, Manage ACL permissions
        perms = FileSystemSasPermissions(
            read=True, write=True, delete=True, list=True, add=True, create=True, move=True, execute=True
        )

        # Generate the SAS token using the user delegation key
        sas_token = generate_file_system_sas(
            account_name=accountName,
            file_system_name=containerName,
            credential=user_delegation_key,
            permission=perms,
            expiry=endTime,
            start=startTime
        )

        # Config spart to use SAS for ADLS Gen2
        Utils.getSparkSession().conf.set(f"fs.azure.account.auth.type.{accountName}.dfs.core.windows.net", "SAS")
        Utils.getSparkSession().conf.set(f"fs.azure.sas.token.provider.type.{accountName}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
        Utils.getSparkSession().conf.set(f"fs.azure.sas.fixed.token.{accountName}.dfs.core.windows.net", sas_token)

    def isParquetReadyForUse(self, relativePath: str) -> bool:
        '''
        check whether the parquet is ready for use or not
        if a parquet is partitioned one, there should be _SUCCESS file once the file is create.
        if not, it is not safe to use the file.
        '''
        Logger.getInstance().writeInfo(f'Checking if the parquet is ready for use: {relativePath}')
        isValid = False

        if self.isDirectory(relativePath):
            Logger.getInstance().writeInfo(f'{relativePath} is a directory. checking _SUCCESS file...')
            if self.hasSuccessFile(relativePath):
                Logger.getInstance().writeInfo(f'{relativePath} has _SUCCESS file.')
                isValid = True
            else:
                Logger.getInstance().writeInfo(f'{relativePath} has no _SUCCESS file.')
        elif self.isFile(relativePath):
            Logger.getInstance().writeInfo(f'{relativePath} is a file. checking size...')
            if self.hasContentInFile(relativePath):
                Logger.getInstance().writeInfo(f'{relativePath} has data. valid to use')
                isValid = True
            else:
                Logger.getInstance().writeInfo(f'{relativePath} has no data.')
        else:
            Logger.getInstance().writeInfo(f'{relativePath} does not exist or is empty or not a file.')

        return isValid

    def getFirstIngestionDateTimeOfDeltaTable(self, path: str, columnName: str) -> datetime:
        '''
        Get the beginning of the delta table.

        path {str} -- file path relative to the root path of the storage account.

        columnName {str} -- the column name to get the beginning of the delta table.
        '''
        Logger.getInstance().writeInfo(f'Getting the beginning of the delta table: {path} with column: {columnName}')
        df = self.loadDeltaTable(path)

        from pyspark.sql.functions import col, min

        # Assuming `df` is your DataFrame and `columnName` is the column you want the minimum value of
        return df.agg(min(col(columnName))).collect()[0][0]

    def hasDataAfter(self, path: str, columnName: str, startTime: str) -> bool:
        '''
        Check if there is data after a certain time in the delta table.

        path {str} -- file path relative to the root path of the storage account.

        columnName {str} -- the column name to get from delta table.

        startTime {datetime} -- the start time to filter the delta table.
        '''
        Logger.getInstance().writeInfo(f'Checking if data exists in the delta table: {path} since {startTime}')

        timeFilter = f"{columnName} > '{startTime}'"
        df = self.loadDeltaTable(path, filter=timeFilter)

        return not df.rdd.isEmpty()

    def getFirstIngestionDateTimeOfDeltaTableSince(self, path: str, columnName: str, startTime: str) -> datetime | None:
        '''
        Get the beginning of the delta table.

        path {str} -- file path relative to the root path of the storage account.

        columnName {str} -- the column name to get the beginning of the delta table.

        startTime {datetime} -- the start time to filter the delta table.
        '''
        Logger.getInstance().writeInfo(f'Getting the beginning of the delta table: {path} since {startTime}')

        timeFilter = None
        if startTime:
            timeFilter = f"{columnName} > '{startTime}'"

        df = self.loadDeltaTable(path, filter=timeFilter)

        from pyspark.sql.functions import col, min

        # Assuming `df` is your DataFrame and columnName is the column you want the minimum value of
        return df.agg(min(col(columnName))).collect()[0][0]

    def getLastIngestionDateTimeOfDeltaTable(self, path: str, columnName: str) -> datetime:
        '''
        Get the end of the delta table.
        path {str} -- file path relative to the root path of the storage account.
        columnName {str} -- the column name to get the end of the delta table.
        '''
        Logger.getInstance().writeInfo(f'Getting the end of the delta table: {path} with column: {columnName}')
        df = self.loadDeltaTable(path)
        # Assuming `df` is your DataFrame and `columnName` is the column you want the maximum value of
        return df.agg(F.max(F.col(columnName))).collect()[0][0]

    def loadDeltaTable(self, path: str, filter: str=None, autoRepartition: bool = False) -> DataFrame:
        '''
        Load a delta table

        arguments:
            path {str} -- file path relative to the root path of the storage account.
            filter {str} -- optional filter for loading the delta table.
                            e.g. "timestampCol >= '2021-01-01' AND timestampCol < '2021-02-01'"
            autoRepartition {bool} -- whether to auto repartition the dataframe based on the optimal partition count. Default is False.
        returns:
            dataframe -- the loaded dataframe.
        '''
        from melius.libraries.Utility import Utils
        sqlContext = Utils.getSqlContext()

        path = self.getAbsolutePath(path)

        Logger.getInstance().writeInfo(f'Loading a delta table from: {path}')

        df = sqlContext.read.format("delta").load(path)

        if filter:
            Logger.getInstance().writeInfo(f'Applying filter: {filter}')
            df = df.filter(filter)

        if autoRepartition:
            Logger.getInstance().writeInfo('Auto repartition is enabled. optimizing partitions...')
            df = DatabricksHelper.optimizeDataFramePartitions(df)
        else:
            Logger.getInstance().writeInfo('Auto repartition is disabled.')

        return df

    def loadJson(self, path: str, schema=None) -> Any:
        '''
        Load a json to dataframe

        path {str} -- file path relative to the root path of the storage account.
        '''
        from melius.libraries.Utility import Utils
        sqlContext = Utils.getSqlContext()

        path = self.getAbsolutePath(path)

        Logger.getInstance().writeInfo(f'Loading a json file from: {path}')

        obj = sqlContext.read.format("json")
        if schema:
            Logger.getInstance().writeInfo(f'Using schema: {schema}')
            obj = obj.schema(schema)

        return obj.load(path)

    def deleteRowsFromDeltaTable(self, path: str, condition: str):
        '''
        Delete rows from a delta table based on the condition.
        '''

        # Databricks notebook or %python cell
        from delta.tables import DeltaTable

        Logger.getInstance().writeInfo(f'Deleting rows from delta table: {path} with condition: {condition}')
        spark = Utils.getSparkSession()

        path = self.getAbsolutePath(path)
        dt = DeltaTable.forPath(spark, path)

        dt.delete(condition)

    def appendDeltaLake(self, dataFrame: DataFrame, path: str, partitionBy: List[str] = None, snappyCompression: bool = True, mergeSchema: bool = False) -> str:
        '''
        Append a delta lake table

        arguments:
            dataFrame {dataframe} -- dataframe to append

            path {str} -- file path relative to the root path of the storage account.

            partitionBy {List[str]} -- the partition columns for saving the delta file, default is None.

            snappyCompression {bool} -- whether to use snappy compression for the delta table, default is True.

            mergeSchema {bool} -- whether to enable mergeSchema for the delta table, default is False.

        returns:
            str -- the absolute path of the delta table.
        '''
        absolutePath = self.getAbsolutePath(path)

        Logger.getInstance().writeInfo(f'Appending to delta table "{absolutePath}" with source and target dataframes')
        self._saveDeltaTable(dataFrame, absolutePath, "append" if self.existsPath(path) else "overwrite", partitionBy, snappyCompression, mergeSchema)
        return absolutePath

    def vacuumDeltaLake(self, path: str,
                        compact: bool = True,
                        retentionDays: int = 45,
                        time_col: str = KnownStrings.SourceIngestionTime,
                        zorder_cols: List[str] = None,
                        predicate: str = None) -> str:
        '''
        vacuum a deltalake table and optionally compact it.

        Arguments:
            path {str} -- file path relative to the root path of the storage account.
            compact {bool} -- whether to compact the delta table before vacuuming, default is True.
            retentionDays {int} -- the retention days for the delta table vacuum, default is 45 days. it should be greater than equal to 7 days.
            time_col {str} -- the time column to use for identify the oldest file in the delta table, default is "SourceIngestionTime".
            zorder_cols {List[str]} -- the columns to use for Z-ORDER BY during compaction, default is None.
            predicate {str} -- the predicate to use for compaction, default is None.

        returns:
            str -- the absolute path of the delta table.
        '''
        absolutePath = self.getAbsolutePath(path)

        Logger.getInstance().writeInfo(f'Vacuuming the delta table: {absolutePath}')

        sqlContext = Utils.getSparkSession()
        delta_tbl = DeltaTable.forPath(sqlContext, absolutePath)

        if compact:
            try:
                opt = delta_tbl.optimize()

                if predicate:
                    try:
                        opt = opt.where(predicate)  # if your runtime supports it
                        Logger.getInstance().writeInfo(f"OPTIMIZE predicate: {predicate}")
                    except Exception as ex:
                        Logger.getInstance().writeInfo(
                            f"Predicate-based optimize not supported in this runtime; "
                            f"optimizing full table instead. Details: {ex}"
                        )
                if zorder_cols:
                    zcols: List[str] = list(zorder_cols)
                    Logger.getInstance().writeInfo(f"Z-ORDER BY: {zcols}")
                    result = opt.executeZOrderBy(zcols)
                else:
                    Logger.getInstance().writeInfo("Compaction only (no Z-ORDER).")
                    result = opt.executeCompaction()

                Logger.getInstance().writeInfo(
                    f'OPTIMIZE completed for {absolutePath}'
                    f'with result: {result}')
            except Exception as e:
                Logger.getInstance().writeError(f'OPTIMIZE failed for {absolutePath}: {e}')
                # optional: decide whether to continue to VACUUM or bail out
                raise
        else:
            Logger.getInstance().writeInfo(f'Compaction is skipped as per the configuration for {absolutePath}')

        try:
            Logger.getInstance().writeInfo(f'DELETE for {absolutePath} with retention of {retentionDays} days')
            cutoff = F.expr(f"current_timestamp() - INTERVAL {retentionDays} DAYS")
            delta_tbl.delete(F.col(time_col) < cutoff)
            Logger.getInstance().writeInfo(f'VACUUM for {absolutePath} with retention of {retentionDays} days')
            deleted_files_df = delta_tbl.vacuum(retentionHours=24 * retentionDays)
            deleted_count = deleted_files_df.count()  # Databricks returns a DF of deleted files
            Logger.getInstance().writeInfo(f'VACUUM completed for {absolutePath}, deleted {deleted_count} files')
        except Exception as e:
            Logger.getInstance().writeError(f'VACUUM failed for {absolutePath}: {e}')
            raise

        try:
            oldest = self._getTheOldestFileInDeltaTable(absolutePath, time_col)
            Logger.getInstance().writeInfo(f'The oldest file in delta table {absolutePath} is from {oldest}')
        except Exception as e:
            Logger.getInstance().writeError(f'Failed to get the oldest file in delta table {absolutePath}: {e}')

        return absolutePath

    def overwriteDeltaTable(self, dataFrame, path: str, partitionBy: List[str] = None):
        '''
        Overwrite a delta table

        dataFrame {dataframe} -- dataframe to overwrite

        path {str} -- file path relative to the root path of the storage account.

        partitionBy {List[str]} -- the partition columns for saving the delta file, default is None.
        '''
        path = self.getAbsolutePath(path)

        self._saveDeltaTable(dataFrame, path, "overwrite", partitionBy)

    def _saveDeltaTable(self, dataFrame, path: str, mode: str, partitionBy: List[str] = None, snappyCompression: bool = True, mergeSchema: bool = False, rowCountHint: int = None):
        '''
        Save a delta table

        dataFrame {dataframe} -- dataframe to save

        path {str} -- file path relative to the root path of the storage account.

        mode {str} -- the write mode for saving the delta file, default is "append". ( "append", "overwrite", "ignore", "error" or "errorifexists")

        partitionBy {List[str]} -- the partition columns for saving the delta file, default is None.

        snappyCompression {bool} -- whether to use snappy compression for the delta file, default is True.

        mergeSchema {bool} -- whether to enable mergeSchema for the delta file, default is False.

        rowCountHint {int} -- a hint for the expected number of rows in the dataframe, default is None.
        '''
        if dataFrame.rdd.isEmpty():
            Logger.getInstance().writeInfo(f'No data in dataframe to save for delta table "{path}".')
            return

        Logger.getInstance().writeInfo(f'Saving data to DeltaTable "{path}" with mode: {mode}')
        obj = dataFrame.write.format("delta") \
            .mode(mode)

        # control the level of compression
        if snappyCompression:
            Logger.getInstance().writeInfo('Using snappy compression for delta table.')
            obj = obj.option("compression", "snappy")

        # control the level of mergeSchema. do only when it is needed.
        if mergeSchema:
            Logger.getInstance().writeInfo('Enabling mergeSchema for delta table.')
            obj = obj.option("mergeSchema", "true")

        if partitionBy and len(partitionBy) > 0:
            obj = obj.partitionBy(*partitionBy)

        obj.save(path)

        # Avoid triggering an extra full dataframe.count() which would cause another expensive job.
        Logger.getInstance().writeInfo(f'Saved data to DeltaTable "{path}" with mode: {mode}')

    def _getTheOldestFileInDeltaTable(self, delta_path: str, time_col: str = "SourceIngestionTime") -> str:
        '''
        Get the oldest file in a delta table

        path {str} -- file path relative to the root path of the storage account.

        time_col {str} -- the time column to get the oldest file, default is "SourceIngestionTime".
        '''
        spark = Utils.getSparkSession()
        log_path = path.join(delta_path, "_delta_log", "*.json")
        log = spark.read.json(log_path).where("add is not null")
        Logger.getInstance().writeInfo(f'Parsed delta log stats from {log_path}')

        stats_schema = T.StructType([
            T.StructField("minValues", T.MapType(T.StringType(), T.StringType()), True),
        ])

        log = log.select(F.from_json(F.col("add.stats"), stats_schema).alias("s"))
        Logger.getInstance().writeInfo(f'Extracted stats from delta log: {log.count()} records found.')

        log = log.select(F.col("s.minValues")[time_col].alias("minv"))
        Logger.getInstance().writeInfo(f'Filtered stats for time column "{time_col}".')

        mins = log.where("minv is not null")
        Logger.getInstance().writeInfo(f'Number of records with non-null "{time_col}": {mins.count()}')

        oldest = (mins
                  .select(F.min(F.to_timestamp("minv")).alias("oldest_event_time"))
                  .first()["oldest_event_time"]
                  )

        return oldest

    def mergeDeltaTable(self, dataFrame: DataFrame, pathDeltaTable: str, pkCols: List[str], condCols: List[str] = None):
        '''
        Merge the delta table with a dataframe.

        dataFrame {dataframe} -- the dataframe to merge with the delta table.
        pathDeltaTable {str} -- Delta table file path relative to the root path of the storage account.
        pkCols {List[str]} -- the primary key columns for the delta table.
        condCols {List[str]} -- the conditional columns for the delta table. default is none to update all columns without checking condition.
        '''
        if dataFrame.rdd.isEmpty():
            Logger.getInstance().writeInfo(f'No data in dataframe to merge for delta table "{pathDeltaTable}".')
            return

        delta_target = self.ensureDeltaTableForWrite(pathDeltaTable, dataFrame.schema)

        if delta_target:
            Logger.getInstance().writeInfo(f'Merging delta table "{pathDeltaTable}" with source and target dataframes')
            dt = delta_target.alias("t").merge(dataFrame.alias("s"), " AND ".join([f"t.{c}=s.{c}" for c in pkCols]))

            if condCols is not None and len(condCols) > 0:
                dt = dt.whenMatchedUpdateAll(condition=" OR ".join([f"t.{f}<>s.{f}" for f in condCols]))
            dt.whenNotMatchedInsertAll().execute()
        else:
            Logger.getInstance().writeInfo(f'Creating a new delta table "{pathDeltaTable}" with source dataframe')
            self.overwriteDeltaTable(dataFrame, pathDeltaTable)

    def mergeDeltaTableWithUpdateCols(self, dataFrame: DataFrame, pathDeltaTable: str, pkCols: List[str], updateCols: List[str]):
        '''
        Merge the delta table with a dataframe, updating only the specified columns on match.

        dataFrame {dataframe} -- the dataframe to merge with the delta table.
        pathDeltaTable {str} -- Delta table file path relative to the root path of the storage account.
        pkCols {List[str]} -- the primary key columns for matching records.
        updateCols {List[str]} -- the columns to update when a match is found.
        '''
        if dataFrame.rdd.isEmpty():
            Logger.getInstance().writeInfo(f'No data in dataframe to merge for delta table "{pathDeltaTable}".')
            return

        delta_target = self.ensureDeltaTableForWrite(pathDeltaTable, dataFrame.schema)

        if delta_target:
            Logger.getInstance().writeInfo(f'Merging delta table "{pathDeltaTable}" with custom update columns: {updateCols}')
            merge_condition = " AND ".join([f"t.{c}=s.{c}" for c in pkCols])
            update_set = {c: f"s.{c}" for c in updateCols}

            (delta_target.alias("t")
             .merge(dataFrame.alias("s"), merge_condition)
             .whenMatchedUpdate(set=update_set)
             .whenNotMatchedInsertAll()
             .execute())
        else:
            Logger.getInstance().writeInfo(f'Creating a new delta table "{pathDeltaTable}" with source dataframe')
            self.overwriteDeltaTable(dataFrame, pathDeltaTable)

    def isDeltaTable(self, path: str) -> bool:
        '''
        Check if the specified path is a valid Delta table.

        path {str} -- file path relative to the root path of the storage account.

        returns:
            bool -- True if the path is a Delta table, False otherwise.
        '''
        absolutePath = self.getAbsolutePath(path)
        spark = Utils.getSparkSession()
        return DeltaTable.isDeltaTable(spark, absolutePath)

    def getLastModifiedTime(self, path: str):
        '''
        Get the last modification timestamp of a Delta table from its transaction log.
        This is an O(1) metadata read -- no full table scan.

        path {str} -- file path relative to the root path of the storage account.

        returns:
            datetime -- the timestamp of the last write operation, or None if unavailable.
        '''
        absolutePath = self.getAbsolutePath(path)
        spark = Utils.getSparkSession()
        row = (DeltaTable.forPath(spark, absolutePath)
               .history(1)
               .select("timestamp")
               .collect())
        if row and row[0][0] is not None:
            return row[0][0]
        return None

    def addColumnsToTable(self, path: str, columns: dict):
        '''
        Add columns to an existing Delta table via ALTER TABLE ADD COLUMNS.

        path {str} -- file path relative to the root path of the storage account.
        columns {dict} -- dictionary of column_name -> spark_type_string (e.g. {"col1": "STRING", "col2": "DOUBLE"}).
        '''
        absolutePath = self.getAbsolutePath(path)
        spark = Utils.getSparkSession()

        for col_name, col_type in columns.items():
            Logger.getInstance().writeInfo(f'Adding column "{col_name}" ({col_type}) to delta table at {absolutePath}')
            spark.sql(f"ALTER TABLE delta.`{absolutePath}` ADD COLUMNS ({col_name} {col_type})")

    def getNewDataOnlyAgainstDeltaLake(self, dataFrame: DataFrame, pathDeltaTable: str, pkCols: List[str], timeCol: str) -> DataFrame:
        '''
        Get a dataframe with only new data by left anti join the delta table with the dataframe based on the primary key columns.

        dataFrame {dataframe} -- the dataframe to antijoin with the delta table for getting new data only.
        pathDeltaTable {str} -- Delta table file path relative to the root path of the storage account.
        pkCols {List[str]} -- the primary key columns for the delta table.
        timeCol {str} -- the time column to use for identify the time range to compare between dataframe and delta lake
        '''
        if dataFrame.rdd.isEmpty():
            Logger.getInstance().writeInfo('No data in dataframe.')
            return dataFrame

        if timeCol is None or timeCol == "":
            raise ValueError('Time column must be provided for getting new data only against delta lake.')

        if timeCol not in dataFrame.columns:
            raise ValueError(f'Time column "{timeCol}" does not exist in the dataframe for getting new data only against delta lake.')

        if not pkCols or len(pkCols) == 0:
            raise ValueError('Primary key columns must be provided for getting new data only against delta lake.')

        # Get the min and max of timeCol from dataFrame
        time_range = dataFrame.agg(
            F.min(F.col(timeCol)).alias("min_time"),
            F.max(F.col(timeCol)).alias("max_time")
        ).collect()[0]

        min_time = time_range["min_time"]
        max_time = time_range["max_time"]

        Logger.getInstance().writeInfo(f'Time range from dataFrame: {min_time} to {max_time}')

        delta_target = self.loadDeltaTable(pathDeltaTable, filter=f"{timeCol} >= '{min_time}' AND {timeCol} <= '{max_time}'")

        if delta_target and not delta_target.rdd.isEmpty():
            Logger.getInstance().writeInfo(f'selecting new data by left anti join the delta table with the dataframe based on the primary key columns: {pkCols}')

            # Spark 4.0: Better anti-join with runtime filter
            newData = dataFrame.join(
                delta_target.select(*pkCols),  # If small enough
                pkCols,
                "left_anti"
            )

            Logger.getInstance().writeInfo('Retrieved new data')
            return newData
        else:
            Logger.getInstance().writeInfo('No delta table found. Returning original dataframe.')
            return dataFrame

    def ensureDeltaTableForWrite(self, path: str, schema: StructType) -> DeltaTable:
        '''
        Ensure that a Delta table exists at the specified path for writing (insert, update, merge)
        If the path does not exist, or is not a Delta table, it will create an empty Delta table with the expected schema.

        arguments:
            path {str} -- file path relative to the root path of the storage account.
            schema {StructType} -- the expected schema of the Delta table.

        returns:
            DeltaTable -- the Delta table instance at the specified path.
        '''
        # Try to open as Delta first

        absolutePath = self.getAbsolutePath(path)
        spark = Utils.getSparkSession()
        Logger.getInstance().writeInfo(f'Ensuring Delta table exists for Write at path: {absolutePath}')
        try:
            return DeltaTable.forPath(spark, absolutePath)
        except Exception:
            Logger.getInstance().writeInfo(f'Path "{path}" is not a Delta table. Attempting to create one.')
            pass  # not a Delta table yet

        if self.existsPath(path):
            Logger.getInstance().writeInfo(f'Path "{path}" exists but is not a Delta table. Deleting it to create a new Delta table.')
            self.deletePath(path)
        else:
            Logger.getInstance().writeInfo(f'Path "{path}" does not exist. Creating a new empty Delta table.')

        # Bootstrap an **empty** Delta table with the expected schema
        (spark.createDataFrame([], schema)
            .write.format("delta").mode("overwrite").save(absolutePath))

        return DeltaTable.forPath(spark, absolutePath)

    def ensureDeltaTableForRead(self, path: str) -> DataFrame:
        '''
        Ensure that a Delta table exists at the specified path for reading.
        If the path does not exist, or is not a Delta table, it will return None.

        arguments:
            path {str} -- file path relative to the root path of the storage account.

        returns:
            DataFrame -- the DataFrame instance of the Delta table at the specified path.
        '''
        absolutePath = self.getAbsolutePath(path)
        Logger.getInstance().writeInfo(f'Ensuring Delta table exists for Read at path: {absolutePath}')

        # Try to open as Delta first
        if not self.existsPath(path):
            Logger.getInstance().writeInfo(f'Path "{path}" does not exist.')
            return None

        spark = Utils.getSqlContext()
        return spark.read.format("delta").load(absolutePath)

    def loadFrom(self, datasetFiles: list):
        '''
        build Dataframe from multiple files.
        '''
        sqlContext = Utils.getSqlContext()

        datasetFiles = list(map(lambda f: f.rstrip('/'), filter(lambda f: self.isParquetReadyForUse(f), datasetFiles)))

        fileType: FileTypes = FileTypes.getFileTypeByExt(datasetFiles[0])
        datasetAbsPath = [self.getAbsolutePath(f_p) for f_p in datasetFiles]

        if fileType == FileTypes.Parquet:
            return sqlContext.read.options(mergeSchema=True).parquet(*datasetAbsPath)
        elif fileType == FileTypes.CSV or fileType == FileTypes.TSV:
            return reduce(
                lambda df1, df2: df1.union(df2),
                map(
                    lambda file: sqlContext.read.options(delimiter=FileTypes.separator_of(fileType), header=True).csv(file),
                    datasetAbsPath))
        elif fileType == FileTypes.JSON:
            return reduce(
                lambda df1, df2: df1.union(df2),
                map(
                    lambda file: sqlContext.read.option("multiline", True).json(file),
                    datasetAbsPath))

        raise UnsupportedOperation(f'{fileType} is not a supported fileType.')

    def saveTo(self, dataFrame, relativePath: str, fileType: FileTypes, partitionCount: int = -1, writeMode: str = "overwrite", partitionBy: List[str] = None) -> str:
        """
        save Dataframe into a file
        the file will be saved like below.

           storageUrl/datasourcePath/dataset/yyyy/MM/dd/yyyyMMddHHmmss_dataset_v0.ext

        Arguments:
        dataFrame {dataframe} -- dataframe to save

        relativePath {str} -- file path relative to the root path of the storage account.

        fileType {enum} -- FileTypes

        partitionCount {int} -- it is only for parquet case. default is -1 for no partition change.

        writeMode {str} -- the write mode for saving the file, default is "overwrite". ( "append", "overwrite", "ignore", "error" or "errorifexists")

        partitionBy {List[str]} -- the partition columns for saving the file, default is None.

        """
        if not dataFrame:
            raise ValueError("The dataframe to save cannot be None or empty.")

        count = dataFrame.count()
        Logger.getInstance().writeInfo(f'save a dataframe ({count}) to: {relativePath} with fileType: {fileType} and partitionCount: {partitionCount}')

        outputPath = self.getAbsolutePath(relativePath)

        # for non parquet format the partition count should be 1
        partitionCount = 1 if fileType != FileTypes.Parquet else partitionCount

        # delete the parquet file
        self.deletePath(relativePath)

        Logger.getInstance().writeInfo(f'Start saving a file: {outputPath}')

        if fileType == FileTypes.Parquet:
            self._saveParquet(outputPath, dataFrame, partitionCount, writeMode, partitionBy)
        elif fileType in [FileTypes.CSV, FileTypes.TSV]:
            self._saveCsv(outputPath, dataFrame, fileType, writeMode)
        elif fileType == FileTypes.JSON:
            self.saveJson(outputPath, dataFrame, writeMode)
        else:
            raise UnsupportedOperation(f'{fileType} is not supported for saving.')

        return outputPath

    def isDirectory(self, path_str: str) -> bool:
        '''
        Check if the given path is a directory.

        path_str {str} -- file path relative to the root path of the storage account.
        '''
        return self.getPathType(path_str) == "Directory"

    def isFile(self, path_str: str) -> bool:
        '''
        Check if the given path is a file.

        path_str {str} -- file path relative to the root path of the storage account.
        '''
        return self.getPathType(path_str) == "File"

    def getListOfFilesUnder(self, path_str: str, recursive: bool = False) -> list:
        '''
        Get a list of files under the given path_str.
        '''
        config = self._getAccessConfig()
        account_url = self.getStorageAccountUrl()
        credential = AccessToken(config=config, scope=BlobStorageConnector.StorageScope)

        containerName = self._config[BlobStorageConnector.Container]
        Logger.getInstance().writeInfo(f'Storage account url: {account_url}')
        Logger.getInstance().writeInfo(f'Search a file({path_str}) under a container({containerName})')

        # normalize trailing slash: remove ending '/' unless it's the root '/'
        if path_str.endswith('/') and path_str != '/':
            path_str = path_str.rstrip('/')
            Logger.getInstance().writeInfo(f'Normalized path by removing trailing slash: {path_str}')

        dataLakeClient = DataLakeServiceClient(account_url=account_url, credential=credential.getCredential())
        file_system_client = dataLakeClient.get_file_system_client(containerName)

        path_client = file_system_client.get_directory_client(path_str)  # or get_file_client()

        paths = path_client.get_paths(recursive=recursive)
        files = []
        for file_path in paths:
            files.append(file_path.name)
            Logger.getInstance().writeInfo(f'Found a "{file_path.name}" file under {path_str}')
        Logger.getInstance().writeInfo(f'Found {len(files)} files under {path_str}')
        return files

    def hasContentInFile(self, path_str: str) -> bool:
        '''
        Check whether a file has any data in it.
        '''
        config = self._getAccessConfig()
        account_url = self.getStorageAccountUrl()
        credential = AccessToken(config=config, scope=BlobStorageConnector.StorageScope)

        containerName = self._config[BlobStorageConnector.Container]
        Logger.getInstance().writeInfo(f'Storage account url: {account_url}')
        Logger.getInstance().writeInfo(f'Search a file({path_str}) under a container({containerName})')

        # normalize trailing slash: remove ending '/' unless it's the root '/'
        if path_str.endswith('/') and path_str != '/':
            path_str = path_str.rstrip('/')
            Logger.getInstance().writeInfo(f'Normalized path by removing trailing slash: {path_str}')

        dataLakeClient = DataLakeServiceClient(account_url=account_url, credential=credential.getCredential())
        file_system_client = dataLakeClient.get_file_system_client(containerName)

        path_client = file_system_client.get_file_client(path_str)  # or get_file_client()
        if path_client.exists():
            properties = path_client.get_file_properties()
            file_size = properties.size
            Logger.getInstance().writeInfo(f'File {path_str} exists with size: {file_size} bytes')
            return file_size > 0
        else:
            Logger.getInstance().writeInfo(f'{path_str} does not exist')

        return False

    def existsPath(self, path_str: str) -> bool:
        '''
        Check if the specified path exists.
        '''
        return self.getPathType(path_str) != "Unknown"

    def getPathType(self, path_str: str) -> str:
        '''
        Get the type of the specified path (file or directory).
        '''
        config = self._getAccessConfig()
        account_url = self.getStorageAccountUrl()
        credential = AccessToken(config=config, scope=BlobStorageConnector.StorageScope)

        containerName = self._config[BlobStorageConnector.Container]
        Logger.getInstance().writeInfo(f'Storage account url: {account_url}')
        Logger.getInstance().writeInfo(f'Search a file({path_str}) under a container({containerName})')

        # normalize trailing slash: remove ending '/' unless it's the root '/'
        if path_str.endswith('/') and path_str != '/':
            path_str = path_str.rstrip('/')
            Logger.getInstance().writeInfo(f'Normalized path by removing trailing slash: {path_str}')

        dataLakeClient = DataLakeServiceClient(account_url=account_url, credential=credential.getCredential())
        file_system_client = dataLakeClient.get_file_system_client(containerName)
        path_client = file_system_client.get_directory_client(path_str)  # or get_file_client()

        if path_client.exists():
            Logger.getInstance().writeInfo(f'Path exists as Directory: {path_str}')
            return "Directory"

        path_client = file_system_client.get_file_client(path_str)  # or get_file_client()
        if path_client.exists():
            Logger.getInstance().writeInfo(f'Path exists as File: {path_str}')
            return "File"

        Logger.getInstance().writeInfo(f'Path does not exist: {path_str}')
        return "Unknown"

    def hasSuccessFile(self, path_str: str) -> bool:
        '''
        check whether the given folder path has _SUCCESS File.
        '''
        return self.existsPath(f'{path_str}/_SUCCESS')

    def getAbsolutePath(self, relativePath: str) -> str:
        ''' Get the absolute path for the given relative path.'''
        return relativePath if relativePath.startswith(self._rootPath) else f"{self._rootPath}{relativePath if relativePath.startswith('/') else '/' + relativePath}".replace('\\', '/')

    def loadLatestFile(self, relativeRootPath: str, startDate: datetime, endDate: datetime, fileNamePrefix: str, fileType: FileTypes) -> Any:
        '''
        Load the latest parquet file in the specified time range.

        relativeRootPath {str} -- file path relative to the root path of the storage account.

        startDate {datetime} -- start date for loading the data.

        endDate {datetime} -- end date for loading the data.

        fileNamePrefix {str} -- file name prefix for loading the data.

        fileType {enum} -- FileTypes
        '''
        includedDataFile, error = self.findLatestOrOldestFileInRange(relativeRootPath, startDate, endDate, fileNamePrefix, fileType)

        if error:
            raise FileNotFoundError(error)

        return self.loadFrom([includedDataFile])

    def findLatestOrOldestFileInRange(
        self,
        rootPath: str,
        start_dt: datetime,
        end_dt: datetime,
        prefix: str,
        fileType: "FileTypes",
        latest: bool = True
    ) -> tuple[str, str]:
        """
        Return the latest or oldest file within [start_dt, end_dt] whose filename embeds a
        timestamp as {prefix}_YYYYMMDDHHmm{ext} under {rootPath}/YYYY/MM/DD.

        Notes
        -----
        - Assumes that the embedded timestamp and the supplied datetimes are in the same
        timezone frame (commonly UTC). Supply timezone-aware or naive datetimes
        consistently.
        - Searches day-by-day, not hour-by-hour, for efficiency; candidates are still
        filtered precisely by timestamp.

        Parameters
        ----------
        rootPath         : Top-level directory or URI prefix (e.g. 'abfss://container@acct.dfs.core.windows.net')
        start_dt, end_dt : Datetime bounds (inclusive)
        prefix           : Filename prefix before the underscore
        fileType         : Member of FileTypes; its extension is used to match files
        latest           : If True, return the latest file; if False, return the oldest

        Returns
        -------
        (file_path, None) if found; (None, error_message) if not found or on error.

        Raises
        ------
        ValueError
            If end_dt < start_dt or timezone-awareness is inconsistent.
        FileNotFoundError
            If no matching file exists in the range.
        """
        # Basic validation
        if end_dt < start_dt:
            raise ValueError("end_dt must be >= start_dt")

        if (start_dt.tzinfo is None) != (end_dt.tzinfo is None):
            raise ValueError("start_dt and end_dt must both be timezone-aware or both be naive")

        file_type_text = "latest" if latest else "oldest"
        Logger.getInstance().writeInfo(
            f"Searching for {file_type_text} {prefix}_YYYYMMDDHHmm{FileTypes.fileExt_of(fileType)} "
            f"under {rootPath} in range: {start_dt.isoformat()} to {end_dt.isoformat()}"
        )

        # Compile a strict pattern: filename == {prefix}_{12-digit ts}{ext}
        ext = re.escape(FileTypes.fileExt_of(fileType))
        _TS_RE = re.compile(rf"^{re.escape(prefix)}_(\d{{12}}){ext}$")

        # Choose scan direction: backward for latest, forward for oldest
        day = end_dt.date() if latest else start_dt.date()
        last_day = start_dt.date() if latest else end_dt.date()

        def _advance(d):
            return d - timedelta(days=1) if latest else d + timedelta(days=1)

        while (day >= last_day) if latest else (day <= last_day):
            # Build day folder path using forward slashes to keep URI schemes intact
            parquet_folder_path = f"{rootPath.rstrip('/')}/{day.strftime('%Y/%m/%d')}"

            in_window = []

            if not self.existsPath(parquet_folder_path):
                Logger.getInstance().writeInfo(f"{parquet_folder_path} doesn't exist. Skipped.")
            else:
                Logger.getInstance().writeInfo(f"Checking folder: {parquet_folder_path}")

                for file_path in self.getListOfFilesUnder(parquet_folder_path):
                    # Match on the base filename only
                    fname = path.basename(file_path)
                    m = _TS_RE.match(fname)
                    if not m:
                        continue

                    ts_str = m.group(1)  # 'YYYYMMDDHHmm'
                    try:
                        ts = datetime.strptime(ts_str, "%Y%m%d%H%M")
                    except ValueError:
                        continue  # Skip malformed timestamp

                    # If inputs are timezone-aware, assume embedded ts is in the same zone
                    if start_dt.tzinfo is not None:
                        ts = ts.replace(tzinfo=start_dt.tzinfo)

                    if start_dt <= ts <= end_dt:
                        if fileType == FileTypes.Parquet:
                            if self.isParquetReadyForUse(file_path):
                                Logger.getInstance().writeInfo(f"File {file_path} is ready for use. Added.")
                                in_window.append((ts, file_path))
                            else:
                                Logger.getInstance().writeInfo(
                                    f"File {file_path} is not a valid parquet file or not ready. Skipped."
                                )
                        else:
                            in_window.append((ts, file_path))

            if in_window:
                # Choose globally correct file for this mode; day ordering guarantees correctness.
                chosen_ts, chosen_path = (
                    max(in_window, key=lambda x: x[0]) if latest else min(in_window, key=lambda x: x[0])
                )
                Logger.getInstance().writeInfo(f"Found the {file_type_text}: {chosen_path}")
                return chosen_path, None

            day = _advance(day)

        return None, f"No {prefix}_YYYYMMDDHHmm{FileTypes.fileExt_of(fileType)} files found between {start_dt.isoformat()} and {end_dt.isoformat()}."

    def deletePath(self, relativePath: str):
        '''
        delete the file or directory at the relative path if exists.

        Arguments:
        relativePath {str} -- file path relative to the root path of the storage account.
        '''

        if self.existsPath(relativePath):
            Logger.getInstance().writeInfo(f'Delete a path since it exists: {relativePath}')
            Utils.getDBUtils().fs.rm(relativePath, True)
        else:
            Logger.getInstance().writeInfo(f'No path to delete: {relativePath} does not exist or is already deleted.')

    def _saveParquet(self, outputPath, outputDf, partitionCount, writeMode: str='overwrite', partitionBy: List[str]=None):
        '''
        save a parquet file
        '''
        outputDf.write.option("parquet.block.size", 256 * 1024*1024)

        if partitionCount > 0:
            # adjust the partition count
            outputDf = Utils.adjustPartition(outputDf, partitionCount)
        else:
            Logger.getInstance().writeInfo('No partition adjustment for the parquet file')

        fileObj = outputDf.write.mode(writeMode)
        if partitionBy and len(partitionBy) > 0:
            fileObj = fileObj.partitionBy(*partitionBy)
        fileObj.parquet(outputPath)

        Logger.getInstance().writeInfo(f'Complete saving a parquet file: {str(outputPath)}')

    def _saveCsv(self, outputPath: str, outputDf, fileType: FileTypes=FileTypes.CSV, writeMode: str='overwrite'):
        ''' save a CSV file in a single partition mode'''
        fileExt = FileTypes.fileExt_of(fileType)
        if not outputPath.endswith(fileExt):
            raise ValueError(f'File extension is not matched with fileType {fileType}: {outputPath}')

        if self.existsPath(outputPath):
            Utils.getDBUtils().fs.rm(outputPath, True)

        Logger.getInstance().writeInfo(f'Start saving a csv file: {str(outputPath)}')

        outputDf.repartition(1).write.mode(writeMode).options(delimiter=FileTypes.separator_of(fileType), header=True).csv(outputPath)

        csvfile = Utils.getDBUtils().fs.ls(outputPath)[-1].path

        Logger.getInstance().writeInfo(f'Complete saving a {fileType} file: {str(csvfile)}')

    def saveJson(self, outputPath: str, outputDf, mode: str='overwrite'):
        '''
        Save a dataframe to a json file.
        outputPath {str} -- file path relative to the root path of the storage account.
        outputDf {dataframe} -- dataframe to save
        mode {str} -- the write mode for saving the json file, default is "overwrite". ( "append", "overwrite", "ignore", "error" or "errorifexists")
        '''
        outputPath = self.getAbsolutePath(outputPath)

        Logger.getInstance().writeInfo(f'Start saving a json file: {str(outputPath)}')

        outputDf.repartition(1).write.mode(mode).format('json').save(outputPath)

        jsonfile = Utils.getDBUtils().fs.ls(outputPath)[-1].path

        Logger.getInstance().writeInfo(f'Complete saving a file: {str(jsonfile)} under {outputPath}')