import abc
from datetime import datetime
import json
import uuid
import re
import pyspark.sql.functions as psf
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField
from melius.libraries.Constants import CycleTypes, KnownStrings
from melius.libraries.Logger import Logger
from melius.libraries.AccessToken import AccessToken
from melius.libraries.DatabricksHelper import DatabricksHelper


class DebugConstants(abc.ABC):
    DEBUG_LOCAL = 'dbutils' not in globals() and 'dbutils' not in locals()


class PathConstants(abc.ABC):
    # constant path strings
    DBFS_STRING = '/dbfs'
    # if you need to update Mount_sources, please check mount sources in databricks first.
    MELIUS_CONFIG = '/melius/config.json'
    DataSources_File = 'datasources.json'


class ConstantStrings(abc.ABC):
    # Constants ConstantsStrings
    DefaultCdeStage = "cleansed"
    ExtractDtColumn = "ExtractDT"
    MeliusConfig_Env = "environment"
    MeliusConfig_RootPath = "meliusPath"
    MeliusConfig_Scope = "scope"
    Environment_Prod = "prod"
    Environment_Test = "test"
    SourceName_CdeProd = "CDEProd"
    SourceName_CdeTest = "CDETest"
    DataSourceConfig_Config = "Config"
    DataSourceConfig_RootPath = "RootPath"


# COMMAND ----------
class Utils(abc.ABC):

    # Melius Config Static member
    _meliusConfig = None

    # DatasetLoaders config in a dict
    _datasetLoadersConfig = None
    _datasetLoadersConfigPath = ''

    @staticmethod
    def loadEnvironmentConfig(path: str = "/dbfs/FileStore/vmwatch/environment.json") -> dict:
        """
        Load the Environment configuration from a JSON file.
        Args:
            path (str): The path to the JSON configuration file. the default path is /dbfs/FileStore/vmwatch/environment.json
        Returns:
            dict: The configuration loaded from the JSON file.
        """
        with open(path, 'r') as f:
            txt = f.read()
            Logger.getInstance().writeInfo(f'Configuration loaded from {path}:\n{txt}')
        config = json.loads(txt)

        if KnownStrings.AccessConfig in config:
            Logger.getInstance().writeInfo('AccessConfig exists in environment.json.')
            AccessToken.setAccessConfig(config[KnownStrings.AccessConfig])
        else:
            Logger.getInstance().writeInfo('AccessConfig does not exist in environment.json.')

        return config

    @staticmethod
    def generateGuidFromString(value: str) -> str:
        """
        Return the same UUID every time this exact string is passed in.
        Uses the SHA-1 name-based algorithm (UUID v5).
        """
        # Pick (or define) a namespace UUID once and keep re-using it.
        # uuid.NAMESPACE_DNS / URL / OID / X500 are predefined, or you can hard-code your own.
        NAMESPACE = uuid.NAMESPACE_DNS
        return str(uuid.uuid5(NAMESPACE, value))

    @staticmethod
    def getSparkConfig(configName):
        '''
        get a spark config value (spark.conf.get())

        Args:
            configName: the config name

        Returns:
            the config value
        '''
        return Utils.getSparkSession().conf.get(configName)

    @staticmethod
    def getDataframeFromDict(dicts):
        """Create a Dataframe from a dictionary"""
        return Utils.getSparkSession().createDataFrame(Row(**x) for x in dicts)

    @staticmethod
    def getNotebookName():
        context = json.loads(Utils.getDBUtils().notebook.entry_point.getDbutils().notebook().getContext().toJson())
        return context['extraContext']['notebook_path'].split('/').pop()

    @staticmethod
    def getNotebookParam(paramName: str, defaultValue=None) -> str:
        """Get the notebook parameter value"""
        dbUtils = Utils.getDBUtils()
        dbUtils.widgets.text(paramName, defaultValue)
        paramValue = dbUtils.widgets.get(paramName)

        Logger.getInstance().writeInfo(f'Notebook parameter "{paramName}" value: {paramValue}')

        return paramValue

    @staticmethod
    def getDatasetName():
        return Utils.getNotebookName()

    @staticmethod
    def validateSchema(dataFrame, schema: StructType) -> bool:
        """
        Validate the schema of a DataFrame against a given schema.
        Args:
            dataFrame (DataFrame): The DataFrame to validate.
            schema (StructType): The expected schema.
        Returns:
            bool: True if the DataFrame's schema matches the expected schema, False otherwise.
        """
        dfSchema = Utils.normalizeSchema(dataFrame.schema)
        expectedSchema = Utils.normalizeSchema(schema)

        if dfSchema == expectedSchema:
            return True
        else:
            Logger.getInstance().writeError(f"Normalized Schema mismatch: {dfSchema} != {expectedSchema}")
            return False

    @staticmethod
    def normalizeSchema(schema: StructType) -> list:
        """
        Normalize a schema by converting it to a list of StructFields.
        """
        return [StructField(f.name, f.dataType, nullable=True) for f in schema]

    @staticmethod
    def adjustPartition(dataFrame, partitionCount):
        '''
        adjust the partition count if the native partition count of the dataframe is not matching with partitionCount parameter

        Arguments:
            dataFrame {DataFrame} -- Dataframe

            partitionCount {number or None} -- if None, no reparition.

        '''
        if not partitionCount:
            Logger.getInstance().writeInfo('Use the native partition count. no repartitioning')
        else:
            Logger.getInstance().writeInfo(f'Adjusting partition count to {partitionCount}...')

            if partitionCount == 1:
                # Due to bug where rdd.getNumPartitions() returning 1 when it is not. Repartition to 1 again.
                dataFrame = dataFrame.repartition(1)
            else:
                nativeCount = dataFrame.rdd.getNumPartitions()
                Logger.getInstance().writeInfo(f'ParitionCount of DF: {nativeCount}')
                Logger.getInstance().writeInfo(f'Suggested PartitionCount: {partitionCount}')
                if nativeCount >= partitionCount:
                    dataFrame = dataFrame.repartition(partitionCount)
                    Logger.getInstance().writeInfo('Repartition since the DF partition count is bigger than the suggested')
                else:
                    Logger.getInstance().writeInfo('No need for repartitioning.')

        return dataFrame

    @staticmethod
    def appendExtractDtColumn(outputDf, targetCycle, cycleType):
        ''' append the ExtractDtColumn '''

        extractDt = Utils.getSameDateDT(targetCycle) if cycleType == CycleTypes.Daily else targetCycle
        Logger.getInstance().writeInfo(f'Appending the ExtractDt column to a dataframe. the value is {extractDt.strftime("%Y-%m-%d %H")}')
        return outputDf.withColumn(ConstantStrings.ExtractDtColumn, psf.unix_timestamp(psf.lit(extractDt), 'yyyy-MM-dd HH:mm:ss').cast("timestamp"))

    @staticmethod
    def getStringForIN(array):
        inText = None
        for x in array:
            if inText is None:
                inText = '"{}"'.format(x)
            else:
                inText = '{}, "{}"'.format(inText, x)

        return inText

    @staticmethod
    def getSameDateDT(dt: datetime):
        """get DT with the date of the given DT"""
        return datetime(dt.year, dt.month, dt.day)

    @staticmethod
    def isStringNoneOrEmpty(string):
        """ check whether the given text is None or empty
            If the string is not str type, it returns False
        """
        return string is None or (isinstance(string, str) and not string.strip())

    @staticmethod
    def setDBUtils(dbutils):
        '''
        Set the DBUtils instance (dbutils).
        '''
        DatabricksHelper.setDBUtils(dbutils)

    @staticmethod
    def getDBUtils():
        '''
        Get the DBUtils instance (dbutils).
        '''
        return DatabricksHelper.getDBUtils()

    @staticmethod
    def setSparkSession(spark):
        '''
        Set the Spark session instance (spark).
        '''
        DatabricksHelper.setSparkSession(spark)

    @staticmethod
    def getSparkSession():
        '''
        Get the Spark session instance (spark).
        '''
        return DatabricksHelper.getSparkSession()

    @staticmethod
    def setSqlContext(context):
        '''
        Set the SQL context instance (sqlContext).
        '''
        DatabricksHelper.setSqlContext(context)

    @staticmethod
    def getSqlContext():
        '''
        Get the SQL context instance (sqlContext).
        '''
        return DatabricksHelper.getSqlContext()

    @staticmethod
    def getSafeDatasetName(datasetName):
        """
        Get the datasetName without any stage info.

        Arguments:
            datasetName {string} -- The name of the dataset

        Returns:
            datasetName {string} -- The datasetName name
        """
        match = re.search(r'(?P<dataset>\w+(\.\w+)?(\.\w+)?)(?P<stageName>[@](\w+)?[/]?(\w+)?)?', datasetName)
        return match.group('dataset')
