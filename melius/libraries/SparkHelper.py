# ===============================
# Code to wrap up Spark Related functionalities
# ===============================
from abc import ABC
from datetime import datetime
from melius.libraries.Logger import Logger
from melius.libraries.Utility import Utils
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number


class SparkHelper(ABC):
    '''
    Spark Helper class for
    - Create view and frame
    - change shuffle count.
    more things will be added
    '''

    debugMode = False

    @staticmethod
    def createViewAndDataFrame(vwName: str, query: str, cache=False, repartition=False, checkPoint=False):
        '''
        Create a view and a dataframe from the given query.
        if cache is true, create a checkpoint
        if repartition is true, repartition the df with the default shuffle partition count
        you may use it when you want to force the execution to change the shuffle count.

        Arguments:
            vwName {string} -- a view name which will be created with the given query

            query {string} -- query to create a dataframe and a view

            cache {bool} -- Optional. cache the data frame from the given query

            repartition {bool} -- Optional. repartition the dataframe based on the current shuffle count

            checkPoint {bool} -- Optional. create a check Point

        Returns a tuple with a view(same as the given name) and a dataframe
        '''
        logger = Logger.getInstance()
        logger.writeInfo('Query:')
        logger.writeInfo(query)
        logger.writeInfo(f'ViewName: {vwName}')
        df = Utils.getSparkSession().sql(query)

        if cache:
            logger.writeInfo(f'cache the dataframe of {vwName}')
            df = df.cache()

        if repartition:
            df = SparkHelper.repartitionByShuffleCount(df)

        df.createOrReplaceTempView(vwName)

        if checkPoint:
            logger.writeInfo(f'check point with the dataframe of {vwName}')
            df.checkpoint()

        if SparkHelper.debugMode or cache:
            SparkHelper.countRows(df)

        return vwName, df

    @staticmethod
    def repartitionByShuffleCount(dfSource):
        '''
        repartition the dfSource based on the current shuffle partition count
        '''
        shuffleCount = int(Utils.getSqlContext().getConf('spark.sql.shuffle.partitions'))
        Logger.getInstance().writeInfo(f'Repartition the dataframe with {shuffleCount}')
        return dfSource.repartition(shuffleCount)

    @staticmethod
    def changeShufflePartitionCount(newShuffleCount: int, vwName: str=None, df=None):
        '''
        change shuffle count after executing the given view/df
        '''
        logger = Logger.getInstance()

        if vwName and df:
            logger.writeInfo(f'Force Execution of the given view/df: {vwName}')
            SparkHelper.countRows(df)

        logger.writeInfo(f'Change the shuffle partitions to {newShuffleCount}')
        Utils.getSqlContext().setConf('spark.sql.shuffle.partitions', f'{newShuffleCount}')

    @staticmethod
    def countRows(df):
        '''
        count rows
        '''
        logger = Logger.getInstance()
        time_start = datetime.now()
        logger.writeInfo('Force execution of the query by counting. it will make the dataframe in memory')
        logger.writeInfo(f'Count: {df.count()}')

        logger.writeInfo(f'Elapsed time: {datetime.now() - time_start}')

    @staticmethod
    def dedupOnLatest(df, id_col='SubscriptionId', ts_col="PreciseTimestamp"):
        '''
        Deduplicate the dataframe based on the latest timestamp column for each Id
        '''
        # In case the incoming batch contains duplicates for the same Id, keep the latest
        w = Window.partitionBy(id_col).orderBy(col(ts_col).desc())
        return (df
                .withColumn("_dedup_rn", row_number().over(w))
                .filter(col("_dedup_rn") == 1)
                .drop("_dedup_rn"))