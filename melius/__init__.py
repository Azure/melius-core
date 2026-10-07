from melius.libraries.DatabricksHelper import DatabricksHelper


def init(dbutils, sparkSession, sqlContext=None):
    DatabricksHelper.setDBUtils(dbutils)
    DatabricksHelper.setSparkSession(sparkSession)
    DatabricksHelper.setSqlContext(sqlContext)