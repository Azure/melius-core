from melius.sparkplatform.SparkPlatform import Databricks
from melius.libraries.Utility import Utils


class SparkPlatformDetector:
    _currentSparkPlatform = None

    @staticmethod
    def getSparkPlatform():
        if (SparkPlatformDetector._currentSparkPlatform is None):
            SparkPlatformDetector._currentSparkPlatform = Databricks(Utils.getDBUtils())
        return SparkPlatformDetector._currentSparkPlatform
