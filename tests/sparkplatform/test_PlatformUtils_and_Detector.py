def test_PlatformUtils_wrappers():
    from melius.sparkplatform.PlatformUtils import PlatformUtils, DatabricksPlatformUtils, SynapsePlatformUtils

    class U:
        def __init__(self):
            self.fs = object()

    p = PlatformUtils(U())
    assert p.fs is not None

    d = DatabricksPlatformUtils(U())
    assert d.fs is not None

    s = SynapsePlatformUtils(U())
    assert s.fs is not None


def test_SparkPlatformDetector_uses_Utils_dbutils(mocker):
    from melius.sparkplatform.SparkPlatformDetector import SparkPlatformDetector
    from melius.libraries.Utility import Utils
    from melius.sparkplatform.SparkPlatform import Databricks

    fake_dbutils = object()
    mocker.patch.object(Utils, "getDBUtils", return_value=fake_dbutils)

    platform = SparkPlatformDetector.getSparkPlatform()
    assert isinstance(platform, Databricks)
    # Ensure it caches value
    assert SparkPlatformDetector.getSparkPlatform() is platform
