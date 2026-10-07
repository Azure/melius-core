import sys
import types
import pytest
import unittest.mock as mock


@pytest.fixture()
def kustosink_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps_types = types.ModuleType("pyspark.sql.types")
    ps_types.StructType = object
    ps_sql.DataFrame = object
    ps_sql.types = ps_types
    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", ps_types)

    # Mock Logger
    mod_logger = types.ModuleType("melius.libraries.Logger")

    class _Logger:
        _instance = None

        @classmethod
        def getInstance(cls):
            if cls._instance is None:
                cls._instance = _Logger()
            return cls._instance

        def writeInfo(self, msg):
            pass

    mod_logger.Logger = _Logger
    monkeypatch.setitem(sys.modules, "melius.libraries.Logger", mod_logger)

    # Mock Utils
    mod_utils = types.ModuleType("melius.libraries.Utility")

    class _Utils:
        @staticmethod
        def generateGuidFromString(s):
            return f"guid-{s}"

    mod_utils.Utils = _Utils
    monkeypatch.setitem(sys.modules, "melius.libraries.Utility", mod_utils)

    # Mock WatermarkHelper (imported but unused here)
    mod_wm = types.ModuleType("melius.libraries.WatermarkHelper")

    class _WatermarkHelper:
        pass

    mod_wm.WatermarkHelper = _WatermarkHelper
    monkeypatch.setitem(sys.modules, "melius.libraries.WatermarkHelper", mod_wm)

    # Mock KustoConnector (return a stable connector instance)
    mod_kc = types.ModuleType("melius.connectors.KustoConnector")

    connector_instance = mock.Mock()
    connector_instance.publishDataframeViaConnector = mock.Mock()

    class _KustoConnector:
        Cluster = "Cluster"
        Database = "Database"
        Table = "Table"

        @staticmethod
        def getConnector(config):
            return connector_instance

    mod_kc.KustoConnector = _KustoConnector
    monkeypatch.setitem(sys.modules, "melius.connectors.KustoConnector", mod_kc)

    # Mock BaseSink
    mod_basesink = types.ModuleType("melius.session.sinks.BaseSink")

    class _BaseSink:
        def __init__(self, dataName, dataVersion):
            self._dataName = dataName
            self._dataVersion = dataVersion
            self._dataGuid = f"guid-{dataName}"

        @property
        def dataName(self):
            return self._dataName

        @property
        def dataVersion(self):
            return self._dataVersion

        @property
        def dataGuid(self):
            return self._dataGuid

    mod_basesink.BaseSink = _BaseSink
    monkeypatch.setitem(sys.modules, "melius.session.sinks.BaseSink", mod_basesink)

    from melius.session.sinks.KustoSink import KustoSink
    return KustoSink


def test_kustosink_initialization(kustosink_module):
    kusto_config = {"Cluster": "test-cluster", "Database": "test-db", "Table": "test-table"}

    sink = kustosink_module(kustoConfig=kusto_config, dataName="test_data", dataVersion="1.0.0")

    assert sink.dataName == "test_data"
    assert sink.dataVersion == "1.0.0"
    assert sink.kustoConfig == kusto_config


def test_kustosink_properties(kustosink_module):
    kusto_config = {"Cluster": "prod-cluster", "Database": "prod-db", "Table": "prod-table"}

    sink = kustosink_module(kustoConfig=kusto_config, dataName="my_model", dataVersion="2.0.0")

    assert sink.dataName == "my_model"
    assert sink.dataVersion == "2.0.0"
    assert sink.dataGuid == "guid-my_model"
    assert sink.kustoCluster == "prod-cluster"
    assert sink.kustoDatabase == "prod-db"
    assert sink.kustoTable == "prod-table"


def test_kustosink_get_kusto_connector(kustosink_module):
    kusto_config = {"Cluster": "test-cluster", "Database": "test-db", "Table": "test-table"}

    sink = kustosink_module(kustoConfig=kusto_config, dataName="test_data", dataVersion="1.0.0")

    connector = sink.getKustoConnector()
    assert connector is not None


def test_kustosink_publish_data_to_kusto(kustosink_module):
    kusto_config = {"Cluster": "test-cluster", "Database": "test-db", "Table": "test-table"}

    sink = kustosink_module(kustoConfig=kusto_config, dataName="test_data", dataVersion="1.0.0")

    mock_df = mock.Mock()
    mock_schema = mock.Mock()

    sink.publishDataToKusto(df=mock_df, schema=mock_schema)

    connector = sink.getKustoConnector()
    connector.publishDataframeViaConnector.assert_called_once()
