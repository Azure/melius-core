import sys
import types
import pytest
import unittest.mock as mock


@pytest.fixture()
def adlsgen2sink_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps_sql.DataFrame = object
    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)

    # Mock Utils
    mod_utils = types.ModuleType("melius.libraries.Utility")

    class _Utils:
        @staticmethod
        def generateGuidFromString(s):
            return f"guid-{s}"

    mod_utils.Utils = _Utils
    monkeypatch.setitem(sys.modules, "melius.libraries.Utility", mod_utils)

    # Mock BlobStorageConnector
    mod_bsc = types.ModuleType("melius.connectors.BlobStorageConnector")

    class _BlobStorageConnector:
        @staticmethod
        def getConnector(config):
            connector = mock.Mock()
            connector.getAbsolutePath = mock.Mock(return_value=f"abfs://container@account.dfs.core.windows.net/{config}")
            return connector

    mod_bsc.BlobStorageConnector = _BlobStorageConnector
    monkeypatch.setitem(sys.modules, "melius.connectors.BlobStorageConnector", mod_bsc)

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

    from melius.session.sinks.AdlsGen2Sink import AdlsGen2Sink
    return AdlsGen2Sink


def test_adlsgen2sink_initialization(adlsgen2sink_module):
    sink = adlsgen2sink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        storageRootPath="/root/path"
    )

    assert sink.dataName == "test_data"
    assert sink.dataVersion == "1.0.0"
    assert sink.storageConfig == "test_storage"
    assert sink.storageRootPath == "/root/path"


def test_adlsgen2sink_properties(adlsgen2sink_module):
    sink = adlsgen2sink_module(
        dataName="my_model",
        dataVersion="2.0.0",
        storageConfig="prod_storage",
        storageRootPath="/production/data"
    )

    assert sink.dataName == "my_model"
    assert sink.dataVersion == "2.0.0"
    assert sink.storageConfig == "prod_storage"
    assert sink.storageRootPath == "/production/data"
    assert sink.dataGuid == "guid-my_model"


def test_adlsgen2sink_get_storage_connector(adlsgen2sink_module):
    sink = adlsgen2sink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        storageRootPath="/root/path"
    )

    connector = sink.getStorageConnector()
    assert connector is not None


def test_adlsgen2sink_get_storage_absolute_path(adlsgen2sink_module):
    sink = adlsgen2sink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        storageRootPath="/root/path"
    )

    abs_path = sink.getStorageAbsolutePath()
    assert abs_path is not None
    assert "abfs://" in abs_path
