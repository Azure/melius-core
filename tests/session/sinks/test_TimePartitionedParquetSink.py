import sys
import types
import pytest
import unittest.mock as mock
from datetime import datetime


@pytest.fixture()
def timepartitionedparquetsink_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps_sql.DataFrame = object
    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)

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

    # Mock Constants
    mod_const = types.ModuleType("melius.libraries.Constants")

    class _FileTypes:
        Parquet = "parquet"

    mod_const.FileTypes = _FileTypes
    monkeypatch.setitem(sys.modules, "melius.libraries.Constants", mod_const)

    # Mock BlobStorageConnector (stable connector instance)
    mod_bsc = types.ModuleType("melius.connectors.BlobStorageConnector")

    connector_instance = mock.Mock()
    connector_instance.getAbsolutePath = mock.Mock(return_value="abfs://container@account/test_storage")
    connector_instance.saveTo = mock.Mock()

    class _BlobStorageConnector:
        @staticmethod
        def getConnector(config):
            return connector_instance

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

    # Mock AdlsGen2Sink
    mod_adlssink = types.ModuleType("melius.session.sinks.AdlsGen2Sink")

    class _AdlsGen2Sink(_BaseSink):
        def __init__(self, dataName, dataVersion, storageConfig, storageRootPath):
            super().__init__(dataName, dataVersion)
            self._storageConfig = storageConfig
            self._storageRootPath = storageRootPath

        def getStorageConnector(self):
            return _BlobStorageConnector.getConnector(self._storageConfig)

        @property
        def storageRootPath(self):
            return self._storageRootPath

        @property
        def storageConfig(self):
            return self._storageConfig

    mod_adlssink.AdlsGen2Sink = _AdlsGen2Sink
    monkeypatch.setitem(sys.modules, "melius.session.sinks.AdlsGen2Sink", mod_adlssink)

    # Ensure a clean import each time so connector mocks don't leak across tests
    sys.modules.pop("melius.session.sinks.TimePartitionedParquetSink", None)

    from melius.session.sinks.TimePartitionedParquetSink import TimePartitionedParquetSink
    return TimePartitionedParquetSink


def test_timepartitionedparquetsink_initialization(timepartitionedparquetsink_module):
    sink = timepartitionedparquetsink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        parquetRootPath="/parquet/root",
        parquetPathPattern="/%Y/%m/%d/data_%Y%m%d.parquet",
    )

    assert sink.dataName == "test_data"
    assert sink.dataVersion == "1.0.0"
    assert sink.parquetRootPath == "/parquet/root"
    assert sink.parquetPathPattern == "/%Y/%m/%d/data_%Y%m%d.parquet"


def test_timepartitionedparquetsink_properties(timepartitionedparquetsink_module):
    sink = timepartitionedparquetsink_module(
        dataName="my_model",
        dataVersion="2.0.0",
        storageConfig="prod_storage",
        parquetRootPath="/production/parquet",
        parquetPathPattern="/%Y/%m/%d/model_%Y%m%d%H.parquet",
    )

    assert sink.dataName == "my_model"
    assert sink.dataVersion == "2.0.0"
    assert sink.parquetRootPath == "/production/parquet"
    assert sink.parquetPathPattern == "/%Y/%m/%d/model_%Y%m%d%H.parquet"
    assert sink.dataGuid == "guid-my_model"


def test_timepartitionedparquetsink_save_to_parquet(timepartitionedparquetsink_module):
    sink = timepartitionedparquetsink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        parquetRootPath="/parquet/root",
        parquetPathPattern="/%Y/%m/%d/data_%Y%m%d.parquet",
    )

    # Guard against connector reuse across imports
    sink.getStorageConnector().saveTo.reset_mock()

    mock_df = mock.Mock()
    path = sink.saveToParquet(mock_df, parquet_path="/test/path.parquet")

    assert path == "/test/path.parquet"
    connector = sink.getStorageConnector()
    connector.saveTo.assert_called_once()


def test_timepartitionedparquetsink_save_to_parquet_none_path_raises_error(timepartitionedparquetsink_module):
    sink = timepartitionedparquetsink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        parquetRootPath="/parquet/root",
        parquetPathPattern="/%Y/%m/%d/data_%Y%m%d.parquet",
    )

    mock_df = mock.Mock()

    with pytest.raises(ValueError, match="parquet_path should not be None"):
        sink.saveToParquet(mock_df, parquet_path=None)


def test_timepartitionedparquetsink_save_to_time_partitioned_parquet(timepartitionedparquetsink_module):
    sink = timepartitionedparquetsink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        parquetRootPath="/parquet/root",
        parquetPathPattern="/%Y/%m/%d/data_%Y%m%d.parquet",
    )

    # Guard against connector reuse across imports
    sink.getStorageConnector().saveTo.reset_mock()

    mock_df = mock.Mock()
    watermark_dt = datetime(2024, 6, 15, 10, 30, 0)

    path = sink.saveToTimePartitionedParquet(mock_df, watermark_dt)

    assert "/2024/06/15/" in path
    assert "20240615" in path
    connector = sink.getStorageConnector()
    connector.saveTo.assert_called_once()
