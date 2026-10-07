import sys
import types
import pytest
import unittest.mock as mock
from datetime import datetime


@pytest.fixture()
def watermark_module(monkeypatch):
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

    # Mock BlobStorageConnector
    mod_bsc = types.ModuleType("melius.connectors.BlobStorageConnector")

    class _BlobStorageConnector:
        @staticmethod
        def getConnector(config):
            connector = mock.Mock()
            connector.getAbsolutePath = mock.Mock(return_value=f"abfs://container@account/{config}")
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

    mod_adlssink.AdlsGen2Sink = _AdlsGen2Sink
    monkeypatch.setitem(sys.modules, "melius.session.sinks.AdlsGen2Sink", mod_adlssink)

    # Mock WatermarkHelper
    mod_wm = types.ModuleType("melius.libraries.WatermarkHelper")

    class _WatermarkHelper:
        saved_watermarks = {}

        @classmethod
        def saveWatermark(cls, connector, path, dataName, dataVersion, watermarkDateTime):
            key = f"{path}/{dataName}/{dataVersion}"
            cls.saved_watermarks[key] = watermarkDateTime

        @classmethod
        def getWatermarkDateTime(cls, connector, path, dataName, dataVersion):
            key = f"{path}/{dataName}/{dataVersion}"
            return cls.saved_watermarks.get(key, None)

    mod_wm.WatermarkHelper = _WatermarkHelper
    monkeypatch.setitem(sys.modules, "melius.libraries.WatermarkHelper", mod_wm)

    from melius.session.Watermark import Watermark
    return Watermark


def test_watermark_initialization(watermark_module):
    watermark = watermark_module(
        dataName="test_model",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        watermarkPath="/watermarks/test"
    )

    assert watermark.dataName == "test_model"
    assert watermark.dataVersion == "1.0.0"
    assert watermark.watermarkPath == "/watermarks/test"
    assert watermark.currentWatermarkDateTime is None
    assert watermark.previousWatermarkDateTime is None


def test_watermark_initialization_with_override(watermark_module):
    override_dt = datetime(2024, 1, 1, 12, 0, 0)
    watermark = watermark_module(
        dataName="test_model",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        watermarkPath="/watermarks/test",
        watermarkOverride=override_dt
    )

    assert watermark.previousWatermarkDateTime == override_dt


def test_watermark_set_current_watermark(watermark_module):
    watermark = watermark_module(
        dataName="test_model",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        watermarkPath="/watermarks/test"
    )

    test_dt = datetime(2024, 6, 15, 10, 30, 0)
    watermark.setCurrentWatermark(test_dt)

    assert watermark.currentWatermarkDateTime == test_dt


def test_watermark_set_current_watermark_twice_raises_error(watermark_module):
    watermark = watermark_module(
        dataName="test_model",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        watermarkPath="/watermarks/test"
    )

    watermark.setCurrentWatermark(datetime(2024, 6, 15, 10, 30, 0))

    with pytest.raises(RuntimeError, match="Overwriting existing current watermark"):
        watermark.setCurrentWatermark(datetime(2024, 6, 16, 10, 30, 0))


def test_watermark_save_current_watermark_to_storage(watermark_module):
    watermark = watermark_module(
        dataName="test_model",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        watermarkPath="/watermarks/test"
    )

    test_dt = datetime(2024, 6, 15, 10, 30, 0)
    watermark.setCurrentWatermark(test_dt)
    watermark.saveCurrentWatermarkToStorage()


def test_watermark_save_without_setting_raises_error(watermark_module):
    watermark = watermark_module(
        dataName="test_model",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        watermarkPath="/watermarks/test"
    )

    with pytest.raises(ValueError, match="Current watermark is not set"):
        watermark.saveCurrentWatermarkToStorage()


def test_watermark_properties(watermark_module):
    watermark = watermark_module(
        dataName="my_model",
        dataVersion="2.0.0",
        storageConfig="prod_storage",
        watermarkPath="/prod/watermarks"
    )

    assert watermark.dataName == "my_model"
    assert watermark.dataVersion == "2.0.0"
    assert watermark.watermarkPath == "/prod/watermarks"
    assert watermark.dataGuid == "guid-my_model"
