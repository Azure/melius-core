import sys
import types
import pytest
import unittest.mock as mock
from datetime import timedelta, datetime


@pytest.fixture()
def timepartitionedparquetsource_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps.StorageLevel = mock.Mock()
    ps_sql.DataFrame = object
    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)

    # Mock Constants (real one imports pyspark.sql.types)
    mod_const = types.ModuleType("melius.libraries.Constants")

    class _FileTypes:
        Parquet = "Parquet"

    mod_const.FileTypes = _FileTypes
    monkeypatch.setitem(sys.modules, "melius.libraries.Constants", mod_const)

    # Mock BaseSource
    mod_basesource = types.ModuleType("melius.session.sources.BaseSource")

    class _BaseSource:
        def __init__(self, session, name, lookBackDelta, backFillStartDateTime):
            self._session = session
            self._name = name
            self._lookBackDelta = lookBackDelta
            self._backFillStartDateTime = backFillStartDateTime

        @property
        def name(self):
            return self._name

        @property
        def lookBackDelta(self):
            return self._lookBackDelta

        @property
        def backFillStartDateTime(self):
            return self._backFillStartDateTime

        @property
        def session(self):
            return self._session

    mod_basesource.BaseSource = _BaseSource
    monkeypatch.setitem(sys.modules, "melius.session.sources.BaseSource", mod_basesource)

    # Mock JobRunSession
    mod_jrs = types.ModuleType("melius.session.JobRunSession")

    class _JobRunSession:
        def __init__(self):
            self.watermark = mock.Mock()

    mod_jrs.JobRunSession = _JobRunSession
    monkeypatch.setitem(sys.modules, "melius.session.JobRunSession", mod_jrs)

    # Mock Watermark
    mod_watermark = types.ModuleType("melius.session.Watermark")

    class _Watermark:
        def __init__(self, dataName, dataVersion, storageConfig, watermarkPath):
            self.dataName = dataName
            self.dataVersion = dataVersion
            self.storageConfig = storageConfig
            self.watermarkPath = watermarkPath
            self.currentWatermarkDateTime = None
            self.previousWatermarkDateTime = None

    mod_watermark.Watermark = _Watermark
    monkeypatch.setitem(sys.modules, "melius.session.Watermark", mod_watermark)

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

    # Mock AdlsGen2Source to align with real implementation naming
    mod_adlssource = types.ModuleType("melius.session.sources.AdlsGen2Source")

    class _AdlsGen2Source(_BaseSource):
        def __init__(self, session, dataName, dataVersion, storageConfig, dataRootPath, lookBackDelta, backFillStartDateTime):
            super().__init__(session, f"DeltaLakeSource_{dataName}_{dataVersion}", lookBackDelta, backFillStartDateTime)
            self._dataName = dataName
            self._dataVersion = dataVersion
            self._storageConfig = storageConfig
            self._dataRootPath = dataRootPath

        @property
        def dataName(self):
            return self._dataName

        @property
        def dataVersion(self):
            return self._dataVersion

        @property
        def storageConfig(self):
            return self._storageConfig

        @property
        def dataRootPath(self):
            return self._dataRootPath

    mod_adlssource.AdlsGen2Source = _AdlsGen2Source
    monkeypatch.setitem(sys.modules, "melius.session.sources.AdlsGen2Source", mod_adlssource)

    from melius.session.sources.TimePartitionedParquetSource import TimePartitionedParquetSource
    return TimePartitionedParquetSource


def test_timepartitionedparquetsource_initialization(timepartitionedparquetsource_module):
    mock_session = mock.Mock()

    source = timepartitionedparquetsource_module(
        session=mock_session,
        dataName="test_source",
        dataVersion="1.0.0",
        storageConfig="test-storage-config",
        parquetRootPath="/parquet/root",
        parquetPathPattern="/%Y/%m/%d/data_%Y%m%d.parquet",
        watermarkStorageConfig="test-storage-config",
        watermarkPath="/watermarks",
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1),
    )

    assert source.name == "DeltaLakeSource_test_source_1.0.0"


def test_timepartitionedparquetsource_properties(timepartitionedparquetsource_module):
    mock_session = mock.Mock()

    source = timepartitionedparquetsource_module(
        session=mock_session,
        dataName="my_source",
        dataVersion="2.0.0",
        storageConfig="prod-storage",
        parquetRootPath="/production/parquet",
        parquetPathPattern="/%Y/%m/%d/model_%Y%m%d%H.parquet",
        watermarkStorageConfig="prod-storage",
        watermarkPath="/prod/watermarks",
        lookBackDelta=timedelta(minutes=30),
        backFillStartDateTime=datetime(2024, 6, 1),
    )

    assert source.name == "DeltaLakeSource_my_source_2.0.0"
    assert source.lookBackDelta == timedelta(minutes=30)
