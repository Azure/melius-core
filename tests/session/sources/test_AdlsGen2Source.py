import sys
import types
import pytest
import unittest.mock as mock
from datetime import timedelta, datetime


@pytest.fixture()
def adlsgen2source_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps_sql.DataFrame = object

    class _StorageLevel:
        DISK_ONLY = object()

    ps.StorageLevel = _StorageLevel
    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)

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

    from melius.session.sources.AdlsGen2Source import AdlsGen2Source
    return AdlsGen2Source


def test_adlsgen2source_initialization(adlsgen2source_module):
    mock_session = mock.Mock()
    lookback = timedelta(hours=1)
    backfill = datetime(2024, 1, 1)

    source = adlsgen2source_module(
        session=mock_session,
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test-storage-config",
        dataRootPath="/root/path",
        lookBackDelta=lookback,
        backFillStartDateTime=backfill,
    )

    assert source.name == "DeltaLakeSource_test_data_1.0.0"
    assert source.lookBackDelta == lookback
    assert source.backFillStartDateTime == backfill


def test_adlsgen2source_properties(adlsgen2source_module):
    mock_session = mock.Mock()

    source = adlsgen2source_module(
        session=mock_session,
        dataName="my_source",
        dataVersion="2.0.0",
        storageConfig="prod-storage",
        dataRootPath="/production/data",
        lookBackDelta=timedelta(minutes=30),
        backFillStartDateTime=datetime(2024, 6, 1),
    )

    assert source.name == "DeltaLakeSource_my_source_2.0.0"
    assert source.lookBackDelta == timedelta(minutes=30)
    assert source.backFillStartDateTime == datetime(2024, 6, 1)


def test_adlsgen2source_get_storage_connector(adlsgen2source_module):
    mock_session = mock.Mock()

    source = adlsgen2source_module(
        session=mock_session,
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test-storage-config",
        dataRootPath="/root/path",
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1),
    )

    connector = source.getStorageConnector()
    assert connector is not None
