import sys
import types
import pytest
import unittest.mock as mock
from datetime import timedelta, datetime


@pytest.fixture()
def kustosource_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps_sql.DataFrame = object
    ps.StorageLevel = mock.Mock()
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

    # Mock WatermarkHelper (real one imports pyspark.sql.types)
    mod_wmh = types.ModuleType("melius.libraries.WatermarkHelper")

    class _WatermarkHelper:
        @staticmethod
        def getUTCStringForKusto(dt):
            return "1970-01-01T00:00:00.000Z"

    mod_wmh.WatermarkHelper = _WatermarkHelper
    monkeypatch.setitem(sys.modules, "melius.libraries.WatermarkHelper", mod_wmh)

    # Mock Constants
    mod_const = types.ModuleType("melius.libraries.Constants")

    class _KnownStrings:
        IngestionTimeMoniker = "IngestionTime"
        IngestionStartDT = "{IngestionStartDT}"
        IngestionEndDT = "{IngestionEndDT}"

    mod_const.KnownStrings = _KnownStrings
    monkeypatch.setitem(sys.modules, "melius.libraries.Constants", mod_const)

    # Mock KustoConnector
    mod_kc = types.ModuleType("melius.connectors.KustoConnector")

    class _KustoConnector:
        Cluster = "Cluster"
        Database = "Database"
        Table = "Table"
        IngestionTimeMoniker = "IngestionTimeMoniker"

        @staticmethod
        def getConnector(config):
            connector = mock.Mock()
            connector.loadDataFrameViaConnector = mock.Mock(return_value=mock.Mock())
            connector.getLastIngestionDateTime = mock.Mock(return_value=None)
            connector.getFirstValueFromQuery = mock.Mock(return_value=None)
            return connector

    mod_kc.KustoConnector = _KustoConnector
    monkeypatch.setitem(sys.modules, "melius.connectors.KustoConnector", mod_kc)

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

    mod_basesource.BaseSource = _BaseSource
    monkeypatch.setitem(sys.modules, "melius.session.sources.BaseSource", mod_basesource)

    # Mock JobRunSession
    mod_jrs = types.ModuleType("melius.session.JobRunSession")

    class _JobRunSession:
        def __init__(self):
            self.watermark = mock.Mock()
            self.watermark.previousWatermarkDateTime = datetime(2024, 6, 1)

    mod_jrs.JobRunSession = _JobRunSession
    monkeypatch.setitem(sys.modules, "melius.session.JobRunSession", mod_jrs)

    from melius.session.sources.KustoSource import KustoSource
    return KustoSource


def test_kustosource_initialization(kustosource_module):
    mock_session = mock.Mock()
    mock_session.watermark = mock.Mock()
    mock_session.watermark.previousWatermarkDateTime = datetime(2024, 6, 1)

    source = kustosource_module(
        session=mock_session,
        kustoCluster="test-cluster",
        kustoTable="test-table",
        kustoDatabase="test-db",
        query="SELECT * FROM test",
        watermarkQuery="SELECT max(timestamp) FROM test",
        parallelism=4,
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1),
        ingestionTimeMoniker="IngestionTime",
    )

    assert source.name == "KustoSource_test-db_test-table"


def test_kustosource_get_kusto_connector(kustosource_module):
    mock_session = mock.Mock()
    mock_session.watermark = mock.Mock()
    mock_session.watermark.previousWatermarkDateTime = datetime(2024, 6, 1)

    source = kustosource_module(
        session=mock_session,
        kustoCluster="test-cluster",
        kustoTable="test-table",
        kustoDatabase="test-db",
        query="SELECT * FROM test",
        watermarkQuery="SELECT max(timestamp) FROM test",
        parallelism=4,
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1),
        ingestionTimeMoniker="IngestionTime",
    )

    connector = source.getKustoConnector()
    assert connector is not None
