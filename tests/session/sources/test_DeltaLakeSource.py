import sys
import types
import pytest
import unittest.mock as mock
from datetime import timedelta, datetime


@pytest.fixture()
def deltalakesource_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps.StorageLevel = mock.Mock()
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

    # Mock Constants
    mod_const = types.ModuleType("melius.libraries.Constants")

    class _KnownStrings:
        SourceIngestionTime = 'SourceIngestionTime'
        pass

    mod_const.KnownStrings = _KnownStrings
    monkeypatch.setitem(sys.modules, "melius.libraries.Constants", mod_const)

    # Mock WatermarkHelper
    mod_wm = types.ModuleType("melius.libraries.WatermarkHelper")

    class _WatermarkHelper:
        pass

    mod_wm.WatermarkHelper = _WatermarkHelper
    monkeypatch.setitem(sys.modules, "melius.libraries.WatermarkHelper", mod_wm)

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

    # Mock AdlsGen2Source
    mod_adlssource = types.ModuleType("melius.session.sources.AdlsGen2Source")

    class _AdlsGen2Source(_BaseSource):
        def __init__(self, session, dataName, dataVersion, storageConfig, dataRootPath, lookBackDelta, backFillStartDateTime):
            super().__init__(session, dataName, lookBackDelta, backFillStartDateTime)
            self._dataVersion = dataVersion
            self._storageConfig = storageConfig
            self._dataRootPath = dataRootPath

    mod_adlssource.AdlsGen2Source = _AdlsGen2Source
    monkeypatch.setitem(sys.modules, "melius.session.sources.AdlsGen2Source", mod_adlssource)

    from melius.session.sources.DeltaLakeSource import DeltaLakeSource
    return DeltaLakeSource


def test_deltalakesource_initialization(deltalakesource_module):
    mock_session = mock.Mock()

    source = deltalakesource_module(
        session=mock_session,
        dataName="test_source",
        dataVersion="1.0.0",
        storageConfig="test-storage-config",
        deltaLakePath="/delta/path",
        watermarkPath="/watermarks",
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1)
    )

    assert source.name == "test_source"


def test_deltalakesource_with_custom_watermark_storage(deltalakesource_module):
    mock_session = mock.Mock()

    source = deltalakesource_module(
        session=mock_session,
        dataName="test_source",
        dataVersion="1.0.0",
        storageConfig="data-storage",
        deltaLakePath="/delta/path",
        watermarkPath="/watermarks",
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1),
        watermarkStorageConfig="watermark-storage"
    )

    assert source.name == "test_source"


# ---------------------------------------------------------------------------
# getMinMaxValuesOfTimeColumn
# ---------------------------------------------------------------------------


class _FakeColumn:
    """Stand-in for a Spark Column so `df[timeColumn] <= value` works."""

    def __le__(self, other):
        return ("le", other)


class _FakeDataFrame:
    """Minimal DataFrame stand-in for exercising getMinMaxValuesOfTimeColumn.

    Records the filter condition and selectExpr arguments so tests can assert
    on them, and returns a single pre-canned row from collect().
    """

    def __init__(self, row):
        self._row = row
        self.filter_condition = None
        self.select_exprs = None

    def __getitem__(self, key):
        return _FakeColumn()

    def filter(self, condition):
        self.filter_condition = condition
        return self

    def selectExpr(self, *exprs):
        self.select_exprs = exprs
        return self

    def collect(self):
        return [self._row]


def _make_source(deltalakesource_module):
    return deltalakesource_module(
        session=mock.Mock(),
        dataName="test_source",
        dataVersion="1.0.0",
        storageConfig="test-storage-config",
        deltaLakePath="/delta/path",
        watermarkPath="/watermarks",
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1),
    )


def test_getminmax_with_preloaded_df_returns_values(deltalakesource_module):
    source = _make_source(deltalakesource_module)
    source.loadFromDeltaTable = mock.Mock()

    minDt = datetime(2024, 1, 1, 0, 0, 0)
    maxDt = datetime(2024, 1, 1, 1, 0, 0)
    df = _FakeDataFrame({"minTimeValue": minDt, "maxTimeValue": maxDt})

    result = source.getMinMaxValuesOfTimeColumn("EventTime", df=df)

    assert result == (minDt, maxDt)
    # A pre-loaded df must not trigger a Delta scan.
    source.loadFromDeltaTable.assert_not_called()


def test_getminmax_loads_from_delta_when_df_not_provided(deltalakesource_module):
    source = _make_source(deltalakesource_module)

    minDt = datetime(2024, 2, 1)
    maxDt = datetime(2024, 2, 2)
    df = _FakeDataFrame({"minTimeValue": minDt, "maxTimeValue": maxDt})
    source.loadFromDeltaTable = mock.Mock(return_value=df)

    result = source.getMinMaxValuesOfTimeColumn("EventTime")

    assert result == (minDt, maxDt)
    source.loadFromDeltaTable.assert_called_once_with(filter=None, autoRepartition=False)


def test_getminmax_returns_none_when_source_returns_none(deltalakesource_module):
    source = _make_source(deltalakesource_module)
    source.loadFromDeltaTable = mock.Mock(return_value=None)

    result = source.getMinMaxValuesOfTimeColumn("EventTime")

    assert result == (None, None)


def test_getminmax_returns_none_for_empty_or_future_only_df(deltalakesource_module):
    source = _make_source(deltalakesource_module)
    # min/max over zero rows (or a fully future-dated, then filtered, frame)
    # yields nulls from the aggregation - no extra emptiness check required.
    df = _FakeDataFrame({"minTimeValue": None, "maxTimeValue": None})

    result = source.getMinMaxValuesOfTimeColumn("EventTime", df=df)

    assert result == (None, None)


def test_getminmax_applies_future_filter_by_default(deltalakesource_module):
    source = _make_source(deltalakesource_module)
    df = _FakeDataFrame({"minTimeValue": datetime(2024, 1, 1), "maxTimeValue": datetime(2024, 1, 2)})

    source.getMinMaxValuesOfTimeColumn("EventTime", df=df)

    assert df.filter_condition is not None


def test_getminmax_skips_future_filter_when_disabled(deltalakesource_module):
    source = _make_source(deltalakesource_module)
    df = _FakeDataFrame({"minTimeValue": datetime(2024, 1, 1), "maxTimeValue": datetime(2024, 1, 2)})

    source.getMinMaxValuesOfTimeColumn("EventTime", df=df, excludeFutureDateTime=False)

    assert df.filter_condition is None


def test_getminmax_builds_min_max_exprs_for_column(deltalakesource_module):
    source = _make_source(deltalakesource_module)
    df = _FakeDataFrame({"minTimeValue": datetime(2024, 1, 1), "maxTimeValue": datetime(2024, 1, 2)})

    source.getMinMaxValuesOfTimeColumn("MyTime", df=df, excludeFutureDateTime=False)

    assert df.select_exprs == (
        "min(MyTime) as minTimeValue",
        "max(MyTime) as maxTimeValue",
    )
