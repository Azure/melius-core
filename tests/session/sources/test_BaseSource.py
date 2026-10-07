import sys
import types
import pytest
import unittest.mock as mock
from datetime import datetime, timedelta


@pytest.fixture()
def basesource_module(monkeypatch):
    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps_sql.DataFrame = object
    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)

    # Mock JobRunSession
    mod_jrs = types.ModuleType("melius.session.JobRunSession")

    class _JobRunSession:
        def __init__(self):
            self.watermark = mock.Mock()

    mod_jrs.JobRunSession = _JobRunSession
    monkeypatch.setitem(sys.modules, "melius.session.JobRunSession", mod_jrs)

    from melius.session.sources.BaseSource import BaseSource
    return BaseSource


def test_basesource_initialization(basesource_module):
    mock_session = mock.Mock()
    lookback = timedelta(hours=1)
    backfill = datetime(2024, 1, 1)

    source = basesource_module(
        session=mock_session,
        name="test_source",
        lookBackDelta=lookback,
        backFillStartDateTime=backfill
    )

    assert source.name == "test_source"
    assert source.lookBackDelta == lookback
    assert source.backFillStartDateTime == backfill
    assert source.session == mock_session


def test_basesource_properties(basesource_module):
    mock_session = mock.Mock()
    lookback = timedelta(minutes=30)
    backfill = datetime(2024, 6, 15)

    source = basesource_module(
        session=mock_session,
        name="my_data_source",
        lookBackDelta=lookback,
        backFillStartDateTime=backfill
    )

    assert source.name == "my_data_source"
    assert source.lookBackDelta == lookback
    assert source.backFillStartDateTime == backfill
    assert source.session == mock_session


def test_basesource_set_current_watermark(basesource_module):
    mock_session = mock.Mock()
    mock_watermark = mock.Mock()
    mock_session.watermark = mock_watermark

    source = basesource_module(
        session=mock_session,
        name="test_source",
        lookBackDelta=timedelta(hours=1),
        backFillStartDateTime=datetime(2024, 1, 1)
    )

    test_datetime = datetime(2024, 12, 1, 10, 0, 0)
    source.setCurrentWatermark(test_datetime)

    mock_watermark.setCurrentWatermark.assert_called_once_with(test_datetime)
