import sys
import types
import pytest
import unittest.mock as mock


@pytest.fixture()
def basesink_module(monkeypatch):
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

    from melius.session.sinks.BaseSink import BaseSink
    return BaseSink


def test_basesink_initialization(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    assert sink.dataName == "test_data"
    assert sink.dataVersion == "1.0.0"
    assert sink.dataGuid == "guid-test_data"


def test_basesink_properties(basesink_module):
    sink = basesink_module(
        dataName="my_model",
        dataVersion="2.0.0"
    )

    assert sink.dataName == "my_model"
    assert sink.dataVersion == "2.0.0"
    assert sink.dataGuid == "guid-my_model"


def test_basesink_watermark_raises_error(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    with pytest.raises(RuntimeError, match="Subclasses must set the _watermark field"):
        _ = sink.watermark


def test_validate_data_quality_none_dataframe(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    is_valid, message = sink.validateDataQuality(None)
    assert not is_valid
    assert message == "DataFrame is None"


def test_validate_data_quality_insufficient_records(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    mock_df = mock.Mock()
    mock_df.count.return_value = 50

    is_valid, message = sink.validateDataQuality(mock_df, min_records=100)
    assert not is_valid
    assert "Insufficient records: 50 < 100" in message


def test_validate_data_quality_missing_columns(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    mock_df = mock.Mock()
    mock_df.count.return_value = 200
    mock_df.columns = ["col1", "col2"]

    is_valid, message = sink.validateDataQuality(
        mock_df,
        min_records=100,
        required_columns=["col1", "col2", "col3"]
    )
    assert not is_valid
    assert "Missing required columns" in message
    assert "col3" in message


def test_validate_data_quality_excessive_nulls(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    mock_df = mock.Mock()
    mock_df.count.return_value = 200
    mock_df.columns = ["col1", "col2"]

    mock_filtered = mock.Mock()
    mock_filtered.count.return_value = 150  # 75% nulls
    mock_df.filter.return_value = mock_filtered
    mock_df.__getitem__ = mock.Mock(return_value=mock.Mock())

    is_valid, message = sink.validateDataQuality(
        mock_df,
        min_records=100,
        required_columns=["col1"]
    )
    assert not is_valid
    assert "null values" in message


def test_validate_data_quality_success(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    mock_df = mock.Mock()
    mock_df.count.return_value = 200
    mock_df.columns = ["col1", "col2"]

    mock_filtered = mock.Mock()
    mock_filtered.count.return_value = 10  # 5% nulls
    mock_df.filter.return_value = mock_filtered
    mock_df.__getitem__ = mock.Mock(return_value=mock.Mock())

    is_valid, message = sink.validateDataQuality(
        mock_df,
        min_records=100,
        required_columns=["col1", "col2"]
    )
    assert is_valid
    assert message == "Data quality checks passed"


def test_validate_data_quality_exception_handling(basesink_module):
    sink = basesink_module(
        dataName="test_data",
        dataVersion="1.0.0"
    )

    mock_df = mock.Mock()
    mock_df.count.side_effect = Exception("Test error")

    is_valid, message = sink.validateDataQuality(mock_df)
    assert not is_valid
    assert "Data quality validation failed" in message
    assert "Test error" in message
