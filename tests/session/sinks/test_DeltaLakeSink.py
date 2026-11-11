import sys
import types
import pytest
import unittest.mock as mock


@pytest.fixture()
def deltalakesink_module(monkeypatch):
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

    # Mock BlobStorageConnector (return a stable connector instance)
    mod_bsc = types.ModuleType("melius.connectors.BlobStorageConnector")

    connector_instance = mock.Mock()
    connector_instance.getAbsolutePath = mock.Mock(return_value="abfs://container@account/test_storage")
    connector_instance.appendDeltaLake = mock.Mock(return_value="/delta/path")
    connector_instance.vacuumDeltaLake = mock.Mock(return_value="/delta/path")

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

    from melius.session.sinks.DeltaLakeSink import DeltaLakeSink
    return DeltaLakeSink


def test_deltalakesink_initialization(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    assert sink.dataName == "test_data"
    assert sink.dataVersion == "1.0.0"
    assert sink.deltaLakePath == "/delta/path"


def test_deltalakesink_properties(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="my_model",
        dataVersion="2.0.0",
        storageConfig="prod_storage",
        deltaLakePath="/production/delta",
    )

    assert sink.dataName == "my_model"
    assert sink.dataVersion == "2.0.0"
    assert sink.deltaLakePath == "/production/delta"
    assert sink.dataGuid == "guid-my_model"


def test_deltalakesink_append_to_deltalake(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    mock_df = mock.Mock()
    mock_df.rdd.isEmpty = mock.Mock(return_value=False)
    sink.appendToDeltaLake(
        dataFrame=mock_df,
        partitionBy=["date"],
        snappyCompression=True,
        mergeSchema=False,
    )

    connector = sink.getStorageConnector()
    connector.appendDeltaLake.assert_called_once()


def test_deltalakesink_vacuum_deltalake(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    sink.vacuumDeltaLake(
        compact=True,
        retentionDays=30,
        time_col="Timestamp",
    )

    connector = sink.getStorageConnector()
    connector.vacuumDeltaLake.assert_called_once()


def test_deltalakesink_overwrite_deltalake(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    mock_df = mock.Mock()
    mock_df.rdd.isEmpty = mock.Mock(return_value=False)
    mock_writer = mock.Mock()
    mock_df.write.format = mock.Mock(return_value=mock_writer)
    mock_writer.mode = mock.Mock(return_value=mock_writer)
    mock_writer.option = mock.Mock(return_value=mock_writer)
    mock_writer.partitionBy = mock.Mock(return_value=mock_writer)
    mock_writer.save = mock.Mock()

    sink.overwriteDeltaLake(
        dataFrame=mock_df,
        partitionBy=["BucketDate"],
        overwriteSchema=True,
    )

    mock_df.write.format.assert_called_once_with("delta")
    mock_writer.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("overwriteSchema", "true")
    mock_writer.partitionBy.assert_called_once_with("BucketDate")
    mock_writer.save.assert_called_once()


def test_deltalakesink_overwrite_empty_df_skips(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    mock_df = mock.Mock()
    mock_df.rdd.isEmpty = mock.Mock(return_value=True)

    sink.overwriteDeltaLake(dataFrame=mock_df)

    mock_df.write.format.assert_not_called() if hasattr(mock_df.write, 'format') else None


def test_deltalakesink_merge_with_update_cols(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    mock_df = mock.Mock()
    connector = sink.getStorageConnector()
    connector.mergeDeltaTableWithUpdateCols = mock.Mock()

    sink.mergeDeltaTableWithUpdateCols(
        dataFrame=mock_df,
        pkCols=["VMId", "Disk", "BucketTime"],
        updateCols=["MedianSyncLatency", "RawReadingCount"],
    )

    connector.mergeDeltaTableWithUpdateCols.assert_called_once_with(
        dataFrame=mock_df,
        pathDeltaTable="/delta/path",
        pkCols=["VMId", "Disk", "BucketTime"],
        updateCols=["MedianSyncLatency", "RawReadingCount"],
    )


def test_deltalakesink_is_delta_table(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    connector = sink.getStorageConnector()
    connector.isDeltaTable = mock.Mock(return_value=True)

    result = sink.isDeltaTable()

    assert result is True
    connector.isDeltaTable.assert_called_once_with("/delta/path")


def test_deltalakesink_delete_by_predicate(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    connector = sink.getStorageConnector()
    connector.deleteRowsFromDeltaTable = mock.Mock()

    sink.deleteByPredicate("BucketTime < '2024-01-01'")

    connector.deleteRowsFromDeltaTable.assert_called_once_with(
        path="/delta/path",
        condition="BucketTime < '2024-01-01'",
    )


def test_deltalakesink_add_columns(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    connector = sink.getStorageConnector()
    connector.addColumnsToTable = mock.Mock()

    sink.addColumns({"new_col": "STRING", "score": "DOUBLE"})

    connector.addColumnsToTable.assert_called_once_with(
        path="/delta/path",
        columns={"new_col": "STRING", "score": "DOUBLE"},
    )


def test_deltalakesink_get_last_modified_time(deltalakesink_module):
    from datetime import datetime

    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    connector = sink.getStorageConnector()
    fake_ts = datetime(2025, 7, 1, 12, 0, 0)
    connector.getLastModifiedTime = mock.Mock(return_value=fake_ts)

    result = sink.getLastModifiedTime()

    connector.getLastModifiedTime.assert_called_once_with("/delta/path")
    assert result == fake_ts


def test_deltalakesink_get_last_modified_time_none(deltalakesink_module):
    sink = deltalakesink_module(
        dataName="test_data",
        dataVersion="1.0.0",
        storageConfig="test_storage",
        deltaLakePath="/delta/path",
    )

    connector = sink.getStorageConnector()
    connector.getLastModifiedTime = mock.Mock(return_value=None)

    result = sink.getLastModifiedTime()

    assert result is None
