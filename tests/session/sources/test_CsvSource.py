import sys
import types
import pytest
import unittest.mock as mock
from datetime import timedelta, datetime


@pytest.fixture()
def csvsource_module(monkeypatch):
    """Set up module mocks and return the CsvSource class along with shared mocks."""

    # Force fresh imports by clearing any cached melius modules
    for mod_name in list(sys.modules.keys()):
        if mod_name.startswith("melius."):
            monkeypatch.delitem(sys.modules, mod_name, raising=False)

    # Mock pyspark
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")
    ps_sql_types = types.ModuleType("pyspark.sql.types")
    ps_sql_streaming = types.ModuleType("pyspark.sql.streaming")
    ps_sql_functions = types.ModuleType("pyspark.sql.functions")

    class _StorageLevel:
        DISK_ONLY = object()

    ps.StorageLevel = _StorageLevel

    # StructType / StructField / DataType stubs
    class _DataType:
        def simpleString(self):
            return self.__class__.__name__.lower().replace('_', '')

    class _StringType(_DataType):
        def __repr__(self):
            return "StringType()"

        def simpleString(self):
            return "string"

    class _IntegerType(_DataType):
        def __repr__(self):
            return "IntegerType()"

        def simpleString(self):
            return "int"

    class _TimestampType(_DataType):
        def __repr__(self):
            return "TimestampType()"

        def simpleString(self):
            return "timestamp"

    class _StructField:
        def __init__(self, name, dataType, nullable=True):
            self.name = name
            self.dataType = dataType
            self.nullable = nullable

    class _StructType:
        def __init__(self, fields=None):
            self.fields = fields or []

    ps_sql_types.StructType = _StructType
    ps_sql_types.StructField = _StructField
    ps_sql_types.StringType = _StringType
    ps_sql_types.IntegerType = _IntegerType
    ps_sql_types.TimestampType = _TimestampType

    ps_sql.DataFrame = object
    ps_sql_streaming.DataStreamReader = object

    # Real-ish F.col / F.lit that return chainable mocks tagged with their name
    def _make_col(name):
        m = mock.Mock(name=f"col({name})")
        m._col_name = name
        m.cast.return_value.alias.return_value = mock.Mock(name=f"col({name}).cast.alias")
        return m

    def _make_lit(value):
        m = mock.Mock(name=f"lit({value})")
        m._lit_value = value
        m.cast.return_value.alias.return_value = mock.Mock(name=f"lit({value}).cast.alias")
        return m

    def _make_expr(expression):
        m = mock.Mock(name=f"expr({expression})")
        m._expr = expression
        m.alias.return_value = mock.Mock(name=f"expr({expression}).alias")
        return m

    ps_sql_functions.col = _make_col
    ps_sql_functions.lit = _make_lit
    ps_sql_functions.expr = _make_expr

    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", ps_sql_types)
    monkeypatch.setitem(sys.modules, "pyspark.sql.streaming", ps_sql_streaming)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", ps_sql_functions)

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

    # Mock BlobStorageConnector
    mod_bsc = types.ModuleType("melius.connectors.BlobStorageConnector")

    class _BlobStorageConnector:
        @staticmethod
        def getConnector(config):
            connector = mock.Mock()
            connector.getAbsolutePath = mock.Mock(
                side_effect=lambda p: f"abfss://container@account.dfs.core.windows.net/{p.lstrip('/')}"
            )
            return connector

    mod_bsc.BlobStorageConnector = _BlobStorageConnector
    monkeypatch.setitem(sys.modules, "melius.connectors.BlobStorageConnector", mod_bsc)

    # Mock Logger
    mod_logger = types.ModuleType("melius.libraries.Logger")

    class _Logger:
        _instance = None

        def __init__(self):
            self.infos = []
            self.errors = []

        @classmethod
        def getInstance(cls):
            if cls._instance is None:
                cls._instance = cls()
            return cls._instance

        def writeInfo(self, msg):
            self.infos.append(msg)

        def writeError(self, msg):
            self.errors.append(msg)

    _Logger._instance = None
    mod_logger.Logger = _Logger
    monkeypatch.setitem(sys.modules, "melius.libraries.Logger", mod_logger)

    # Mock Utility - shared mock_spark so tests can inspect calls
    mock_spark = mock.Mock(name="SparkSession")
    mod_utils = types.ModuleType("melius.libraries.Utility")

    class _Utils:
        _spark = mock_spark

        @staticmethod
        def getSparkSession():
            return _Utils._spark

        @staticmethod
        def getSqlContext():
            return mock.Mock()

    mod_utils.Utils = _Utils
    monkeypatch.setitem(sys.modules, "melius.libraries.Utility", mod_utils)

    # Mock AdlsGen2Source dependencies
    mod_constants = types.ModuleType("melius.libraries.Constants")

    class _KnownStrings:
        SourceIngestionTime = "SourceIngestionTime"

    mod_constants.KnownStrings = _KnownStrings
    monkeypatch.setitem(sys.modules, "melius.libraries.Constants", mod_constants)

    mod_watermark = types.ModuleType("melius.libraries.WatermarkHelper")
    mod_watermark.WatermarkHelper = mock.Mock()
    monkeypatch.setitem(sys.modules, "melius.libraries.WatermarkHelper", mod_watermark)

    # Import after all mocks
    from melius.session.sources.CsvSource import CsvSource
    return CsvSource, ps_sql_types, _Utils


def _make_source(csvsource_module, **overrides):
    """Helper to create a CsvSource with sensible defaults."""
    CsvSource, ps_sql_types, _ = csvsource_module

    defaults = dict(
        session=mock.Mock(),
        dataName="test_data",
        dataVersion="1.0",
        storageConfig="test-config",
        csvRootPath="/raw/csvfiles",
        lookBackDelta=timedelta(minutes=30),
        backFillStartDateTime=datetime(2024, 1, 1),
    )
    defaults.update(overrides)
    return CsvSource(**defaults)


def _setup_spark_for_autoloader(csvsource_module):
    """Configure the shared mock SparkSession for Auto Loader chain calls.
    Returns (mock_spark, mock_reader) so tests can inspect calls."""
    _, _, _Utils = csvsource_module

    mock_spark = mock.Mock(name="SparkSession")
    mock_reader = mock.Mock(name="StreamReader")
    mock_spark.readStream.format.return_value = mock_reader
    mock_reader.option.return_value = mock_reader
    mock_reader.schema.return_value = mock_reader
    mock_reader.load.return_value = mock.Mock(name="streaming_df")

    _Utils._spark = mock_spark
    return mock_spark, mock_reader


# ============================================================
# Initialization Tests
# ============================================================

class TestCsvSourceInit:

    def test_default_csv_options(self, csvsource_module):
        source = _make_source(csvsource_module)

        assert source._header is True
        assert source._delimiter == ","
        assert source._inferSchema is True
        assert source._multiLine is False
        assert source._encoding == "UTF-8"
        assert source._nullValue == ""
        assert source._dateFormat == "yyyy-MM-dd"
        assert source._timestampFormat == "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"

    def test_custom_csv_options(self, csvsource_module):
        source = _make_source(
            csvsource_module,
            header=False,
            delimiter="\t",
            inferSchema=False,
            multiLine=True,
            encoding="ISO-8859-1",
            nullValue="NA",
            dateFormat="MM/dd/yyyy",
            timestampFormat="yyyy-MM-dd HH:mm:ss",
        )

        assert source._header is False
        assert source._delimiter == "\t"
        assert source._inferSchema is False
        assert source._multiLine is True
        assert source._encoding == "ISO-8859-1"
        assert source._nullValue == "NA"
        assert source._dateFormat == "MM/dd/yyyy"
        assert source._timestampFormat == "yyyy-MM-dd HH:mm:ss"

    def test_schema_defaults_to_none(self, csvsource_module):
        source = _make_source(csvsource_module)
        assert source._schema is None
        assert source._targetSchema is None

    def test_schema_and_target_schema_set(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module
        schema = ps_sql_types.StructType([
            ps_sql_types.StructField("col1", ps_sql_types.StringType()),
        ])
        target = ps_sql_types.StructType([
            ps_sql_types.StructField("col1", ps_sql_types.IntegerType()),
        ])

        source = _make_source(csvsource_module, schema=schema, targetSchema=target)

        assert source._schema is schema
        assert source._targetSchema is target

    def test_csv_root_path_stored(self, csvsource_module):
        source = _make_source(csvsource_module, csvRootPath="/my/csv/path")
        assert source.csvRootPath == "/my/csv/path"


# ============================================================
# Property Tests
# ============================================================

class TestCsvSourceProperties:

    def test_target_schema_getter(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module
        schema = ps_sql_types.StructType([
            ps_sql_types.StructField("a", ps_sql_types.StringType()),
        ])
        source = _make_source(csvsource_module, targetSchema=schema)
        assert source.targetSchema is schema

    def test_target_schema_setter(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module
        source = _make_source(csvsource_module)
        assert source.targetSchema is None

        new_schema = ps_sql_types.StructType([
            ps_sql_types.StructField("x", ps_sql_types.IntegerType()),
        ])
        source.targetSchema = new_schema
        assert source.targetSchema is new_schema

    def test_schema_getter(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module
        schema = ps_sql_types.StructType([
            ps_sql_types.StructField("col", ps_sql_types.StringType()),
        ])
        source = _make_source(csvsource_module, schema=schema)
        assert source.schema is schema

    def test_schema_setter(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module
        source = _make_source(csvsource_module)
        assert source.schema is None

        new_schema = ps_sql_types.StructType([
            ps_sql_types.StructField("y", ps_sql_types.StringType()),
        ])
        source.schema = new_schema
        assert source.schema is new_schema

    def test_csv_root_path_property(self, csvsource_module):
        source = _make_source(csvsource_module, csvRootPath="/data/input")
        assert source.csvRootPath == "/data/input"


# ============================================================
# loadCsvWithAutoLoader Tests
# ============================================================

class TestLoadCsvWithAutoLoader:

    def test_sets_cloudfiles_format_and_csv_options(self, csvsource_module):
        source = _make_source(csvsource_module)
        mock_spark, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(schemaLocation="/schema/loc")

        # Verify readStream.format("cloudFiles") was called
        mock_spark.readStream.format.assert_called_once_with("cloudFiles")

        # Verify key options were set
        option_calls = [call.args for call in mock_reader.option.call_args_list]
        option_keys = [c[0] for c in option_calls]

        assert "cloudFiles.format" in option_keys
        assert "header" in option_keys
        assert "delimiter" in option_keys
        assert "encoding" in option_keys
        assert "cloudFiles.schemaLocation" in option_keys

    def test_csv_option_values_match_defaults(self, csvsource_module):
        source = _make_source(csvsource_module)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(schemaLocation="/schema/loc")

        option_calls = [call.args for call in mock_reader.option.call_args_list]
        assert ("cloudFiles.format", "csv") in option_calls
        assert ("header", "true") in option_calls
        assert ("delimiter", ",") in option_calls
        assert ("encoding", "UTF-8") in option_calls
        assert ("multiLine", "false") in option_calls

    def test_uses_provided_schema_over_infer(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module
        schema = ps_sql_types.StructType([
            ps_sql_types.StructField("col1", ps_sql_types.StringType()),
        ])
        source = _make_source(csvsource_module, schema=schema, inferSchema=True)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(schemaLocation="/schema/loc")

        # schema() should be called with the provided schema
        mock_reader.schema.assert_called_once_with(schema)

    def test_infer_schema_when_no_schema_provided(self, csvsource_module):
        source = _make_source(csvsource_module, schema=None, inferSchema=True)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(schemaLocation="/schema/loc")

        # Schema should NOT be called
        mock_reader.schema.assert_not_called()
        # inferColumnTypes option should be set
        option_calls = [call.args for call in mock_reader.option.call_args_list]
        assert ("cloudFiles.inferColumnTypes", "true") in option_calls

    def test_no_infer_when_both_disabled(self, csvsource_module):
        source = _make_source(csvsource_module, schema=None, inferSchema=False)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(schemaLocation="/schema/loc")

        mock_reader.schema.assert_not_called()
        option_calls = [call.args for call in mock_reader.option.call_args_list]
        assert ("cloudFiles.inferColumnTypes", "true") not in option_calls

    def test_max_bytes_per_trigger_set_when_provided(self, csvsource_module):
        source = _make_source(csvsource_module)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(
            schemaLocation="/schema/loc",
            maxBytesPerTrigger="10g"
        )

        option_calls = [call.args for call in mock_reader.option.call_args_list]
        assert ("cloudFiles.maxBytesPerTrigger", "10g") in option_calls

    def test_max_files_per_trigger_set(self, csvsource_module):
        source = _make_source(csvsource_module)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(
            schemaLocation="/schema/loc",
            maxFilesPerTrigger=500
        )

        option_calls = [call.args for call in mock_reader.option.call_args_list]
        assert ("cloudFiles.maxFilesPerTrigger", "500") in option_calls

    def test_custom_file_pattern(self, csvsource_module):
        source = _make_source(csvsource_module)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(
            schemaLocation="/schema/loc",
            filePattern="*.tsv"
        )

        option_calls = [call.args for call in mock_reader.option.call_args_list]
        assert ("pathGlobFilter", "*.tsv") in option_calls

    def test_include_existing_files_false(self, csvsource_module):
        source = _make_source(csvsource_module)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(
            schemaLocation="/schema/loc",
            includeExistingFiles=False
        )

        option_calls = [call.args for call in mock_reader.option.call_args_list]
        assert ("cloudFiles.includeExistingFiles", "false") in option_calls

    def test_loads_from_absolute_source_path(self, csvsource_module):
        source = _make_source(csvsource_module, csvRootPath="/raw/data")
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(schemaLocation="/schema/loc")

        # load() should be called with the absolute path from the connector
        mock_reader.load.assert_called_once()
        load_path = mock_reader.load.call_args[0][0]
        assert "raw/data" in load_path

    def test_returns_streaming_dataframe(self, csvsource_module):
        source = _make_source(csvsource_module)
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        result = source.loadCsvWithAutoLoader(schemaLocation="/schema/loc")
        assert result is mock_reader.load.return_value

    def test_schema_location_uses_source_connector_by_default(self, csvsource_module):
        """When schemaLocationConnector is None, schemaLocation resolves via the source connector."""
        source = _make_source(csvsource_module, csvRootPath="/raw/data")
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        source.loadCsvWithAutoLoader(schemaLocation="schema_registry")

        # cloudFiles.schemaLocation should be resolved via source connector
        option_calls = {c.args[0]: c.args[1] for c in mock_reader.option.call_args_list}
        schema_loc = option_calls["cloudFiles.schemaLocation"]
        assert "schema_registry" in schema_loc
        assert "container@account" in schema_loc

    def test_schema_location_uses_custom_connector_when_provided(self, csvsource_module):
        """When schemaLocationConnector is provided, it resolves schemaLocation instead of the source connector."""
        source = _make_source(csvsource_module, csvRootPath="/raw/data")
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        custom_connector = mock.Mock(name="CustomSchemaConnector")
        custom_connector.getAbsolutePath.return_value = (
            "abfss://meta@customacct.dfs.core.windows.net/schema_registry"
        )

        source.loadCsvWithAutoLoader(
            schemaLocation="schema_registry",
            schemaLocationConnector=custom_connector
        )

        # Custom connector should be called for schema location
        custom_connector.getAbsolutePath.assert_called_once_with("schema_registry")

        # cloudFiles.schemaLocation should use the custom connector's resolved path
        option_calls = {c.args[0]: c.args[1] for c in mock_reader.option.call_args_list}
        schema_loc = option_calls["cloudFiles.schemaLocation"]
        assert schema_loc == "abfss://meta@customacct.dfs.core.windows.net/schema_registry"

    def test_custom_schema_connector_does_not_affect_source_path(self, csvsource_module):
        """Source data path should still use the source connector, not the schema connector."""
        source = _make_source(csvsource_module, csvRootPath="/raw/data")
        _, mock_reader = _setup_spark_for_autoloader(csvsource_module)

        custom_connector = mock.Mock(name="CustomSchemaConnector")
        custom_connector.getAbsolutePath.return_value = (
            "abfss://meta@customacct.dfs.core.windows.net/schema_registry"
        )

        source.loadCsvWithAutoLoader(
            schemaLocation="schema_registry",
            schemaLocationConnector=custom_connector
        )

        # load() should use the source connector path, not the custom one
        load_path = mock_reader.load.call_args[0][0]
        assert "container@account" in load_path
        assert "raw/data" in load_path
        assert "customacct" not in load_path


# ============================================================
# applyTargetSchema Tests
# ============================================================

class TestApplyTargetSchema:

    def test_returns_df_unchanged_when_no_target_schema(self, csvsource_module):
        source = _make_source(csvsource_module, targetSchema=None)
        mock_df = mock.Mock()
        result = source.applyTargetSchema(mock_df)
        assert result is mock_df

    def test_selects_correct_number_of_columns(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module

        target = ps_sql_types.StructType([
            ps_sql_types.StructField("name", ps_sql_types.StringType()),
            ps_sql_types.StructField("age", ps_sql_types.IntegerType()),
        ])
        source = _make_source(csvsource_module, targetSchema=target)

        mock_df = mock.Mock()
        mock_df.columns = ["name", "age", "extra_col"]
        mock_df.select.return_value = mock.Mock(name="selected_df")

        source.applyTargetSchema(mock_df)

        # select() should be called with exactly 2 columns (extra_col dropped)
        mock_df.select.assert_called_once()
        select_args = mock_df.select.call_args[0][0]
        assert len(select_args) == 2

    def test_fills_missing_columns_with_null_literal(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module

        target = ps_sql_types.StructType([
            ps_sql_types.StructField("name", ps_sql_types.StringType()),
            ps_sql_types.StructField("missing_col", ps_sql_types.IntegerType()),
        ])
        source = _make_source(csvsource_module, targetSchema=target)

        mock_df = mock.Mock()
        mock_df.columns = ["name"]  # missing_col is NOT in the data
        mock_df.select.return_value = mock.Mock()

        source.applyTargetSchema(mock_df)

        # select should be called with 2 items (one col, one lit)
        mock_df.select.assert_called_once()
        select_args = mock_df.select.call_args[0][0]
        assert len(select_args) == 2

    def test_empty_target_schema_returns_empty_select(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module

        target = ps_sql_types.StructType([])
        source = _make_source(csvsource_module, targetSchema=target)

        mock_df = mock.Mock()
        mock_df.columns = ["a", "b"]
        mock_df.select.return_value = mock.Mock()

        source.applyTargetSchema(mock_df)

        # select should be called with empty list
        mock_df.select.assert_called_once_with([])

    def test_all_columns_missing_fills_all_with_null(self, csvsource_module):
        _, ps_sql_types, _ = csvsource_module

        target = ps_sql_types.StructType([
            ps_sql_types.StructField("x", ps_sql_types.StringType()),
            ps_sql_types.StructField("y", ps_sql_types.IntegerType()),
        ])
        source = _make_source(csvsource_module, targetSchema=target)

        mock_df = mock.Mock()
        mock_df.columns = []  # no columns at all
        mock_df.select.return_value = mock.Mock()

        source.applyTargetSchema(mock_df)

        select_args = mock_df.select.call_args[0][0]
        assert len(select_args) == 2


# ============================================================
# getStorageConnector Tests
# ============================================================

class TestGetStorageConnector:

    def test_returns_connector(self, csvsource_module):
        source = _make_source(csvsource_module)
        connector = source.getStorageConnector()
        assert connector is not None

    def test_connector_resolves_absolute_path(self, csvsource_module):
        source = _make_source(csvsource_module)
        connector = source.getStorageConnector()

        abs_path = connector.getAbsolutePath("/some/path")
        assert "some/path" in abs_path
