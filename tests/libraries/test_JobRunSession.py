import sys
import types
import importlib
import datetime as dt
import unittest.mock as mock
import pytest


# Updated tests reflect the simplified JobRunSession API: only ipython & interactive_shell
# are constructor parameters; optional telemetry/storage attributes are set dynamically in tests.


@pytest.fixture()
def jobrun_module(monkeypatch):
    # Minimal pyspark shims
    ps = types.ModuleType("pyspark")
    ps_sql = types.ModuleType("pyspark.sql")

    class _StorageLevel:
        MEMORY_AND_DISK = "MEMORY_AND_DISK"

    ps.StorageLevel = _StorageLevel
    ps_sql.DataFrame = object
    monkeypatch.setitem(sys.modules, "pyspark", ps)
    monkeypatch.setitem(sys.modules, "pyspark.sql", ps_sql)

    # Stub Utils.getDBUtils
    mod_utils = types.ModuleType("melius.libraries.Utility")

    class _Utility:
        def getDBUtils(self):
            class _Dbutils:
                class notebook:
                    def exit(self, *_a, **_k):
                        return {}
                notebook = notebook()
            return _Dbutils()

    mod_utils.Utils = _Utility()
    monkeypatch.setitem(sys.modules, "melius.libraries.Utility", mod_utils)

    # Stub DatabricksHelper
    mod_dbh = types.ModuleType("melius.libraries.DatabricksHelper")

    class _SparkConf:
        def get(self, *_a, **_k):
            return None

    class _SparkContext:
        def getConf(self):
            return _SparkConf()

    class _Spark:
        sparkContext = _SparkContext()

    class _DatabricksHelper:
        @staticmethod
        def getDBUtils():
            return None

        @staticmethod
        def getSparkSession():
            return _Spark()

    mod_dbh.DatabricksHelper = _DatabricksHelper
    monkeypatch.setitem(sys.modules, "melius.libraries.DatabricksHelper", mod_dbh)

    # Stub Logger with capture helpers
    mod_logger = types.ModuleType("melius.libraries.Logger")

    class _Logger:
        _instance = None

        def __init__(self):
            self.infos = []
            self.exceptions = []

        @classmethod
        def getInstance(cls):
            if cls._instance is None:
                cls._instance = _Logger()
            return cls._instance

        def writeInfo(self, msg):
            self.infos.append(msg)

        def writeException(self, ex, msg):
            self.exceptions.append((ex, msg))

        def exception(self, msg):
            self.exceptions.append((None, msg))

    mod_logger.Logger = _Logger
    monkeypatch.setitem(sys.modules, "melius.libraries.Logger", mod_logger)

    # Stub WatermarkHelper
    mod_wm = types.ModuleType("melius.libraries.WatermarkHelper")

    class _WMH:
        calls = []

        @staticmethod
        def saveWatermark(storage, path, key, value):
            _WMH.calls.append((storage, path, key, value))
    mod_wm.WatermarkHelper = _WMH
    monkeypatch.setitem(sys.modules, "melius.libraries.WatermarkHelper", mod_wm)

    # Avoid importing heavy dependencies transitively pulled by JobRunSession imports
    # (Watermark -> storage connectors -> delta) and KustoConnector (azure-kusto, pyspark.sql.types, etc.)

    # Minimal Watermark stub (JobRunSession only needs currentWatermarkDateTime + saveCurrentWatermarkToStorage)
    mod_watermark = types.ModuleType("melius.session.Watermark")

    class _Watermark:
        def __init__(self, currentWatermarkDateTime=None):
            self.currentWatermarkDateTime = currentWatermarkDateTime

        def saveCurrentWatermarkToStorage(self):
            return None

    mod_watermark.Watermark = _Watermark
    monkeypatch.setitem(sys.modules, "melius.session.Watermark", mod_watermark)

    # Minimal KustoConnector stub (tests pass kustoConnector=None; this prevents importing azure-kusto)
    mod_kusto = types.ModuleType("melius.connectors.KustoConnector")

    class _KustoConnector:
        def publishSingleRecordViaQueuedIngest(self, *_a, **_k):
            return None

    mod_kusto.KustoConnector = _KustoConnector
    monkeypatch.setitem(sys.modules, "melius.connectors.KustoConnector", mod_kusto)
    mod = importlib.import_module("melius.session.JobRunSession")
    yield mod


class DummyKustoClient:
    def __init__(self):
        self.records = []

    def publishSingleRecordViaQueuedIngest(self, record, tableName=None):
        self.records.append(record)


def _fake_shell():
    # Imitate InteractiveShell: provide set_custom_exc
    shell = mock.Mock()
    shell.set_custom_exc = mock.Mock()
    return shell


def _fake_ipython():
    ip = mock.Mock()

    # events registry with register/unregister capturing
    class _Events:
        def __init__(self):
            self.registered = []
            self.unregistered = []

        def register(self, name, fn):
            self.registered.append((name, fn))

        def unregister(self, name, fn):
            self.unregistered.append((name, fn))

    ip.events = _Events()
    return ip


def _new_session(jobrun_module):
    config = jobrun_module.JobRunConfig(kustoConnector=None)
    return jobrun_module.JobRunSession(ipython=_fake_ipython(), interactive_shell=_fake_shell(), config=config)


def test_persist_dataframe_does_not_probe_emptiness(jobrun_module):
    sess = _new_session(jobrun_module)
    sess._kusto_client = None
    df = mock.Mock()
    df.persist.return_value = df

    result = sess.persistDataframe(df)

    df.rdd.isEmpty.assert_not_called()
    df.persist.assert_called_once_with(jobrun_module.StorageLevel.MEMORY_AND_DISK)
    assert result is df
    assert df in sess._persisted_dfs


def test_persist_dataframe_persists_and_tracks(jobrun_module):
    sess = _new_session(jobrun_module)
    df = mock.Mock()
    df.persist.return_value = df
    sess.persistDataframe(df)
    assert df in sess._persisted_dfs
    # _send_kusto_record misuse shouldn't remove df from tracking even if it errors internally


def test_persist_dataframe_with_custom_level(jobrun_module):
    sess = _new_session(jobrun_module)
    sess._kusto_client = None
    df = mock.Mock()
    custom = object()
    sess.persistDataframe(df, persist_mode=custom)
    df.persist.assert_called_once_with(custom)


def test_persist_dataframe_propagates_persist_failure(jobrun_module):
    sess = _new_session(jobrun_module)
    df = mock.Mock()
    df.persist.side_effect = RuntimeError("persist failed")

    with pytest.raises(RuntimeError, match="persist failed"):
        sess.persistDataframe(df)

    assert sess._persisted_dfs == []


def test_exitJobRunSession_unpersists_and_clears(jobrun_module):
    sess = _new_session(jobrun_module)
    sess._kusto_client = None

    df1 = mock.Mock()
    df2 = mock.Mock()
    df2.unpersist.side_effect = Exception("boom")

    sess._persisted_dfs.extend([df1, df2])

    # New API: exitJobRunSession(exit_mode, record, watermark)
    watermark = jobrun_module.Watermark(currentWatermarkDateTime=None)
    sess.exitJobRunSession(jobrun_module.JobRunExitMode.SUCCESS, {"Message": "done"}, watermark)

    df1.unpersist.assert_called_once()
    df2.unpersist.assert_called_once()
    assert len(sess._persisted_dfs) == 0


def test_exitJobRun_success_calls_notebook_exit(jobrun_module, monkeypatch):
    sess = _new_session(jobrun_module)
    exit_called = mock.Mock()

    class _Notebook:
        def exit(self, msg):
            exit_called(msg)

    class _Dbutils:
        notebook = _Notebook()

    # Patch the Utils symbol inside the imported module to ensure usage
    class _Utils:
        @staticmethod
        def getDBUtils():
            return _Dbutils()

    monkeypatch.setattr(jobrun_module, "Utils", _Utils)
    sess._kusto_client = None

    watermark = jobrun_module.Watermark(currentWatermarkDateTime=None)
    sess.exitJobRunSession(jobrun_module.JobRunExitMode.SUCCESS, {"Message": "all good"}, watermark)

    exit_called.assert_called_once()
    # Message includes elapsed time + exit mode suffix
    assert "all good" in exit_called.call_args[0][0]


def test_exitJobRun_failure_does_not_call_notebook_exit(jobrun_module, monkeypatch):
    sess = _new_session(jobrun_module)
    exit_called = mock.Mock()

    class _Notebook:
        def exit(self, *_a, **_k):
            exit_called()

    class _Dbutils:
        notebook = _Notebook()

    from melius.libraries.Utility import Utils
    monkeypatch.setattr(Utils, "getDBUtils", lambda: _Dbutils())
    sess._kusto_client = None

    watermark = jobrun_module.Watermark(currentWatermarkDateTime=None)
    sess.exitJobRunSession(jobrun_module.JobRunExitMode.FAILURE, {"Message": "fail"}, watermark)

    exit_called.assert_not_called()


def test_watermark_persisted_on_exit(jobrun_module, monkeypatch):
    sess = _new_session(jobrun_module)
    sess._kusto_client = None

    when = dt.datetime(2025, 5, 5, 5, 5, 5)
    watermark = jobrun_module.Watermark(currentWatermarkDateTime=when)
    watermark.saveCurrentWatermarkToStorage = mock.Mock()

    sess.exitJobRunSession(jobrun_module.JobRunExitMode.SUCCESS, {"Message": "done"}, watermark)

    watermark.saveCurrentWatermarkToStorage.assert_called_once()


def test_error_handlers_idempotent(jobrun_module):
    sess = _new_session(jobrun_module)
    first_registered = len(sess._ipython.events.registered)
    sess._init_error_handling()
    # No additional registrations
    assert len(sess._ipython.events.registered) == first_registered


def test_post_run_cell_triggers_failure(jobrun_module):
    sess = _new_session(jobrun_module)
    # Patch exitJobRun to observe call; current implementation passes a string (not dict)
    called = {}

    def _exit(mode, record):
        called['mode'] = mode
        called['record'] = record

    sess.exitJobRun = _exit  # type: ignore
    sess._pre_run_cell(types.SimpleNamespace(cell_id="c1", raw_cell="print(1)"))
    result = types.SimpleNamespace(error_in_exec=Exception("boom"))
    sess._post_run_cell(result)
    assert called['mode'] == jobrun_module.JobRunExitMode.FAILURE
    assert isinstance(called['record'], str)
    assert 'Uncaught exception in cell' in called['record']


def test_persist_dataframe_none_skips(jobrun_module):
    sess = _new_session(jobrun_module)
    sess._kusto_client = None
    sess.persistDataframe(None)
    assert sess._persisted_dfs == []


# -------------------------
# _safe_tag tests
# -------------------------

def test_safe_tag_getOrElse_returns_value(jobrun_module):
    """getOrElse succeeds and returns a non-empty string."""
    scala_map = mock.Mock()
    scala_map.getOrElse.return_value = "12345"
    result = jobrun_module.JobRunSession._safe_tag(scala_map, "jobId")
    assert result == "12345"
    scala_map.getOrElse.assert_called_once_with("jobId", "")


def test_safe_tag_getOrElse_empty_returns_none(jobrun_module):
    """getOrElse returns empty string (key missing) -> None."""
    scala_map = mock.Mock()
    scala_map.getOrElse.return_value = ""
    result = jobrun_module.JobRunSession._safe_tag(scala_map, "jobId")
    assert result is None


def test_safe_tag_fallback_to_isDefined(jobrun_module):
    """getOrElse throws -> falls back to get().isDefined()/get()."""
    scala_map = mock.Mock()
    scala_map.getOrElse.side_effect = Exception("Py4J error")
    opt = mock.Mock()
    opt.isDefined.return_value = True
    opt.get.return_value = "67890"
    scala_map.get.return_value = opt
    result = jobrun_module.JobRunSession._safe_tag(scala_map, "runId")
    assert result == "67890"


def test_safe_tag_fallback_isDefined_false(jobrun_module):
    """getOrElse throws, get().isDefined() is False -> None."""
    scala_map = mock.Mock()
    scala_map.getOrElse.side_effect = Exception("Py4J error")
    opt = mock.Mock()
    opt.isDefined.return_value = False
    scala_map.get.return_value = opt
    result = jobrun_module.JobRunSession._safe_tag(scala_map, "runId")
    assert result is None


def test_safe_tag_both_approaches_fail(jobrun_module):
    """Both getOrElse and get/isDefined fail -> None."""
    scala_map = mock.Mock()
    scala_map.getOrElse.side_effect = Exception("no getOrElse")
    scala_map.get.side_effect = Exception("no get")
    result = jobrun_module.JobRunSession._safe_tag(scala_map, "jobId")
    assert result is None


# -------------------------
# _collect_databricks_context with dbutils available
# -------------------------

def test_collect_context_with_dbutils_tags(jobrun_module, monkeypatch):
    """When dbutils provides context tags, _job_id/_run_id/_workspace are populated."""
    # Build mock dbutils -> context -> tags (Scala Map mock)
    scala_map = mock.Mock()

    def _getOrElse(key, default):
        return {"jobId": "111", "runId": "222", "browserHostName": "myworkspace.net"}.get(key, default)

    scala_map.getOrElse.side_effect = _getOrElse

    ctx = mock.Mock()
    ctx.tags.return_value = scala_map

    notebook = mock.Mock()
    notebook.getContext.return_value = ctx

    entry_dbutils = mock.Mock()
    entry_dbutils.notebook.return_value = notebook

    entry_point = mock.Mock()
    entry_point.getDbutils.return_value = entry_dbutils

    dbutils_mock = mock.Mock()
    dbutils_mock.notebook.entry_point = entry_point

    # Patch DatabricksHelper in the loaded module
    monkeypatch.setattr(
        jobrun_module.DatabricksHelper, "getDBUtils", staticmethod(lambda: dbutils_mock)
    )

    sess = _new_session(jobrun_module)
    assert sess._job_id == "111"
    assert sess._run_id == "222"
    assert sess._workspace == "myworkspace.net"


def test_collect_context_dbutils_exception_logged(jobrun_module, monkeypatch):
    """When ctx.tags() throws, the exception is logged via the outer except."""
    # Build mock where getContext succeeds but ctx.tags() throws
    ctx = mock.Mock()
    ctx.tags.side_effect = Exception("tags boom")

    notebook = mock.Mock()
    notebook.getContext.return_value = ctx

    entry_dbutils = mock.Mock()
    entry_dbutils.notebook.return_value = notebook

    entry_point = mock.Mock()
    entry_point.getDbutils.return_value = entry_dbutils

    dbutils_mock = mock.Mock()
    dbutils_mock.notebook.entry_point = entry_point

    monkeypatch.setattr(
        jobrun_module.DatabricksHelper, "getDBUtils", staticmethod(lambda: dbutils_mock)
    )

    sess = _new_session(jobrun_module)
    # job_id should remain None (tags() failed)
    assert sess._job_id is None
    # The exception should have been logged via writeException
    logger = jobrun_module.Logger.getInstance()
    context_exceptions = [(ex, msg) for ex, msg in logger.exceptions if "[DatabricksContext]" in msg]
    assert any("dbutils context failed" in msg for _, msg in context_exceptions)
