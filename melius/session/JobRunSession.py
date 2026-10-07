from __future__ import annotations
import sys
import traceback
import datetime
from typing import Any
from enum import Enum
import uuid
from pyspark.sql import DataFrame
from pyspark import StorageLevel

from melius.libraries.Logger import Logger
from melius.libraries.DatabricksHelper import DatabricksHelper
from melius.libraries.Utility import Utils
from melius.connectors.KustoConnector import KustoConnector
from melius.session.Watermark import Watermark

# Provide fallbacks when pyspark types are not available (e.g. running unit tests outside Databricks)
if "DataFrame" not in globals() or "StorageLevel" not in globals():
    # When running unit tests outside of Databricks, these may not be available.
    DataFrame = Any  # type: ignore  # noqa: F811
    StorageLevel = None  # type: ignore  # noqa: F811


class JobRunExitMode(Enum):
    """
    JobRunExitMode enum
    """
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    NONEWDATA = "NONEWDATA"  # No new data, but not an error. save watermark to move forward.
    NONEWDATARETRY = "NONEWDATARETRY"  # No new data, but not an error. However, Not save watermark to retry the same cycle.


class JobRunConfig:
    """
    Configuration class that holds parameters needed to initialize a JobRunSession.
    """

    def __init__(
        self,
        kustoConnector: KustoConnector = None
    ) -> None:
        """
        Initialize the job run configuration.

        Args:
            kustoConnector: Optional KustoConnector for telemetry
        """
        self.kustoConnector = kustoConnector


class JobRunSession:
    """
    JobRunSession captures metadata about a Databricks job run, provides simple
    dataframe persistence helpers, watermark setting, and emits lightweight
    telemetry (Kusto) records.

    Designed to be used inside an Azure Databricks notebook.
    """
    _telemetryTable = "JobRunTelemetry"

    def __init__(
        self,
        ipython: Any,
        interactive_shell: Any,
        config: JobRunConfig
    ) -> None:
        """Initializes a new JobRunSession."""
        self._sys = sys
        self._ipython = ipython
        self._interactive_shell = interactive_shell
        self._kustoConnector = config.kustoConnector

        # Runtime-collected information (from Databricks)
        self._workspace = None
        self._spark_cluster_id = None
        self._job_id = None
        self._run_id = None
        self._sessionId = str(uuid.uuid4())

        Logger.getInstance().writeInfo(f"Initialized JobRunSession with sessionId: {self._sessionId}")

        # Start time (UTC)
        self._start_time_utc = datetime.datetime.utcnow()

        # Persisted dataframe references (so we can unpersist on exit)
        self._persisted_dfs = []  # type: ignore

        # Collect databricks context and initialize
        self._collect_databricks_context()
        self._init_error_handling()
        self._isExitedAlready = False

    # -------------------------
    # Databricks context utils
    # -------------------------
    @staticmethod
    def _safe_tag(scala_map, key: str) -> str | None:
        """Extract a value from a Scala ``Map`` via Py4J.

        Uses ``getOrElse(key, "")`` which works on all Scala Map
        implementations.  An empty-string default is required because
        Scala's by-name parameter crashes with Python ``None`` (JVM null)
        when the key is absent.

        Falls back to ``get(key).isDefined/get`` if ``getOrElse`` is
        unavailable.
        """
        # Approach 1: getOrElse with "" default (safe for missing keys)
        try:
            val = scala_map.getOrElse(key, "")
            return str(val) if val else None
        except Exception:
            pass
        # Approach 2: get() returns Scala Option, check isDefined
        try:
            opt = scala_map.get(key)
            if opt.isDefined():
                return str(opt.get())
        except Exception:
            pass
        return None

    def _collect_databricks_context(self) -> None:
        """
        Attempts to collect workspace, cluster id, job and run ids
        from Databricks runtime context.
        """
        try:
            # Prefer dbutils context if available
            try:
                # dbutils access inside libraries: use entry_point
                dbutils = DatabricksHelper.getDBUtils()

                if dbutils:
                    ctx = None
                    try:
                        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
                    except Exception:
                        # Some environments expose tags() directly
                        try:
                            ctx = dbutils.notebook.getContext()
                        except Exception:
                            ctx = None

                    if ctx:
                        tags = ctx.tags()
                        self._job_id = (
                            self._safe_tag(tags, "jobId")
                            or self._safe_tag(tags, "job_id")
                        )
                        self._run_id = (
                            self._safe_tag(tags, "runId")
                            or self._safe_tag(tags, "run_id")
                        )
                        self._workspace = (
                            self._safe_tag(tags, "browserHostName")
                            or self._safe_tag(tags, "workspaceUrl")
                        )
                        Logger.getInstance().writeInfo(
                            f"[DatabricksContext] job_id={self._job_id!r}, "
                            f"run_id={self._run_id!r}, workspace={self._workspace!r}"
                        )
                # Try spark.conf fallbacks
            except Exception as _ctx_ex:
                Logger.getInstance().writeException(
                    _ctx_ex, "[DatabricksContext] dbutils context failed"
                )

            try:
                spark = DatabricksHelper.getSparkSession()
                conf = spark.sparkContext.getConf()  # type: ignore
                # Common spark conf keys in Databricks
                self._spark_cluster_id = (
                    conf.get("spark.databricks.clusterUsageTags.clusterId", None)
                    or conf.get("spark.databricks.clusterId", None)
                )
                if not self._workspace:
                    self._workspace = (
                        conf.get("spark.databricks.workspaceUrl", None)
                        or conf.get("spark.databricks.workspaceHostname", None)
                    )
            except Exception:
                # If Spark isn't available, leave values as None
                pass
        except Exception as ex:
            # Use existing Logger API (production Logger lacks 'exception')
            Logger.getInstance().writeException(ex, "Failed to collect Databricks context")

    # -------------------------
    # Catch unhandled exceptions globally
    # -------------------------

    # Fired before each cell
    def _pre_run_cell(self, info):
        self.cell_start = datetime.datetime.now()
        # info.raw_cell is present on modern IPython
        self.cell_source = getattr(info, "raw_cell", None)
        self.cell_id = getattr(info, "cell_id", None)

    # Fired after each cell
    def _post_run_cell(self, result):
        err = getattr(result, "error_in_exec", None) or getattr(result, "error_before_exec", None)
        if err is None:
            return
        tb_text = "".join(traceback.format_exception(type(err), err, err.__traceback__))

        try:
            self.exitJobRun(JobRunExitMode.FAILURE, f"Uncaught exception in cell ({self.cell_id}): {tb_text}")
        except Exception:
            # Avoid masking the original error if logging fails
            pass

    def exitJobRun(self, mode: JobRunExitMode, message: str) -> None:
        raise NotImplementedError("Subclasses must implement exitJobRun().")

    def _init_error_handling(self) -> None:
        """
        Register a global exception handler that will forward exceptions to Kusto.
        """
        if hasattr(self._ipython, "_dbx_errhook_registered"):
            Logger.getInstance().writeInfo("Databricks error handlers are already registered.")
            return

        Logger.getInstance().writeInfo("Databricks error handlers are not registered. registering them...")

        self._ipython.events.register("pre_run_cell", self._pre_run_cell)
        self._ipython.events.register("post_run_cell", self._post_run_cell)

        # Fallback: also log via a custom exception handler (works across IPython internals)
        def _log_and_reraise(shell, etype, evalue, tb, tb_offset=None):
            try:
                exc_info = "".join(traceback.format_exception(etype, evalue, tb))
                self.exitJobRun(JobRunExitMode.FAILURE, f"Uncaught exception: {exc_info}")
            except Exception:
                pass
            finally:
                return shell.showtraceback((etype, evalue, tb), tb_offset=tb_offset)

        self._interactive_shell.set_custom_exc((Exception,), _log_and_reraise)
        self._ipython._dbx_errhook_registered = True

    def _unregister_error_handlers(self):
        if not hasattr(self._ipython, "_dbx_errhook_registered"):
            Logger.getInstance().writeInfo("Databricks error handlers are not registered.")
            return

        try:
            self._ipython.events.unregister("pre_run_cell", self._pre_run_cell)
            self._ipython.events.unregister("post_run_cell", self._post_run_cell)
        except Exception:
            pass

    def _send_kusto_record(self, record: dict) -> None:
        """
        Send a single record to Kusto. If a kusto_client was provided, this method
        will attempt to use it. Otherwise, fall back to logging to stdout.
        """
        try:
            # Fallback: log the record (Databricks will collect logs)
            Logger.getInstance().writeInfo(f"KustoRecord: {str(record)}")
            if self._kustoConnector is not None:
                self._kustoConnector.publishSingleRecordViaQueuedIngest(record, tableName=self._telemetryTable)
        except Exception as ex:
            Logger.getInstance().writeException(ex, "Failed while sending record to Kusto fallback/logging.")

    # -------------------------
    # DataFrame helpers
    # -------------------------
    def persistDataframe(self, dataframe: DataFrame, persist_mode: StorageLevel = StorageLevel.MEMORY_AND_DISK) -> DataFrame:  # type: ignore
        """
        Persist a Spark DataFrame using the provided persist_mode and remember it
        so it can be unpersisted on exit.

        Persistence remains lazy. Do not probe for emptiness here because that
        would execute the DataFrame before its storage level is registered.

        persist_mode: one of MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, MEMORY_ONLY_SER, etc.
        """
        if dataframe is None:
            Logger.getInstance().writeInfo("DataFrame is None; skipping persist.")
            return dataframe

        try:
            dataframe = dataframe.persist(persist_mode)
            # Keep reference for later unpersist
            self._persisted_dfs.append(dataframe)
            Logger.getInstance().writeInfo(f"Persisted DataFrame with mode {persist_mode}.")
        except Exception as ex:
            Logger.getInstance().writeException(ex, "Failed to persist dataframe")
            raise

        return dataframe

    # -------------------------
    # Exit handling
    # -------------------------
    def exitJobRunSession(self, exit_mode: JobRunExitMode, record: dict, watermark: Watermark) -> None:
        """
        Cleanly exit a job run: send a Kusto record and unpersist persisted dataframes.

        Note: This method does not terminate the process or notebook. It only performs
        cleanup and telemetry. If you want to stop execution, call sys.exit() separately.

        Args:
            exit_mode: JobRunExitMode indicating success or failure
            record: Dictionary representing the Kusto record to send
            watermark: Watermark object containing watermark info
        """
        if record is None:
            raise ValueError("record cannot be None")

        if self._isExitedAlready:
            Logger.getInstance().writeInfo("exitJobRunSession has already been called; skipping duplicate call.")
            return

        # Persist watermark if available
        if exit_mode != JobRunExitMode.FAILURE and exit_mode != JobRunExitMode.NONEWDATARETRY and watermark is not None and watermark.currentWatermarkDateTime is not None:
            try:
                watermark.saveCurrentWatermarkToStorage()
            except Exception as ex:
                Logger.getInstance().writeException(ex, "Failed to persist watermark")
        else:
            Logger.getInstance().writeInfo(f"Skipping watermark persist due to {exit_mode.value} exit mode")

        # Send final telemetry (include elapsed time)
        try:
            elapsed = datetime.datetime.utcnow() - self._start_time_utc
            record['Message'] = f"{record['Message']} (Elapsed: {elapsed})"
            self._send_kusto_record(record)
        except Exception as ex:
            Logger.getInstance().writeException(ex, "Failed sending job finished record")

        # Unpersist dataframes
        for df in self._persisted_dfs:
            try:
                if df is not None:
                    try:
                        df.unpersist()
                    except Exception:
                        # Try persist(False) fallback
                        try:
                            Logger.getInstance().writeInfo("Unpersist failed; trying persist(False) fallback")
                            df.persist(False)
                        except Exception:
                            Logger.getInstance().writeInfo("persist(False) fallback also failed")
                            pass
            except Exception as ex:
                Logger.getInstance().writeException(ex, "Failed to unpersist a dataframe")
        # Clear the list
        self._persisted_dfs.clear()

        # Unregister Databricks error handlers
        self._unregister_error_handlers()

        if exit_mode != JobRunExitMode.FAILURE:
            try:
                if exit_mode == JobRunExitMode.NONEWDATARETRY:
                    message = f'{record.get("Message", "")} - Exiting with mode {exit_mode.value} - No Watermark updated'
                else:
                    message = f'{record.get("Message", "")} - Exiting with mode {exit_mode.value} - Watermark: {watermark.currentWatermarkDateTime}'
                self._isExitedAlready = True
                Utils.getDBUtils().notebook.exit(message)
            except Exception as ex:
                Logger.getInstance().writeException(ex, "Failed calling notebook.exit")

    # -------------------------
    # Expose some read-only properties
    # -------------------------
    @property
    def start_time_utc(self) -> datetime.datetime:
        return self._start_time_utc

    @property
    def job_context(self) -> dict:
        return {
            "workspace": self._workspace,
            "cluster_id": self._spark_cluster_id,
            "job_id": self._job_id,
            "run_id": self._run_id,
        }

    @property
    def watermark(self) -> Any:
        raise NotImplementedError("Subclasses must implement watermark property.")