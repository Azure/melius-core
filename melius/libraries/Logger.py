# Databricks notebook source
import json
from datetime import datetime, timezone
from typing import Any, Optional
import traceback


class Logger(object):
    """Log info with optional Kusto sink.

    By default, all log output goes to stdout only (existing behavior).
    Call ``enableKustoSink()`` to additionally push every log message
    to a Kusto table via ``KustoConnector.publishSingleRecordViaQueuedIngest``.
    """
    _instance = None
    _debugMode = False

    @staticmethod
    def getInstance(outputPath: str=None):
        if Logger._instance is None:
            Logger._instance = Logger()

        return Logger._instance

    def __init__(self):
        """ Initialize Logger class """
        self._kusto_connector = None
        self._kusto_table: str = ""
        self._kusto_context: dict[str, str] = {}
        self._sending_to_kusto: bool = False  # re-entrancy guard

    # ------------------------------------------------------------------
    # Kusto sink configuration
    # ------------------------------------------------------------------
    def enableKustoSink(
        self,
        kusto_connector: Any,
        table_name: str,
        context: Optional[dict[str, str]] = None,
    ) -> None:
        """Enable Kusto ingestion for every log message.

        Args:
            kusto_connector: A ``KustoConnector`` instance used for ingestion.
            table_name: Target Kusto table name (e.g. ``"PrismJobLogs"``).
            context: Optional dict of extra fields to include in every record
                (e.g. ``SessionId``, ``JobName``, ``Environment``, etc.).
                These are merged into each Kusto row.
        """
        self._kusto_connector = kusto_connector
        self._kusto_table = table_name
        self._kusto_context = context or {}
        self._writeLog("Info", f"Kusto logging sink enabled -> table: {table_name}")

    def disableKustoSink(self) -> None:
        """Turn off the Kusto sink (revert to stdout-only)."""
        self._kusto_connector = None
        self._kusto_table = ""
        self._kusto_context = {}

    def updateKustoContext(self, context: dict[str, str]) -> None:
        """Merge additional fields into the Kusto context.

        Useful for updating context mid-session (e.g. after ``beginSession``
        populates the watermark/data details).
        """
        self._kusto_context.update(context)

    @property
    def isKustoSinkEnabled(self) -> bool:
        """Return True if the Kusto sink is active."""
        return self._kusto_connector is not None

    # ------------------------------------------------------------------
    # Public logging methods (unchanged signatures)
    # ------------------------------------------------------------------
    def writeInfo(self, message: str):
        """Write a Info message
        """
        self._writeLog("Info", message)

    def writeWarning(self, message: str):
        """Write a warning message
        """
        self._writeLog("Warning", message)

    def writeError(self, message: str):
        """Write an Error message
        """
        self._writeLog("Error", message)

    def writeException(self, ex: Exception, message: str):
        """Write an Exception message
        """
        self._writeLog("Exception", f"Exception: {ex}\r\nMessage: {message}")
        if Logger._debugMode:
            raise ex

    def logBeginOfMethod(self):
        '''
        Record the beginning of a method
        '''
        lst = traceback.extract_stack(None, 2)
        self.writeInfo(f'The beginning of {lst[0][2]} at {lst[0]}')

    def logEndOfMethod(self):
        '''
        Record the end of a method
        '''
        lst = traceback.extract_stack(None, 2)
        self.writeInfo(f'The end of {lst[0][2]} at {lst[0]}')

    def _writeLog(self, logType: str, message: str):
        """Write a Log line
        """
        raw_message = message  # preserve original before formatting for Kusto
        try:
            message = '{}:[{}] {}'.format(datetime.utcnow(), logType, message)
            print(message, flush=True)
        except Exception as e:
            # it should not break the operation.
            # so handle exception here.
            print(str(e), flush=True)

        # Kusto sink (fire-and-forget) - pass raw message, not the formatted one
        if self.isKustoSinkEnabled:
            self._sendToKusto(logType, raw_message)

    def _sendToKusto(self, level: str, message: str) -> None:
        """Build a record from the context and ingest into Kusto.

        The Logger is schema-agnostic - the field names come entirely
        from the context dict provided by the caller (e.g. PrismLogRecord).
        Logger only sets PreciseTimeStamp, Level, and Message per call.
        Failures are swallowed so logging never breaks the running job.

        A re-entrancy guard prevents infinite loops when KustoConnector
        internally calls Logger methods during ingestion.
        """
        if self._sending_to_kusto:
            return  # prevent infinite recursion

        try:
            self._sending_to_kusto = True
            # Start with a copy of the context (field names enforced by caller)
            record = dict(self._kusto_context)
            # Override per-call fields
            record["PreciseTimeStamp"] = datetime.now(timezone.utc).replace(tzinfo=None).isoformat() + "Z"
            record["Level"] = level.upper()
            record["Message"] = message

            self._kusto_connector.publishSingleRecordViaQueuedIngest(
                record, tableName=self._kusto_table
            )
        except Exception as ex:
            # Never break the running job because of logging.
            # Print to stdout only (cannot recurse into Kusto sink here).
            print(f"[Logger] Failed to send log to Kusto: {ex}", flush=True)
        finally:
            self._sending_to_kusto = False

    def convertDictToJson(self, dict: dict):
        '''
        convert a dictionary to Json format
        '''
        return json.dumps(dict, indent=4)