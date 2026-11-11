"""Unit tests for Logger Kusto sink functionality.

Tests cover enableKustoSink, disableKustoSink, updateKustoContext,
isKustoSinkEnabled, _sendToKusto, re-entrancy guard, and raw message
preservation in _writeLog.
"""
import unittest.mock as mock
import pytest

from melius.libraries.Logger import Logger


@pytest.fixture(autouse=True)
def reset_logger_singleton():
    """Reset Logger singleton before each test to avoid cross-test pollution."""
    Logger._instance = None
    yield
    Logger._instance = None


class TestEnableKustoSink:

    def test_enable_sets_connector_and_table(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "TestTable")
        assert logger._kusto_connector is connector
        assert logger._kusto_table == "TestTable"

    def test_enable_sets_context(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        ctx = {"SessionId": "abc", "JobName": "testjob"}
        logger.enableKustoSink(connector, "TestTable", context=ctx)
        assert logger._kusto_context["SessionId"] == "abc"
        assert logger._kusto_context["JobName"] == "testjob"

    def test_enable_with_none_context_defaults_to_empty(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "TestTable", context=None)
        assert logger._kusto_context == {}

    def test_enable_logs_info_message(self, capsys):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "MyTable")
        captured = capsys.readouterr()
        assert "Kusto logging sink enabled -> table: MyTable" in captured.out


class TestDisableKustoSink:

    def test_disable_clears_connector(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "TestTable", context={"key": "val"})
        logger.disableKustoSink()
        assert logger._kusto_connector is None
        assert logger._kusto_table == ""
        assert logger._kusto_context == {}

    def test_disable_stops_kusto_push(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "TestTable")
        connector.reset_mock()

        logger.disableKustoSink()
        logger.writeInfo("should not go to kusto")
        connector.publishSingleRecordViaQueuedIngest.assert_not_called()


class TestIsKustoSinkEnabled:

    def test_disabled_by_default(self):
        logger = Logger.getInstance()
        assert logger.isKustoSinkEnabled is False

    def test_enabled_after_enable(self):
        logger = Logger.getInstance()
        logger.enableKustoSink(mock.Mock(), "T")
        assert logger.isKustoSinkEnabled is True

    def test_disabled_after_disable(self):
        logger = Logger.getInstance()
        logger.enableKustoSink(mock.Mock(), "T")
        logger.disableKustoSink()
        assert logger.isKustoSinkEnabled is False


class TestUpdateKustoContext:

    def test_update_merges_fields(self):
        logger = Logger.getInstance()
        logger.enableKustoSink(mock.Mock(), "T", context={"A": "1"})
        logger.updateKustoContext({"B": "2"})
        assert logger._kusto_context == {"A": "1", "B": "2"}

    def test_update_overwrites_existing_key(self):
        logger = Logger.getInstance()
        logger.enableKustoSink(mock.Mock(), "T", context={"A": "1"})
        logger.updateKustoContext({"A": "updated"})
        assert logger._kusto_context["A"] == "updated"


class TestSendToKusto:

    def test_publishes_record_with_correct_fields(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        ctx = {"SessionId": "s1", "Environment": "dev", "JobName": "job1"}
        logger.enableKustoSink(connector, "PrismJobLogs", context=ctx)
        connector.reset_mock()  # clear the enableKustoSink call

        logger.writeInfo("hello world")

        connector.publishSingleRecordViaQueuedIngest.assert_called_once()
        call_args = connector.publishSingleRecordViaQueuedIngest.call_args
        record = call_args[0][0] if call_args[0] else call_args[1].get("record", call_args[0][0])

        assert record["Level"] == "INFO"
        assert record["Message"] == "hello world"
        assert record["SessionId"] == "s1"
        assert record["Environment"] == "dev"
        assert record["JobName"] == "job1"
        assert "PreciseTimeStamp" in record
        assert record["PreciseTimeStamp"].endswith("Z")

    def test_warning_level_is_uppercase(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "T")
        connector.reset_mock()

        logger.writeWarning("warn msg")

        record = connector.publishSingleRecordViaQueuedIngest.call_args[0][0]
        assert record["Level"] == "WARNING"
        assert record["Message"] == "warn msg"

    def test_error_level_is_uppercase(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "T")
        connector.reset_mock()

        logger.writeError("err msg")

        record = connector.publishSingleRecordViaQueuedIngest.call_args[0][0]
        assert record["Level"] == "ERROR"

    def test_table_name_passed_correctly(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "MyCustomTable")
        connector.reset_mock()

        logger.writeInfo("test")

        call_kwargs = connector.publishSingleRecordViaQueuedIngest.call_args
        assert call_kwargs[1]["tableName"] == "MyCustomTable"

    def test_raw_message_not_formatted(self):
        """_writeLog should pass original message to Kusto, not the stdout-formatted one."""
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "T")
        connector.reset_mock()

        logger.writeInfo("clean message")

        record = connector.publishSingleRecordViaQueuedIngest.call_args[0][0]
        # Should be the raw message, not "timestamp:[Info] clean message"
        assert record["Message"] == "clean message"
        assert ":[Info]" not in record["Message"]

    def test_no_push_when_sink_disabled(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.writeInfo("no sink")
        connector.publishSingleRecordViaQueuedIngest.assert_not_called()


class TestReentrancyGuard:

    def test_recursive_call_is_blocked(self):
        """When KustoConnector.publish internally calls Logger, it should not recurse."""
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")

        call_count = 0

        def side_effect_publish(record, tableName=None):
            nonlocal call_count
            call_count += 1
            # Simulate KustoConnector internally calling Logger
            logger.writeInfo("internal log from connector")

        connector.publishSingleRecordViaQueuedIngest.side_effect = side_effect_publish
        logger.enableKustoSink(connector, "T")
        connector.reset_mock()
        call_count = 0

        logger.writeInfo("trigger")

        # publish should be called exactly once (the re-entrant call is blocked)
        assert call_count == 1

    def test_guard_resets_after_call(self):
        """After a successful send, the guard should be reset for the next call."""
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        logger.enableKustoSink(connector, "T")
        connector.reset_mock()

        logger.writeInfo("first")
        logger.writeInfo("second")

        assert connector.publishSingleRecordViaQueuedIngest.call_count == 2

    def test_guard_resets_on_exception(self):
        """If publish raises, the guard should still reset."""
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        connector.publishSingleRecordViaQueuedIngest.side_effect = RuntimeError("boom")
        logger.enableKustoSink(connector, "T")
        connector.reset_mock()
        connector.publishSingleRecordViaQueuedIngest.side_effect = RuntimeError("boom")

        logger.writeInfo("should not crash")  # should not raise
        # Guard should be reset, so next call should attempt again
        logger.writeInfo("also fine")
        assert connector.publishSingleRecordViaQueuedIngest.call_count == 2


class TestExceptionSwallowing:

    def test_publish_exception_does_not_raise(self):
        logger = Logger.getInstance()
        connector = mock.Mock(name="KustoConnector")
        connector.publishSingleRecordViaQueuedIngest.side_effect = Exception("kusto down")
        logger.enableKustoSink(connector, "T")
        connector.reset_mock()
        connector.publishSingleRecordViaQueuedIngest.side_effect = Exception("kusto down")

        # Should NOT raise
        logger.writeInfo("this should be fine")
        logger.writeWarning("also fine")
        logger.writeError("still fine")