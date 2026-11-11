import types
import sys
import pytest


def test_AzureOpenAIClient_create_completion_with_retries_success(mocker):
    # Seed a minimal openai module for import time
    if "openai" not in sys.modules:
        m = types.ModuleType("openai")

        class AzureOpenAI:  # pragma: no cover - will be patched below
            pass
        m.AzureOpenAI = AzureOpenAI
        sys.modules["openai"] = m
    from melius.restapis.AzureOpenAIClient import AzureOpenAIClient

    # Fake AzureOpenAI with a simple return
    class FakeCompletions:
        def create(self, **kwargs):
            return {"ok": True, "kwargs": kwargs}

    class FakeChat:
        completions = FakeCompletions()

    class FakeClient:
        chat = FakeChat()

    # Patch AzureOpenAI constructor to return FakeClient
    mocker.patch("melius.restapis.AzureOpenAIClient.AzureOpenAI", return_value=FakeClient())
    # Avoid real token provider, the client will not use it directly in test
    mocker.patch("melius.libraries.AccessToken.AccessToken.getAccessTokenProvider", return_value=lambda: "token")

    client = AzureOpenAIClient({"EndPoint": "https://example", "ApiVersion": "2025-01-01-preview"})
    res = client.create_completion_with_retries(messages=[{"role": "user", "content": "hi"}], model="gpt-test")
    assert res["ok"] is True


def test_AzureOpenAIAssistant_run_assistant_with_retries_happy_path(mocker):
    # Seed a minimal openai module for import time
    if "openai" not in sys.modules:
        m = types.ModuleType("openai")

        class AzureOpenAI:  # pragma: no cover - will be patched below
            pass
        m.AzureOpenAI = AzureOpenAI
        sys.modules["openai"] = m
    from melius.restapis.AzureOpenAIAssistant import AzureOpenAIAssistant

    # Build a fake AzureOpenAI client surface used by the assistant code path
    class _Obj:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    class FakeMessagesAPI:
        def create(self, thread_id, role, content):
            return _Obj(id="msg1")

        def list(self, thread_id, run_id):
            return FakeMessagesList()

    class FakeThreadsAPI:
        def __init__(self):
            self.messages = FakeMessagesAPI()

        def create(self):
            return _Obj(id="thread1")

        def runs(self):
            raise NotImplementedError

    class FakeRunsAPI:
        def create_and_poll(self, thread_id, assistant_id):
            return _Obj(status="completed")

    class FakeAssistantsAPI:
        def list(self):
            return _Obj(data=[])

        def create(self, **kwargs):
            return _Obj(id="asst1")

        def update(self, assistant_id, tools, tool_resources):
            return _Obj(id=assistant_id)

    class FakeFilesAPI:
        def create(self, file, purpose):
            return _Obj(id="file1")

        def retrieve(self, file_id):
            return _Obj(filename="doc.txt")

    class FakeVectorStoresAPI:
        def create(self, name, file_ids):
            return _Obj(id="vec1")

    class FakeClient:
        def __init__(self):
            self.beta = _Obj(
                assistants=FakeAssistantsAPI(),
                threads=_Obj(create=lambda: _Obj(id="thread1"), messages=FakeMessagesAPI(), runs=FakeRunsAPI())
            )
            self.files = FakeFilesAPI()
            self.vector_stores = FakeVectorStoresAPI()

        class beta:
            pass

    class FakeText:
        annotations = []
        value = 'hello'

    # Messages listing return structure
    class FakeMessageContent:
        text = FakeText()

    class FakeMessagesList:
        def __iter__(self):
            return iter([_Obj(content=[FakeMessageContent()])])

    class FakeRun:
        status = "completed"
        id = "run1"

    # Patch the client creation and message list
    mocker.patch("melius.restapis.AzureOpenAIAssistant.AzureOpenAI", return_value=FakeClient())
    mocker.patch("melius.libraries.AccessToken.AccessToken.getAccessTokenProvider", return_value=lambda: "token")

    clientMock = mocker.MagicMock()
    clientMock.beta.threads.runs.create_and_poll = mocker.MagicMock(return_value=FakeRun())
    clientMock.beta.threads.messages = FakeMessagesAPI()
    mocker.patch(
        "melius.restapis.AzureOpenAIAssistant.AzureOpenAIAssistant._getOrCreateAzureOpenAiAssistantClient",
        return_value=clientMock
    )

    asst = AzureOpenAIAssistant({"EndPoint": "https://example", "Model": "gpt-test"}, "nb", None, "instr")
    out = asst.run_assistant_with_retries("{\"x\":1}", retries=1)
    assert out == "hello"


def test_AzureOpenAIAssistant_processRunStatus_raises_on_failure():
    from melius.restapis.AzureOpenAIAssistant import AzureOpenAIAssistant

    class Run:
        status = "failed"
        last_error = types.SimpleNamespace(code="X", message="boo")

    with pytest.raises(Exception):
        AzureOpenAIAssistant({"EndPoint": "https://example", "Model": "m"}).processRunStatus(
            Run(), 1, "{}"
        )