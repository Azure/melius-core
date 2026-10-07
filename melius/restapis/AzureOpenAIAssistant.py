import time
import traceback
import json
from melius.libraries.AccessToken import AccessToken
from melius.libraries.Logger import Logger
from openai import AzureOpenAI
import re


class AzureOpenAIAssistant(AccessToken):
    '''
    This class is deprecated. we will develop a new version with ResponseAPI.
    Azure OpenAI Assistant
    Uses Azure OpenAI to create and manage assistants, threads, and messages.
    It is thread safe.
    '''

    EndPoint = "EndPoint"
    ApiVersion = "ApiVersion"
    Model = "Model"

    def __init__(self, config: dict, notebook_name: str, search_file_name: str, instructions: str):
        super().__init__('https://cognitiveservices.azure.com/.default')

        self._endpoint = config[AzureOpenAIAssistant.EndPoint]
        self._apiVersion = config.get(AzureOpenAIAssistant.ApiVersion, "2025-01-01-preview")
        self._model = config[AzureOpenAIAssistant.Model]  # replace with model name e.g., "gpt-4o-mini"
        self._client = self._getOrCreateAzureOpenAiAssistantClient()
        self._assistant = self._getOrCreateAzureOpenAIAssistant(
            notebook_name=notebook_name,
            search_file_name=search_file_name,
            instructions=instructions
        )

    def _getOrCreateAzureOpenAiAssistantClient(self) -> AzureOpenAI:
        ''' get the Azure OpenAI GLOBAL_CLIENT '''
        if not hasattr(self, '_client') or self._client is None:
            Logger.getInstance().writeInfo("Creating new Azure OpenAI client")
            return AzureOpenAI(
                azure_endpoint=self._endpoint,
                azure_ad_token_provider=self.getAccessTokenProvider(),
                api_version=self._apiVersion
            )

        Logger.getInstance().writeInfo("Reusing existing Azure OpenAI client")
        return self._client

    def _getOrCreateAzureOpenAIAssistant(self, notebook_name, search_file_name, instructions):
        ''' get or create the Azure OpenAI assistant '''
        assistants = self._client.beta.assistants.list()
        assistant = None
        # The number of assistants is expected to be small, so this approach is acceptable.
        assistant = None
        for a in assistants.data:
            if a.name == notebook_name:
                assistant = a
                break

        if assistant is not None:
            return assistant

        # if assistant does not exist
        # create the assistant, file and vector_ store
        # Upload the user provided file to OpenAI
        assistant = self._client.beta.assistants.create(
            name=notebook_name,
            model=self._model,
            instructions=instructions,
            temperature=0.01,
            top_p=1
        )

        if search_file_name is not None:

            with open(search_file_name, "rb") as file:
                search_file = self._client.files.create(
                    file=file, purpose="assistants"
                )

            vector_store = self._client.vector_stores.create(
                name=notebook_name,
                file_ids=[search_file.id]
            )

            assistant = self._client.beta.assistants.update(
              assistant_id=assistant.id,
              tools=[{"type": "file_search"}],
              tool_resources={"file_search": {"vector_store_ids": [vector_store.id]}}
            )

        return assistant

    def run_assistant_with_retries(
        self,
        user_message,
        retries=5,
        initial_delay=3
    ):
        ''' Run the assistant with retries on failure '''
        delay = initial_delay

        thread = self._client.beta.threads.create()

        # Add a user question to the thread
        _ = self._client.beta.threads.messages.create(
            thread_id=thread.id,
            role="user",
            content=user_message
        )

        retVal = None

        for attempt in range(1, retries + 1):
            # Clear out return value on each iteration
            retVal = None
            suggested_delay = 0

            try:
                run = self._client.beta.threads.runs.create_and_poll(
                    thread_id=thread.id,
                    assistant_id=self._assistant.id
                )

                Logger.getInstance().writeInfo(f"Run Finished. Status: [{run.status}]")

                self.processRunStatus(run, attempt, user_message)

                messages = list(self._client.beta.threads.messages.list(thread_id=thread.id, run_id=run.id))

                message_content = messages[0].content[0].text

                annotations = message_content.annotations
                citations = []
                for index, annotation in enumerate(annotations):
                    message_content.value = message_content.value.replace(annotation.text, f"[{index}]")
                    if file_citation := getattr(annotation, "file_citation", None):
                        cited_file = self._client.files.retrieve(file_citation.file_id)
                        citations.append(f"[{index}] {cited_file.filename}")

                retVal = message_content.value
                return retVal
            except Exception as e:
                Logger.getInstance().writeInfo(f"[Attempt: {attempt}], Exception: {e}, Retrying in: {delay * attempt}s ")
                Logger.getInstance().writeInfo(traceback.format_exc())
                if attempt >= retries:
                    trace_back_text = traceback.print_exc()
                    Logger.getInstance().writeException(ex=e, message=f"[Attempt: {attempt}], Exception: {e}, Traceback: {trace_back_text}")
                    raise
                else:
                    # Check if the exception message contains a retry delay (expect integer seconds)
                    exMsg = str(e)
                    m = re.search(r"Try again in\s*([0-9]+)\s*seconds?", exMsg, re.IGNORECASE)
                    if m:
                        try:
                            suggested_delay = int(m.group(1))
                            Logger.getInstance().writeInfo(f"AI end point suggest to delay: {suggested_delay}s")
                        except Exception:
                            Logger.getInstance().writeInfo(f"Failed to parse retry delay from message: {exMsg}")

            suggested_delay = max(delay * attempt, suggested_delay)
            Logger.getInstance().writeInfo(f"Waiting for {suggested_delay}s before next retry")
            time.sleep(suggested_delay)

        return retVal

    def processRunStatus(self, run, attempt, user_message):
        if run.status != "completed":
            failure_error = run.last_error.code + " - " + run.last_error.message
            run_status = f"[Attempt: {attempt}], [User Message: {json.loads(user_message)}], [Run Status: {run.status}], [ErrorMessage: {failure_error}]"
            Logger.getInstance().writeError(run_status)
            raise Exception(f"{run_status}")
