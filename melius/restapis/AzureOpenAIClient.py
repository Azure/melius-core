import time
from melius.libraries.AccessToken import AccessToken
from melius.libraries.Logger import Logger
from openai import AzureOpenAI


class AzureOpenAIClient(AccessToken):
    ''' Azure OpenAI Client '''

    maxRetryCount = 3
    backOffTimeInSeconds = 60
    EndPoint = "EndPoint"
    Deployment = "Deployment"
    ApiVersion = "ApiVersion"

    def __init__(self, config: dict):

        self._endpoint = config[AzureOpenAIClient.EndPoint]
        self._apiVersion = config.get(AzureOpenAIClient.ApiVersion, "2025-01-01-preview")

        super().__init__('https://cognitiveservices.azure.com/.default')

    def _getAzureOpenAIClient(self) -> AzureOpenAI:
        ''' get the Azure OpenAI client '''

        # create the client
        client = AzureOpenAI(
            azure_endpoint=self._endpoint,
            azure_ad_token_provider=self.getAccessTokenProvider(),
            api_version=self._apiVersion
        )

        return client

    def create_completion_with_retries(
        self,
        messages,
        model,
        max_tokens=10000,
        retries=5,
        initial_delay=3
    ):
        ''' create a completion with retries '''

        delay = initial_delay
        for attempt in range(1, retries + 1):
            try:
                client = self._getAzureOpenAIClient()

                return client.chat.completions.create(
                            model=model,
                            messages=messages,
                            max_tokens=max_tokens,
                            temperature=0,
                            top_p=1,
                            frequency_penalty=0,
                            presence_penalty=0,
                            stop=None,
                            stream=False
                        )
            except Exception as e:
                # 429 from OpenAI ? retry
                if attempt == retries:
                    raise

                if '429' in str(e):
                    Logger.getInstance().writeInfo(f"[Attempt {attempt}] Rate limited. Retrying in {delay}s...")
                else:
                    Logger.getInstance().writeInfo(f"[Attempt {attempt}] Error: {e}. Retrying in {delay}s...")

            time.sleep(delay * attempt)