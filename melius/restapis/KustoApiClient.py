import requests
import time
from urllib.parse import urljoin
from melius.restapis.BaseApiClient import BaseApiClient
from melius.libraries.Logger import Logger


class KustoApiClient(BaseApiClient):
    ''' Kusto Api Caller '''

    maxRetryCount = 3
    backOffTimeInSeconds = 60

    def __init__(self, clusterUrl, database, config=None):
        """Initialize the Kusto API client.

        Args:
            clusterUrl: Full URL of the Kusto cluster (e.g. ``https://mycluster.kusto.windows.net/``).
            database: Name of the Kusto database to issue commands against.
            config: Optional access configuration dict forwarded to
                ``BaseApiClient`` / ``AccessToken`` for config-driven
                authentication. Defaults to ``None`` (legacy global auth).
        """
        self.clusterUrl = clusterUrl
        self.database = database

        headerBase = {
            'Accept': 'application/json',
            'Content-type': 'application/json; charset=utf-8'
            }

        super().__init__('https://kusto.kusto.windows.net/.default', headerBase, config=config)

    def postManagementApi(self, command: str):
        ''' post a command to the Kusto Management Api '''
        body = {
            'db': self.database,
            "csl": command
        }

        return self._post('v1/rest/mgmt', body)

    def postQueryApi(self, query: str):
        body = {
            'db': self.database,
            "csl": query
        }

        return self._post('v2/rest/query', body)

    def _post(self, relativeUrl, body):
        ''' post a command to the Kusto Management Api '''
        retryCount = 0

        url = urljoin(self.clusterUrl, relativeUrl)

        while True:
            headers = self._buildHeaders()

            Logger.getInstance().writeInfo(f'Kusto Cluster: {url}')
            Logger.getInstance().writeInfo(f'Kusto Api call Body: {body}')

            try:
                resp = requests.post(
                    url,
                    headers=headers,
                    json=body,
                    timeout=180)

                if resp.status_code == 200:
                    return resp.json()["Tables"]
                elif resp.status_code == 429:
                    # throttled... and we need to wait for a while.
                    Logger.getInstance().writeInfo(f'Kusto command was throttled...error code: {resp.status_code}')

                    retryCount = retryCount + 1

                    if retryCount <= KustoApiClient.maxRetryCount:
                        waitTime = retryCount * KustoApiClient.backOffTimeInSeconds
                        Logger.getInstance().writeInfo(f'Kusto command retry: {retryCount} and wait in {waitTime} in seconds')
                        time.sleep(waitTime)
                        continue
                    else:
                        raise Exception(f'Fail to retry Kusto command even after {KustoApiClient.maxRetryCount} retries')
                else:
                    raise Exception(f'failed to call Kusto Management API: {resp.status_code} \nerror content: {resp.json()}')

            except requests.exceptions.RequestException as e:
                Logger.getInstance().writeError(f'An error occurred during the Kusto API call(Post): {e}')
                raise
