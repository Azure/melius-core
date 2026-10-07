from abc import ABC
from melius.libraries.AccessToken import AccessToken


class BaseApiClient(ABC):
    ''' Base Api Client with an Authorization bearer token '''

    def __init__(self, scope, headerBase, config=None):
        """Initialize the API client with auth scope, headers, and optional config.

        Args:
            scope: OAuth scope for token acquisition.
            headerBase: Base headers dict included in every request.
            config: Optional access configuration dict (e.g. accessType,
                tenantId, clientId) forwarded to ``AccessToken`` for
                config-driven authentication. Defaults to ``None``
                (legacy global auth).
        """
        self.headerBase = headerBase
        self._token = AccessToken(scope, config=config)

    def getAccessToken(self):
        return self._token.getAccessToken()

    def getClientId(self):
        return self._token.getClientId()

    def _buildHeaders(self):
        """Creates a request header with a bearer token.

        Returns:
            The request header.
        """

        headers = self.headerBase.copy()

        headers.update({"Authorization": f"Bearer {self._token.getAccessToken()}"})

        return headers
