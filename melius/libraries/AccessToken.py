from abc import ABC
from azure.identity import DefaultAzureCredential, get_bearer_token_provider, ManagedIdentityCredential, CertificateCredential, ClientAssertionCredential
from melius.libraries.Logger import Logger
from melius.libraries.DatabricksHelper import DatabricksHelper
from melius.libraries.Constants import KnownStrings
import jwt


class AccessToken(ABC):
    global_config = None
    defaultScope = "https://management.azure.com/.default"

    @staticmethod
    def setAccessConfig(config: dict):
        '''
        Set the configuration for the AccessToken class.
        this will be used as the default configuration for all instances of AccessToken.
        :param config: The configuration dictionary.
        '''
        if config is None or not isinstance(config, dict):
            raise ValueError('Invalid configuration provided for AccessToken. It must be a dictionary.')

        Logger.getInstance().writeInfo('Setting global configuration for AccessToken.')
        AccessToken.global_config = config

    @staticmethod
    def getSafeScope(scope: str=None):
        '''
        Get a safe scope for the access token.
        If the scope is None, it will return the default scope.
        :param scope: The scope for the access token.
        :return: The safe scope.
        '''
        if scope is None:
            return AccessToken.defaultScope
        return scope if scope.endswith('.default') else f"{scope}/.default"

    ''' Base Api Client with an Authorization bearer token '''
    def __init__(self, scope=None, config: dict=None):
        '''
          Constructor for the AccessToken class.
          :param scope: The scope for the access token.
          :param config: The configuration dictionary.
        '''
        self._scope = scope
        self._response = None

        if config is None:
            if AccessToken.global_config is None:
                Logger.getInstance().writeInfo('No identity configuration provided for AccessToken. Using DefaultAzureCredential.')
                self._config = None
            else:
                Logger.getInstance().writeInfo('Using global configuration for AccessToken.')
                self._config = AccessToken.global_config
        else:
            Logger.getInstance().writeInfo('Using provided configuration for AccessToken')
            self._config = config

    def getAccessToken(self):
        '''
        get an access token
        '''

        self._accessToken = self._refreshADToken()

        return self._accessToken

    def getCredential(self):
        '''
            get an access token credential
        '''
        credential = self._getCredential()

        self._printAccountInfo(credential)

        return credential

    def getTenantIdAndClientId(self) -> tuple:
        '''
            get the tenant ID and client ID from the access token
        '''
        credential = self._getCredential()

        return self._printAccountInfo(credential)

    def _getCredential(self):
        '''
        Get the credential based on the configuration.
        for internal use only.
        :return: The credential for the access token.
        '''
        credential = None

        if not hasattr(self, '_config') or self._config is None:
            Logger.getInstance().writeInfo('Using DefaultAzureCredential for access token.')
            credential = DefaultAzureCredential()  # CodeQL [SM05139] Okay use of DefaultAzureCredential since this code is running in Azure Databricks. it uses the dbxmanagedidentity to get the access token.
        elif KnownStrings.AccessType in self._config:
            if self._config[KnownStrings.AccessType] == KnownStrings.AccessType_MSI and KnownStrings.Config_ClientId in self._config:
                Logger.getInstance().writeInfo('Using ManagedIdentityCredential for access token.')
                credential = ManagedIdentityCredential(client_id=self._config[KnownStrings.Config_ClientId])
            elif self._config[KnownStrings.AccessType] == KnownStrings.AccessType_AppId:
                Logger.getInstance().writeInfo('Using CertificateCredential for access token.')
                pem_string = DatabricksHelper.getDBUtils().secrets.get(
                    self._config[KnownStrings.Config_Scope],
                    self._config[KnownStrings.Config_Cert])
                pem_bytes = pem_string.replace("\\n", "\n").encode()
                credential = CertificateCredential(
                    tenant_id=self._config[KnownStrings.Config_TenantId],
                    client_id=self._config[KnownStrings.Config_ClientId],
                    certificate_data=pem_bytes,
                    send_certificate_chain=True)  # for SNI
            elif self._config[KnownStrings.AccessType] == KnownStrings.AccessType_FIC:
                Logger.getInstance().writeInfo('Using FIC credential (MI -> App Registration token exchange).')
                fic_tenant_id = self._config[KnownStrings.Config_TenantId]
                fic_client_id = self._config[KnownStrings.Config_ClientId]
                mi_credential = ManagedIdentityCredential()
                credential = ClientAssertionCredential(
                    tenant_id=fic_tenant_id,
                    client_id=fic_client_id,
                    func=lambda: mi_credential.get_token(AccessToken.getSafeScope("api://AzureADTokenExchange")).token,
                )
            else:
                raise ValueError(f'Invalid access configuration: {str(self._config)}')
        else:
            raise ValueError(f'Invalid configuration for access token: {self._config}')
        return credential

    def _printAccountInfo(self, credential) -> tuple:
        '''
            Print account information for the credential.
        '''
        token = credential.get_token(AccessToken.getSafeScope(self._scope)).token          # signed JWT

        decoded_token = jwt.decode(token, options={"verify_signature": False})

        tenant_id = decoded_token["tid"]                     # ? your tenant GUID
        client_id = decoded_token.get("azp") or decoded_token.get("appid")                   # ? your client ID
        Logger.getInstance().writeInfo(f'the used Client Info: TenantId: {tenant_id}, ClientId: {client_id}')
        return tenant_id, client_id

    def getAccessTokenProvider(self):
        '''
            get an access token provider
        '''

        return get_bearer_token_provider(
            self.getCredential(),
            AccessToken.getSafeScope(self._scope)
        )

    def getClientId(self):
        '''
            get the client ID from the access token
        '''
        self._refreshADToken()

        return self._clientId

    def _refreshADToken(self):
        """Refreshes the bearer token if needed.

        Raises:
            Exception -- If an bearer token cannot be acquired.
        """
        Logger.getInstance().writeInfo('Renewing credentials for Rest API call')

        try:
            credential = self.getCredential()

            response = credential.get_token(AccessToken.getSafeScope(self._scope))
            decoded_token = jwt.decode(response.token, options={"verify_signature": False})
            self._clientId = decoded_token.get("appid")
            return response.token
        except Exception:
            Logger.getInstance().writeError('Failed to renew client credentials.')
            raise
