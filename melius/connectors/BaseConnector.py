from abc import ABC
from melius.libraries.Constants import FileTypes


class ConnectorAuthModes(ABC):
    """
    Enum for authentication types.
    """
    ManagedIdentiy = 1
    InterActive = 2
    Sni = 3


class BaseConnector(ABC):
    _authMode = ConnectorAuthModes.ManagedIdentiy

    @staticmethod
    def setAuthMode(authMode: ConnectorAuthModes):
        """
        Sets the authentication mode for the connector.
        """
        BaseConnector._authMode = authMode

    @staticmethod
    def getAuthMode():
        """
        Returns the authentication mode for the connector.
        """
        return BaseConnector._authMode

    def _getFileTypeFromConfig(self, config: dict, defaultFileType: FileTypes):
        return FileTypes.value_of(config['DefaultFileType']) if 'DefaultFileType' in config else defaultFileType

    def getDefaultFileType(self):
        """
        Returns the default file type.
        """
        return FileTypes.Unknown