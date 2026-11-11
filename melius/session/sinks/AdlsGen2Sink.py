from __future__ import annotations
from melius.session.sinks.BaseSink import BaseSink
from melius.connectors.BlobStorageConnector import BlobStorageConnector


class AdlsGen2Sink(BaseSink):
    """
    Base Data Sink class that holds core parameters needed to initialize a Sink object instance.
    it should be inherited by specific Sink implementations.
    """

    def __init__(
        self,
        dataName: str,
        dataVersion: str,
        storageConfig: str,
        storageRootPath: str
    ) -> None:
        """
        Initialize the health model configuration.

        Args:
            modelVersion: Version of the health model (e.g., "1.0.0")
            modelName: Name of the health model
        """
        super().__init__(
            dataVersion=dataVersion,
            dataName=dataName
        )
        self._storageConfig = storageConfig
        self._storageRootPath = storageRootPath

    def getStorageConnector(self) -> BlobStorageConnector:
        """
        Get the BlobStorageConnector for the source data.
        """
        return BlobStorageConnector.getConnector(self._storageConfig)

    def getStorageAbsolutePath(self) -> str:
        """
        Get the base storage absolute path for the source data.
        """
        return self.getStorageConnector().getAbsolutePath(self._storageRootPath)

    @property
    def storageRootPath(self) -> str:
        """
        Get the storage root path for the source data.
        """
        return self._storageRootPath

    @property
    def storageConfig(self) -> str:
        """
        Get the storage configuration name for the source data.
        """
        return self._storageConfig