from __future__ import annotations
from datetime import datetime

from melius.libraries.Logger import Logger
from melius.session.sinks.AdlsGen2Sink import AdlsGen2Sink
from melius.libraries.WatermarkHelper import WatermarkHelper


class Watermark(AdlsGen2Sink):
    """
    Watermark handles writing data to Watermark Delta Lake storage.
    """

    def __init__(
        self,
        dataName: str,
        dataVersion: str,
        storageConfig: str,
        watermarkPath: str,
        watermarkOverride: datetime = None
    ) -> None:
        """
        Initialize the DeltaLake sink object.

        Args:
            dataName: Name of the data
            dataVersion: Version of the data (e.g., "1.0.0")
            storageConfigName: Name of the storage configuration to use
            watermarkPath: Path for storing watermark data
            watermarkOverride: Optional override for the value of the previous watermark
        """
        super().__init__(
            dataName=dataName,
            dataVersion=dataVersion,
            storageConfig=storageConfig,
            storageRootPath=watermarkPath
        )

        self._currentWatermarkDateTime: datetime = None

        if watermarkOverride is not None:
            Logger.getInstance().writeInfo(f"Using watermark override for the previous Watermark: {watermarkOverride}")
            self._previousWatermarkDateTime = watermarkOverride
        else:
            self._previousWatermarkDateTime = None

    def setCurrentWatermark(self, watermarkDateTime: datetime):
        """
        Set the current watermark.

        Args:
            watermarkDateTime: Watermark datetime value to set
        """
        Logger.getInstance().writeInfo(f"Setting current watermark of {self.dataName}_{self.dataVersion} to: {watermarkDateTime}")

        if self._currentWatermarkDateTime is not None:
            raise RuntimeError("Overwriting existing current watermark value is not allowed")

        self._currentWatermarkDateTime = watermarkDateTime

    def saveCurrentWatermarkToStorage(self) -> None:
        """
        Save the current watermark to storage.
        """
        if self._currentWatermarkDateTime is None:
            raise ValueError("Current watermark is not set. Cannot save to storage.")

        Logger.getInstance().writeInfo(f"Saving current watermark({self.currentWatermarkDateTime}) to storage: {self.watermarkPath}")

        WatermarkHelper.saveWatermark(
            self.getStorageConnector(),
            self.watermarkPath,
            self.dataName,
            self.dataVersion,
            self.currentWatermarkDateTime
        )

    @property
    def watermarkPath(self) -> str:
        """
        Get the watermark Delta Lake path.
        """
        return self._storageRootPath

    @property
    def currentWatermarkDateTime(self) -> datetime:
        """
        Get the current watermark datetime.
        """
        return self._currentWatermarkDateTime

    @property
    def previousWatermarkDateTime(self) -> str:
        """
        Get the previous watermark datetime from storage.
        """

        if self._previousWatermarkDateTime is None:
            Logger.getInstance().writeInfo(f"Fetching previous watermark from storage: {self.watermarkPath}")
            self._previousWatermarkDateTime = WatermarkHelper.getWatermarkDateTime(self.getStorageConnector(),
                                                                                   self.watermarkPath,
                                                                                   self.dataName,
                                                                                   self.dataVersion)
        else:
            Logger.getInstance().writeInfo("Using the cached previous watermark")

        Logger.getInstance().writeInfo(f"Previous watermark is: {self._previousWatermarkDateTime}")
        return self._previousWatermarkDateTime