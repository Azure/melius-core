from __future__ import annotations
from datetime import timedelta, datetime

from melius.session.JobRunSession import JobRunSession


class BaseSource:
    """
    BaseSource is an abstract class for source data handling in VM Watch Health Models.
    """

    def __init__(
        self,
        session: JobRunSession,
        name: str,
        lookBackDelta: timedelta,
        backFillStartDateTime: datetime
    ) -> None:
        """
        Initialize the BaseModelDataSource.

        Args:
            config: Base model data configuration
            vmwatchSession: VMWatchSession instance for context and utilities
        """
        self._lookBackDelta = lookBackDelta
        self._backFillStartDateTime = backFillStartDateTime
        self._name = name
        self._session = session
        self._startDateTime = None  # Initial startDateTime with None. it should be set based on Destination Watermark.

    def setCurrentWatermark(self, startDateTime: datetime):
        """
        Set the current watermark for data extraction.

        Args:
            startDateTime: The start datetime to set as the current watermark.
        """
        self._session.watermark.setCurrentWatermark(startDateTime)

    @property
    def name(self) -> str:
        """
        Get the name of the data source.

        Returns:
            The name of the data source.
        """
        return self._name

    @property
    def lookBackDelta(self) -> timedelta:
        """
        Get the look back delta for data extraction.

        Returns:
            The look back delta.
        """
        return self._lookBackDelta

    @property
    def backFillStartDateTime(self) -> datetime:
        """
        Get the back fill start datetime for data extraction.

        Returns:
            The back fill start datetime.
        """
        return self._backFillStartDateTime

    @property
    def session(self) -> JobRunSession:
        """
        Get the JobRunSession associated with this source.

        Returns:
            The JobRunSession instance.
        """
        return self._session