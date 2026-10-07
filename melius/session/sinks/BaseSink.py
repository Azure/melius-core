from __future__ import annotations
from pyspark.sql import DataFrame
from typing import Any

from melius.libraries.Utility import Utils


class BaseSink:
    """
    Base Data Sink class that holds core parameters needed to initialize a Sink object instance.
    it should be inherited by specific Sink implementations.
    """

    def __init__(
        self,
        dataName: str,
        dataVersion: str
    ) -> None:
        """
        Initialize the health model configuration.

        Args:
            modelVersion: Version of the health model (e.g., "1.0.0")
            modelName: Name of the health model
        """
        self._dataVersion = dataVersion
        self._dataName = dataName
        self._dataGuid = Utils.generateGuidFromString(self._dataName)

    def validateDataQuality(
        self,
        df: DataFrame,
        min_records: int = 100,
        required_columns: list = None
    ) -> tuple[bool, str]:
        """
        Perform basic data quality checks on the processed DataFrame.

        Args:
            df: DataFrame to validate
            min_records: Minimum number of records required
            required_columns: List of required column names

        Returns:
            Tuple of (is_valid, error_message)
        """
        if df is None:
            return False, "DataFrame is None"

        try:
            record_count = df.count()
            if record_count < min_records:
                return False, f"Insufficient records: {record_count} < {min_records}"

            if required_columns:
                df_columns = set(df.columns)
                missing_columns = set(required_columns) - df_columns
                if missing_columns:
                    return False, f"Missing required columns: {missing_columns}"

            # Check for excessive nulls in key columns
            if required_columns:
                for col in required_columns:
                    null_count = df.filter(df[col].isNull()).count()
                    null_percentage = (null_count / record_count) * 100
                    if null_percentage > 50:  # More than 50% nulls is concerning
                        return False, f"Column '{col}' has {null_percentage:.1f}% null values"

            return True, "Data quality checks passed"

        except Exception as e:
            return False, f"Data quality validation failed: {str(e)}"

    @property
    def dataName(self) -> str:
        return self._dataName

    @property
    def dataVersion(self) -> str:
        return self._dataVersion

    @property
    def dataGuid(self) -> str:
        return self._dataGuid

    @property
    def watermark(self) -> Any:
        raise RuntimeError("Subclasses must set the _watermark field.")