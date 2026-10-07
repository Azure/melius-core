from __future__ import annotations
import json
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from melius.libraries.Logger import Logger
from melius.connectors.KustoConnector import KustoConnector
from melius.session.sinks.BaseSink import BaseSink


class KustoSink(BaseSink):
    """
    KustoSink handles writing data to Kusto Database.
    """

    def __init__(
        self,
        kustoConfig: str,
        dataName: str,
        dataVersion: str
    ) -> None:
        """
        Initialize the DeltaLake Sink object.

        Args:
            kustoConfig: Kusto configuration for the target Kusto table
            dataName: Name of the data
            dataVersion: Version of the data (e.g., "1.0.0")
        """
        super().__init__(
            dataName=dataName,
            dataVersion=dataVersion
        )
        self._kustoConfig = kustoConfig
        Logger.getInstance().writeInfo(f"Initialized KustoSink for data: {dataName}, version: {dataVersion}")
        Logger.getInstance().writeInfo(f"Kusto Config: \n{json.dumps(kustoConfig, indent=2)}")

    def getKustoConnector(self) -> KustoConnector:
        """
        Get the KustoConnector for the source data.
        """
        return KustoConnector.getConnector(self._kustoConfig)

    def publishDataToKusto(
        self,
        df: DataFrame,
        schema: StructType
    ) -> None:
        """
        Publish a DataFrame to the Kusto table.

        Args:
            df: DataFrame to publish
            schema: Schema to enforce when publishing
        """
        connector = self.getKustoConnector()

        connector.publishDataframeViaConnector(
            df,
            self.kustoTable,
            schema=schema
        )
        Logger.getInstance().writeInfo(f"Successfully wrote data to the Kusto: {self.kustoCluster}/{self.kustoDatabase}/{self.kustoTable}")

    @property
    def kustoConfig(self) -> str:
        return self._kustoConfig

    @property
    def kustoCluster(self) -> str:
        return self.kustoConfig[KustoConnector.Cluster]

    @property
    def kustoDatabase(self) -> str:
        return self.kustoConfig[KustoConnector.Database]

    @property
    def kustoTable(self) -> str:
        return self.kustoConfig[KustoConnector.Table]