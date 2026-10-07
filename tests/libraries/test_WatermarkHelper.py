import unittest
from unittest.mock import MagicMock, patch
from melius.libraries.WatermarkHelper import WatermarkHelper
from melius.connectors.BlobStorageConnector import BlobStorageConnector


class TestWatermarkHelper(unittest.TestCase):

    @patch('melius.libraries.Logger.Logger.getInstance')
    def test_getMinValueOfColumnFromDeltaTable_no_table(self, mock_logger):
        connector = MagicMock(spec=BlobStorageConnector)
        connector.loadDeltaTable.side_effect = Exception("Table not found")

        result = WatermarkHelper.getMinValueOfColumnFromDeltaTable(connector, 'path/to/source', 'column')

        self.assertIsNone(result)
        mock_logger().writeInfo.assert_called_with("Delta table not found with exception 'Table not found'. return None")

    def test_getKeyName(self):
        result = WatermarkHelper.getKeyName('cluster', 'database', 'table')
        self.assertEqual(result, 'cluster.database.table')


if __name__ == '__main__':
    unittest.main()