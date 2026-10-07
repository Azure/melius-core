import pathlib
import unittest.mock as mock
from melius.sparkplatform.SparkPlatform import Databricks


def test_SparkPlatform_Databriks_getPathWithoutFileSystemRoot_str(mocker):
    mockDbUtil = mock.Mock()
    db = Databricks(mockDbUtil)
    expected_path = pathlib.Path("/test/path")
    assert db.getPathWithoutFileSystemRoot('/dbfs/test/path') == expected_path
    assert db.getPathWithoutFileSystemRoot(pathlib.Path('/dbfs/test/path')) == expected_path
