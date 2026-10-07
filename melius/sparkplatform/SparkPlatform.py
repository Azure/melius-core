import pathlib
from abc import ABC
from melius.sparkplatform.PlatformUtils import DatabricksPlatformUtils


class Databricks(ABC):
    sparkUtil = None

    def __init__(self, sparkUtil):
        self.sparkUtil = sparkUtil

    def getPlatformUtils(self):
        return DatabricksPlatformUtils(self.sparkUtil)

    def removeFileSystemFromPath(self, filePath: str) -> str:
        return filePath.replace("/dbfs", "")

    def getPathWithoutFileSystemRoot(self, path):
        """Strips the preceding /dbfs in the path if present"""
        if isinstance(path, pathlib.Path):
            if len(path.parts) > 0 and path.parts[1] == 'dbfs':
                noDbfsPath = '/' / path.relative_to('/dbfs')
                return pathlib.Path(noDbfsPath)
            return pathlib.Path(path)
        elif isinstance(path, str):
            if path.startswith('/dbfs/'):
                path = path[5:]
            return pathlib.Path(path)
        raise ValueError(f'Instance({path}) not of type pathlib.Path or str')

    def getSecret(self, scopeName, keyName):
        return self.sparkUtil.secrets.get(scopeName, keyName)
