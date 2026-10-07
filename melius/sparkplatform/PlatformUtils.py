from abc import ABC


class PlatformUtils(ABC):

    def __init__(self, utils):
        self.utils = utils
        super().__init__()

    @property
    def fs(self):
        return self.utils.fs


class SynapsePlatformUtils(PlatformUtils):

    def __init__(self, mssparkutils):
        super().__init__(mssparkutils)


class DatabricksPlatformUtils(PlatformUtils):

    def __init__(self, dbutils):
        super().__init__(dbutils)