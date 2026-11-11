# Databricks notebook source
from enum import Enum
from datetime import timedelta
from io import UnsupportedOperation


class TableState(Enum):
    TableNotExists = 1
    ExtentNotExists = 2
    ExtentExists = 3

    def __eq__(self, other):
        return self.value == other.value


class FileTypes(Enum):
    ''' indicate the file type to load '''
    Parquet = "Parquet"
    CSV = "CSV"
    TSV = "TSV"
    JSON = "JSON"
    Unknown = "Unknown"

    def __eq__(self, other):
        return self.value == other.value

    @staticmethod
    def value_of(value) -> Enum:
        for m, mm in FileTypes.__members__.items():
            if m == value:
                return mm
        raise ValueError(f"no matching enum: {value}")

    @staticmethod
    def separator_of(value) -> str:
        if value == FileTypes.CSV:
            return ','
        elif value == FileTypes.TSV:
            return '\t'

        return None

    @staticmethod
    def fileExt_of(value) -> str:
        if value == FileTypes.CSV:
            return '.csv'
        elif value == FileTypes.TSV:
            return '.tsv'
        elif value == FileTypes.Parquet:
            return '.parquet'
        elif value == FileTypes.JSON:
            return '.json'

        raise UnsupportedOperation(f'{value} is not supported.')

    @staticmethod
    def getFileTypeByExt(file: str):
        file = file.strip()

        fileType = FileTypes.Unknown

        if file.endswith(FileTypes.fileExt_of(FileTypes.Parquet)):
            fileType = FileTypes.Parquet
        elif file.endswith(FileTypes.fileExt_of(FileTypes.CSV)):
            fileType = FileTypes.CSV
        elif file.endswith(FileTypes.fileExt_of(FileTypes.TSV)):
            fileType = FileTypes.TSV
        elif file.endswith(FileTypes.fileExt_of(FileTypes.JSON)):
            fileType = FileTypes.JSON

        return fileType


class CycleTypes(Enum):
    Hourly = "Hourly"
    Daily = "Daily"
    Every2Hour = "Every2Hours"
    Every4Hour = "Every4Hours"
    Minute = "Minute"
    Second = "Second"

    def __eq__(self, other):
        return False if other is None else self.value == other.value

    @staticmethod
    def value_of(value) -> Enum:
        for m, mm in CycleTypes.__members__.items():
            if m == value:
                return mm
        raise ValueError(f"no matching enum: {value}")

    @staticmethod
    def format_of(value) -> str:
        ''' return the str format of datetime based on the cycle type.
        '''
        format = "%m/%d/%Y %H"
        if value:
            if value == CycleTypes.Minute:
                format = "%m/%d/%Y %H:%M"
            elif value == CycleTypes.Second:
                format = "%m/%d/%Y %H:%M:%S"
            elif value == CycleTypes.Daily:
                format = "%m/%d/%Y 00"

        return format

    @staticmethod
    def getTimeDelta(cycleType):
        ''' get timedelta for the given cycle '''
        delta = None
        if cycleType == CycleTypes.Hourly:
            delta = timedelta(hours=1)
        elif cycleType == CycleTypes.Daily:
            delta = timedelta(days=1)
        elif cycleType == CycleTypes.Every2Hour:
            delta = timedelta(hours=1)  # it is intended value. please don't change
        elif cycleType == CycleTypes.Every4Hour:
            delta = timedelta(hours=1)  # it is intended value. please don't change
        elif cycleType == CycleTypes.Minute:
            delta = timedelta(minutes=1)
        elif cycleType == CycleTypes.Second:
            delta = timedelta(seconds=1)
        else:
            raise ValueError(f'{cycleType} is not recognized type.')

        return delta

    @staticmethod
    def isValidCycle(cycleType, targetCycle):
        ''' check whether cycle is valid '''
        if cycleType == CycleTypes.Hourly or cycleType == CycleTypes.Minute or cycleType == CycleTypes.Second:
            return True
        elif cycleType == CycleTypes.Daily:
            return targetCycle.hour == 0
        elif cycleType == CycleTypes.Every2Hour:
            return (targetCycle.hour % 2) == 0
        elif cycleType == CycleTypes.Every4Hour:
            return (targetCycle.hour % 4) == 0

        raise ValueError(f'{cycleType} is not recognized type.')


class KnownStrings():
    '''
    Known strings for the system
    '''
    DataDeltaTable = "data_delta_table"
    WatermarkDeltaTable = "watermark_delta_table"
    IngestionStartDT = "##IngestionStartDT##"
    IngestionEndDT = "##IngestionEndDT##"
    AzCoreRegions = "##AzCoreRegions##"
    AzCoreClusterPlaceholder = "###AzCoreCluster###"
    Storage_Container_Parquets = "parquets"
    SourceIngestionTime = "SourceIngestionTime"
    IngestionTimeMoniker = "ingestion_time()"

    Config_BackFillStartDateTime = "backFillStartDateTime"
    Config_AzCoreGeo = "azcoreGeo"
    Config_Environment = "environment"
    Config_Storages = "storages"
    Config_Kustos = "kustos"
    Config_Logging = "logging"
    Config_KustoCluster = "kustoCluster"
    Config_KustoDatabase = "kustoDatabase"
    Config_KustoTable = "kustoTable"
    Config_AzureOpenAI = "azureOpenAIs"
    Config_AzureOpenAI_LLM = "llm"
    Config_AzureOpenAI_Assistant = "assistant"

    AccessConfig = "accessConfig"
    AccessType = 'accessType'
    AccessType_MSI = 'MSI'
    AccessType_AppId = 'AppId'
    AccessType_FIC = 'FIC'
    Config_ClientId = 'clientId'
    Config_TenantId = 'tenantId'
    Config_Cert = 'cert'
    Config_Scope = 'scope'

    # Federated Identity Credential (FIC) configuration keys
    Config_Fic = 'fic'
    Config_Fic_TenantId = 'tenantId'
    Config_Fic_ClientId = 'clientId'
