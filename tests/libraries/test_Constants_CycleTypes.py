import pytest


def test_CycleTypes_format_and_frequency():
    from melius.libraries.Constants import CycleTypes

    assert CycleTypes.format_of(CycleTypes.Hourly) == "%m/%d/%Y %H"
    assert CycleTypes.format_of(CycleTypes.Daily) == "%m/%d/%Y 00"
    assert CycleTypes.format_of(CycleTypes.Minute) == "%m/%d/%Y %H:%M"
    assert CycleTypes.format_of(CycleTypes.Second) == "%m/%d/%Y %H:%M:%S"


def test_CycleTypes_isValidCycle():
    from melius.libraries.Constants import CycleTypes
    from datetime import datetime

    d0 = datetime(2024, 1, 1, 0, 0, 0)
    d1 = datetime(2024, 1, 1, 1, 0, 0)

    assert CycleTypes.isValidCycle(CycleTypes.Daily, d0) is True
    assert CycleTypes.isValidCycle(CycleTypes.Daily, d1) is False
    assert CycleTypes.isValidCycle(CycleTypes.Every2Hour, d0) is True
    assert CycleTypes.isValidCycle(CycleTypes.Every2Hour, d1) is False
    assert CycleTypes.isValidCycle(CycleTypes.Every4Hour, d0) is True


def test_KnownStrings_constants_exist():
    from melius.libraries.Constants import KnownStrings
    # just access a couple to ensure module is importable and attributes exist
    assert isinstance(KnownStrings.Config_AzureOpenAI, str)
    assert isinstance(KnownStrings.Config_AzureOpenAI_LLM, str)


def test_core_constants_exclude_application_schemas():
    from melius.libraries import Constants

    with pytest.raises(AttributeError):
        getattr(Constants, "KnownTableSchemas")


@pytest.mark.parametrize("name", [
    "DataBase_VMWatchHealth",
    "Table_VMHealthIssues",
    "Table_HealthIssues",
    "Table_VMHealthIssueRca",
    "Table_VMWatchSignalRawLogs",
    "Table_VMWatchSignalLogs",
    "Table_VMWatchSignalLogsV2",
    "Config_Storages_VMWatchLogs",
    "Config_Storages_HealthData",
    "Config_Storages_AzPrism",
    "Config_Kustos_AzPrism",
    "Config_Kustos_HealthIssuesTable",
    "Config_Kustos_HealthIssueRcaTable",
])
def test_core_constants_exclude_application_config_keys(name):
    from melius.libraries.Constants import KnownStrings

    with pytest.raises(AttributeError):
        getattr(KnownStrings, name)


@pytest.mark.parametrize("name,expected", [
    ("DataDeltaTable", "data_delta_table"),
    ("WatermarkDeltaTable", "watermark_delta_table"),
    ("IngestionStartDT", "##IngestionStartDT##"),
    ("IngestionEndDT", "##IngestionEndDT##"),
    ("AzCoreRegions", "##AzCoreRegions##"),
    ("AzCoreClusterPlaceholder", "###AzCoreCluster###"),
    ("Storage_Container_Parquets", "parquets"),
    ("SourceIngestionTime", "SourceIngestionTime"),
    ("IngestionTimeMoniker", "ingestion_time()"),
    ("Config_BackFillStartDateTime", "backFillStartDateTime"),
    ("Config_AzCoreGeo", "azcoreGeo"),
    ("Config_Environment", "environment"),
    ("Config_Storages", "storages"),
    ("Config_Kustos", "kustos"),
    ("Config_Logging", "logging"),
    ("Config_KustoCluster", "kustoCluster"),
    ("Config_KustoDatabase", "kustoDatabase"),
    ("Config_KustoTable", "kustoTable"),
    ("Config_AzureOpenAI", "azureOpenAIs"),
    ("Config_AzureOpenAI_LLM", "llm"),
    ("Config_AzureOpenAI_Assistant", "assistant"),
    ("AccessConfig", "accessConfig"),
    ("AccessType", "accessType"),
    ("AccessType_MSI", "MSI"),
    ("AccessType_AppId", "AppId"),
    ("AccessType_FIC", "FIC"),
    ("Config_ClientId", "clientId"),
    ("Config_TenantId", "tenantId"),
    ("Config_Cert", "cert"),
    ("Config_Scope", "scope"),
    ("Config_Fic", "fic"),
    ("Config_Fic_TenantId", "tenantId"),
    ("Config_Fic_ClientId", "clientId"),
])
def test_core_config_key_values_are_unchanged(name, expected):
    from melius.libraries.Constants import KnownStrings

    assert getattr(KnownStrings, name) == expected
