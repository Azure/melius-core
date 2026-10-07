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