import pytest
from datetime import date, datetime, timedelta
from melius.libraries.Constants import CycleTypes
from melius.libraries.Utility import ConstantStrings


@pytest.fixture
def startDate():
    return datetime.combine(date.today(), datetime.min.time())


@pytest.fixture
def endDate(startDate, numOfDays=1):
    return startDate + timedelta(days=numOfDays)


@pytest.fixture
def endDateOlderThanStartDate(startDate, numOfDays=1):
    return startDate - timedelta(days=numOfDays)


@pytest.fixture(params=['raw', 'cleansed', 'published'])
def stage(request):
    return request.param


@pytest.fixture(params=[CycleTypes.Daily, CycleTypes.Hourly, CycleTypes.Every4Hour, CycleTypes.Every2Hour])
def cycleType(request):
    return request.param


@pytest.fixture
def meliusConfig():
    return {
        ConstantStrings.MeliusConfig_RootPath: '/mnt/melius',
        ConstantStrings.MeliusConfig_Env: ConstantStrings.Environment_Test,
        ConstantStrings.MeliusConfig_Scope: 'test'
    }