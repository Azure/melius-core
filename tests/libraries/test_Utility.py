import pytest
import pathlib
import unittest.mock as mock
from melius.libraries.Utility import Utils


@pytest.fixture
def meliusDevConfig():
    return {'environment': 'dev', 'meliusPath': '/mnt/cdewrite/teams/MSU/dev', 'scope': 'Secrets123'}


@pytest.fixture
def refreshUtils():
    Utils._datasetLoadersConfig = None


@pytest.fixture
def dataset():
    return 'testset.msu.common'


@pytest.fixture
def datasetWithStage():
    return 'testset.msu.common@raw'


@pytest.fixture
def feedName():
    return 'AllocableVmCountByCluster.msu.common'


@pytest.fixture
def path():
    return pathlib.PurePosixPath('/dbfs/cde/test/common')


def mockUpdateWithScopeValues(config):
    return config


def test_generateGuidFromString(mocker):
    assert Utils.generateGuidFromString('DiscIOModel') == 'b9b40107-249c-519f-bed2-1613f7eb2ece'
    assert Utils.generateGuidFromString('DiskSystemEvents') == '25113a9f-3520-526a-a7b2-625317f50fee'
    assert Utils.generateGuidFromString('DiskIopsBaseData') == 'fd77a801-8edb-5b34-98fa-24b938e388ca'
    assert Utils.generateGuidFromString('DiskIopsHealthModel') == '1f5c11ba-0902-5713-81b6-65b05b197fff'
    assert Utils.generateGuidFromString('NetworkSystemEvents') == '1338daa5-e192-5fd7-ba2c-fbb6951de663'
    assert Utils.generateGuidFromString('TcpStatsBaseData') == '706fb07c-90f1-5c3f-9011-a41134168244'
    assert Utils.generateGuidFromString('TcpStatsHealthModel') == '2a0ce415-7b5a-502d-827c-961992c070b4'
    assert Utils.generateGuidFromString('TcpStatsHealthModelV2') == '8e1f0ad0-f9cd-5ffb-88ed-17c6ef97bbc0'


def test_adjustPartition_None(mocker):
    dataFrame = mock.Mock()
    wi = mocker.patch('melius.libraries.Logger.Logger.writeInfo')

    _ = Utils.adjustPartition(dataFrame, None)

    wi.assert_called_with('Use the native partition count. no repartitioning')


def test_adjustPartition_Bigger(mocker):
    dataFrame = mock.Mock()
    dataFrame.rdd.getNumPartitions.return_value = 6
    wi = mocker.patch('melius.libraries.Logger.Logger.writeInfo')

    _ = Utils.adjustPartition(dataFrame, 5)

    wi.assert_called_with('Repartition since the DF partition count is bigger than the suggested')


def test_adjustPartition_Smaller(mocker):
    dataFrame = mock.Mock()
    dataFrame.rdd.getNumPartitions.return_value = 3
    wi = mocker.patch('melius.libraries.Logger.Logger.writeInfo')

    _ = Utils.adjustPartition(dataFrame, 5)

    wi.assert_called_with('No need for repartitioning.')