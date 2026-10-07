import pytest
from melius.restapis.BaseApiClient import BaseApiClient


@pytest.fixture
def clientId():
    return 'testClientId'


@pytest.fixture
def secret():
    return 'testSecret'


@pytest.fixture
def tenant():
    return 'testTenant'


@pytest.fixture
def resource():
    return 'testResource'


@pytest.fixture
def headerBase():
    return {'base': 'testHeaderBase'}


@pytest.fixture
def apiClient(resource, headerBase):
    return BaseApiClient(resource, headerBase)


def test_ApiClientInstanceCreation(apiClient):
    assert apiClient is not None


def test_BuildHeader(apiClient, mocker):
    # setup mocks
    mocker.patch.object(apiClient._token, 'getAccessToken', return_value='TestToken123')
    headers = apiClient._buildHeaders()
    assert headers is not None
    assert headers['Authorization'] == 'Bearer TestToken123'


def test_ApiClientWithConfig(resource, headerBase, mocker):
    """BaseApiClient passes config to AccessToken when provided."""
    mock_at = mocker.patch('melius.restapis.BaseApiClient.AccessToken')
    config = {"accessType": "FIC", "tenantId": "t1", "clientId": "c1"}
    BaseApiClient(resource, headerBase, config=config)
    mock_at.assert_called_once_with(resource, config=config)


def test_ApiClientWithoutConfig(resource, headerBase, mocker):
    """BaseApiClient passes config=None to AccessToken by default (backward compat)."""
    mock_at = mocker.patch('melius.restapis.BaseApiClient.AccessToken')
    BaseApiClient(resource, headerBase)
    mock_at.assert_called_once_with(resource, config=None)
