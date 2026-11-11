import pytest
import unittest.mock as mock
from melius.restapis.KustoApiClient import KustoApiClient


@pytest.fixture
def clusterUrl():
    return 'https://www.testClusterUrl.net'


@pytest.fixture
def database():
    return 'testDatabase'


@pytest.fixture
def kustoCommand():
    return 'testCommand'


@pytest.fixture
def kustoApiClient(clusterUrl, database):
    return KustoApiClient(clusterUrl, database)


@pytest.fixture
def kustoApiClientWithConfig(clusterUrl, database):
    config = {"accessType": "FIC", "tenantId": "t1", "clientId": "c1"}
    return KustoApiClient(clusterUrl, database, config=config)


def test_kustoApiClientInstanceCreated(kustoApiClient, clusterUrl, database):
    assert kustoApiClient is not None
    assert kustoApiClient.clusterUrl == clusterUrl
    assert kustoApiClient.database == database


def mockResponseWithCode(code):
    mockResponse = mock.Mock()
    mockResponse.status_code = code

    if code == 200:
        mockResponse.json.return_value = {'Tables': [{'Rows': [1,2,3,4,5]}]}

    return mockResponse


def test_postManagementApi(kustoApiClient, kustoCommand, database, clusterUrl, mocker):
    # setup mock
    mocker.patch.object(kustoApiClient._token, 'getAccessToken', return_value='TestToken123')
    postMockCall = mocker.patch('melius.restapis.KustoApiClient.requests.post', return_value=mockResponseWithCode(200))

    # call under test
    response = kustoApiClient.postManagementApi(kustoCommand)
    headers = kustoApiClient._buildHeaders()
    payload = {
      'db': database,
      "csl": kustoCommand
    }

    url = f'{clusterUrl}/v1/rest/mgmt'

    # assertions
    assert postMockCall.called
    assert response == [{'Rows': [1,2,3,4,5]}]
    postMockCall.assert_called_with(url, headers=headers, json=payload, timeout=180)


def test_kustoApiClientWithConfig(kustoApiClientWithConfig, clusterUrl, database):
    """KustoApiClient created with config passes it through to BaseApiClient/AccessToken."""
    assert kustoApiClientWithConfig is not None
    assert kustoApiClientWithConfig.clusterUrl == clusterUrl
    assert kustoApiClientWithConfig.database == database


def test_kustoApiClientWithoutConfig_backward_compat(clusterUrl, database):
    """KustoApiClient without config param still works (backward compat)."""
    client = KustoApiClient(clusterUrl, database)
    assert client is not None
    assert client.clusterUrl == clusterUrl


def test_kustoApiClientConfigPassedToAccessToken(clusterUrl, database, mocker):
    """Verify config is forwarded to AccessToken via BaseApiClient."""
    mock_at = mocker.patch('melius.restapis.BaseApiClient.AccessToken')
    config = {"accessType": "FIC", "tenantId": "t1", "clientId": "c1"}
    KustoApiClient(clusterUrl, database, config=config)
    mock_at.assert_called_once_with('https://kusto.kusto.windows.net/.default', config=config)


def test_postManagementApiException(kustoApiClient, kustoCommand, mocker):
    with pytest.raises(Exception):
        # setup mock
        mocker.patch.object(kustoApiClient._token, 'getAccessToken', return_value='TestToken123')
        mocker.patch('melius.restapis.KustoApiClient.requests.post', return_value=mockResponseWithCode(500))

        # call under test
        kustoApiClient.postManagementApi(kustoCommand)


def test_postManagementApiThrottlingAndMaxRetries(kustoApiClient, kustoCommand, database, clusterUrl, mocker):
    # setup mock
    mocker.patch.object(kustoApiClient._token, 'getAccessToken', return_value='TestToken123')
    postMockCall = mocker.patch('melius.restapis.KustoApiClient.requests.post', return_value=mockResponseWithCode(429))
    sleepMockCall = mocker.patch('time.sleep')

    # call under test
    try:
        _ = kustoApiClient.postManagementApi(kustoCommand)
    except Exception as e:
        if f'Fail to retry Kusto command even after {KustoApiClient.maxRetryCount} retries' not in str(e):
            raise e

    headers = kustoApiClient._buildHeaders()

    payload = {
      'db': database,
      "csl": kustoCommand
    }

    url = f'{clusterUrl}/v1/rest/mgmt'

    # assertions
    assert postMockCall.called
    assert postMockCall.call_count == KustoApiClient.maxRetryCount + 1  # total try is retry count + first call.
    sleepMockCall.assert_called_with(kustoApiClient.backOffTimeInSeconds * 3)  # retried 3 times. so last wait was the 3 times of backoffTime.
    postMockCall.assert_called_with(url, headers=headers, json=payload, timeout=180)


def test_postManagementApiThrottlingAndRetrySuccessfully(kustoApiClient, kustoCommand, database, clusterUrl, mocker):
    # setup mock
    mocker.patch.object(kustoApiClient._token, 'getAccessToken', return_value='TestToken123')
    postMockCall = mocker.patch('melius.restapis.KustoApiClient.requests.post')
    sleepMockCall = mocker.patch('time.sleep')
    mock1stResponse = mock.Mock()
    mock1stResponse.status_code = 429
    mock2ndResponse = mock.Mock()
    mock2ndResponse.status_code = 200
    mock2ndResponse.json.return_value = {'Tables': [{'Rows': [1,2,3,4,5]}]}
    postMockCall.side_effect = [mock1stResponse, mock2ndResponse]

    # call under test
    response = kustoApiClient.postManagementApi(kustoCommand)

    headers = kustoApiClient._buildHeaders()
    payload = {
      'db': database,
      "csl": kustoCommand
    }
    url = f'{clusterUrl}/v1/rest/mgmt'
    # assertions
    assert postMockCall.called
    assert postMockCall.call_count == 2
    assert response == [{'Rows': [1,2,3,4,5]}]
    sleepMockCall.assert_called_with(kustoApiClient.backOffTimeInSeconds)
    postMockCall.assert_called_with(url, headers=headers, json=payload, timeout=180)
