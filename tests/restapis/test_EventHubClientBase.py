import pytest
import unittest.mock as mock
from melius.restapis.EventHubClientBase import EventHubClientBase
from azure.eventhub import EventData


@pytest.fixture
def connectionString():
    return 'connectionString'


@pytest.fixture
def eventhubName():
    return 'eventhubName'


@pytest.fixture
def client(connectionString, eventhubName):
    return EventHubClientBase(connectionString, eventhubName)


def test_EventGridClientBaseInstanceCreation(client):
    assert client is not None


def test_PublishEvents_Succeed(client:EventHubClientBase, mocker):
    # setup mocks
    mocker.patch('melius.libraries.Logger.Logger')
    sleepMockCall = mocker.patch('time.sleep')
    mockProducer = mock.Mock()
    mockBatch = mock.Mock()
    mockfrom_connection = mocker.patch('azure.eventhub.EventHubProducerClient.from_connection_string', return_value=mockProducer)
    mockProducer.create_batch.return_value = mockBatch

    event = EventData('event Data')
    client.publishEvent(event)

    mockfrom_connection.assert_called_with(conn_str="connectionString", eventhub_name='eventhubName')
    mockBatch.add.assert_called_with(event)
    mockProducer.send_batch.assert_called_once()
    sleepMockCall.call_count == 0