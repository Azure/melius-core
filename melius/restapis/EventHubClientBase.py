import time
from abc import ABC
from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData
from melius.libraries.Logger import Logger
from msrest.exceptions import HttpOperationError


class EventHubClientBase(ABC):
    '''
    Event Grid Client base class to support various event grid end points in future
    '''
    maxRetryCount = 3
    backOffTimeInSeconds = 60

    def __init__(self, connectionString, eventhubName):
        self.connectionString = connectionString
        self.eventhubName = eventhubName

    def publishEvent(self, eventData: EventData):
        ''' post an event data to event Hub '''
        retryCount = 0

        while True:
            try:
                producer = EventHubProducerClient.from_connection_string(conn_str=self.connectionString, eventhub_name=self.eventhubName)
                event_data_batch = producer.create_batch()
                event_data_batch.add(eventData)
                producer.send_batch(event_data_batch)

                Logger.getInstance().writeInfo('Published events.')
                break
            except HttpOperationError as e:
                Logger.getInstance().writeInfo(f'Failed to publish events with Exception: {e}')
                retryCount = retryCount + 1

                if retryCount <= EventHubClientBase.maxRetryCount:
                    waitTime = retryCount * EventHubClientBase.backOffTimeInSeconds
                    Logger.getInstance().writeInfo(f'Event hub publication retry due to Exception: {e} On retry count {retryCount} and waiting {waitTime} seconds.')
                    time.sleep(waitTime)
                    continue
                else:
                    raise Exception(f'Fail to retry event grid publication after {EventHubClientBase.maxRetryCount} retries, with Exception: {e}')
