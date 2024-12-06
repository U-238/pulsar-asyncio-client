import sys
import time
import pytest
import pulsar
import pulsar_asyncio
import asyncio
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('tests.test_pulsar_asyncio')


class PulsarTestContext:
    TEST_URL = 'pulsar://localhost:6650'
    TEST_TOPIC = 'non-persistent://public/default/test-topic'
    TEST_SUBSCRIPTION = 'test-sub'

    _client = None
    _consumer = None
    _producer = None

    def __init__(self, pulsar_client_class):
        if not issubclass(pulsar_client_class, pulsar.Client):
            raise ValueError("pulsar_client_class must be a subclass of pulsar.Client")
        self._pulsar_client_class = pulsar_client_class

    @property
    def client(self):
        if not self._client:
            self._client = self._pulsar_client_class(self.TEST_URL, logger=logger)
        return self._client

    def get_test_producer_sync(self):
        if not self._producer:
            self._producer = self.client.create_producer(self.TEST_TOPIC)
        return self._producer

    async def get_test_producer_async(self):
        if not self._producer:
            self._producer = await self.client.create_producer(self.TEST_TOPIC)
        return self._producer

    def start_test_consumer(self, message_listener):
        if not self._consumer:
            self._consumer = self.client.subscribe(
                self.TEST_TOPIC,
                subscription_name=self.TEST_SUBSCRIPTION,
                message_listener=message_listener
            )
        else:
            raise RuntimeError("Consumer already started")

    def stop_test_consumer(self):
        if self._consumer:
            self._consumer.close()
            self._consumer = None

    def close(self):
        if self._client:
            self._client.close()
            self._client = None

    async def close_async(self):
        if self._client:
            await self._client.close()
            self._client = None


def handle_message_sync(consumer, message):
    msg = message.data().decode('utf-8')
    logger.info(f"Started synchronously handling message: {msg}")
    time.sleep(0.1)
    logger.info(f"Finished synchronously handling message: {msg}")
    consumer.acknowledge(message)

def test_sync():
    pulsar_ctx = PulsarTestContext(pulsar.Client)
    pulsar_ctx.start_test_consumer(handle_message_sync)
    producer = pulsar_ctx.get_test_producer_sync()
    for i in range(10):
        producer.send(('Test Message %d' % i).encode('utf-8'))
    time.sleep(1.5)
    pulsar_ctx.stop_test_consumer()
    pulsar_ctx.close()

async def handle_message_async(consumer, message):
    msg = message.data().decode('utf-8')
    logger.info(f"Started asynchronously handling message: {msg}")
    await asyncio.sleep(0.1)
    logger.info(f"Finished asynchronously handling message: {msg}")
    consumer.acknowledge(message)

async def _test_async():
    pulsar_ctx = PulsarTestContext(pulsar_asyncio.Client)
    pulsar_ctx.start_test_consumer(handle_message_async)
    producer = await pulsar_ctx.get_test_producer_async()
    for i in range(10):
        await producer.send(('Test Message %d' % i).encode('utf-8'))
    await asyncio.sleep(1.5)
    pulsar_ctx.stop_test_consumer()
    await pulsar_ctx.close_async()

def test_async():
    asyncio.run(_test_async())

if __name__ == '__main__':
    sys.exit(pytest.main())
