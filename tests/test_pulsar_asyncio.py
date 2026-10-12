"""Test coroutine listeners with real upstream clients and a Pulsar broker."""

import asyncio
import threading
from uuid import uuid4

import pulsar
import pytest
from pulsar.asyncio import Client as AsyncClient

from pulsar_asyncio import coroutine_listener

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("publisher_kind", ["sync", "async"])
def test_coroutine_listener_with_upstream_producers(publisher_kind):
    async def run():
        topic = f"persistent://public/default/coroutine-listener-{uuid4().hex}"
        subscriber = pulsar.Client("pulsar://localhost:6650")
        publisher = (
            pulsar.Client("pulsar://localhost:6650")
            if publisher_kind == "sync"
            else AsyncClient("pulsar://localhost:6650")
        )
        consumer = None
        completed = asyncio.Event()
        received = []
        loop_thread = threading.get_ident()

        async def listener(consumer, message):
            assert threading.get_ident() == loop_thread
            await asyncio.sleep(0.01)
            consumer.acknowledge(message)
            received.append((message.value(), message.properties()))
            if len(received) == 5:
                completed.set()

        try:
            consumer = await asyncio.to_thread(
                subscriber.subscribe,
                topic,
                "listener-test",
                schema=pulsar.schema.StringSchema(),
                message_listener=coroutine_listener(listener),
                crypto_failure_action=pulsar.ConsumerCryptoFailureAction.FAIL,
            )
            if publisher_kind == "sync":
                producer = await asyncio.to_thread(
                    publisher.create_producer,
                    topic,
                    schema=pulsar.schema.StringSchema(),
                )
                for i in range(5):
                    await asyncio.to_thread(
                        producer.send, f"message-{i}", properties={"test": "listener"}
                    )
            else:
                producer = await publisher.create_producer(
                    topic, schema=pulsar.schema.StringSchema()
                )
                for i in range(5):
                    await producer.send(f"message-{i}", properties={"test": "listener"})
            await asyncio.wait_for(completed.wait(), 10)
            assert sorted(received) == [
                (f"message-{i}", {"test": "listener"}) for i in range(5)
            ]
        finally:
            if consumer is not None:
                await asyncio.to_thread(consumer.unsubscribe)
            await asyncio.to_thread(subscriber.close)
            if publisher_kind == "sync":
                await asyncio.to_thread(publisher.close)
            else:
                await publisher.close()

    asyncio.run(run())
