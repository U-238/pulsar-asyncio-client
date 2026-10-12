# Pulsar AsyncIO Client

A coroutine-listener adapter for the Apache Pulsar Python client. Producers,
consumers, readers, configuration, and exceptions come directly from upstream.

This library's functionality has been substantially reduced because similar
functionality is now available in upstream `pulsar-client`, including its
[`pulsar.asyncio` API](https://github.com/apache/pulsar-client-python/blob/v3.13.0/pulsar/asyncio.py).
The library now provides only `coroutine_listener`, which adapts async message
handlers for upstream's synchronous subscription API.

## Installation

Requires Python 3.10–3.14 and `pulsar-client>=3.13.0`. Native wheel platform
requirements follow [upstream](https://github.com/apache/pulsar-client-python/tree/v3.13.0).

```shell
pip install pulsar-asyncio-client
```

## Usage

Wrap an async handler with `coroutine_listener` and pass it to the synchronous
upstream client's subscription. The adapter captures the running event loop and
schedules a separate task for each message received on Pulsar's native threads.

```python
import asyncio

import pulsar
from pulsar.asyncio import Client as AsyncClient

from pulsar_asyncio import coroutine_listener


async def main():
    topic = "persistent://public/default/my-topic"
    subscriber = pulsar.Client("pulsar://localhost:6650")
    publisher = AsyncClient("pulsar://localhost:6650")
    received = asyncio.Event()
    consumer = None

    async def handle_message(consumer, message):
        print(message.data().decode("utf-8"))
        await asyncio.sleep(0.1)
        consumer.acknowledge(message)
        received.set()

    try:
        consumer = await asyncio.to_thread(
            subscriber.subscribe,
            topic,
            subscription_name="my-sub",
            message_listener=coroutine_listener(handle_message),
        )
        producer = await publisher.create_producer(topic)
        await producer.send(b"Hello from upstream asyncio")
        await asyncio.wait_for(received.wait(), timeout=10)
    finally:
        if consumer is not None:
            await asyncio.to_thread(consumer.close)
        await asyncio.to_thread(subscriber.close)
        await publisher.close()


asyncio.run(main())
```

`subscribe()` and consumer methods retain upstream's synchronous behavior. For
potentially blocking operations, use `asyncio.to_thread`. Asynchronous publishing
uses the separate upstream `pulsar.asyncio.Client`; its producer batching defaults
and exception types apply.

To create the adapter outside the event loop's thread, pass
`coroutine_listener(handle_message, loop=event_loop)` explicitly. Keep that loop
running while consuming messages. Close the consumer and finish any in-flight
handlers before closing the loop. Callbacks arriving after loop closure are
ignored and are not acknowledged.

Handlers run concurrently, so completion order is not guaranteed. Each handler
must acknowledge its message and handle application errors. Unhandled exceptions
are reported through asyncio's exception handler; the adapter does not retry or
acknowledge failed handlers.

## Migration

The library now exports only `coroutine_listener`. Replace the former
`pulsar_asyncio.Client.subscribe(message_listener=handler)` with
`pulsar.Client.subscribe(message_listener=coroutine_listener(handler))` on an
upstream client instance. Use `pulsar.asyncio.Client` for asynchronous producers,
consumers, and readers, and `pulsar.asyncio.PulsarException` for its errors.

## Development

Install development dependencies with [PDM](https://pdm-project.org/):

```shell
pdm install -G test -G lint
pdm run lint
pdm run lint-fix
pdm run test
```

Run broker integration tests with Pulsar listening at `localhost:6650`:

```shell
pdm run test-integration
```

CI runs unit tests on Python 3.10–3.14 and integration tests against a Pulsar
broker. [Ruff](https://docs.astral.sh/ruff/) checks lint and formatting. The
generated `pulsar_asyncio/version.py` file is excluded.
