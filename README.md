# Pulsar AsyncIO Client

Wrapper for `pulsar-client` that publishes messages asynchronously and handles consumed messages in an `asyncio` event loop.

## Installation

```shell
$ pip install pulsar-asyncio-client
```

## Usage

```python
import asyncio
import pulsar_asyncio

PULSAR_URL = 'pulsar://localhost:6650'
PULSAR_TOPIC = 'non-persistent://public/default/my-topic'
PULSAR_SUBSCRIPTION = 'my-sub'

async def handle_message_async(consumer, message):
    message_text = message.data().decode('utf-8')
    print(f"Started asynchronously handling message: {message_text}")
    await asyncio.sleep(0.1)
    print(f"Finished asynchronously handling message: {message_text}")
    consumer.acknowledge(message)

async def main():
    # Init Pulsar client
    client = pulsar_asyncio.Client(PULSAR_URL)
    
    # Start a consumer with an async message listener
    consumer = client.subscribe(
        PULSAR_TOPIC,
        subscription_name=PULSAR_SUBSCRIPTION,
        message_listener=handle_message_async
    )

    # Asynchronously publish some messages
    producer = await client.create_producer(PULSAR_TOPIC)
    for i in range(10):
        message_text = f"My Message #{i}"
        print(f"About to asynchronously publish message: {message_text}")
        await producer.send(message_text.encode('utf-8'))

    # Allow some time for all messages to be consumed
    await asyncio.sleep(1)

    # Stop consumer
    consumer.close()
    
    # Stop client
    await client.close()

if __name__ == '__main__':
    asyncio.run(main())
```
