import asyncio
from typing import Union, Any
from functools import partial
import pulsar
import _pulsar
from _pulsar import CompressionType, PartitionsRoutingMode, BatchingType, ProducerConfiguration, \
    ProducerAccessMode,  DeadLetterPolicyBuilder  # noqa: F401

__all__ = ['Client', 'Producer', 'PulsarException']

def _check_type(var_type, var, name):
    if not isinstance(var, var_type):
        raise ValueError("Argument %s is expected to be of type '%s' and not '%s'"
                         % (name, var_type.__name__, type(var).__name__))

def _check_type_or_none(var_type, var, name):
    if var is not None and not isinstance(var, var_type):
        raise ValueError("Argument %s is expected to be either None or of type '%s'"
                         % (name, var_type.__name__))

def _set_future(fut: asyncio.Future, result: _pulsar.Result, value: Any):
    if result == _pulsar.Result.Ok:
        fut.set_result(value)
    else:
        fut.set_exception(PulsarException(result))

def _set_future_threadsafe(fut: asyncio.Future, result: _pulsar.Result, value: Any):
    fut.get_loop().call_soon_threadsafe(_set_future, fut, result, value)

def _message_listener(loop, message_listener_coro, consumer, message):
    loop.create_task(message_listener_coro(consumer, message), name=f"MessageListenerTask-{message.message_id()}")


class PulsarException(BaseException):
    """
    The exception that wraps the Pulsar error code
    """

    def __init__(self, result: pulsar.Result) -> None:
        """
        Create the Pulsar exception.

        Parameters
        ----------
        result: pulsar.Result
            The error code of the underlying Pulsar APIs.
        """
        self._result = result

    def error(self) -> pulsar.Result:
        """
        Returns the Pulsar error code.
        """
        return self._result

    def __str__(self):
        """
        Convert the exception to string.
        """
        return f'{self._result.value} {self._result.name}'


class Producer:
    """
    The Pulsar message producer, used to publish messages on a topic.
    """

    def __init__(self, producer: _pulsar.Producer) -> None:
        """
        Create the producer.
        Users should not call this constructor directly. Instead, create the
        producer via `Client.create_producer`.

        Parameters
        ----------
        producer: _pulsar.Producer
            The underlying Producer object from the C extension.
        """
        self._producer: _pulsar.Producer = producer

    async def send(self, content: bytes) -> pulsar.MessageId:
        """
        Send a message asynchronously.

        parameters
        ----------
        content: bytes
            The message payload

        Returns
        -------
        pulsar.MessageId
            The message id that represents the persisted position of the message.

        Raises
        ------
        PulsarException
        """
        builder = _pulsar.MessageBuilder()
        builder.content(content)
        fut = asyncio.get_running_loop().create_future()
        self._producer.send_async(builder.build(), partial(_set_future_threadsafe, fut))
        msg_id = await fut
        return pulsar.MessageId(
            msg_id.partition(),
            msg_id.ledger_id(),
            msg_id.entry_id(),
            msg_id.batch_index(),
        )

    async def close(self) -> None:
        """
        Close the producer.

        Raises
        ------
        PulsarException
        """
        fut = asyncio.get_running_loop().create_future()
        self._producer.close_async(partial(_set_future_threadsafe, fut, value=None))
        await fut


class Client(pulsar.Client):
    """
    The Pulsar client. A single client instance can be used to create producers
    and consumers on multiple topics.

    The client will share the same connection pool and threads across all
    producers and consumers.
    """

    def subscribe(self, *args, message_listener=None, loop=None, **kwargs):
        """
        Subscribe to the given topic and subscription combination and handle messages asynchronously.

        Parameters
        ----------

        topic:
            The name of the topic, list of topics or regex pattern. This method will accept these forms:
            * ``topic='my-topic'``
            * ``topic=['topic-1', 'topic-2', 'topic-3']``
            * ``topic=re.compile('persistent://public/default/topic-*')``
        subscription_name: str
            The name of the subscription.
        consumer_type: ConsumerType, default=ConsumerType.Exclusive
            Select the subscription type to be used when subscribing to the topic.
        schema: pulsar.schema.Schema, default=pulsar.schema.BytesSchema
            Define the schema of the data that will be received by this consumer.
        message_listener: optional
            Sets a message listener for the consumer. When the listener is set, the application will
            receive messages through it. Calls to ``consumer.receive()`` will not be allowed.
            The message listener must be a coroutine that accepts (consumer, message), for example:

            .. code-block:: python

                async def my_listener(consumer, message):
                    # process message
                    consumer.acknowledge(message)
        loop: asyncio.AbstractEventLoop, optional
            Sets the asyncio event loop that `message_listener` will run on. If not specified,
            ``asyncio.get_running_loop()`` will be used.
        receiver_queue_size: int, default=1000
            Sets the size of the consumer receive queue. The consumer receive queue controls how many messages can be
            accumulated by the consumer before the application calls `receive()`. Using a higher value could potentially
            increase the consumer throughput at the expense of higher memory utilization. Setting the consumer queue
            size to zero decreases the throughput of the consumer by disabling pre-fetching of messages.

            This approach improves the message distribution on shared subscription by pushing messages only to those
            consumers that are ready to process them. Neither receive with timeout nor partitioned topics can be used
            if the consumer queue size is zero. The `receive()` function call should not be interrupted when the
            consumer queue size is zero. The default value is 1000 messages and should work well for most use cases.
        max_total_receiver_queue_size_across_partitions: int, default=50000
            Set the max total receiver queue size across partitions. This setting will be used to reduce the
            receiver queue size for individual partitions
        consumer_name: str, optional
            Sets the consumer name.
        unacked_messages_timeout_ms: int, optional
            Sets the timeout in milliseconds for unacknowledged messages. The timeout needs to be greater than
            10 seconds. An exception is thrown if the given value is less than 10 seconds. If a successful
            acknowledgement is not sent within the timeout, all the unacknowledged messages are redelivered.
        negative_ack_redelivery_delay_ms: int, default=60000
            The delay after which to redeliver the messages that failed to be processed
            (with the ``consumer.negative_acknowledge()``)
        broker_consumer_stats_cache_time_ms: int, default=30000
            Sets the time duration for which the broker-side consumer stats will be cached in the client.
        is_read_compacted: bool, default=False
            Selects whether to read the compacted version of the topic
        properties: dict, optional
            Sets the properties for the consumer. The properties associated with a consumer can be used for
            identify a consumer at broker side.
        pattern_auto_discovery_period: int, default=60
            Periods of seconds for consumer to auto discover match topics.
        initial_position: InitialPosition, default=InitialPosition.Latest
          Set the initial position of a consumer when subscribing to the topic.
          It could be either: ``InitialPosition.Earliest`` or ``InitialPosition.Latest``.
        crypto_key_reader: CryptoKeyReader, optional
            Symmetric encryption class implementation, configuring public key encryption messages for the producer
            and private key decryption messages for the consumer
        replicate_subscription_state_enabled: bool, default=False
            Set whether the subscription status should be replicated.
        max_pending_chunked_message: int, default=10
          Consumer buffers chunk messages into memory until it receives all the chunks of the original message.
          While consuming chunk-messages, chunks from same message might not be contiguous in the stream, and they
          might be mixed with other messages' chunks. so, consumer has to maintain multiple buffers to manage
          chunks coming from different messages. This mainly happens when multiple publishers are publishing
          messages on the topic concurrently or publisher failed to publish all chunks of the messages.

          If it's zero, the pending chunked messages will not be limited.
        auto_ack_oldest_chunked_message_on_queue_full: bool, default=False
          Buffering large number of outstanding uncompleted chunked messages can create memory pressure, and it
          can be guarded by providing the maxPendingChunkedMessage threshold. See setMaxPendingChunkedMessage.
          Once, consumer reaches this threshold, it drops the outstanding unchunked-messages by silently acking
          if autoAckOldestChunkedMessageOnQueueFull is true else it marks them for redelivery.
        start_message_id_inclusive: bool, default=False
          Set the consumer to include the given position of any reset operation like Consumer::seek.
        batch_receive_policy: class ConsumerBatchReceivePolicy
          Set the batch collection policy for batch receiving.
        key_shared_policy: class ConsumerKeySharedPolicy
            Set the key shared policy for use when the ConsumerType is KeyShared.
        batch_index_ack_enabled: Enable the batch index acknowledgement.
            It should be noted that this option can only work when the broker side also enables the batch index
            acknowledgement. See the `acknowledgmentAtBatchIndexLevelEnabled` config in `broker.conf`.
        regex_subscription_mode: RegexSubscriptionMode, optional
            Set the regex subscription mode for use when the topic is a regex pattern.

            Supported modes:

            * PersistentOnly: By default only subscribe to persistent topics.
            * NonPersistentOnly: Only subscribe to non-persistent topics.
            * AllTopics: Subscribe to both persistent and non-persistent topics.
        dead_letter_policy: class ConsumerDeadLetterPolicy
          Set dead letter policy for consumer.
          By default, some messages are redelivered many times, even to the extent that they can never be
          stopped. By using the dead letter mechanism, messages have the max redelivery count, when they're
          exceeding the maximum number of redeliveries. Messages are sent to dead letter topics and acknowledged
          automatically.
        """

        if message_listener:
            _loop = loop or asyncio.get_running_loop()
            _listener = partial(_message_listener, _loop, message_listener)
        else:
            _listener = None
        return super().subscribe(*args, message_listener=_listener, **kwargs)

    async def create_producer(self, topic,
                        producer_name=None,
                        schema=pulsar.schema.BytesSchema(),
                        initial_sequence_id=None,
                        send_timeout_millis=30000,
                        compression_type: CompressionType = CompressionType.NONE,
                        max_pending_messages=1000,
                        max_pending_messages_across_partitions=50000,
                        block_if_queue_full=False,
                        batching_enabled=False,
                        batching_max_messages=1000,
                        batching_max_allowed_size_in_bytes=128*1024,
                        batching_max_publish_delay_ms=10,
                        chunking_enabled=False,
                        message_routing_mode: PartitionsRoutingMode = PartitionsRoutingMode.RoundRobinDistribution,
                        lazy_start_partitioned_producers=False,
                        properties=None,
                        batching_type: BatchingType = BatchingType.Default,
                        encryption_key=None,
                        crypto_key_reader: Union[None, pulsar.CryptoKeyReader] = None,
                        access_mode: ProducerAccessMode = ProducerAccessMode.Shared,
                        ):
        """
        Asynchronously create a new producer on a given topic.

        Parameters
        ----------

        topic: str
            The topic name
        producer_name: str, optional
            Specify a name for the producer. If not assigned, the system will generate a globally unique name
            which can be accessed with `Producer.producer_name()`. When specifying a name, it is app to the user
            to ensure that, for a given topic, the producer name is unique across all Pulsar's clusters.
        schema: pulsar.schema.Schema, default=pulsar.schema.BytesSchema
            Define the schema of the data that will be published by this producer, e.g,
            ``schema=JsonSchema(MyRecordClass)``.

            The schema will be used for two purposes:
                * Validate the data format against the topic defined schema
                * Perform serialization/deserialization between data and objects
        initial_sequence_id: int, optional
            Set the baseline for the sequence ids for messages published by the producer. First message will be
            using ``(initialSequenceId + 1)`` as its sequence id and subsequent messages will be assigned
            incremental sequence ids, if not otherwise specified.
        send_timeout_millis: int, default=30000
            If a message is not acknowledged by the server before the ``send_timeout`` expires, an error will be reported.
        compression_type: CompressionType, default=CompressionType.NONE
            Set the compression type for the producer. By default, message payloads are not compressed.

            Supported compression types:

            * CompressionType.LZ4
            * CompressionType.ZLib
            * CompressionType.ZSTD
            * CompressionType.SNAPPY

            ZSTD is supported since Pulsar 2.3. Consumers will need to be at least at that release in order to
            be able to receive messages compressed with ZSTD.

            SNAPPY is supported since Pulsar 2.4. Consumers will need to be at least at that release in order to
            be able to receive messages compressed with SNAPPY.
        batching_enabled: bool, default=False
            When automatic batching is enabled, multiple calls to `send` can result in a single batch to be sent to the
            broker, leading to better throughput, especially when publishing small messages.
            All messages in a batch will be published as a single batched message. The consumer will be delivered
            individual messages in the batch in the same order they were enqueued.
        batching_max_messages: int, default=1000
            When you set this option to a value greater than 1, messages are queued until this threshold or
            `batching_max_allowed_size_in_bytes` is reached or batch interval has elapsed.
        batching_max_allowed_size_in_bytes: int, default=128*1024
            When you set this option to a value greater than 1, messages are queued until this threshold or
            `batching_max_messages` is reached or batch interval has elapsed.
        batching_max_publish_delay_ms: int, default=10
            The batch interval in milliseconds. Queued messages will be sent in batch after this interval even if both
            the threshold of `batching_max_messages` and `batching_max_allowed_size_in_bytes` are not reached.
        max_pending_messages: int, default=1000
            Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
        max_pending_messages_across_partitions: int, default=50000
            Set the max size of the queue holding the messages pending to receive an acknowledgment across partitions
            from the broker.
        block_if_queue_full: bool, default=False
            Set whether `send_async` operations should block when the outgoing message queue is full.
        message_routing_mode: PartitionsRoutingMode, default=PartitionsRoutingMode.RoundRobinDistribution
            Set the message routing mode for the partitioned producer.

            Supported modes:

            * ``PartitionsRoutingMode.RoundRobinDistribution``
            * ``PartitionsRoutingMode.UseSinglePartition``
        lazy_start_partitioned_producers: bool, default=False
            This config affects producers of partitioned topics only. It controls whether producers register
            and connect immediately to the owner broker of each partition or start lazily on demand. The internal
            producer of one partition is always started eagerly, chosen by the routing policy, but the internal
            producers of any additional partitions are started on demand, upon receiving their first message.

            Using this mode can reduce the strain on brokers for topics with large numbers of partitions and when
            the SinglePartition routing policy is used without keyed messages. Because producer connection can be
            on demand, this can produce extra send latency for the first messages of a given partition.
        properties: dict, optional
            Sets the properties for the producer. The properties associated with a producer can be used for identify
            a producer at broker side.
        batching_type: BatchingType, default=BatchingType.Default
            Sets the batching type for the producer.

            There are two batching type: DefaultBatching and KeyBasedBatching.

            DefaultBatching will batch single messages:
                (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
            ... into single batch message:
                [(k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)]

            KeyBasedBatching will batch incoming single messages:
                (k1, v1), (k2, v1), (k3, v1), (k1, v2), (k2, v2), (k3, v2), (k1, v3), (k2, v3), (k3, v3)
            ... into single batch message:
                [(k1, v1), (k1, v2), (k1, v3)], [(k2, v1), (k2, v2), (k2, v3)], [(k3, v1), (k3, v2), (k3, v3)]
        chunking_enabled: bool, default=False
            If message size is higher than allowed max publish-payload size by broker then chunking_enabled helps
            producer to split message into multiple chunks and publish them to broker separately and in order.
            So, it allows client to successfully publish large size of messages in pulsar.
        encryption_key: str, optional
            The key used for symmetric encryption, configured on the producer side
        crypto_key_reader: CryptoKeyReader, optional
            Symmetric encryption class implementation, configuring public key encryption messages for the producer
            and private key decryption messages for the consumer
        access_mode: ProducerAccessMode, optional
            Set the type of access mode that the producer requires on the topic.

            Supported modes:

            * Shared: By default multiple producers can publish on a topic.
            * Exclusive: Require exclusive access for producer.
                         Fail immediately if there's already a producer connected.
            * WaitForExclusive: Producer creation is pending until it can acquire exclusive access.
            * ExclusiveWithFencing: Acquire exclusive access for the producer.
                                    Any existing producer will be removed and invalidated immediately.
        """
        _check_type(str, topic, 'topic')
        _check_type_or_none(str, producer_name, 'producer_name')
        _check_type(pulsar.schema.Schema, schema, 'schema')
        _check_type_or_none(int, initial_sequence_id, 'initial_sequence_id')
        _check_type(int, send_timeout_millis, 'send_timeout_millis')
        _check_type(CompressionType, compression_type, 'compression_type')
        _check_type(int, max_pending_messages, 'max_pending_messages')
        _check_type(int, max_pending_messages_across_partitions, 'max_pending_messages_across_partitions')
        _check_type(bool, block_if_queue_full, 'block_if_queue_full')
        _check_type(bool, batching_enabled, 'batching_enabled')
        _check_type(int, batching_max_messages, 'batching_max_messages')
        _check_type(int, batching_max_allowed_size_in_bytes, 'batching_max_allowed_size_in_bytes')
        _check_type(int, batching_max_publish_delay_ms, 'batching_max_publish_delay_ms')
        _check_type(bool, chunking_enabled, 'chunking_enabled')
        _check_type_or_none(dict, properties, 'properties')
        _check_type(BatchingType, batching_type, 'batching_type')
        _check_type_or_none(str, encryption_key, 'encryption_key')
        _check_type_or_none(pulsar.CryptoKeyReader, crypto_key_reader, 'crypto_key_reader')
        _check_type(bool, lazy_start_partitioned_producers, 'lazy_start_partitioned_producers')
        _check_type(ProducerAccessMode, access_mode, 'access_mode')

        conf = ProducerConfiguration()
        conf.send_timeout_millis(send_timeout_millis)
        conf.compression_type(compression_type)
        conf.max_pending_messages(max_pending_messages)
        conf.max_pending_messages_across_partitions(max_pending_messages_across_partitions)
        conf.block_if_queue_full(block_if_queue_full)
        conf.batching_enabled(batching_enabled)
        conf.batching_max_messages(batching_max_messages)
        conf.batching_max_allowed_size_in_bytes(batching_max_allowed_size_in_bytes)
        conf.batching_max_publish_delay_ms(batching_max_publish_delay_ms)
        conf.partitions_routing_mode(message_routing_mode)
        conf.batching_type(batching_type)
        conf.chunking_enabled(chunking_enabled)
        conf.lazy_start_partitioned_producers(lazy_start_partitioned_producers)
        conf.access_mode(access_mode)
        if producer_name:
            conf.producer_name(producer_name)
        if initial_sequence_id:
            conf.initial_sequence_id(initial_sequence_id)
        if properties:
            for k, v in properties.items():
                conf.property(k, v)

        conf.schema(schema.schema_info())
        if encryption_key:
            conf.encryption_key(encryption_key)
        if crypto_key_reader:
            conf.crypto_key_reader(crypto_key_reader.cryptoKeyReader)

        if batching_enabled and chunking_enabled:
            raise ValueError("Batching and chunking of messages can't be enabled together.")

        fut = asyncio.get_running_loop().create_future()
        self._client.create_producer_async(topic, conf, partial(_set_future_threadsafe, fut))
        return Producer(await fut)

    async def close(self) -> None:
        """
        Close the client and all the associated producers and consumers

        Raises
        ------
        PulsarException
        """
        fut = asyncio.get_running_loop().create_future()
        self._client.close_async(partial(_set_future_threadsafe, fut, value=None))
        await fut
