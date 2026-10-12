import asyncio
import gc
import threading
from unittest.mock import Mock

import pytest

from pulsar_asyncio import coroutine_listener


def message():
    result = Mock()
    result.message_id.return_value = "test-id"
    return result


def test_native_thread_wakes_idle_loop_and_preserves_arguments():
    async def run():
        loop = asyncio.get_running_loop()
        completed = loop.create_future()
        consumer, received = Mock(), message()
        loop_thread = threading.get_ident()

        async def listener(actual_consumer, actual_message):
            assert threading.get_ident() == loop_thread
            assert actual_consumer is consumer
            assert actual_message is received
            completed.set_result(asyncio.current_task().get_name())

        callback = coroutine_listener(listener)
        # Invoke after the event loop has gone idle waiting for the handler.
        timer = threading.Timer(0.05, callback, args=(consumer, received))
        timer.start()
        try:
            assert await asyncio.wait_for(completed, 1) == "MessageListenerTask-test-id"
        finally:
            timer.join()
        consumer.acknowledge.assert_not_called()

    asyncio.run(run())


def test_explicit_loop_allows_adapter_creation_outside_running_loop():
    loop = asyncio.new_event_loop()
    completed = loop.create_future()

    async def listener(consumer, received):
        assert asyncio.get_running_loop() is loop
        completed.set_result(received)

    try:
        callback = coroutine_listener(listener, loop=loop)
        received = message()
        thread = threading.Thread(target=callback, args=(Mock(), received))
        thread.start()
        thread.join()
        assert loop.run_until_complete(asyncio.wait_for(completed, 1)) is received
        loop.run_until_complete(asyncio.sleep(0))
    finally:
        loop.close()


def test_default_loop_requires_a_running_event_loop():
    async def listener(consumer, received):
        pass

    with pytest.raises(RuntimeError, match="no running event loop"):
        coroutine_listener(listener)


def test_messages_run_concurrently_and_survive_garbage_collection():
    async def run():
        started = asyncio.Queue()
        completed = asyncio.Queue()
        release = asyncio.Event()

        async def listener(consumer, received):
            started.put_nowait(received)
            await release.wait()
            completed.put_nowait(received)

        callback = coroutine_listener(listener)
        first, second = message(), message()
        callback(Mock(), first)
        callback(Mock(), second)
        assert await asyncio.wait_for(started.get(), 1) is first
        assert await asyncio.wait_for(started.get(), 1) is second
        gc.collect()
        release.set()
        assert await asyncio.wait_for(completed.get(), 1) is first
        assert await asyncio.wait_for(completed.get(), 1) is second
        await asyncio.sleep(0)

    asyncio.run(run())


def test_handler_errors_reach_asyncio_exception_handler():
    async def run():
        loop = asyncio.get_running_loop()
        reported = loop.create_future()
        loop.set_exception_handler(lambda loop, context: reported.set_result(context))
        consumer = Mock()
        error = ValueError("handler failed")

        async def listener(consumer, received):
            raise error

        callback = coroutine_listener(listener)
        callback(consumer, message())
        context = await asyncio.wait_for(reported, 1)
        assert context["exception"] is error
        consumer.acknowledge.assert_not_called()
        consumer.negative_acknowledge.assert_not_called()

    asyncio.run(run())


def test_handler_can_acknowledge_after_awaiting():
    async def run():
        completed = asyncio.Event()
        consumer, received = Mock(), message()

        async def listener(actual_consumer, actual_message):
            await asyncio.sleep(0)
            actual_consumer.acknowledge(actual_message)
            completed.set()

        coroutine_listener(listener)(consumer, received)
        await asyncio.wait_for(completed.wait(), 1)
        consumer.acknowledge.assert_called_once_with(received)

    asyncio.run(run())


def test_callbacks_after_loop_closure_are_ignored():
    loop = asyncio.new_event_loop()
    called = Mock()

    async def listener(consumer, received):
        called()

    callback = coroutine_listener(listener, loop=loop)
    loop.close()
    callback(Mock(), message())
    called.assert_not_called()


def test_unexpected_scheduling_errors_are_not_suppressed():
    loop = Mock(spec=asyncio.AbstractEventLoop)
    loop.is_closed.return_value = False
    loop.call_soon_threadsafe.side_effect = RuntimeError("scheduling failed")

    async def listener(consumer, received):
        pass

    callback = coroutine_listener(listener, loop=loop)
    with pytest.raises(RuntimeError, match="scheduling failed"):
        callback(Mock(), message())
