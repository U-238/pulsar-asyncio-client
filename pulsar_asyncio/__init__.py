"""Adapt coroutine listeners for the upstream Pulsar client's subscriptions."""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import pulsar

__all__ = ["coroutine_listener"]


def coroutine_listener(
    handler: Callable[[pulsar.Consumer, pulsar.Message], Coroutine[Any, Any, None]],
    *,
    loop: asyncio.AbstractEventLoop | None = None,
) -> Callable[[pulsar.Consumer, pulsar.Message], None]:
    """Wrap an async handler for ``pulsar.Client.subscribe(message_listener=...)``.

    The handler receives the upstream consumer and message. Each message runs in
    a separate task on the selected event loop. By default, capture the currently
    running loop when this adapter is created. An explicit loop can be supplied
    when subscribing from another thread.

    The handler is responsible for acknowledging messages and handling failures.
    Unhandled task exceptions are reported by asyncio. Close the consumer before
    closing the loop; callbacks arriving after loop closure are ignored.
    """
    event_loop = loop if loop is not None else asyncio.get_running_loop()
    tasks: set[asyncio.Task] = set()

    def schedule(consumer, message):
        task = event_loop.create_task(
            handler(consumer, message),
            name=f"MessageListenerTask-{message.message_id()}",
        )
        # Keep a strong reference until completion so suspended tasks survive GC.
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    def callback(consumer, message):
        # Pulsar invokes listeners on native threads. Schedule task creation on
        # the event loop thread, waking its selector even when the loop is idle.
        try:
            event_loop.call_soon_threadsafe(schedule, consumer, message)
        except RuntimeError:
            if not event_loop.is_closed():
                raise

    return callback
