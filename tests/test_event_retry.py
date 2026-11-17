import asyncio
import os
from collections.abc import AsyncIterator
from uuid import uuid4

import pytest

pytest.importorskip("tenacity")
pytest.importorskip("dishka_faststream")
pytest.importorskip("faststream.rabbit")

from dishka import Provider, Scope, provide
from faststream import AckPolicy, StreamMessage
from faststream.exceptions import NackMessage
from faststream.rabbit import RabbitBroker as NativeBroker
from faststream.rabbit import TestRabbitBroker
from hook_support import handler

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.rabbit import (
    RabbitExchange,
    RabbitPublisher,
)
from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
from papilio_tasks.apps.events.subscribers.rabbit import (
    RabbitQueue,
    RabbitSubscriber,
)
from papilio_tasks.infra.faststream import retry as engine
from papilio_tasks.infra.faststream.brokers.backends.rabbit import RabbitBroker
from papilio_tasks.tools.hooks.subscribe import SubscribeHooks
from papilio_tasks.tools.retry import Backoff, Retry


def declarations():
    class Created(RabbitPublisher[int]):
        exchange = RabbitExchange("retry")
        routing_key = "created"

    class Reader(RabbitSubscriber[int]):
        publisher = Created
        queue = RabbitQueue("reader")

    return Created, Reader


async def test_retry_has_fresh_scope_and_only_failed_subscriber_repeats():
    created, base = declarations()
    calls, resources, closed, seen_messages = [], [], [], []

    class Resource:
        pass

    class Reader(base):
        retry = Retry(attempts=3, delay=0, errors=(ConnectionError,))

        def __init__(self, resource: Resource, message: StreamMessage):
            self.resource = resource
            self.message = message

        async def run(self, event: int) -> None:
            assert resources[:-1] == closed
            calls.append(event)
            seen_messages.append(self.message)
            if len(calls) < 3:
                raise ConnectionError("transient")

    observed = []

    class Other(base):
        queue = RabbitQueue("other")

        async def run(self, event: int) -> None:
            observed.append(event)

    class Dependencies(Provider):
        @provide(scope=Scope.REQUEST)
        async def resource(self) -> AsyncIterator[Resource]:
            item = Resource()
            resources.append(item)
            try:
                yield item
            finally:
                closed.append(item)

        reader = provide(Reader, scope=Scope.REQUEST)
        other = provide(Other, scope=Scope.REQUEST)

    native = NativeBroker(logger=None)
    reg = RabbitRegistrar(RabbitBroker(native))
    reg.publisher(created)
    reg.subscriber(Reader)
    reg.subscriber(Other)
    app = create_app(registrar=reg, providers=[Dependencies()])
    try:
        async with TestRabbitBroker(native):
            await created.publish(7)
        assert calls == [7, 7, 7]
        assert observed == [7]
        assert len(resources) == len({id(item) for item in resources}) == 3
        assert closed == resources
        assert all(message is seen_messages[0] for message in seen_messages)
    finally:
        await app.stop()


@pytest.mark.parametrize("mode", ["exhaust", "unmatched", "cancel", "native"])
async def test_retry_preserves_error_and_attempt_budget(mode):
    created, base = declarations()
    calls = []
    error = (
        ConnectionError("retry")
        if mode == "exhaust"
        else ValueError("permanent")
        if mode == "unmatched"
        else asyncio.CancelledError()
        if mode == "cancel"
        else NackMessage()
    )

    class Reader(base):
        retry = Retry(attempts=3, delay=0, errors=(Exception,))

        async def run(self, event: int) -> None:
            calls.append(event)
            raise error

    if mode == "unmatched":
        Reader.retry = Retry(attempts=3, delay=0, errors=(ConnectionError,))
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    native = NativeBroker(logger=None)
    reg = RabbitRegistrar(RabbitBroker(native))
    reg.publisher(created)
    reg.subscriber(Reader)
    app = create_app(registrar=reg, providers=[provider])
    try:
        async with TestRabbitBroker(native):
            if mode == "native":
                await created.publish(9)
            else:
                with pytest.raises(type(error)) as caught:
                    await created.publish(9)
                assert caught.value is error
        assert len(calls) == (3 if mode == "exhaust" else 1)
    finally:
        await app.stop()


@pytest.mark.parametrize("policy", [AckPolicy.ACK_FIRST, AckPolicy.MANUAL])
@pytest.mark.parametrize("selection", ["class", "registrar", "broker"])
def test_retry_rejects_incompatible_acknowledgement(policy, selection):
    _, base = declarations()

    class Reader(base):
        retry = Retry(attempts=2, delay=0, errors=(Exception,))

        async def run(self, event: int) -> None: ...

    if selection == "class":
        Reader.ack_policy = policy
    native = NativeBroker(
        logger=None,
        **({"ack_policy": policy} if selection == "broker" else {}),
    )
    reg = RabbitRegistrar(RabbitBroker(native))
    with pytest.raises(ValueError, match="ack"):
        reg.subscriber(
            Reader,
            **({"ack_policy": policy} if selection == "registrar" else {}),
        )


async def test_disable_inherited_retry_and_fixed_backoff_wait(monkeypatch):
    created, base = declarations()
    attempts, waits = [], []

    async def sleep(delay):
        waits.append(delay)

    monkeypatch.setattr(engine, "sleep", sleep)

    class Parent(base):
        retry = Retry(
            attempts=3,
            delay=Backoff(initial=1, multiplier=2, max_delay=5),
            errors=(ConnectionError,),
        )

        async def run(self, event: int) -> None:
            attempts.append(event)
            if len(attempts) < 3:
                raise ConnectionError()

    class Child(Parent):
        pass

    class Disabled(Parent):
        retry = None
        queue = RabbitQueue("disabled")

        async def run(self, event: int) -> None: ...

    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Child)
    provider.provide(Disabled)
    native = NativeBroker(logger=None)
    reg = RabbitRegistrar(RabbitBroker(native))
    reg.publisher(created)
    reg.subscriber(Child)
    reg.subscriber(Disabled)
    app = create_app(registrar=reg, providers=[provider])
    try:
        async with TestRabbitBroker(native):
            await created.publish(2)
        assert attempts == [2, 2, 2]
        assert waits == [1, 2]
    finally:
        await app.stop()


async def test_after_hook_failure_retries_whole_execution(monkeypatch):
    created, base = declarations()
    calls, after_calls, errors, closed, sleeps = [], [], [], [], []

    class Reader(base):
        retry = Retry(attempts=2, delay=1, errors=(LookupError,))

        async def run(self, event: int) -> None:
            calls.append(event)

    async def after(call):
        after_calls.append(call)
        if len(after_calls) == 1:
            raise LookupError("after")

    async def failed(call):
        errors.append(call)

    async def hooks() -> AsyncIterator[SubscribeHooks]:
        try:
            yield SubscribeHooks(
                after_run=(handler(after),), on_error=(handler(failed),)
            )
        finally:
            closed.append(True)

    async def sleep(delay):
        assert closed == [True]
        sleeps.append(delay)

    monkeypatch.setattr(engine, "sleep", sleep)
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    provider.provide(hooks, provides=SubscribeHooks)
    native = NativeBroker(logger=None)
    reg = RabbitRegistrar(RabbitBroker(native))
    reg.publisher(created)
    reg.subscriber(Reader)
    app = create_app(
        registrar=reg, providers=[provider], subscribe_hooks=SubscribeHooks
    )
    try:
        async with TestRabbitBroker(native):
            await created.publish(5)
        assert calls == [5, 5]
        assert len(errors) == 1 and errors[0].stage == "after_run"
        assert closed == [True, True] and sleeps == [1]
    finally:
        await app.stop()


async def test_cancellation_during_wait_does_not_start_another_attempt(
    monkeypatch,
):
    created, base = declarations()
    calls = []

    class Reader(base):
        retry = Retry(attempts=3, delay=1, errors=(ConnectionError,))

        async def run(self, event: int) -> None:
            calls.append(event)
            raise ConnectionError()

    async def sleep(delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr(engine, "sleep", sleep)
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    native = NativeBroker(logger=None)
    reg = RabbitRegistrar(RabbitBroker(native))
    reg.publisher(created)
    reg.subscriber(Reader)
    app = create_app(registrar=reg, providers=[provider])
    try:
        async with TestRabbitBroker(native):
            with pytest.raises(asyncio.CancelledError):
                await created.publish(4)
        assert calls == [4]
    finally:
        await app.stop()


def test_invalid_retry_policy_is_rejected():
    _, base = declarations()

    class Reader(base):
        retry = True

        async def run(self, event: int) -> None: ...

    reg = RabbitRegistrar(RabbitBroker(NativeBroker(logger=None)))
    with pytest.raises(TypeError, match="Retry or None"):
        reg.subscriber(Reader)


async def test_live_redis_local_retry_and_ack():
    # Local imports isolate the optional Redis backend from these unit tests.
    pytest.importorskip("faststream.redis")
    from faststream.redis import RedisBroker as NativeRedis
    from faststream.redis import StreamSub
    from redis.asyncio import Redis

    from papilio_tasks.apps.events.publishers.streams import StreamPublisher
    from papilio_tasks.apps.events.registry.streams import StreamRegistrar
    from papilio_tasks.apps.events.subscribers.streams import StreamSubscriber
    from papilio_tasks.infra.faststream.brokers.backends.redis import (
        RedisBroker,
    )

    url = os.environ.get("TEST_REDIS_URL")
    if not url:
        pytest.skip("Requires TEST_REDIS_URL")
    name = "papilio_retry_" + uuid4().hex
    calls = []
    done = asyncio.Event()

    class Created(StreamPublisher[int]):
        stream = name

    class Reader(StreamSubscriber[int]):
        publisher = Created
        stream = StreamSub(name, group="reader", consumer="one")
        ack_policy = AckPolicy.REJECT_ON_ERROR
        retry = Retry(attempts=3, delay=0, errors=(ConnectionError,))

        async def run(self, event: int) -> None:
            calls.append(event)
            if len(calls) < 3:
                raise ConnectionError()
            done.set()

    native = NativeRedis(url, logger=None)
    reg = StreamRegistrar(RedisBroker(native))
    reg.publisher(Created)
    reg.subscriber(Reader)
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Reader)
    app = create_app(registrar=reg, providers=[provider])
    client = Redis.from_url(url)
    try:
        await app.start()
        await Created.publish(8)
        await asyncio.wait_for(done.wait(), 10)
        await app.stop()
        assert calls == [8, 8, 8]
        assert (await client.xpending(name, "reader"))["pending"] == 0
    finally:
        await app.stop()
        await client.delete(name)
        await client.aclose()
