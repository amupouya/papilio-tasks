"""Independent publications keep native transport and consumer semantics."""

import asyncio
import os
from importlib import import_module
from textwrap import dedent
from uuid import uuid4

import pytest
from dishka import FromDishka
from faststream import AckPolicy, StreamMessage
from pydantic import BaseModel

from papilio_tasks.apps.events.application import create_app


class Event(BaseModel):
    value: int


@pytest.fixture(
    params=[
        "rabbit",
        "kafka",
        "nats",
        "jetstream",
        "streams",
        "channels",
        "lists",
    ]
)
def family(request):
    kind = request.param
    backend = {
        "jetstream": "nats",
        "streams": "redis",
        "channels": "redis",
        "lists": "redis",
    }.get(kind, kind)
    title = backend.capitalize()
    native = import_module("faststream." + backend)
    address = os.environ.get("TEST_" + backend.upper() + "_URL")
    broker = getattr(native, title + "Broker")(
        *([address] if address else []), logger=None
    )
    suffix = uuid4().hex[:12]
    prefix = f"module_{suffix}."
    destination = prefix + "created"
    transport = import_module(
        "papilio_tasks.infra.faststream.brokers.backends." + backend
    )
    registry = import_module("papilio_tasks.apps.events.registry." + kind)
    registrar = getattr(
        registry,
        {
            "jetstream": "Jet",
            "streams": "Stream",
            "channels": "Channel",
            "lists": "List",
        }.get(kind, title)
        + "Registrar",
    )
    reg = registrar(getattr(transport, title + "Broker")(broker))
    router_type = getattr(
        import_module("papilio_tasks.apps.events.routers." + backend),
        title + "Router",
    )
    factory = getattr(
        import_module("papilio_tasks.apps.events.publishers." + kind),
        "publisher",
    )
    options, consumer = {}, {}
    if kind == "rabbit":
        options = dict(
            exchange=native.RabbitExchange("events_" + suffix),
            routing_key=destination,
        )
        consumer = dict(
            queue=native.RabbitQueue(
                "workers_" + suffix, queue_type=native.QueueType.QUORUM
            )
        )
    elif kind == "kafka":
        options = dict(topic=destination, key=b"fixed")
        consumer = dict(
            group_id="workers_" + suffix, auto_offset_reset="earliest"
        )
    elif kind in ("nats", "jetstream"):
        options = dict(subject=destination)
        if kind == "jetstream":
            options["stream"] = native.JStream(
                "events_" + suffix, subjects=[destination]
            )
            consumer = dict(durable="workers_" + suffix, pull_sub=True)
        else:
            consumer = dict(queue="workers")
    elif kind == "streams":
        options = dict(stream=native.StreamSub(destination, maxlen=100))
        consumer = dict(
            stream=native.StreamSub(
                "created", group="workers", consumer="one", last_id="0"
            )
        )
    elif kind == "channels":
        options = dict(channel=destination)
    else:
        options = dict(list=destination)
    return (
        kind,
        native,
        broker,
        reg,
        router_type,
        factory,
        options,
        consumer,
        prefix,
    )


async def test_independent_publication_native_delivery_and_prefix(family):
    (
        kind,
        native,
        broker,
        reg,
        router_type,
        factory,
        options,
        consumer,
        prefix,
    ) = family
    route = factory(Event, outbox_key="created.v1", **options)
    router, seen = router_type(prefix=prefix, include_in_schema=False), []

    @router.subscriber(publisher=route, **consumer)
    async def receive(event: Event) -> None:
        assert isinstance(event, Event)
        seen.append(event.value)

    app = create_app(
        registrar=reg, publishers=[route, route], routers=[router]
    )
    try:
        assert len(broker.subscribers) == 1
        if kind == "rabbit":
            assert (
                broker.subscribers[0].queue.arguments["x-queue-type"]
                == "quorum"
            )
        test = getattr(native, "Test" + type(broker).__name__)
        async with test(broker):
            await route.publish(Event(value=7))
        assert seen == [7]
    finally:
        await app.stop()
    with pytest.raises(RuntimeError, match="not registered"):
        await route.publish(Event(value=8))


async def test_subscription_reference_does_not_bind_a_sender(family):
    _, _, broker, reg, router_type, factory, options, consumer, prefix = family
    route = factory(Event, **options)
    router = router_type(prefix=prefix)

    @router.subscriber(publisher=route, **consumer)
    async def receive(event: Event) -> None:
        pass

    app = create_app(registrar=reg, routers=[router])
    try:
        assert len(broker.subscribers) == 1
        assert not reg.has_publisher(route)
        with pytest.raises(RuntimeError, match="not registered"):
            await route.publish(Event(value=1))
    finally:
        await app.stop()


async def test_native_subscription_override_remains_available():
    from faststream.kafka import KafkaBroker, TestKafkaBroker

    from papilio_tasks.apps.events.publishers.kafka import publisher
    from papilio_tasks.apps.events.registry.kafka import KafkaRegistrar
    from papilio_tasks.apps.events.routers.kafka import KafkaRouter
    from papilio_tasks.infra.faststream.brokers.backends.kafka import (
        KafkaBroker as Transport,
    )

    native = KafkaBroker(logger=None)
    route = publisher(Event, topic="first")
    router, seen = KafkaRouter(), []

    @router.subscriber("first", "second", publisher=route, group_id="workers")
    async def receive(event: Event) -> None:
        seen.append(event.value)

    app = create_app(
        registrar=KafkaRegistrar(Transport(native)), routers=[router]
    )
    try:
        async with TestKafkaBroker(native):
            await native.publish(Event(value=1), "first")
            await native.publish(Event(value=2), "second")
        assert seen == [1, 2]
    finally:
        await app.stop()


async def test_discovery_deduplicates_handles_and_isolates_consumers(
    tmp_path, monkeypatch
):
    from test_function_events import registry

    monkeypatch.syspath_prepend(str(tmp_path))
    package = tmp_path / "standalone_discovery"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "publishers.py").write_text(
        dedent("""
        from papilio_tasks.apps.events.publishers.nats import publisher
        created = publisher(dict, subject="created", outbox_key="created.v1")
        alias = created
    """)
    )
    (package / "subscribers.py").write_text(
        "raise RuntimeError('consumer imported')"
    )
    module = import_module("standalone_discovery.publishers")
    app = create_app(
        registrar=registry(),
        publishers=["standalone_discovery", module.created],
    )
    try:
        assert app.registrar.has_publisher(module.created)
        assert not app.registrar.broker.native.subscribers
    finally:
        await app.stop()


def test_router_native_settings_are_explicit_and_retry_is_per_handler():
    from inspect import signature

    from papilio_tasks.apps.events.routers.nats import NatsRouter

    parameters = signature(NatsRouter).parameters
    assert {
        "prefix",
        "middlewares",
        "parser",
        "decoder",
        "include_in_schema",
        "ack_policy",
    } <= parameters.keys()
    router = NatsRouter(ack_policy=AckPolicy.ACK)
    assert router.options["ack_policy"] is AckPolicy.ACK


def test_references_reject_transport_and_prefix_mismatch():
    from test_function_events import registry

    from papilio_tasks.apps.events.publishers.kafka import (
        publisher as kafka_publisher,
    )
    from papilio_tasks.apps.events.publishers.nats import publisher
    from papilio_tasks.apps.events.routers.nats import NatsRouter

    for route, prefix, match in [
        (kafka_publisher(Event, topic="created"), "", "transport"),
        (publisher(Event, subject="other.created"), "module.", "prefix"),
    ]:
        router = NatsRouter(prefix=prefix)

        @router.subscriber(publisher=route)
        async def receive(event: Event) -> None:
            pass

        with pytest.raises(ValueError, match=match):
            create_app(registrar=registry(), routers=[router])


async def test_bootstrap_options_reach_references_without_mutation():
    from faststream.nats import TestNatsBroker
    from test_function_events import registry

    from papilio_tasks.apps.events.publishers.nats import publisher
    from papilio_tasks.apps.events.routers.nats import NatsRouter

    route = publisher(
        Event, subject="original", headers={"origin": "declaration"}
    )
    reg, router, seen = registry(), NatsRouter(prefix="module."), []
    route.include(
        reg, subject="module.selected", headers={"origin": "bootstrap"}
    )

    @router.subscriber(publisher=route)
    async def receive(
        event: Event, *, message: FromDishka[StreamMessage]
    ) -> None:
        seen.append((event.value, message.headers["origin"]))

    app = create_app(registrar=reg, publishers=[route], routers=[router])
    try:
        async with TestNatsBroker(reg.broker.native):
            await route.publish(Event(value=1))
        assert seen == [(1, "bootstrap")]
        assert route.options["subject"] == "original"
        assert route.options["headers"] == {"origin": "declaration"}
    finally:
        await app.stop()
    fresh = create_app(registrar=registry(), publishers=[route])
    try:
        assert (
            fresh.registrar.publisher_options(route)["subject"] == "original"
        )
    finally:
        await fresh.stop()


async def test_live_independent_publication_and_reference(family):
    kind, _, broker, reg, router_type, factory, options, consumer, prefix = (
        family
    )
    backend = {
        "jetstream": "nats",
        "streams": "redis",
        "channels": "redis",
        "lists": "redis",
    }.get(kind, kind)
    if not os.environ.get("TEST_" + backend.upper() + "_URL"):
        pytest.skip("Requires isolated " + backend)
    route = factory(Event, **options)
    router, seen, done = router_type(prefix=prefix), [], asyncio.Event()

    @router.subscriber(publisher=route, **consumer)
    async def receive(event: Event) -> None:
        seen.append(event.value)
        done.set()

    app = create_app(registrar=reg, publishers=[route], routers=[router])
    try:
        await app.start()
        await route.publish(Event(value=7))
        await asyncio.wait_for(done.wait(), 20)
        assert seen == [7]
    finally:
        await app.stop()


async def test_rabbit_explicit_wildcard_binding_is_not_replaced():
    from faststream.rabbit import (
        RabbitBroker,
        RabbitExchange,
        RabbitQueue,
        TestRabbitBroker,
    )

    from papilio_tasks.apps.events.publishers.rabbit import publisher
    from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
    from papilio_tasks.apps.events.routers.rabbit import RabbitRouter
    from papilio_tasks.infra.faststream.brokers.backends.rabbit import (
        RabbitBroker as Transport,
    )

    exchange = RabbitExchange("events", type="topic")
    route = publisher(Event, exchange=exchange, routing_key="orders.created")
    queue = RabbitQueue("workers", routing_key="orders.*")
    router, native, seen = RabbitRouter(), RabbitBroker(logger=None), []

    @router.subscriber(publisher=route, queue=queue)
    async def receive(event: Event) -> None:
        seen.append(event.value)

    app = create_app(
        registrar=RabbitRegistrar(Transport(native)),
        publishers=[route],
        routers=[router],
    )
    try:
        async with TestRabbitBroker(native):
            await route.publish(Event(value=1))
            await native.publish(
                Event(value=2),
                exchange=exchange,
                routing_key="orders.cancelled",
            )
        assert seen == [1, 2]
        assert queue.routing_key == "orders.*"
    finally:
        await app.stop()


@pytest.mark.parametrize("backend", ["kafka", "lists"])
async def test_native_batch_publication_keeps_its_list_contract(backend):
    if backend == "kafka":
        from faststream.kafka import KafkaBroker, TestKafkaBroker

        from papilio_tasks.apps.events.publishers.kafka import publisher
        from papilio_tasks.apps.events.registry.kafka import KafkaRegistrar
        from papilio_tasks.apps.events.routers.kafka import KafkaRouter
        from papilio_tasks.infra.faststream.brokers.backends.kafka import (
            KafkaBroker as Transport,
        )

        native, router = KafkaBroker(logger=None), KafkaRouter()
        reg, tester = KafkaRegistrar(Transport(native)), TestKafkaBroker
        route = publisher(list[Event], topic="batch", batch=True)
        options = dict(group_id="workers", batch=True)
    else:
        from faststream.redis import ListSub, RedisBroker, TestRedisBroker

        from papilio_tasks.apps.events.publishers.lists import publisher
        from papilio_tasks.apps.events.registry.lists import ListRegistrar
        from papilio_tasks.apps.events.routers.redis import RedisRouter
        from papilio_tasks.infra.faststream.brokers.backends.redis import (
            RedisBroker as Transport,
        )

        native, router = RedisBroker(logger=None), RedisRouter()
        reg, tester = ListRegistrar(Transport(native)), TestRedisBroker
        route = publisher(list[Event], list=ListSub("batch", batch=True))
        options = dict(list=ListSub("batch", batch=True))
    seen = []

    @router.subscriber(publisher=route, **options)
    async def receive(events: list[Event]) -> None:
        seen.extend(event.value for event in events)

    app = create_app(registrar=reg, publishers=[route], routers=[router])
    try:
        async with tester(native):
            await route.publish([Event(value=1), Event(value=2)])
        assert seen == [1, 2]
    finally:
        await app.stop()


async def test_handler_ack_overrides_router_default_for_retry():
    from faststream.nats import JStream, TestNatsBroker
    from test_function_events import registry

    from papilio_tasks.apps.events.publishers.nats import publisher
    from papilio_tasks.apps.events.routers.nats import NatsRouter
    from papilio_tasks.tools.retry import Retry

    route = publisher(Event, subject="created", stream=JStream("retry_events"))
    router, seen = NatsRouter(ack_policy=AckPolicy.ACK_FIRST), []

    @router.subscriber(
        publisher=route,
        ack_policy=AckPolicy.ACK,
        retry=Retry(attempts=2, delay=0, errors=(ValueError,)),
    )
    async def receive(event: Event) -> None:
        seen.append(event.value)
        if len(seen) == 1:
            raise ValueError("transient")

    reg = registry()
    app = create_app(registrar=reg, publishers=[route], routers=[router])
    try:
        async with TestNatsBroker(reg.broker.native):
            await route.publish(Event(value=1))
        assert seen == [1, 1]
    finally:
        await app.stop()


async def test_rabbit_default_exchange_keeps_queue_ownership_explicit():
    from faststream.rabbit import RabbitBroker, TestRabbitBroker

    from papilio_tasks.apps.events.publishers.rabbit import publisher
    from papilio_tasks.apps.events.registry.rabbit import RabbitRegistrar
    from papilio_tasks.apps.events.routers.rabbit import RabbitRouter
    from papilio_tasks.infra.faststream.brokers.backends.rabbit import (
        RabbitBroker as Transport,
    )

    route, seen = publisher(Event, queue="module.work"), []
    invalid = RabbitRouter(prefix="module.")

    @invalid.subscriber(publisher=route)
    async def missing_queue(event: Event) -> None:
        pass

    with pytest.raises(ValueError, match="own queue"):
        create_app(
            registrar=RabbitRegistrar(Transport(RabbitBroker(logger=None))),
            routers=[invalid],
        )

    router, native = RabbitRouter(prefix="module."), RabbitBroker(logger=None)

    @router.subscriber(publisher=route, queue="work")
    async def receive(event: Event) -> None:
        seen.append(event.value)

    app = create_app(
        registrar=RabbitRegistrar(Transport(native)),
        publishers=[route],
        routers=[router],
    )
    try:
        async with TestRabbitBroker(native):
            await route.publish(Event(value=1))
        assert seen == [1]
    finally:
        await app.stop()
