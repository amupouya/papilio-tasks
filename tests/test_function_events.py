import sys
from collections.abc import AsyncIterator
from importlib import import_module
from textwrap import dedent

import pytest
from dishka import FromDishka, Provider, Scope, provide
from faststream import AckPolicy, StreamMessage
from faststream.nats import NatsBroker as NativeNats
from faststream.nats import TestNatsBroker
from hook_support import handler
from pydantic import BaseModel

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.base import publish
from papilio_tasks.apps.events.publishers.nats import NatsPublisher
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
from papilio_tasks.apps.events.routers.nats import NatsRouter
from papilio_tasks.apps.events.subscribers.nats import NatsSubscriber
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
from papilio_tasks.tools.hooks.publish import PublishHooks
from papilio_tasks.tools.hooks.subscribe import SubscribeHooks
from papilio_tasks.tools.retry import Retry


class Event(BaseModel):
    value: int


class Service:
    def __init__(self, message: StreamMessage):
        self.message = message
        self.closed = False


class Services(Provider):
    scope = Scope.REQUEST

    def __init__(self):
        super().__init__()
        self.instances = []

    @provide
    async def service(self, message: StreamMessage) -> AsyncIterator[Service]:
        item = Service(message)
        self.instances.append(item)
        try:
            yield item
        finally:
            item.closed = True


def registry():
    return NatsRegistrar(NatsBroker(NativeNats(logger=None)))


async def test_events_prefix_dto_scopes_hooks_and_release():
    router, services, seen, phases = (
        NatsRouter(prefix="module."),
        Services(),
        [],
        [],
    )
    created = router.publisher(
        Event, subject="created", outbox_key="module.created.v1"
    )

    @router.subscriber("created", queue="workers", dependency=Service)
    async def receive(service: Service, event: Event) -> None:
        assert isinstance(event, Event)
        assert not service.closed
        seen.append((event.value, service.message.headers))

    async def before(event):
        phases.append("before")

    async def after(event):
        phases.append("after")

    async def sent(event):
        phases.append("sent")

    class Receiving(SubscribeHooks):
        def __init__(self):
            super().__init__(
                before_run=(handler(before),), after_run=(handler(after),)
            )

    class Sending(PublishHooks):
        def __init__(self):
            super().__init__(after_send=(handler(sent),))

    services.provide(Receiving)
    services.provide(Sending)
    reg = registry()
    app = create_app(
        registrar=reg,
        routers=[router, router],
        providers=[services],
        subscribe_hooks=Receiving,
        publish_hooks=Sending,
    )
    assert not services.instances
    assert len(reg.broker.native.subscribers) == 1
    assert (
        reg.broker.native.subscribers[0].subject.broker_address
        == "module.created"
    )

    @publish(created, select=lambda value: Event(value=value))
    async def operation(value: int) -> int:
        return value

    try:
        async with TestNatsBroker(reg.broker.native):
            assert await operation(3) == 3
            await created.publish(
                Event(value=4), headers={"event_id": "stable"}
            )
        assert [value for value, _ in seen] == [3, 4]
        assert seen[1][1]["event_id"] == "stable"
        assert phases == ["before", "after", "sent"] * 2
        assert len(services.instances) == 2 and all(
            item.closed for item in services.instances
        )
        # The decorator leaves the original subscriber directly callable.
        await receive(Service(services.instances[0].message), Event(value=5))
        assert seen[-1][0] == 5
    finally:
        await app.stop()
    with pytest.raises(RuntimeError, match="not registered"):
        await created.publish(Event(value=6))


async def test_function_retry_fresh_dependencies_and_class_consumer_coexist():
    router, services, calls, legacy = NatsRouter(), Services(), [], []
    created = router.publisher(Event, subject="created")

    @router.subscriber(
        "created",
        dependency=Service,
        retry=Retry(attempts=2, delay=0, errors=(ValueError,)),
    )
    async def retrying(service: Service, event: Event) -> None:
        calls.append(service)
        if len(calls) == 1:
            raise ValueError("transient")

    class Created(NatsPublisher[Event]):
        subject = "created"

    class Legacy(NatsSubscriber[Event]):
        publisher = Created
        queue = "legacy"

        async def run(self, event: Event) -> None:
            legacy.append(event.value)

    services.provide(Legacy)
    reg = registry()
    reg.subscriber(Legacy)
    app = create_app(registrar=reg, routers=[router], providers=[services])
    try:
        async with TestNatsBroker(reg.broker.native):
            await created.publish(Event(value=7))
        assert len(calls) == 2 and calls[0] is not calls[1]
        assert all(item.closed for item in calls)
        assert legacy == [7]
    finally:
        await app.stop()


@pytest.mark.parametrize("ack", [AckPolicy.MANUAL, AckPolicy.ACK_FIRST])
def test_function_retry_rejects_incompatible_ack_including_router_defaults(
    ack,
):
    router = NatsRouter(ack_policy=ack)

    @router.subscriber(
        "created", retry=Retry(attempts=2, delay=0, errors=(ValueError,))
    )
    async def receive(event: Event) -> None:
        pass

    with pytest.raises(ValueError, match="ack after execution"):
        create_app(registrar=registry(), routers=[router])


async def test_discovery_capabilities_and_shared_router(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    folder = tmp_path / "event_functions"
    folder.mkdir()
    (folder / "__init__.py").write_text("")
    (folder / "publishers.py").write_text(
        dedent("""
        from pydantic import BaseModel
        from papilio_tasks.apps.events.routers.nats import NatsRouter
        class Event(BaseModel): value: int
        router = NatsRouter()
        created = router.publisher(
                Event, subject='created', outbox_key='created.v1'
            )
        seen = []
        @router.subscriber('created')
        async def receive(event: Event) -> None: seen.append(event.value)
    """)
    )
    (folder / "subscribers.py").write_text("from .publishers import router")
    module = import_module("event_functions.publishers")
    producer = create_app(registrar=registry(), publishers=["event_functions"])
    assert len(producer.registrar.broker.native.subscribers) == 0
    await producer.stop()
    consumer = create_app(
        registrar=registry(), subscribers=["event_functions"]
    )
    assert len(consumer.registrar.broker.native.subscribers) == 1
    with pytest.raises(RuntimeError, match="not registered"):
        await module.created.publish(module.Event(value=1))
    await consumer.stop()
    app = create_app(
        registrar=registry(),
        publishers=["event_functions"],
        subscribers=["event_functions"],
        routers=[module.router],
    )
    try:
        assert len(app.registrar.broker.native.subscribers) == 1
        async with TestNatsBroker(app.registrar.broker.native):
            await module.created.publish(module.Event(value=8))
        assert module.seen == [8]
    finally:
        await app.stop()


@pytest.mark.parametrize("transport", ["nats", "rabbit", "kafka", "redis"])
@pytest.mark.parametrize("style", ["plain", "markers"])
async def test_function_routes_compile_with_native_transports(
    transport, style
):
    native = import_module("faststream." + transport)
    module = import_module("papilio_tasks.apps.events.routers." + transport)
    title = transport.capitalize()
    router = getattr(module, title + "Router")()
    if transport == "nats":
        options = {"subject": "created"}
        sub = {"subject": "created", "queue": "worker"}
        reg_module = "nats"
    elif transport == "rabbit":
        options = {
            "exchange": native.RabbitExchange("events"),
            "routing_key": "created",
        }
        sub = {
            "exchange": native.RabbitExchange("events"),
            "queue": native.RabbitQueue("worker", routing_key="created"),
        }
        reg_module = "rabbit"
    elif transport == "kafka":
        options = {"topic": "created"}
        sub = {}
        reg_module = "kafka"
    else:
        options = {"stream": "created"}
        sub = {
            "stream": native.StreamSub(
                "created", group="workers", consumer="one"
            )
        }
        reg_module = "streams"
    created = router.publisher(Event, **options)
    seen = []

    async def receive(event: Event) -> None:
        seen.append(event.value)

    services = Services()
    if style == "markers":

        async def receive(
            event: Event, *, service: FromDishka[Service]
        ) -> None:
            assert not service.closed
            seen.append(event.value)

    if transport == "kafka":
        router.subscriber("created", group_id="worker")(receive)
    else:
        router.subscriber(**sub)(receive)
    wrapped_type = getattr(
        import_module(
            "papilio_tasks.infra.faststream.brokers.backends." + transport
        ),
        title + "Broker",
    )
    registrar_type = getattr(
        import_module("papilio_tasks.apps.events.registry." + reg_module),
        ("Stream" if transport == "redis" else title) + "Registrar",
    )
    broker = getattr(native, title + "Broker")(logger=None)
    reg = registrar_type(wrapped_type(broker))
    app = create_app(registrar=reg, routers=[router], providers=[services])
    tester = getattr(native, "Test" + title + "Broker")
    try:
        async with tester(broker):
            await created.publish(Event(value=9))
        assert seen == [9]
        if style == "markers":
            assert (
                len(services.instances) == 1 and services.instances[0].closed
            )
    finally:
        await app.stop()


@pytest.mark.parametrize("route", ["list", "channel", "jetstream"])
@pytest.mark.parametrize("style", ["plain", "markers"])
async def test_native_redis_modes_and_jetstream_function_routes(route, style):
    seen = []
    if route == "jetstream":
        from faststream.nats import JStream, PullSub

        from papilio_tasks.apps.events.registry.jetstream import JetRegistrar

        router = NatsRouter()
        options = {"subject": "created", "stream": JStream("events")}
        subscriber = {**options, "durable": "worker", "pull_sub": PullSub()}
        reg = JetRegistrar(NatsBroker(NativeNats(logger=None)))
        tester = TestNatsBroker
    else:
        from faststream.redis import RedisBroker as NativeRedis
        from faststream.redis import TestRedisBroker

        from papilio_tasks.apps.events.routers.redis import RedisRouter
        from papilio_tasks.infra.faststream.brokers.backends.redis import (
            RedisBroker,
        )

        router = RedisRouter()
        options = {route: "created"}
        subscriber = options
        registrar = import_module(
            "papilio_tasks.apps.events.registry."
            + ("lists" if route == "list" else "channels")
        )
        registrar_type = getattr(
            registrar,
            "ListRegistrar" if route == "list" else "ChannelRegistrar",
        )
        reg = registrar_type(RedisBroker(NativeRedis(logger=None)))
        tester = TestRedisBroker
    created = router.publisher(Event, **options)

    async def receive(event: Event) -> None:
        seen.append(event.value)

    services = Services()
    if style == "markers":

        async def receive(
            event: Event, *, service: FromDishka[Service]
        ) -> None:
            assert not service.closed
            seen.append(event.value)

    router.subscriber(**subscriber)(receive)
    app = create_app(registrar=reg, routers=[router], providers=[services])
    try:
        async with tester(reg.broker.native):
            await created.publish(Event(value=1))
        assert seen == [1]
        if style == "markers":
            assert (
                len(services.instances) == 1 and services.instances[0].closed
            )
    finally:
        await app.stop()


@pytest.mark.parametrize("transport", ["rabbit", "redis"])
async def test_router_schema_prefixes_survive_rebinding(transport):
    native = import_module("faststream." + transport)
    cls = getattr(
        import_module("papilio_tasks.apps.events.routers." + transport),
        transport.capitalize() + "Router",
    )
    router = cls(prefix="module.")
    if transport == "rabbit":
        exchange = native.RabbitExchange("events")
        created = router.publisher(
            int, exchange=exchange, routing_key="created"
        )
        options = {
            "queue": native.RabbitQueue("worker", routing_key="created"),
            "exchange": exchange,
        }
        kind, prefix = "rabbit", "Rabbit"
    else:
        created = router.publisher(int, stream="created")
        options = {
            "stream": native.StreamSub(
                "created", group="workers", consumer="one"
            )
        }
        kind, prefix = "streams", "Stream"
    seen = []

    @router.subscriber(**options)
    async def receive(event: int) -> None:
        seen.append(event)

    async def run():
        broker = getattr(native, transport.capitalize() + "Broker")(
            logger=None
        )
        wrapper = getattr(
            import_module(
                "papilio_tasks.infra.faststream.brokers.backends." + transport
            ),
            transport.capitalize() + "Broker",
        )
        registrar = getattr(
            import_module("papilio_tasks.apps.events.registry." + kind),
            prefix + "Registrar",
        )(wrapper(broker))
        app = create_app(registrar=registrar, routers=[router])
        try:
            async with getattr(
                native, "Test" + transport.capitalize() + "Broker"
            )(broker):
                await created.publish(1)
        finally:
            await app.stop()

    await run()
    await run()
    assert seen == [1, 1]


async def test_module_publishers_and_task_consumers_discover_independently(
    tmp_path, monkeypatch
):
    monkeypatch.syspath_prepend(str(tmp_path))
    folder = tmp_path / "boundary_functions"
    (folder / "tasks").mkdir(parents=True)
    (folder / "__init__.py").write_text("")
    (folder / "tasks" / "__init__.py").write_text("")
    (folder / "domain.py").write_text(
        "from pydantic import BaseModel\nclass Event(BaseModel): value: int\n"
    )
    (folder / "publishers.py").write_text(
        dedent("""
        from papilio_tasks.apps.events.routers.nats import NatsRouter
        from .domain import Event
        SUBJECT = 'created'
        router = NatsRouter()
        created = router.publisher(Event, subject=SUBJECT)
    """)
    )
    (folder / "command.py").write_text("from .publishers import created\n")
    (folder / "tasks" / "subscribers.py").write_text(
        dedent("""
        from papilio_tasks.apps.events.routers.nats import NatsRouter
        from ..domain import Event
        from ..publishers import SUBJECT
        router = NatsRouter()
        seen = []
        @router.subscriber(SUBJECT)
        async def receive(event: Event) -> None: seen.append(event.value)
    """)
    )
    command = import_module("boundary_functions.command")
    producer = create_app(
        registrar=registry(), publishers=["boundary_functions"]
    )
    assert "boundary_functions.tasks.subscribers" not in sys.modules
    assert producer.registrar.broker.native.subscribers == []
    await producer.stop()
    app = create_app(
        registrar=registry(),
        publishers=["boundary_functions"],
        subscribers=["boundary_functions"],
    )
    try:
        async with TestNatsBroker(app.registrar.broker.native):
            await command.created.publish(
                import_module("boundary_functions.domain").Event(value=9)
            )
        assert import_module("boundary_functions.tasks.subscribers").seen == [
            9
        ]
    finally:
        await app.stop()
