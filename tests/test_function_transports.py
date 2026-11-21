import asyncio
import os
from contextlib import aclosing
from uuid import uuid4

import pytest
from dishka import FromDishka
from taskiq import TaskiqMessage
from taskiq.receiver import Receiver
from taskiq_nats import PullBasedJetStreamBroker
from test_function_tasks import Payload, Service, Services

from papilio_tasks.apps.projections.application import (
    create_broker as projects,
)
from papilio_tasks.apps.projections.registry.nats import (
    NatsRegistrar as Projects,
)
from papilio_tasks.apps.projections.router import ProjectionRouter
from papilio_tasks.apps.schedulers import Task, TaskRouter
from papilio_tasks.apps.schedulers.application import create_broker
from papilio_tasks.apps.schedulers.registry.nats import NatsRegistrar
from papilio_tasks.infra.taskiq.brokers.backends.nats import JetStreamBroker


@pytest.mark.parametrize("family", ["scheduler", "projection"])
@pytest.mark.parametrize(
    "style", ["legacy", "markers", "contract", "contract_empty"]
)
async def test_function_taskiq_nats_delivery_without_redis(family, style):
    url = os.environ.get("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS with JetStream")
    name, seen, services = "functions_" + uuid4().hex, [], Services()
    native = PullBasedJetStreamBroker(
        url,
        subject=name + ".task",
        stream_name=name,
        durable=name,
        pull_consume_timeout=1,
    )
    transport = JetStreamBroker(native)

    async def read(service: Service, payload: Payload) -> int:
        assert isinstance(payload, Payload)
        assert not service.closed
        return payload.value

    async def transform(service: Service, data: int) -> str:
        assert not service.closed
        return str(data)

    async def write(service: Service, data: str) -> int:
        seen.append((data, service))
        return int(data)

    async def native_read(
        payload: Payload,
        *,
        service: FromDishka[Service],
        message: FromDishka[TaskiqMessage],
    ) -> int:
        assert isinstance(message, TaskiqMessage)
        return await read(service, payload)

    async def native_transform(
        data: int, *, service: FromDishka[Service]
    ) -> str:
        return await transform(service, data)

    async def native_write(data: str, *, service: FromDishka[Service]) -> int:
        return await write(service, data)

    async def native_read_all(
        *, service: FromDishka[Service], message: FromDishka[TaskiqMessage]
    ) -> int:
        return await native_read(
            Payload(value=7), service=service, message=message
        )

    if family == "scheduler":
        router = TaskRouter()

        if style == "contract_empty":
            task = Task[[], int](name=name + ".contract_empty")

            @router.task(task)
            async def tick(
                *,
                service: FromDishka[Service],
                message: FromDishka[TaskiqMessage],
            ) -> int:
                value = await native_read_all(service=service, message=message)
                seen.append((str(value), service))
                return value

        elif style in {"markers", "contract"}:
            reference = Task[[Payload], int](name=name + ".contract")
            decorator = (
                router.task(reference)
                if style == "contract"
                else router.task(payload=Payload)
            )

            @decorator
            async def task(
                payload: Payload,
                *,
                service: FromDishka[Service],
                message: FromDishka[TaskiqMessage],
            ) -> int:
                value = await native_read(
                    payload, service=service, message=message
                )
                seen.append((str(value), service))
                return value

            if style == "contract":
                task = reference

        else:

            @router.task(dependency=Service)
            async def task(service: Service, payload: Payload) -> int:
                value = await read(service, payload)
                seen.append((str(value), service))
                return value

        broker = create_broker(
            registrar=NatsRegistrar(transport),
            routers=[router],
            providers=[services],
        )
    else:
        router = ProjectionRouter()
        if style == "contract_empty":
            task = Task[[], int](name=name + ".contract_empty")
            router.projection(
                task,
                read=native_read_all,
                transform=native_transform,
                write=native_write,
            )
        elif style == "contract":
            task = Task[[Payload], int](name=name + ".contract")
            router.projection(
                task,
                read=native_read,
                transform=native_transform,
                write=native_write,
            )
        elif style == "markers":
            task = router.projection(
                payload=Payload,
                read=native_read,
                transform=native_transform,
                write=native_write,
            )
        else:
            task = router.projection(
                read=read, transform=transform, write=write, dependency=Service
            )
        broker = projects(
            registrar=Projects(transport),
            routers=[router],
            providers=[services],
        )
    broker.is_worker_process = True
    try:
        await broker.startup()
        if style == "contract_empty":
            await task.enqueue()
        else:
            await task.enqueue(Payload(value=7))
        assert services.instances == []
        async with aclosing(broker.listen()) as messages:
            async with asyncio.timeout(15):
                delivery = await anext(messages)
                wire = broker.formatter.loads(delivery.data)
                expected = [] if style == "contract_empty" else [{"value": 7}]
                assert wire.args == expected and not wire.kwargs
                await Receiver(broker).callback(delivery)
        assert len(seen) == 1 and seen[0][0] == "7"
        assert seen[0][1].closed
        assert broker.state.dishka_container_registry == {}
        await native.client.flush()
        info = await native.js.consumer_info(name, name)
        assert info.num_ack_pending == 0
    finally:
        try:
            if native.client.is_connected:
                await native.js.delete_stream(name)
        finally:
            await broker.shutdown()


@pytest.mark.parametrize("style", ["legacy", "markers"])
async def test_function_jetstream_native_prefix_local_retry_and_headers(style):
    # Keep Taskiq transport checks independent of the optional Events family.
    from faststream import StreamMessage
    from faststream.nats import JStream, PullSub
    from faststream.nats import NatsBroker as NativeNats
    from test_function_events import Service as EventService
    from test_function_events import Services as EventServices

    from papilio_tasks.apps.events.application import create_app
    from papilio_tasks.apps.events.registry.jetstream import JetRegistrar
    from papilio_tasks.apps.events.routers.nats import NatsRouter
    from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
    from papilio_tasks.tools.retry import Retry

    url = os.environ.get("TEST_NATS_URL")
    if not url:
        pytest.skip("Requires isolated NATS with JetStream")
    name, calls, services, done = (
        "functions_" + uuid4().hex,
        [],
        EventServices(),
        asyncio.Event(),
    )
    native = NativeNats(url, logger=None)
    stream = JStream(name)
    router = NatsRouter(prefix=name + ".")
    created = router.publisher(Payload, subject="created", stream=stream)

    if style == "markers":

        @router.subscriber(
            "created",
            stream=stream,
            durable=name,
            pull_sub=PullSub(),
            retry=Retry(attempts=2, delay=0, errors=(ConnectionError,)),
        )
        async def receive(
            event: Payload,
            *,
            service: FromDishka[EventService],
            message: FromDishka[StreamMessage],
        ) -> None:
            assert message is service.message
            assert message.headers["event_id"] == "stable"
            calls.append(service)
            if len(calls) == 1:
                raise ConnectionError("transient")
            done.set()

    else:

        @router.subscriber(
            "created",
            stream=stream,
            durable=name,
            pull_sub=PullSub(),
            dependency=EventService,
            retry=Retry(attempts=2, delay=0, errors=(ConnectionError,)),
        )
        async def receive(service: EventService, event: Payload) -> None:
            assert isinstance(event, Payload)
            assert service.message.headers["event_id"] == "stable"
            calls.append(service)
            if len(calls) == 1:
                raise ConnectionError("transient")
            done.set()

    app = create_app(
        registrar=JetRegistrar(NatsBroker(native)),
        routers=[router],
        providers=[services],
    )
    connection = None
    try:
        await app.start()
        connection = native.connection
        await created.publish(Payload(value=3), headers={"event_id": "stable"})
        await asyncio.wait_for(done.wait(), 15)
    finally:
        try:
            if connection is not None and connection.is_connected:
                await connection.jetstream().delete_stream(name)
        finally:
            await app.stop()
    assert len(calls) == 2 and calls[0] is not calls[1]
    assert all(service.closed for service in calls)
