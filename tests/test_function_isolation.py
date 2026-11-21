import subprocess
import sys

import pytest


@pytest.mark.parametrize("family", ["scheduler", "projection", "events"])
def test_function_router_optional_dependency_boundaries(family):
    code = """
import importlib.abc
import sys
family = sys.argv[1]
blocked = {'papilio', 'opentelemetry', 'sqlalchemy', 'sqlalchemy_inbox',
           'faststream_outbox', 'redis', 'taskiq_redis', 'aio_pika',
           'aiokafka', 'kafka', 'taskiq_aio_pika', 'taskiq_aio_kafka'}
if family == 'events': blocked.add('taskiq')
else:
    blocked.update({'faststream', 'dishka_faststream', 'nats', 'taskiq_nats'})
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in blocked:
            raise ImportError('Unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Block())
if family == 'scheduler':
    from papilio_tasks.apps.schedulers import Task, TaskRouter
    from papilio_tasks.apps.schedulers.application import create_broker
    router = TaskRouter()
    @router.task()
    async def task(value: int) -> int: return value
    reference = Task[[], int](name='isolated.scheduler')
    @router.task(reference)
    async def tick() -> int: return 1
    broker = create_broker(routers=[router])
elif family == 'projection':
    from papilio_tasks.apps.projections.router import Task, ProjectionRouter
    from papilio_tasks.apps.projections.application import create_broker
    router = ProjectionRouter()
    async def operation(value: int) -> int: return value
    task = router.projection(
        read=operation, transform=operation, write=operation,
    )
    reference = Task[[int], int](name='isolated.projection')
    router.projection(
        reference, read=operation, transform=operation, write=operation,
    )
    broker = create_broker(routers=[router])
else:
    from faststream.nats import NatsBroker as NativeNats
    from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker
    from papilio_tasks.apps.events.routers.nats import NatsRouter
    from papilio_tasks.apps.events.publishers.nats import publisher
    from papilio_tasks.apps.events.registry.nats import NatsRegistrar
    from papilio_tasks.apps.events.application import create_app
    router = NatsRouter()
    route = router.publisher(int, subject='created')
    independent = publisher(int, subject='independent')
    @router.subscriber('created')
    async def handler(event: int) -> None: pass
    reg = NatsRegistrar(NatsBroker(NativeNats(logger=None)))
    app = create_app(registrar=reg, routers=[router], publishers=[independent])
"""
    result = subprocess.run(
        [sys.executable, "-c", code, family],
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 0, result.stderr
