import subprocess
import sys

import pytest


def test_apps_initialization_and_tools_have_no_runtime_dependencies():
    code = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        blocked = ('taskiq', 'dishka', 'faststream', 'redis', 'aio_pika',
                   'taskiq_nats', 'taskiq_aio_kafka', 'kafka', 'nats',
                   'papilio_tasks.infra', 'papilio_tasks.apps.schedulers',
                   'papilio_tasks.apps.projections',
                   'papilio_tasks.apps.events')
        if any(fullname == p or fullname.startswith(p + '.') for p in blocked):
            raise AssertionError('Unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Block())
import papilio_tasks.apps
import papilio_tasks.tools
from papilio_tasks.tools.bootstrap import Bootstrapper
from papilio_tasks.tools.retry import Retry
from papilio_tasks.tools.hooks import Hook, Handler, emit
from papilio_tasks.tools.hooks.projection import Hooks, Failure
assert Bootstrapper().classes('schedulers', object) == []
assert Retry(attempts=1, delay=0, errors=(ValueError,)).attempts == 1
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)


def test_native_adapters_do_not_import_application_or_transport_families():
    for module in ("dishka", "sqlalchemy", "opentelemetry.trace"):
        pytest.importorskip(module)
    code = """
import asyncio
import importlib.abc
import sys
blocked = ('papilio_tasks.apps', 'taskiq', 'faststream', 'faststream_outbox',
           'dishka_faststream', 'redis', 'aio_pika', 'aiokafka', 'nats')
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if any(fullname == p or fullname.startswith(p + '.') for p in blocked):
            raise AssertionError('Unexpected dependency: ' + fullname)
sys.meta_path.insert(0, Block())
from dishka import FromDishka, Provider, Scope, make_async_container
from papilio_tasks.infra.dishka import Function
from papilio_tasks.infra.sqlalchemy.dead_letters import DeadLetters
from papilio_tasks.infra.sqlalchemy.dlq_monitoring import Observer
class Service: pass
async def handler(data: int, *, service: FromDishka[Service]) -> int:
    return data * 2
async def main():
    provider = Provider(scope=Scope.REQUEST)
    provider.provide(Service)
    container = make_async_container(provider)
    try:
        async with container() as scope:
            bound = await Function(handler).bind(scope)
            assert await bound(21) == 42
    finally:
        await container.close()
asyncio.run(main())
"""
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)
