"""Real Papilio CLI processes, with an explicitly configured test Redis."""

import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from uuid import uuid4

import pytest
from redis import Redis as SyncRedis

pytestmark = pytest.mark.skipif(
    not os.getenv("TEST_REDIS_URL"), reason="No test Redis"
)


def test_one_retry_beat_routes_two_native_worker_processes(tmp_path):
    package = tmp_path / "shared_retry"
    package.mkdir()
    (package / "__init__.py").touch()
    (package / "tasks.py").write_text(
        textwrap.dedent(r"""
        import os
        from pathlib import Path
        from dishka import Provider, Scope
        from taskiq import TaskiqEvents
        from papilio_tasks.apps.taskiq import RetryRuntime
        from papilio_tasks.tools.retry import Retry
        from papilio_tasks.infra.taskiq.sources.backends.redis import (
            RedisSource,
        )
        from papilio_tasks.apps.schedulers.backends.redis import (
            RedisScheduler, RedisStreamBroker,
        )
        from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar
        from papilio_tasks.apps.schedulers.application import create_broker
        from papilio_tasks.apps.projections.backends.redis import RedisDirect
        from papilio_tasks.apps.projections.registry.redis import (
            RedisRegistrar as Projects,
        )
        from papilio_tasks.apps.projections.application import (
            create_broker as create_projection,
        )

        def record(value):
            with Path(os.environ['RETRY_EVENTS']).open('a') as output:
                output.write(value + '\n')

        class Job(RedisScheduler):
            retry = Retry(attempts=2, delay=0, errors=(ConnectionError,))
            attempts = 0
            async def run(self, value: int) -> int:
                type(self).attempts += 1
                if self.attempts == 1:
                    raise ConnectionError('temporary')
                record('done:job:' + str(value))
                return value

        class Project(RedisDirect[int, int]):
            retry = Job.retry
            attempts = 0
            def __init__(self):
                super().__init__()
            async def read(self, value: int) -> int:
                return value
            async def write(self, data: int) -> int:
                type(self).attempts += 1
                if self.attempts == 1:
                    raise ConnectionError('temporary')
                record('done:project:' + str(data))
                return data

        prefix = os.environ['RETRY_PREFIX']
        source = RedisSource(
            os.environ['TEST_REDIS_URL'], prefix=prefix+'-plans',
        )
        native_start = source.native.startup
        native_stop = source.native.shutdown
        async def opening():
            await native_start()
            record('source_open:' + os.environ.get('RETRY_ROLE', 'publisher'))
        async def closing():
            await native_stop()
            record('source_close:' + os.environ.get('RETRY_ROLE', 'publisher'))
        source.native.startup, source.native.shutdown = opening, closing
        runtime = RetryRuntime(source=source)
        provider = Provider(scope=Scope.REQUEST)
        provider.provide(Job)
        provider.provide(Project)
        jobs = RedisRegistrar(RedisStreamBroker(
            os.environ['TEST_REDIS_URL'], queue_name=prefix+'-jobs',
            consumer_group_name=prefix+'-jobs', consumer_id='0',
        ))
        projects = Projects(RedisStreamBroker(
            os.environ['TEST_REDIS_URL'], queue_name=prefix+'-projects',
            consumer_group_name=prefix+'-projects', consumer_id='0',
        ))
        jobs.include(Job)
        projects.include(Project)
        first = create_broker(
            registrar=jobs, providers=[provider], retry=runtime,
        )
        second = create_projection(
            registrar=projects, providers=[provider], retry=runtime,
        )
        @first.on_event(TaskiqEvents.WORKER_STARTUP)
        async def job_ready(state): record('ready:job')
        @second.on_event(TaskiqEvents.WORKER_STARTUP)
        async def project_ready(state): record('ready:project')
        beat = runtime.create_beat(brokers=[first, second])
    """)
    )
    prefix = "shared-cli-" + uuid4().hex
    events = tmp_path / "shared-events.txt"
    env = {
        **os.environ,
        "RETRY_PREFIX": prefix,
        "RETRY_EVENTS": str(events),
        "PYTHONPATH": os.pathsep.join(
            [str(tmp_path), os.environ.get("PYTHONPATH", "")]
        ),
    }
    processes, logs = [], []

    def start(command, target, role, *options):
        log = (tmp_path / (role + ".log")).open("w+")
        logs.append(log)
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "papilio_tasks",
                "scheduler",
                command,
                "shared_retry.tasks:" + target,
                "--app-dir",
                str(tmp_path),
                *options,
            ],
            env=env | {"RETRY_ROLE": role},
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        processes.append(process)

    def wait_for(value):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            output = "\n".join(Path(log.name).read_text() for log in logs)
            assert all(p.poll() is None for p in processes), output
            if events.exists() and value in events.read_text().splitlines():
                return
            time.sleep(0.05)
        pytest.fail("Missing " + value + ": " + output)

    try:
        start("worker", "first", "jobs", "--workers", "1")
        start("worker", "second", "projects", "--workers", "1")
        wait_for("ready:job")
        wait_for("ready:project")
        start("beat", "beat", "beat", "--update-interval", "1")
        wait_for("source_open:beat")
        subprocess.run(
            [
                sys.executable,
                "-c",
                textwrap.dedent("""
                import asyncio
                from shared_retry.tasks import Job, Project, first, second
                async def publish():
                    await asyncio.gather(first.startup(), second.startup())
                    try:
                        await asyncio.gather(
                            Job.enqueue(41), Project.enqueue(42),
                        )
                    finally:
                        await asyncio.gather(
                            first.shutdown(), second.shutdown(),
                        )
                asyncio.run(publish())
            """),
            ],
            env=env,
            check=True,
            capture_output=True,
            text=True,
            timeout=15,
        )
        wait_for("done:job:41")
        wait_for("done:project:42")
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.send_signal(signal.SIGINT)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for log in logs:
            log.close()
        with SyncRedis.from_url(env["TEST_REDIS_URL"]) as redis:
            keys = list(redis.scan_iter("*" + prefix + "*"))
            if keys:
                redis.delete(*keys)
    recorded = events.read_text().splitlines()
    for role in ("jobs", "projects", "beat"):
        assert recorded.count("source_open:" + role) == 1
        assert recorded.count("source_close:" + role) == 1
    assert "source_open:publisher" not in recorded


@pytest.mark.parametrize("source_kind", ["memory", "redis", "retry"])
@pytest.mark.parametrize("kind", ["redis", "nats", "kafka"])
def test_worker_and_beat_paths_execute_and_close_scopes(
    tmp_path, source_kind, kind
):
    pytest.importorskip("taskiq_redis")
    if not os.getenv(f"TEST_{kind.upper()}_URL"):
        pytest.skip("Requires isolated " + kind)
    if kind in ("nats", "kafka"):
        pytest.importorskip(
            "taskiq_nats" if kind == "nats" else "taskiq_aio_kafka"
        )
    package = tmp_path / "cli_app"
    feature = package / "modules" / "reports"
    feature.mkdir(parents=True)
    for directory in (package, package / "modules", feature):
        (directory / "__init__.py").touch()
    events = tmp_path / "events.txt"
    (feature / "schedulers.py").write_text(
        textwrap.dedent("""
        import os
        from pathlib import Path
        from papilio_tasks.tools.retry import Retry
        from papilio_tasks.apps.schedulers.backends.redis import RedisScheduler

        if os.environ['CLI_KIND'] == 'nats':
            from papilio_tasks.apps.schedulers.backends.nats import (
                NatsScheduler as RedisScheduler,
            )
        elif os.environ['CLI_KIND'] == 'kafka':
            from papilio_tasks.apps.schedulers.backends.kafka import (
                KafkaScheduler as RedisScheduler,
            )

        def record(value):
            with Path(os.environ['CLI_EVENTS']).open('a') as stream:
                stream.write(value + '\\n')

        class Root: pass
        class Service: pass
        class Report(RedisScheduler):
            retry = (Retry(attempts=2, delay=2, errors=(ConnectionError,))
                     if os.environ['CLI_SOURCE'] == 'retry' else None)
            attempts = 0
            def __init__(self, service: Service, root: Root):
                self.service = service
            async def run(self, value: int):
                if self.retry is not None:
                    type(self).attempts += 1
                    if self.attempts == 1:
                        record('failed:' + str(value))
                        raise ConnectionError('temporary')
                record('executed:' + str(value))
    """)
    )
    (feature / "providers.py").write_text(
        textwrap.dedent("""
        from collections.abc import AsyncIterator
        from dishka import Provider, Scope, provide
        from .schedulers import Root, Service, Report, record
        class Jobs(Provider):
            scope = Scope.REQUEST
            report = provide(Report)
            @provide
            async def service(self) -> AsyncIterator[Service]:
                try:
                    yield Service()
                finally:
                    record('request_closed')
            @provide(scope=Scope.APP)
            async def root(self) -> AsyncIterator[Root]:
                try:
                    yield Root()
                finally:
                    record('root_closed')
    """)
    )
    (package / "tasks.py").write_text(
        textwrap.dedent("""
        import os
        from datetime import UTC, datetime, timedelta
        from taskiq import ScheduledTask, ScheduleSource, TaskiqEvents
        from papilio_tasks.apps.schedulers.application import (
            create_broker, create_beat,
        )
        from papilio_tasks.apps.schedulers.registry.redis import RedisRegistrar
        from papilio_tasks.apps.schedulers.backends.redis import (
            RedisStreamBroker,
        )
        from papilio_tasks.infra.taskiq.sources.base import Source
        from .modules.reports.providers import Jobs
        from .modules.reports.schedulers import Report, record

        name = os.environ['CLI_QUEUE']
        if os.environ['CLI_KIND'] == 'nats':
            from taskiq_nats import PullBasedJetStreamBroker
            from papilio_tasks.apps.schedulers.backends.nats import (
                JetStreamBroker,
            )
            from papilio_tasks.apps.schedulers.registry.nats import (
                NatsRegistrar,
            )
            registrar = NatsRegistrar(JetStreamBroker(
                PullBasedJetStreamBroker(
                    os.environ['TEST_NATS_URL'], subject=name,
                    stream_name=name, durable=name, pull_consume_timeout=1,
                ),
            ))
        elif os.environ['CLI_KIND'] == 'kafka':
            from taskiq_aio_kafka import AioKafkaBroker
            from papilio_tasks.apps.schedulers.backends.kafka import (
                KafkaBroker,
            )
            from papilio_tasks.apps.schedulers.registry.kafka import (
                KafkaRegistrar,
            )
            native = AioKafkaBroker(
                os.environ['TEST_KAFKA_URL'], kafka_topic=name,
            )
            native.configure_consumer(
                group_id=name, auto_offset_reset='earliest',
            )
            registrar = KafkaRegistrar(KafkaBroker(native))
        else:
            registrar = RedisRegistrar(RedisStreamBroker(
                os.environ['TEST_REDIS_URL'], queue_name=name,
                consumer_group_name=name, consumer_id='0',
            ))
        retry_source = None
        if os.environ['CLI_SOURCE'] == 'retry':
            from papilio_tasks.infra.taskiq.sources.backends.redis import (
                RedisSource,
            )
            retry_source = RedisSource(
                os.environ['TEST_REDIS_URL'], prefix=os.environ['CLI_PREFIX'],
            )
        broker = create_broker(
            registrar=registrar, providers=[Jobs()],
            modules=['cli_app.modules'], retry_source=retry_source,
        )
        @broker.on_event(TaskiqEvents.WORKER_STARTUP)
        async def ready(state): record('worker_ready')

        class Plans(ScheduleSource):
            def __init__(self):
                self.items = [ScheduledTask(
                    task_name=Report.task().task_name,
                    labels=dict(Report.task().labels), args=[17], kwargs={},
                    time=datetime.now(UTC) + timedelta(seconds=1),
                )]
            async def startup(self): record('source_started')
            async def shutdown(self): record('source_closed')
            async def get_schedules(self): return self.items
            async def post_send(self, task): self.items = []

        if os.environ['CLI_SOURCE'] in ('redis', 'retry'):
            from papilio_tasks.infra.taskiq.sources.backends.redis import (
                RedisSource,
            )
            source = retry_source if retry_source is not None else RedisSource(
                os.environ['TEST_REDIS_URL'], prefix=os.environ['CLI_PREFIX'],
            )
            # Add work after observing beat's first empty native read.
            read_schedules = source.native.get_schedules
            async def observed_read():
                items = await read_schedules()
                record('source_read:' + str(len(items)))
                return items
            source.native.get_schedules = observed_read
        else:
            source = Source(Plans())
        beat = create_beat(broker, sources=[source])
    """)
    )
    env = {
        **os.environ,
        "CLI_EVENTS": str(events),
        "CLI_QUEUE": "papilio-cli-" + uuid4().hex,
        "CLI_PREFIX": "papilio-source-" + uuid4().hex,
        "CLI_SOURCE": source_kind,
        "CLI_KIND": kind,
    }
    # Also used to run the same test against the isolated installed wheel.
    executable = os.getenv("TEST_PAPILIO_CLI")
    command = (
        [executable] if executable else [sys.executable, "-m", "papilio_tasks"]
    )
    processes = []
    logs = []

    def start(kind, target, *options):
        log = (tmp_path / f"{kind}.log").open("w+")
        logs.append(log)
        proc = subprocess.Popen(
            [
                *command,
                "scheduler",
                kind,
                target,
                "--app-dir",
                str(tmp_path),
                *options,
            ],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        processes.append(proc)
        return proc

    def output():
        for log in logs:
            if not log.closed:
                log.flush()
        return "\n".join(Path(log.name).read_text() for log in logs)

    def wait_for(value):
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert all(p.poll() is None for p in processes), output()
            if events.exists() and value in events.read_text().splitlines():
                return
            time.sleep(0.05)
        pytest.fail(f"Missing {value}: {output()}")

    try:
        start(
            "worker",
            "cli_app.tasks:broker",
            "--workers",
            "1",
            "--max-fails",
            "0",
        )
        wait_for("worker_ready")
        start("beat", "cli_app.tasks:beat", "--update-interval", "1")
        if source_kind in ("redis", "retry"):
            wait_for("source_read:0")
            subprocess.run(
                [
                    sys.executable,
                    "-c",
                    """
import asyncio
from datetime import UTC, datetime, timedelta
from cli_app.tasks import Report, source

async def publish():
    await source.native.startup()
    try:
        await Report.at(
            source, datetime.now(UTC) + timedelta(seconds=3), 17,
        )
    finally:
        await source.native.shutdown()

asyncio.run(publish())
""",
                ],
                env={
                    **env,
                    "PYTHONPATH": os.pathsep.join(
                        [str(tmp_path), env.get("PYTHONPATH", "")]
                    ),
                },
                check=True,
                capture_output=True,
                text=True,
                timeout=15,
            )
        wait_for("executed:17")
        wait_for("request_closed")
    finally:
        for proc in reversed(processes):
            if proc.poll() is None:
                proc.send_signal(signal.SIGINT)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait(timeout=5)
        for log in logs:
            log.close()
        # This test owns the unique stream and group, never existing app keys.
        from redis import Redis

        with Redis.from_url(env["TEST_REDIS_URL"]) as redis:
            redis.delete(env["CLI_QUEUE"])
            keys = list(redis.scan_iter(match=env["CLI_PREFIX"] + "*"))
            if keys:
                redis.delete(*keys)
        if kind in ("nats", "kafka"):
            cleanup = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    """
import asyncio, os
name = os.environ['CLI_QUEUE']
if os.environ['CLI_KIND'] == 'nats':
    import nats
    async def cleanup():
        client = await nats.connect(os.environ['TEST_NATS_URL'])
        try:
            await client.jetstream().delete_stream(name)
        finally:
            await client.close()
    asyncio.run(cleanup())
else:
    from kafka.admin import KafkaAdminClient
    admin = KafkaAdminClient(bootstrap_servers=os.environ['TEST_KAFKA_URL'])
    try:
        admin.delete_topics([name])
    finally:
        admin.close()
""",
                ],
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
            assert cleanup.returncode == 0, cleanup.stderr
    entries = events.read_text().splitlines()
    assert entries.count("executed:17") == 1, output()
    assert entries.count("request_closed") == (
        2 if source_kind == "retry" else 1
    )
    if source_kind == "retry":
        assert entries.count("failed:17") == 1
        assert entries.index("failed:17") < entries.index("executed:17")
    assert entries.count("root_closed") == 1
    if source_kind == "memory":
        assert (
            entries.count("source_started")
            == entries.count("source_closed")
            == 1
        )
    else:
        assert entries.index("source_read:0") < entries.index("source_read:1")
    assert all(proc.returncode == 0 for proc in processes), output()
