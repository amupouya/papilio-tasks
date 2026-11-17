import asyncio

import pytest

pytest.importorskip("opentelemetry.sdk")
pytest.importorskip("faststream_outbox")

from faststream_outbox import OutboxBroker
from monitoring_support import observations as observations
from opentelemetry.trace import StatusCode
from sqlalchemy import event
from sqlalchemy.exc import IntegrityError
from test_dead_letters import archive, manager
from test_outbox import count
from test_outbox import database as database


async def test_native_observations_correlate_without_identity_metric_labels(
    database, observations
):
    engine, table, dlq, _ = database
    identifier = await archive(engine, dlq)
    monitoring = observations.monitoring
    tracer = monitoring.tracer_provider.get_tracer("admin-test")
    with tracer.start_as_current_span("admin") as parent:
        with pytest.raises(ValueError, match="rollback"):
            async with engine.begin() as connection:
                letters = manager(
                    connection, table, dlq, monitoring=monitoring
                )
                await letters.list(queue="orders.created.v1")
                await letters.get(identifier)
                assert await letters.replay(
                    identifier, queue="orders.created.v1"
                )
                assert await letters.get(identifier) is None
                raise ValueError("rollback")
    assert await count(engine, dlq) == 1
    spans = observations.spans.get_finished_spans()
    operations = [s for s in spans if s.name.startswith("papilio_tasks.dlq.")]
    assert len(operations) == 4
    assert all(
        s.context.trace_id == parent.get_span_context().trace_id
        for s in operations
    )
    assert all(
        s.parent.span_id == parent.get_span_context().span_id
        for s in operations
    )
    replay = next(s for s in operations if s.name.endswith(".replay"))
    assert replay.attributes["papilio_tasks.dlq.id"] == identifier
    assert replay.attributes["papilio_tasks.dlq.rows"] == 1
    points = observations.points("papilio_tasks.dlq.operations")
    assert sum(p.value for p in points) == 4
    assert all(p.attributes["status"] == "success" for p in points)
    assert all(
        set(p.attributes) <= {"operation", "queue", "status"} for p in points
    )
    assert (
        sum(
            p.count
            for p in observations.points(
                "papilio_tasks.dlq.operation.duration"
            )
        )
        == 4
    )
    assert all(
        "payload" not in s.attributes and "headers" not in s.attributes
        for s in operations
    )


async def test_failed_replay_is_observed_and_archive_survives(
    database, observations
):
    engine, table, dlq, _ = database
    identifier = await archive(engine, dlq, timer_id="same")
    async with engine.begin() as connection:
        await OutboxBroker(outbox_table=table).publish(
            "existing",
            queue="orders.created.v1",
            session=connection,
            timer_id="same",
        )
    with pytest.raises(IntegrityError):
        async with engine.begin() as connection:
            await manager(
                connection, table, dlq, monitoring=observations.monitoring
            ).replay(identifier, queue="orders.created.v1")
    assert await count(engine, dlq) == 1
    point = observations.points("papilio_tasks.dlq.operations")[0]
    assert point.attributes == {
        "operation": "replay",
        "queue": "orders.created.v1",
        "status": "error",
    }
    span = observations.spans.get_finished_spans()[0]
    assert span.status.status_code is StatusCode.ERROR


async def test_cancellation_closes_operation_and_keeps_archive(
    database, observations
):
    engine, table, dlq, _ = database
    identifier = await archive(engine, dlq)
    entered = asyncio.Event()

    async def competing():
        async with engine.begin() as connection:
            await manager(
                connection, table, dlq, monitoring=observations.monitoring
            ).replay(identifier, queue="orders.created.v1")

    def executing(conn, cursor, statement, parameters, context, many):
        if "WITH replayed_dead_letter" in statement:
            entered.set()

    task = None
    async with engine.connect() as connection:
        transaction = await connection.begin()
        await manager(connection, table, dlq).replay(
            identifier, queue="orders.created.v1"
        )
        event.listen(engine.sync_engine, "before_cursor_execute", executing)
        try:
            task = asyncio.create_task(competing())
            await asyncio.wait_for(entered.wait(), 5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            event.remove(
                engine.sync_engine, "before_cursor_execute", executing
            )
            if task is not None and not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await transaction.rollback()
    assert await count(engine, dlq) == 1
    assert await count(engine, table) == 0
    assert len(observations.spans.get_finished_spans()) == 1
    point = observations.points("papilio_tasks.dlq.operations")[0]
    assert point.attributes["status"] == "cancelled"
