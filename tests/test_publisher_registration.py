from unittest.mock import AsyncMock, Mock

import pytest
from faststream.nats import NatsBroker as NativeNats

from papilio_tasks.apps.events.application import create_app
from papilio_tasks.apps.events.publishers.base import PublisherRoute
from papilio_tasks.apps.events.publishers.nats import NatsPublisher
from papilio_tasks.apps.events.registry.nats import NatsRegistrar
from papilio_tasks.infra.faststream.brokers.backends.nats import NatsBroker


@pytest.fixture(params=["class", "route"])
def publication(request):
    class Created(NatsPublisher[int]):
        subject = "created"

    return Created if request.param == "class" else PublisherRoute(int)


def registrar():
    return NatsRegistrar(NatsBroker(NativeNats(logger=None)))


async def test_binding_owner_rejects_duplicate_before_creating_sender(
    publication,
):
    first, second = registrar(), registrar()
    sender = AsyncMock(return_value="native-result")
    create = Mock(return_value=sender)
    try:
        first.bind_publisher(publication, create, {"subject": "created"})
        assert first.has_publisher(publication)
        with pytest.raises(ValueError, match="already registered"):
            second.bind_publisher(publication, create, {"subject": "other"})
        create.assert_called_once_with()
        assert not second.has_publisher(publication)
        assert await publication.publish(7, headers={"origin": "test"}) == (
            "native-result"
        )
        sender.assert_awaited_once_with(7, headers={"origin": "test"})
        first.close()
        second.bind_publisher(publication, create, {"subject": "other"})
        assert create.call_count == 2
        assert await publication.publish(8) == "native-result"
    finally:
        first.close()
        second.close()
    with pytest.raises(RuntimeError, match="not registered"):
        await publication.publish(9)


def test_failed_native_creation_does_not_claim_publication(publication):
    owner = registrar()
    create = Mock(side_effect=ValueError("native route"))
    try:
        with pytest.raises(ValueError, match="native route"):
            owner.bind_publisher(publication, create, {})
        assert not owner.has_publisher(publication)
        create.side_effect = None
        create.return_value = AsyncMock()
        owner.bind_publisher(publication, create, {})
        assert owner.has_publisher(publication)
    finally:
        owner.close()


@pytest.mark.parametrize("state", ["attached", "closed"])
async def test_finished_registration_rejects_native_creation(
    publication, state
):
    owner = registrar()
    app = create_app(registrar=owner)
    create = Mock(return_value=AsyncMock())
    try:
        if state == "closed":
            await app.stop()
        with pytest.raises(RuntimeError, match="before creating"):
            owner.bind_publisher(publication, create, {})
        create.assert_not_called()
    finally:
        await app.stop()
