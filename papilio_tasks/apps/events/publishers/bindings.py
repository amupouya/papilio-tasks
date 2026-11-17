from .contracts import Sender

_senders: dict[object, tuple[object, Sender]] = {}


def check(cls: object) -> None:
    if cls in _senders:
        name = getattr(cls, "__qualname__", cls)
        raise ValueError(f"Publisher already registered: {name}")


def add(cls: object, owner: object, sender: Sender) -> None:
    check(cls)
    _senders[cls] = (owner, sender)


def get(cls: object) -> Sender:
    try:
        return _senders[cls][1]
    except KeyError:
        raise RuntimeError(
            f"Publisher is not registered: {getattr(cls, '__qualname__', cls)}"
        ) from None


def release(owner: object) -> None:
    for cls, (registered, _) in tuple(_senders.items()):
        if registered is owner:
            del _senders[cls]
