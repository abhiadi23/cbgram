#  cbgram - native TL codec glue
#
#  Binds cbcrypto's Rust TL codec (tl_parse / tl_write) to the generated classes.
#  The layout table is emitted by compiler/api/compiler.py (raw/tl_spec.py) from
#  the same schema the Python classes are generated from.

import logging
import os
from io import BytesIO
from typing import Any, Optional, Tuple

from .future_salt import FutureSalt
from .future_salts import FutureSalts
from .list import List
from .message import Message
from .msg_container import MsgContainer
from .tl_object import TLObject, objects

log = logging.getLogger(__name__)

_impl = None
_enabled = False


def _fallback(start: int, buf: bytes) -> Tuple[Any, int]:
    """Read one object the native table does not cover with the pure-Python reader."""
    bio = BytesIO(buf)
    bio.seek(start)
    obj = TLObject.read(bio)
    return obj, bio.tell()


def _on_skip(cid: int) -> None:
    log.warning("Skipping message with unknown constructor %s", cid)


def init() -> bool:
    """Enable the native codec. Returns False (and leaves Python in charge) if
    it is unavailable or disabled with CBGRAM_NATIVE_TL=0."""
    global _impl, _enabled

    if _enabled:
        return True

    if os.environ.get("CBGRAM_NATIVE_TL", "1") == "0":
        return False

    try:
        from pyrogram.crypto._backend import impl
        from ..tl_spec import SPEC

        if not hasattr(impl, "tl_init"):
            return False

        impl.tl_init(SPEC, {
            "resolver": objects.__getitem__,
            "fallback": _fallback,
            "list": List,
            "message": Message,
            "container": MsgContainer,
            "fsalts": FutureSalts,
            "fsalt": FutureSalt,
            "on_skip": _on_skip,
        })
    except Exception as e:  # pragma: no cover
        log.warning("Native TL codec unavailable, using the Python one: %s", e)
        return False

    _impl = impl
    _enabled = True
    return True


def parse(data: bytes) -> Any:
    """TLObject.read(BytesIO(data))"""
    if _enabled:
        return _impl.tl_parse(data)

    return TLObject.read(BytesIO(data))


def write(obj: TLObject) -> bytes:
    """obj.write()"""
    if _enabled:
        return _impl.tl_write(obj)

    return obj.write()


def enabled() -> bool:
    return _enabled
