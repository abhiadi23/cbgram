"""cbgram's crypto backend: cbcrypto only, fork-safe."""

import os

import pytest

from pyrogram.crypto import _backend


def test_the_backend_is_cbcrypto_and_has_the_transfer_core():
    assert _backend.name == "cbcrypto"
    for fn in ("download", "upload", "_after_fork", "pack_message", "unpack_message"):
        assert hasattr(_backend.impl, fn), fn


@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_a_forked_child_still_packs_messages_and_gets_fresh_padding():
    """cbcrypto registers its own at-fork hook; without it the child would replay the parent's padding."""
    ak, sid = os.urandom(256), os.urandom(8)
    akid = os.urandom(8)
    parent = _backend.impl.pack_message(1, 1, b"x" * 8, 5, sid, ak, akid)  # warms the parent's padding buffer
    r, w = os.pipe()
    pid = os.fork()
    if pid == 0:  # child
        try:
            child = _backend.impl.pack_message(1, 1, b"x" * 8, 5, sid, ak, akid)
            os.write(w, child)
        finally:
            os._exit(0)
    os.close(w)
    data = b""
    while chunk := os.read(r, 4096):
        data += chunk
    os.waitpid(pid, 0)
    again = _backend.impl.pack_message(1, 1, b"x" * 8, 5, sid, ak, akid)
    # same inputs, but random padding: the child and the parent must not produce the same packet
    assert len(data) == len(again) and data != again and data != parent
