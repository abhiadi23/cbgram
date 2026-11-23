import os as _os

_os.environ.setdefault("CBGRAM_LOGS", "all")  # the library is silent by default; tests read its logs

import os

import pytest

from pyrogram.session.internals import MsgId


@pytest.fixture(scope="session")
def auth_key():
    return os.urandom(256)


@pytest.fixture(scope="session")
def session_id():
    return os.urandom(8)


@pytest.fixture(scope="session")
def auth_key_id(auth_key):
    from hashlib import sha1
    return sha1(auth_key).digest()[-8:]


@pytest.fixture
def msg_id():
    return MsgId()


@pytest.fixture(autouse=True)
def _python_transfer_path_unless_testing_the_core(request, monkeypatch):
    """The older transfer tests exercise the pure-Python download/upload path with stripped-down
    fake clients. Keep them on that path; the test_core_* modules turn the Rust core back on."""
    if request.module.__name__.rsplit(".", 1)[-1].startswith("test_core_"):
        return
    monkeypatch.setenv("CBGRAM_RUST_CORE", "0")
