"""Tests for pyrogram.core_transfer (the Python half of the Rust transfer core)."""

import asyncio
import hashlib
import os
import time
from types import SimpleNamespace

import pytest

import pyrogram
from pyrogram import core_transfer, raw
from pyrogram.errors import FileReferenceExpired, FloodWait
from pyrogram.file_id import FileType

from tests.mock_dc import MockDC

AK = os.urandom(256)
MB = 1 << 20


class FakeClient:
    """Just the attributes core_transfer reads from a real Client."""

    def __init__(self, dc, *, premium=False, bot=False, proxy=None):
        self.proxy = proxy
        self.get_file_semaphore = asyncio.Semaphore(2)
        self.app_version, self.device_model, self.system_version, self.lang_code = "1.0", "test", "x", "en"
        self.init_connection_params = None
        self.ipv6 = False
        self.executor = None
        self.me = SimpleNamespace(is_bot=bot, is_premium=premium)
        self.storage = SimpleNamespace(api_id=self._api_id, dc_id=self._dc_id)
        self._dc = dc
        self._session = SimpleNamespace(
            auth_key=AK, auth_key_id=hashlib.sha1(AK).digest()[-8:], salt=dc.salt
        )
        self._n = 0

    async def _api_id(self):
        return 1

    async def _dc_id(self):
        return 2

    async def get_session(self, dc_id, is_media=False):
        return self._session

    async def get_dc_option(self, dc_id, is_media=False, ipv6=False, is_cdn=False):
        return SimpleNamespace(ip_address="127.0.0.1", port=self._dc.port)

    def rnd_id(self):
        self._n += 1
        return 1000 + self._n

    _file_location = staticmethod(pyrogram.Client._file_location)


def file_id(dc_id=2):
    return SimpleNamespace(
        file_type=FileType.DOCUMENT, dc_id=dc_id, media_id=42, access_hash=7,
        file_reference=b"ref", thumbnail_size="",
    )


@pytest.fixture
def make_dc():
    made = []

    def make(**kw):
        files = kw.pop("files", None)
        dc = MockDC(AK, files=files, **kw)
        dc.start()
        made.append(dc)
        return dc

    yield make
    for d in made:
        d.stop()


def dl(client, data, tmp_path, progress=None, args=()):
    async def go():
        path = str(tmp_path / "f.bin")
        with open(path, "wb") as f:
            f.truncate(len(data))
            f.flush()
            ok = await core_transfer.download(client, file_id(), len(data), f, progress, args)
        return ok, open(path, "rb").read()

    return asyncio.run(go())


# ---------------------------------------------------------------- download

def test_download_writes_the_file_and_reports_progress(make_dc, tmp_path):
    data = os.urandom(7 * MB + 5)
    client = FakeClient(make_dc(files={42: data}))
    seen = []
    ok, got = dl(client, data, tmp_path, lambda cur, tot, *a: seen.append((cur, tot, a)), ("x",))
    assert ok and got == data
    cur = [c for c, _, _ in seen]
    assert cur == sorted(cur) and cur[-1] == len(data) and all(t == len(data) and a == ("x",) for _, t, a in seen)


def test_async_progress_callbacks_work_too(make_dc, tmp_path):
    data = os.urandom(3 * MB)
    client = FakeClient(make_dc(files={42: data}))
    seen = []

    async def prog(cur, tot):
        await asyncio.sleep(0)
        seen.append(cur)

    ok, got = dl(client, data, tmp_path, prog)
    assert ok and got == data and seen[-1] == len(data)


def test_stop_transmission_from_the_callback_stops_the_download(make_dc, tmp_path):
    data = os.urandom(40 * MB)
    client = FakeClient(make_dc(files={42: data}))

    def prog(cur, tot):
        if cur > 3 * MB:
            raise pyrogram.StopTransmission()

    t = time.time()
    with pytest.raises(pyrogram.StopTransmission):
        dl(client, data, tmp_path, prog)
    assert time.time() - t < 10


def test_a_failing_progress_callback_does_not_break_the_download(make_dc, tmp_path):
    data = os.urandom(3 * MB)
    client = FakeClient(make_dc(files={42: data}))

    def prog(cur, tot):
        raise RuntimeError("user bug")

    ok, got = dl(client, data, tmp_path, prog)
    assert ok and got == data


def test_rpc_errors_become_the_real_cbgram_exceptions(make_dc, tmp_path):
    data = os.urandom(2 * MB)
    with pytest.raises(FloodWait) as e:
        dl(FakeClient(make_dc(files={42: data}, flood_offsets={0}, flood_secs=500)), data, tmp_path)
    assert e.value.value == 500
    with pytest.raises(FileReferenceExpired):
        dl(FakeClient(make_dc(files={42: data}, error=(400, "FILE_REFERENCE_EXPIRED"))), data, tmp_path)


def test_network_failure_is_an_oserror_like_the_python_path(tmp_path):
    dead = SimpleNamespace(port=1, salt=0)
    client = FakeClient(dead)
    with pytest.raises((OSError, TimeoutError)):
        dl(client, os.urandom(MB), tmp_path)


def test_cdn_files_and_unsupported_setups_fall_back_to_python(make_dc, tmp_path, monkeypatch):
    data = os.urandom(2 * MB)
    ok, _ = dl(FakeClient(make_dc(files={42: data}, cdn=True)), data, tmp_path)
    assert ok is False                                      # CDN redirect -> Python follows it
    ok, _ = dl(FakeClient(make_dc(files={42: data}), proxy={"scheme": "socks5"}), data, tmp_path)
    assert ok is False                                      # proxies stay on the Python path
    monkeypatch.setenv("CBGRAM_RUST_CORE", "0")
    ok, _ = dl(FakeClient(make_dc(files={42: data})), data, tmp_path)
    assert ok is False                                      # kill switch


def test_cancelling_the_asyncio_task_stops_the_engine_thread(make_dc, tmp_path):
    data = os.urandom(3 * MB)
    client = FakeClient(make_dc(files={42: data}, flood_offsets={MB}, flood_secs=25))

    async def go():
        path = str(tmp_path / "f.bin")
        with open(path, "wb") as f:
            f.truncate(len(data))
            task = asyncio.create_task(core_transfer.download(client, file_id(), len(data), f, None, ()))
            await asyncio.sleep(0.6)  # engine is now sleeping out a 25 s flood wait
            t = time.time()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            return time.time() - t

    assert asyncio.run(go()) < 3, "engine thread must notice the cancel, not finish the 25 s sleep"


def test_downloading_two_files_at_once(make_dc, tmp_path):
    a, b = os.urandom(5 * MB), os.urandom(6 * MB)
    dc = make_dc(files={42: a})
    dc2 = make_dc(files={42: b})
    ca, cb_ = FakeClient(dc), FakeClient(dc2)

    async def go():
        pa, pb = str(tmp_path / "a"), str(tmp_path / "b")
        fa, fb = open(pa, "wb"), open(pb, "wb")
        fa.truncate(len(a)), fb.truncate(len(b))
        r = await asyncio.gather(
            core_transfer.download(ca, file_id(), len(a), fa, None, ()),
            core_transfer.download(cb_, file_id(), len(b), fb, None, ()),
        )
        fa.close(), fb.close()
        return r, open(pa, "rb").read(), open(pb, "rb").read()

    r, ga, gb = asyncio.run(go())
    assert r == [True, True] and ga == a and gb == b


# ---------------------------------------------------------------- handle_download hook

def test_handle_download_uses_the_core_and_falls_back_on_cdn(make_dc, tmp_path):
    data = os.urandom(4 * MB + 1)

    class Host(FakeClient):
        called_python_path = False

        async def get_file(self, file_id, file_size, offset, limit, progress, progress_args, _write_file=None):
            Host.called_python_path = True
            for i in range(0, len(data), MB):
                chunk = data[i:i + MB]
                _write_file.seek(i)
                _write_file.write(chunk)
            if False:
                yield b""

    async def run(host):
        packet = (file_id(), str(tmp_path), "out.bin", False, len(data), None, ())
        return await pyrogram.Client.handle_download(host, packet)

    host = Host(make_dc(files={42: data}))
    path = asyncio.run(run(host))
    assert open(path, "rb").read() == data and Host.called_python_path is False

    os.remove(path)
    host = Host(make_dc(files={42: data}, cdn=True))
    path = asyncio.run(run(host))
    assert open(path, "rb").read() == data and Host.called_python_path is True


def test_handle_download_stop_transmission_returns_none_and_cleans_up(make_dc, tmp_path):
    data = os.urandom(30 * MB)
    host = FakeClient(make_dc(files={42: data}))

    def prog(cur, tot):
        if cur > 2 * MB:
            raise pyrogram.StopTransmission()

    packet = (file_id(), str(tmp_path), "out.bin", False, len(data), prog, ())
    assert asyncio.run(pyrogram.Client.handle_download(host, packet)) is None
    assert os.listdir(tmp_path) == []


# ---------------------------------------------------------------- upload

def up(client, data, tmp_path, progress=None, name="movie.mkv"):
    path = tmp_path / name
    path.write_bytes(data)
    return asyncio.run(core_transfer.upload(client, str(path), progress, ()))


def test_big_upload_returns_inputfilebig(make_dc, tmp_path):
    data = os.urandom(11 * MB + 9)
    dc = make_dc()
    r = up(FakeClient(dc), data, tmp_path)
    parts = -(-len(data) // (512 * 1024))
    assert isinstance(r, raw.types.InputFileBig) and r.parts == parts and r.name == "movie.mkv"
    assert b"".join(dc.parts[r.id][i] for i in range(parts)) == data


def test_small_upload_returns_inputfile_with_md5(make_dc, tmp_path):
    data = os.urandom(3 * MB + 1)
    dc = make_dc()
    r = up(FakeClient(dc), data, tmp_path, name="a.bin")
    assert isinstance(r, raw.types.InputFile) and r.md5_checksum == hashlib.md5(data).hexdigest()
    assert b"".join(dc.parts[r.id][i] for i in range(r.parts)) == data


def test_upload_progress_and_stop(make_dc, tmp_path):
    data = os.urandom(8 * MB)
    seen = []
    up(FakeClient(make_dc()), data, tmp_path, progress=lambda c, t: seen.append(c))
    assert seen == sorted(seen) and seen[-1] == len(data)

    def stop(c, t):
        if c > MB:
            raise pyrogram.StopTransmission()

    with pytest.raises(pyrogram.StopTransmission):
        up(FakeClient(make_dc()), os.urandom(40 * MB), tmp_path, progress=stop)


def test_upload_errors_use_cbgram_exceptions(make_dc, tmp_path):
    with pytest.raises(FloodWait):
        up(FakeClient(make_dc(flood_offsets={0}, flood_secs=500)), os.urandom(2 * MB), tmp_path)


def test_upload_falls_back_for_what_the_core_does_not_cover(make_dc, tmp_path):
    import io

    client = FakeClient(make_dc())
    assert asyncio.run(core_transfer.upload(client, io.BytesIO(b"x" * 10), None, ())) is NotImplemented
    (tmp_path / "empty").write_bytes(b"")
    assert asyncio.run(core_transfer.upload(client, str(tmp_path / "empty"), None, ())) is NotImplemented
    assert asyncio.run(core_transfer.upload(client, str(tmp_path / "missing"), None, ())) is NotImplemented
    (tmp_path / "x").write_bytes(b"x" * 100)
    assert asyncio.run(core_transfer.upload(FakeClient(make_dc(), proxy=object()), str(tmp_path / "x"), None, ())) is NotImplemented


def test_init_prefix_is_the_session_init_minus_the_query(make_dc):
    client = FakeClient(make_dc())
    prefix = asyncio.run(core_transfer._init_prefix(client))
    full = prefix + raw.functions.Ping(ping_id=0).write()
    obj = raw.core.TLObject.read(__import__("io").BytesIO(full))
    assert isinstance(obj, raw.functions.InvokeWithLayer) and isinstance(obj.query, raw.functions.InitConnection)
    assert obj.query.api_id == 1 and obj.query.query.ping_id == 0
