#  cbgram - Python side of the Rust transfer core
#
#  The heavy lifting (sockets, MTProto framing and crypto, acks, salts, flood waits,
#  reconnects, pacing, file I/O) lives in ``cbcrypto.download`` / ``cbcrypto.upload``.
#  This module only does what needs Python: sessions and auth keys, DC lookup, the TL request
#  templates, progress callbacks, error mapping.
#
#  ``download()`` / ``upload()`` return "not handled" for anything the core does not cover
#  (proxy, in-memory targets, CDN files, resumed uploads, ...) and the caller then simply
#  uses the pure-Python path. Set CBGRAM_RUST_CORE=0 to turn the core off.

import asyncio
import functools
import hashlib
import inspect
import logging
import math
import os
import threading
from typing import Callable, Optional, Union

import cbcrypto

import pyrogram
from pyrogram import raw, utils
from pyrogram.errors import RPCError
from pyrogram.raw.all import layer
from pyrogram.session import Session
from pyrogram.session.internals import MsgId

log = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024
PART_SIZE = 512 * 1024
UPLOAD_POOL_MAX = 20


def enabled(client) -> bool:
    return os.environ.get("CBGRAM_RUST_CORE", "1") != "0" and not client.proxy


class CdnRedirect(Exception):
    """The file lives on a CDN: the Rust core does not follow redirects, Python's path does."""


class _Cancelled(Exception):
    """Raised inside the engine's progress callback to stop a transfer the asyncio side gave up on."""


class _Progress:
    """Bridges the engine thread's 25 Hz tick to the user's (sync or async) progress callback."""

    def __init__(self, client, callback, args, total):
        self.client = client
        self.loop = asyncio.get_running_loop()
        self.callback = callback
        self.args = args
        self.total = total
        self.cancelled = threading.Event()
        self.stop_exc: Optional[BaseException] = None
        self._latest = 0
        self._sent = -1
        self._task: Optional[asyncio.Task] = None

    # runs in the engine thread
    def tick(self, done: int) -> None:
        if self.cancelled.is_set():
            raise _Cancelled()
        if self.stop_exc is not None:
            raise self.stop_exc
        if self.callback and done != self._latest:
            self._latest = done
            self.loop.call_soon_threadsafe(self._kick)

    # runs on the event loop
    def _kick(self) -> None:
        if self._task is None or self._task.done():
            self._task = self.loop.create_task(self._drain())

    async def _drain(self) -> None:
        while self._latest != self._sent and self.stop_exc is None:
            value = self._latest
            self._sent = value
            await self._report(value)

    async def _report(self, sent: int) -> None:
        func = functools.partial(self.callback, min(sent, self.total), self.total, *self.args)
        try:
            if inspect.iscoroutinefunction(self.callback):
                await func()
            else:
                await self.loop.run_in_executor(self.client.executor, func)
        except pyrogram.StopTransmission as e:
            self.stop_exc = e
        except Exception as e:
            log.warning("Progress callback error: %s", e)

    async def finish(self) -> None:
        if self._task is not None:
            await asyncio.gather(self._task, return_exceptions=True)
        if self.stop_exc is not None:
            raise self.stop_exc
        if self.callback:
            self._latest = self.total
            if self._sent != self.total:
                self._sent = self.total
                await self._report(self.total)
            if self.stop_exc is not None:
                raise self.stop_exc


def _raise_core_error(e: "cbcrypto.CoreError", rpc_type) -> None:
    kind, code, message = e.args[:3]
    if kind == "rpc":
        RPCError.raise_it(raw.types.RpcError(error_code=code, error_message=message), rpc_type)
    if kind == "cdn":
        raise CdnRedirect() from None
    if kind == "timeout":
        raise TimeoutError(message) from None
    raise OSError(f"{kind}: {message}") from None


async def _run_engine(progress: _Progress, fn: Callable, rpc_type):
    """Run a blocking engine call in a worker thread; an asyncio cancel stops it cleanly."""
    loop = asyncio.get_running_loop()
    fut = loop.run_in_executor(None, functools.partial(fn, progress=progress.tick))
    try:
        return await asyncio.shield(fut)
    except asyncio.CancelledError:
        progress.cancelled.set()
        try:
            await fut
        except BaseException:
            pass
        raise
    except cbcrypto.CoreError as e:
        _raise_core_error(e, rpc_type)
    except _Cancelled:
        raise asyncio.CancelledError() from None


async def _init_prefix(client) -> bytes:
    """invokeWithLayer(initConnection(...)) up to, but excluding, the wrapped query."""
    wrapped = raw.functions.InvokeWithLayer(
        layer=layer,
        query=raw.functions.InitConnection(
            api_id=await client.storage.api_id(),
            app_version=client.app_version,
            device_model=client.device_model,
            system_version=client.system_version,
            system_lang_code=client.lang_code,
            lang_code=client.lang_code,
            lang_pack="",
            params=(
                utils.obj_to_jsonvalue(client.init_connection_params)
                if client.init_connection_params
                else None
            ),
            query=raw.functions.Ping(ping_id=0),  # placeholder, always the last 12 bytes
        ),
    ).write()
    return wrapped[:-12]


def _socket_buffer() -> int:
    return int(os.environ.get("CBGRAM_SOCKET_BUFFER", 0))


async def _endpoint(client, dc_id: int):
    session = await client.get_session(dc_id, is_media=True)
    option = await client.get_dc_option(dc_id, is_media=True, ipv6=client.ipv6)
    return session, option.ip_address, option.port


def _flags(client):
    me = client.me
    return (
        bool(getattr(me, "is_bot", False)),
        bool(getattr(me, "is_premium", False)),
    )


def _slow_start():
    return os.environ.get("CBGRAM_SLOW_START", "1") != "0"


async def download(client, file_id, file_size: int, file, progress, progress_args) -> bool:
    """Download ``file_id`` into the open (pre-sized) local file. False = not handled, use Python."""
    path = getattr(file, "name", None)

    if not enabled(client) or file_size <= 0 or not isinstance(path, str):
        return False

    async with client.get_file_semaphore:
        location = client._file_location(file_id)
        dc_id = file_id.dc_id
        is_bot, is_premium = _flags(client)
        env = os.environ.get

        if is_bot:
            pool, workers = int(env("CBGRAM_DL_POOL_BOT", 5)), int(env("CBGRAM_DL_WORKERS_BOT", 3))
            rate, burst = int(env("CBGRAM_DL_RATE_BOT", 16)), int(env("CBGRAM_DL_BURST_BOT", 8))
        elif is_premium:
            pool, workers = int(env("CBGRAM_DL_POOL_PREMIUM", 6)), int(env("CBGRAM_DL_WORKERS_PREMIUM", 4))
            rate, burst = int(env("CBGRAM_DL_RATE_PREMIUM", 150)), int(env("CBGRAM_DL_BURST_PREMIUM", 35))
        else:
            pool, workers = int(env("CBGRAM_DL_POOL_USER", 5)), int(env("CBGRAM_DL_WORKERS_USER", 3))
            rate, burst = int(env("CBGRAM_DL_RATE_USER", 8)), int(env("CBGRAM_DL_BURST_USER", 8))

        session, host, port = await _endpoint(client, dc_id)
        tracker = _Progress(client, progress, progress_args, file_size)
        prefix = raw.functions.upload.GetFile(location=location, offset=0, limit=0).write()[:-12]

        call = functools.partial(
            cbcrypto.download,
            host=host,
            port=port,
            auth_key=bytes(session.auth_key),
            auth_key_id=bytes(session.auth_key_id),
            salt=session.salt,
            init_prefix=await _init_prefix(client),
            req_prefix=prefix,
            path=path,
            file_size=file_size,
            chunk_size=CHUNK_SIZE,
            connections=max(1, min(pool, math.ceil(file_size / CHUNK_SIZE))),
            inflight=max(1, workers),
            paced=is_bot or not is_premium,
            rate=float(rate),
            burst=float(burst),
            ceiling=float(max(rate, 150)),
            growth=float(env("CBGRAM_DL_GROWTH", 1.5)) if _slow_start() else 1.0,
            slow_start_cap=float(env("CBGRAM_DL_SLOWSTART_CAP", 40)),
            time_offset=float(MsgId.time_offset),
            recv_buffer=_socket_buffer(),
            read_timeout=float(Session.MEDIA_WAIT_TIMEOUT),
        )

        try:
            await _run_engine(tracker, call, raw.functions.upload.GetFile)
        except CdnRedirect:
            return False
        await tracker.finish()
        return True


async def upload(client, path, progress, progress_args):
    """Upload a local file by path. Returns InputFile/InputFileBig, or NotImplemented = use Python."""
    if not enabled(client) or not isinstance(path, (str, os.PathLike)):
        return NotImplemented

    path = os.fspath(path)

    try:
        file_size = os.path.getsize(path)
    except OSError:
        return NotImplemented  # let the Python path produce its usual error

    if file_size == 0:
        return NotImplemented

    is_bot, is_premium = _flags(client)
    limit_mib = 4000 if is_premium else 2000

    if file_size > limit_mib * 1024 * 1024:
        raise ValueError(f"Can't upload files bigger than {limit_mib} MiB")

    env = os.environ.get
    total_parts = math.ceil(file_size / PART_SIZE)
    is_big = file_size > 10 * 1024 * 1024
    pool_cap = max(1, math.ceil(total_parts / 2))

    if is_bot:
        rate = int(env("CBGRAM_UPLOAD_RATE_BOT", 32))
        pool = min(int(env("CBGRAM_UPLOAD_POOL_BOT", 8)), UPLOAD_POOL_MAX, pool_cap)
    elif is_premium:
        rate = int(env("CBGRAM_UPLOAD_RATE_PREMIUM", 300))
        pool = min(int(env("CBGRAM_UPLOAD_POOL_PREMIUM", 14)), UPLOAD_POOL_MAX, pool_cap)
    else:
        rate = int(env("CBGRAM_UPLOAD_RATE_USER", 32))
        pool = min(int(env("CBGRAM_UPLOAD_POOL_USER", 12)), UPLOAD_POOL_MAX, pool_cap)

    with open(path, "rb") as fp:
        file_name = utils.get_file_name(fp, fallback="file.jpg")

    file_id = client.rnd_id()
    dc_id = await client.storage.dc_id()
    session, host, port = await _endpoint(client, dc_id)
    tracker = _Progress(client, progress, progress_args, file_size)

    md5_task = None
    if not is_big:
        md5_task = asyncio.get_running_loop().run_in_executor(None, _md5_of_file, path)

    call = functools.partial(
        cbcrypto.upload,
        host=host,
        port=port,
        auth_key=bytes(session.auth_key),
        auth_key_id=bytes(session.auth_key_id),
        salt=session.salt,
        init_prefix=await _init_prefix(client),
        path=path,
        file_size=file_size,
        file_id=file_id,
        part_size=PART_SIZE,
        is_big=is_big,
        connections=max(1, pool),
        inflight=2,
        paced=is_bot or not is_premium,
        rate=float(rate),
        burst=16.0,
        ceiling=float(max(rate, 300)),
        step=1.0,
        growth=float(env("CBGRAM_UPLOAD_GROWTH", 1.5)) if _slow_start() else 1.0,
        slow_start_cap=float(env("CBGRAM_UPLOAD_SLOWSTART_CAP", 80)),
        time_offset=float(MsgId.time_offset),
        recv_buffer=_socket_buffer(),
        read_timeout=float(Session.MEDIA_WAIT_TIMEOUT),
    )
    rpc_type = raw.functions.upload.SaveBigFilePart if is_big else raw.functions.upload.SaveFilePart

    try:
        await _run_engine(tracker, call, rpc_type)
    except BaseException:
        if md5_task is not None:
            md5_task.cancel()
        raise

    await tracker.finish()

    if is_big:
        return raw.types.InputFileBig(id=file_id, parts=total_parts, name=file_name)

    return raw.types.InputFile(
        id=file_id, parts=total_parts, name=file_name, md5_checksum=await md5_task
    )


def _md5_of_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(4 * 1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()
