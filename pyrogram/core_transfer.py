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
UPLOAD_POOL_MAX = 32
UPLOAD_WORKERS_DEFAULT = 4
UPLOAD_POOL_DEFAULT = 16
DOWNLOAD_WORKERS_DEFAULT = 4

def _flood_log(rpc_name: str):
    """The only log line of a transfer: Telegram asked us to wait, we retry after that many seconds."""
    def on_flood(secs: float) -> None:
        log.warning("FloodWait on %s: retrying after %d seconds", rpc_name, max(1, round(secs)))
    return on_flood


def _accepts(fn, name: str) -> bool:
    """True if the compiled engine function takes keyword ``name`` (older cbcrypto builds do not)."""
    return name in (getattr(fn, "__text_signature__", None) or "")


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


# ---- speed policy -------------------------------------------------------------------------------
# One stable band for bots and normal accounts, expressed in MiB/s and converted to requests/s from
# the chunk size, so a small file and a huge one see the same limits:
#   start  - where every transfer begins (no slow ramp from a few MiB/s: small files finish before it ends)
#   max    - the ceiling the pacer probes to and never exceeds
# On a flood the pacer backs off gently, never below roughly half the band unless the server keeps
# refusing, and climbs straight back (see cbcrypto's pacer). Premium accounts keep their own limits.
TARGET_START_MIBS = 30.0   # downloads start here
TARGET_MAX_MIBS = 45.0     # the highest rate the pacer will go to
UPLOAD_START_MIBS = 32.0   # uploads start here and climb toward TARGET_MAX_MIBS
UPLOAD_BURST_MIB = 80.0    # Telegram lets a bot send fast for roughly this much, then only the sustained rate
UPLOAD_SUSTAIN_MIBS = 22.0  # what a bot can send continuously without FloodWait (wzgram sits at 19-20)
TARGET_FLOOR_MIBS = 20.0   # a flood never cuts the rate below this
BAND_GROWTH = 1.1    # ramp inside the band: +10% per quarter second, so a limit is met gently, not overshot
DOWNLOAD_POOL = 14   # parallel connections: one connection carries only a few MiB/s, so this sets the plateau


def _log_transfer(kind: str, file_size: int, pool: int, workers: int, part: int, rate: float, ceiling: float) -> None:
    """One line per transfer: what window and pace the engine was given, and whether the clock is sane.

    The engine copies ``MsgId.time_offset`` at start. A wrong offset there makes every msg_id it sends
    look hours old, so the server rejects them and the transfer crawls or restarts.
    """
    window_mib = pool * workers * part / (1024 * 1024)
    log.info(
        "%s: %.1f MiB, %d connections x %d in flight x %d KiB = %.1f MiB window "
        "(max ~%.0f MiB/s at 0.5 s round trip), pace %.0f..%.0f req/s",
        kind, file_size / (1024 * 1024), pool, workers, part // 1024, window_mib,
        window_mib / 0.5, rate, ceiling,
    )

    if abs(MsgId.time_offset) > 30:
        log.warning(
            "%s starts with a clock offset of %.1fs: msg_ids will look wrong to Telegram and the "
            "transfer will stall. Check for a stale clock sample (see MsgId.sync).",
            kind, MsgId.time_offset,
        )


def _band(chunk: int):
    """(start, max) in requests/s for chunks of ``chunk`` bytes."""
    scale = (1024 * 1024) / chunk
    env = os.environ.get
    return (float(env("CBGRAM_TARGET_START", TARGET_START_MIBS)) * scale,
            float(env("CBGRAM_TARGET_MAX", TARGET_MAX_MIBS)) * scale)


def _floor(chunk: int) -> float:
    """Storm floor in requests/s for chunks of ``chunk`` bytes."""
    return float(os.environ.get("CBGRAM_TARGET_FLOOR", TARGET_FLOOR_MIBS)) * (1024 * 1024) / chunk


def _download_chunk(file_size: int, pool: int) -> int:
    """Smaller chunks for small files so that all connections get work: a 4 MiB file is 4 chunks of
    1 MiB (4 connections, 1 each) but 8 chunks of 512 KiB (8 connections). Telegram accepts any limit
    that divides 1 MiB and is a multiple of 4 KiB."""
    if file_size < 2 * 1024 * 1024:
        return 256 * 1024
    if file_size < pool * 1024 * 1024:
        return 512 * 1024
    return CHUNK_SIZE


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

        if is_premium and not is_bot:
            pool, workers = int(env("CBGRAM_DL_POOL_PREMIUM", 6)), int(env("CBGRAM_DL_WORKERS_PREMIUM", 4))
            chunk = CHUNK_SIZE
            rate, burst = int(env("CBGRAM_DL_RATE_PREMIUM", 150)), int(env("CBGRAM_DL_BURST_PREMIUM", 35))
            ceiling = float(max(rate, 150))
            floor = 1.0
            cap = float(env("CBGRAM_DL_SLOWSTART_CAP", 40))
        else:
            kind = "BOT" if is_bot else "USER"
            pool, workers = int(env(f"CBGRAM_DL_POOL_{kind}", DOWNLOAD_POOL)), int(env(f"CBGRAM_DL_WORKERS_{kind}", DOWNLOAD_WORKERS_DEFAULT))
            chunk = _download_chunk(file_size, pool)
            start, top = _band(chunk)
            rate = int(env(f"CBGRAM_DL_RATE_{kind}", round(start)))
            burst = int(env(f"CBGRAM_DL_BURST_{kind}", 8))
            cap = float(env("CBGRAM_DL_SLOWSTART_CAP", top))
            ceiling = float(max(rate, cap))
            floor = _floor(chunk)

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
            chunk_size=chunk,
            connections=max(1, min(pool, math.ceil(file_size / chunk))),
            inflight=max(1, workers),
            paced=is_bot or not is_premium,
            rate=float(rate),
            burst=float(burst),
            ceiling=ceiling,
            growth=float(env("CBGRAM_DL_GROWTH", 1.5 if is_premium and not is_bot else BAND_GROWTH)) if _slow_start() else 1.0,
            slow_start_cap=cap,
            time_offset=float(MsgId.time_offset),
            recv_buffer=_socket_buffer(),
            read_timeout=float(Session.MEDIA_WAIT_TIMEOUT),
            **({"floor": floor} if _accepts(cbcrypto.download, "floor") else {}),
            **({"on_flood": _flood_log("upload.GetFile")} if _accepts(cbcrypto.download, "on_flood") else {}),
        )

        _log_transfer("download", file_size, pool, workers, chunk, rate, ceiling)

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
    pool_cap = max(1, total_parts)  # the engine shares parts out evenly, so every connection gets work

    if is_premium and not is_bot:
        workers = int(env("CBGRAM_UPLOAD_WORKERS_PREMIUM", 2))
        rate = int(env("CBGRAM_UPLOAD_RATE_PREMIUM", 300))
        pool = min(int(env("CBGRAM_UPLOAD_POOL_PREMIUM", 14)), UPLOAD_POOL_MAX, pool_cap)
        ceiling = float(max(rate, 300))
        cap = float(env("CBGRAM_UPLOAD_SLOWSTART_CAP", 80))
        floor = 1.0
        burst_credit = sustain = 0.0
    else:
        kind = "BOT" if is_bot else "USER"
        start, top = _band(PART_SIZE)
        # start at a rate Telegram accepts and climb while there are no flood waits, instead of racing up
        # to the maximum and being flooded on every file (3-4 floods, ~5 s frozen, then a cut to ~20)
        start = float(env("CBGRAM_UPLOAD_START_MIBS", UPLOAD_START_MIBS)) * (1024 * 1024) / PART_SIZE
        # requests in flight per connection. Throughput of a connection is (in-flight bytes / round trip),
        # so with 3 x 12 connections only 18 MiB are in the air: at 0.6 s per SaveBigFilePart that is
        # ~30 MiB/s no matter what the pacer allows. 16 x 4 x 512 KiB = 32 MiB in the air lifts that to ~55. The pacer still caps requests/s, so a bigger
        # window cannot raise the request rate, it only lets the paced rate be reached.
        workers = int(env(f"CBGRAM_UPLOAD_WORKERS_{kind}", UPLOAD_WORKERS_DEFAULT))
        rate = int(env(f"CBGRAM_UPLOAD_RATE_{kind}", round(start)))
        pool = min(int(env(f"CBGRAM_UPLOAD_POOL_{kind}", UPLOAD_POOL_DEFAULT)), UPLOAD_POOL_MAX, pool_cap)
        cap = float(env("CBGRAM_UPLOAD_SLOWSTART_CAP", rate))  # = start rate: no exponential overshoot
        ceiling = float(max(rate, top))
        floor = _floor(PART_SIZE)
        burst_credit = float(env("CBGRAM_UPLOAD_BURST_MIB", UPLOAD_BURST_MIB)) * (1024 * 1024) / PART_SIZE
        sustain = float(env("CBGRAM_UPLOAD_SUSTAIN_MIBS", UPLOAD_SUSTAIN_MIBS)) * (1024 * 1024) / PART_SIZE

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
        inflight=max(1, workers),
        paced=is_bot or not is_premium,
        rate=float(rate),
        burst=float(max(8, round(rate * 0.15))),  # small: a 30-request spike in the first second is itself a flood trigger
        ceiling=ceiling,
        step=1.0,
        growth=float(env("CBGRAM_UPLOAD_GROWTH", 1.5 if is_premium and not is_bot else BAND_GROWTH)) if _slow_start() else 1.0,
        slow_start_cap=cap,
        time_offset=float(MsgId.time_offset),
        recv_buffer=_socket_buffer(),
        read_timeout=float(Session.MEDIA_WAIT_TIMEOUT),
        **({"floor": floor} if _accepts(cbcrypto.upload, "floor") else {}),
        **({"burst_credit": burst_credit, "sustain": sustain} if _accepts(cbcrypto.upload, "burst_credit") else {}),
        **({"on_flood": _flood_log("upload.SaveBigFilePart" if is_big else "upload.SaveFilePart")} if _accepts(cbcrypto.upload, "on_flood") else {}),
    )
    rpc_type = raw.functions.upload.SaveBigFilePart if is_big else raw.functions.upload.SaveFilePart

    _log_transfer("upload", file_size, pool, workers, PART_SIZE, rate, ceiling)

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
