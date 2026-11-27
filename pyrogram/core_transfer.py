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
import time
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
UPLOAD_POOL_MAX = 50
UPLOAD_WORKERS_DEFAULT = 8
UPLOAD_POOL_DEFAULT = 18
DOWNLOAD_WORKERS_DEFAULT = 4   # requests in flight per connection (4 x 4 = 16 MiB in the air; was 15 x 5 = 75, which got connections shed)

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
        self.base = 0   # bytes already on disk when a transfer is resumed
        self.cancelled = threading.Event()
        self.stop_exc: Optional[BaseException] = None
        self._latest = 0
        self._sent = -1
        self._task: Optional[asyncio.Task] = None

    # runs in the engine thread
    def tick(self, done: int) -> None:
        done += self.base
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
        except Exception:
            pass

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
TARGET_MAX_MIBS = 60.0     # the highest rate the pacer will go to (uploads; pacer backs off on FloodWait)
UPLOAD_START_MIBS = 32.0   # uploads start at what a bot is allowed from the first second, then climb toward TARGET_MAX_MIBS
UPLOAD_BURST_MIB = 40.0    # fast start allowance; kept small because a bigger bank was flooded inside the first second
UPLOAD_SUSTAIN_MIBS = 32.0  # what a bot can send continuously without FloodWait
TARGET_FLOOR_MIBS = 20.0   # a flood never cuts the rate below this
BAND_GROWTH = 1.1    # ramp inside the band: +10% per quarter second, so a limit is met gently, not overshot
UPLOAD_READ_TIMEOUT = 15.0  # a part that is not answered within this is requeued on a fresh connection (was 60 s of frozen speed)
DL_BURST_MIB = 40.0      # DOWNLOAD fast-start bank, same idea as UPLOAD_BURST_MIB
DL_SUSTAIN_MIBS = 24.0   # DOWNLOAD steady rate the pacer starts from and then learns up/down (like UPLOAD_SUSTAIN_MIBS)
DL_READ_TIMEOUT = 15.0   # a chunk not answered in this long is requeued on a fresh connection (was 60 s of frozen speed)
DL_TARGET_START_MIBS = 24.0  # DOWNLOAD only: start rate (uploads keep TARGET_START_MIBS)
DL_TARGET_MAX_MIBS = 60.0    # DOWNLOAD only: ceiling (uploads keep TARGET_MAX_MIBS); pacer backs off on FloodWait
DL_TARGET_FLOOR_MIBS = 20.0  # DOWNLOAD only: a flood never cuts below this (keeps the average near 20+)
DOWNLOAD_POOL = 4    # parallel connections for ONE download (ferogram caps at 4: more and Telegram sheds them with a reset)
DOWNLOAD_CONN_BUDGET = 12   # all downloads together (ferogram's global ceiling is 12)
DOWNLOAD_MAX_RECONNECTS = 25   # consecutive failures per connection (engine default 5 gave up after ~7 s)
DOWNLOAD_RETRIES = 6           # resumed retries after a reset; speed settings stay the same

_dl_state = {"active": 0}
_rtt_cache: dict = {}


async def _measure_rtt(host: str, port: int) -> Optional[float]:
    """Round-trip time to the DC in seconds (best of 2 TCP connects, cached 10 min), None if unknown."""
    hit = _rtt_cache.get(host)
    if hit and time.monotonic() - hit[1] < 600:
        return hit[0]
    best = None
    for _ in range(2):
        t0 = time.perf_counter()
        try:
            _, w = await asyncio.wait_for(asyncio.open_connection(host, port), 3.0)
        except Exception:
            continue
        d = time.perf_counter() - t0
        try:
            w.close()
        except Exception:
            pass
        best = d if best is None else min(best, d)
    if best is not None:
        _rtt_cache[host] = (best, time.monotonic())
    return best


def _adapt_to_distance(pool: int, workers: int, rtt: Optional[float]):
    """A far server (Heroku US/EU -> a Singapore DC) gets less speed per TCP connection, because the
    path is long and loss-based congestion control backs off. More connections and more requests in
    flight make up for it. Near servers keep the small, safe numbers. Needs no config; CBGRAM_DL_ADAPTIVE=0
    turns it off. A reset still halves everything on the retry, so a too-greedy guess corrects itself."""
    env = os.environ.get
    if rtt is None or env("CBGRAM_DL_ADAPTIVE", "1") != "1":
        return pool, workers
    if rtt > float(env("CBGRAM_DL_VFAR_RTT", 0.20)):
        return max(pool, int(env("CBGRAM_DL_VFAR_POOL", 8))), max(workers, 5)
    if rtt > float(env("CBGRAM_DL_FAR_RTT", 0.09)):
        return max(pool, int(env("CBGRAM_DL_FAR_POOL", 6))), max(workers, 5)
    return pool, workers


def _conn_share() -> int:
    """Connections this download may open: the global budget split over the downloads running now."""
    budget = int(os.environ.get("CBGRAM_DL_CONN_BUDGET", DOWNLOAD_CONN_BUDGET))
    return max(3, budget // max(1, _dl_state["active"]))


def _band(chunk: int, download: bool = False):
    """(start, max) in requests/s for chunks of ``chunk`` bytes."""
    scale = (1024 * 1024) / chunk
    env = os.environ.get
    if download:
        return (float(env("CBGRAM_DL_TARGET_START", DL_TARGET_START_MIBS)) * scale,
                float(env("CBGRAM_DL_TARGET_MAX", DL_TARGET_MAX_MIBS)) * scale)
    return (float(env("CBGRAM_TARGET_START", TARGET_START_MIBS)) * scale,
            float(env("CBGRAM_TARGET_MAX", TARGET_MAX_MIBS)) * scale)


def _floor(chunk: int, download: bool = False) -> float:
    """Storm floor in requests/s for chunks of ``chunk`` bytes."""
    if download:
        return float(os.environ.get("CBGRAM_DL_TARGET_FLOOR", DL_TARGET_FLOOR_MIBS)) * (1024 * 1024) / chunk
    return float(os.environ.get("CBGRAM_TARGET_FLOOR", TARGET_FLOOR_MIBS)) * (1024 * 1024) / chunk


def _upload_pool(file_size: int) -> int:
    """Connections for an upload: few for small files (no ramp-up cost), the full pool for big ones."""
    mib = file_size / (1024 * 1024)
    if mib < 20:
        return 6
    if mib < 100:
        return 12
    return UPLOAD_POOL_DEFAULT


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


def _resume_point(path: str, file_size: int, chunk: int, floor_idx: int = 0) -> int:
    """Byte offset of the first chunk that is still all zeros (not yet written), chunk aligned.

    The file is pre-sized, so chunks the engine has not written read back as zeros. Chunks finish
    nearly in order, so everything before the first hole is done and a resumed transfer only
    re-fetches from there on (the few chunks past the hole that did arrive are fetched again)."""
    try:
        with open(path, "rb") as f:
            idx = floor_idx
            while idx * chunk < file_size:
                f.seek(idx * chunk)
                block = f.read(min(chunk, file_size - idx * chunk))
                if not block or block.count(0) == len(block):
                    break
                idx += 1
            return idx * chunk
    except OSError:
        return 0


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
            burst_credit = sustain = 0.0
        else:
            kind = "BOT" if is_bot else "USER"
            pool, workers = int(env(f"CBGRAM_DL_POOL_{kind}", DOWNLOAD_POOL)), int(env(f"CBGRAM_DL_WORKERS_{kind}", DOWNLOAD_WORKERS_DEFAULT))
            _, _h, _p = await _endpoint(client, dc_id)
            rtt = await _measure_rtt(_h, _p)
            pool, workers = _adapt_to_distance(pool, workers, rtt)
            # far servers get double the shared connection budget (they need more sockets for the same speed)
            pool = min(pool, _conn_share() * (2 if rtt and rtt > float(env("CBGRAM_DL_FAR_RTT", 0.09)) else 1))
            log.info("Download from DC%s: RTT %s -> %d connections x %d in flight",
                     dc_id, "unknown" if rtt is None else "%.0f ms" % (rtt * 1000), pool, workers)
            chunk = _download_chunk(file_size, pool)
            start, top = _band(chunk, download=True)
            rate = int(env(f"CBGRAM_DL_RATE_{kind}", round(start)))
            burst = int(env(f"CBGRAM_DL_BURST_{kind}", 8))
            cap = float(env("CBGRAM_DL_SLOWSTART_CAP", top))
            ceiling = float(max(rate, cap))
            floor = _floor(chunk, download=True)
            burst_credit = float(env("CBGRAM_DL_BURST_MIB", DL_BURST_MIB)) * (1024 * 1024) / chunk
            sustain = float(env("CBGRAM_DL_SUSTAIN_MIBS", DL_SUSTAIN_MIBS)) * (1024 * 1024) / chunk

        _dl_state["active"] += 1
        try:
            session, host, port = await _endpoint(client, dc_id)
            prefix = raw.functions.upload.GetFile(location=location, offset=0, limit=0).write()[:-12]
            init_prefix = await _init_prefix(client)
            max_reconnects = int(env("CBGRAM_DL_MAX_RECONNECTS", DOWNLOAD_MAX_RECONNECTS))
            retries = int(env("CBGRAM_DL_RETRIES", DOWNLOAD_RETRIES))

            resume_at = 0
            for attempt in range(retries + 1):
                # each retry after a reset halves pool and rate (a reset means we were too aggressive); CBGRAM_DL_RETRY_GENTLE=0 keeps them
                gentle = 2 ** attempt if env("CBGRAM_DL_RETRY_GENTLE", "1") == "1" else 1
                a_pool = max(2, pool // gentle)
                a_rate = max(floor, float(rate) / gentle)
                a_ceiling = max(a_rate, ceiling / gentle)
                a_cap = max(a_rate, cap / gentle)
                a_credit = burst_credit / gentle
                a_sustain = max(floor, sustain / gentle) if sustain else 0.0
                tracker = _Progress(client, progress, progress_args, file_size)
                tracker.base = resume_at

                call = functools.partial(
                    cbcrypto.download,
                    host=host,
                    port=port,
                    auth_key=bytes(session.auth_key),
                    auth_key_id=bytes(session.auth_key_id),
                    salt=session.salt,
                    init_prefix=init_prefix,
                    req_prefix=prefix,
                    path=path,
                    file_size=file_size,
                    start_offset=resume_at,
                    chunk_size=chunk,
                    connections=max(1, min(a_pool, math.ceil((file_size - resume_at) / chunk))),
                    inflight=max(1, workers),
                    paced=is_bot or not is_premium,
                    rate=a_rate,
                    burst=float(max(burst, round(a_rate * 0.15))),  # small spike, like uploads
                    ceiling=a_ceiling,
                    step=1.0,
                    growth=float(env("CBGRAM_DL_GROWTH", 1.5 if is_premium and not is_bot else BAND_GROWTH)) if _slow_start() else 1.0,
                    slow_start_cap=a_cap,
                    time_offset=float(MsgId.time_offset),
                    recv_buffer=_socket_buffer(),
                    read_timeout=float(env("CBGRAM_DL_READ_TIMEOUT", DL_READ_TIMEOUT)),
                    **({"burst_credit": a_credit, "sustain": a_sustain} if _accepts(cbcrypto.download, "burst_credit") else {}),
                    **({"max_reconnects": max_reconnects} if _accepts(cbcrypto.download, "max_reconnects") else {}),
                    **({"floor": floor} if _accepts(cbcrypto.download, "floor") else {}),
                    **({"on_flood": _flood_log("upload.GetFile")} if _accepts(cbcrypto.download, "on_flood") else {}),
                )

                try:
                    await _run_engine(tracker, call, raw.functions.upload.GetFile)
                except CdnRedirect:
                    return False
                except (OSError, TimeoutError) as e:
                    # "Connection reset by peer" and friends: Telegram dropped us. Wait, then start the
                    # file again, gentler. (Chunks land out of order, so there is no safe resume point.)
                    if attempt >= retries:
                        raise
                    wait = min(2.0 * (attempt + 1), 10.0)
                    resume_at = await asyncio.get_running_loop().run_in_executor(
                        None, _resume_point, path, file_size, chunk, resume_at // chunk)
                    log.warning("Download interrupted (%s); resuming at %.1f of %.1f MiB (retry %d/%d in %.0fs)",
                                e, resume_at / 1048576, file_size / 1048576, attempt + 1, retries, wait)
                    await asyncio.sleep(wait)
                    if resume_at >= file_size:
                        await tracker.finish()
                        return True
                    continue
                await tracker.finish()
                return True
        finally:
            _dl_state["active"] -= 1


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
        pool = min(int(env(f"CBGRAM_UPLOAD_POOL_{kind}", _upload_pool(file_size))), UPLOAD_POOL_MAX, pool_cap)
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
        read_timeout=float(env("CBGRAM_UPLOAD_READ_TIMEOUT", UPLOAD_READ_TIMEOUT)),
        **({"floor": floor} if _accepts(cbcrypto.upload, "floor") else {}),
        **({"burst_credit": burst_credit, "sustain": sustain} if _accepts(cbcrypto.upload, "burst_credit") else {}),
        **({"on_flood": _flood_log("upload.SaveBigFilePart" if is_big else "upload.SaveFilePart")} if _accepts(cbcrypto.upload, "on_flood") else {}),
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


# ---------------------------------------------------------------------------------------------
# streaming: the same Rust engine, but the reader gets the bytes while they arrive
# ---------------------------------------------------------------------------------------------

STREAM_CHUNK = 1024 * 1024


def stream_supported(client) -> bool:
    """True if the Rust core can stream (needs cbcrypto >= 0.6.0 and no proxy)."""
    return enabled(client) and hasattr(cbcrypto, "StreamCtl")


def stream_chunk() -> int:
    """Chunk size of a stream. Telegram accepts any size that divides 1 MiB and is a multiple of 4 KiB."""
    c = int(os.environ.get("CBGRAM_STREAM_CHUNK", STREAM_CHUNK))
    if c < 4096 or c % 4096 or (1024 * 1024) % c:
        return STREAM_CHUNK
    return c


async def stream_download(client, file_id, file_size: int, path: Optional[str], start_offset: int, ctl, notify: Callable[[], None]) -> bool:
    """Run ONE engine job that writes ``file_id`` into the pre-sized file ``path`` from ``start_offset`` on.

    ``ctl`` is a ``cbcrypto.StreamCtl``: the reader moves ``ctl.consumed()`` forward and reads
    ``ctl.watermark()`` (everything below it is on disk). The engine never runs more than the ctl's
    window ahead of the reader, so an idle or paused player costs no bandwidth. ``notify`` is called
    (from the engine thread) whenever new bytes landed. Returns False for a CDN file (use Python).
    Returns when the file is complete, or quietly when ``ctl.cancel()`` was called.
    """
    chunk = stream_chunk()
    path = path or os.devnull                           # in-memory streams (StreamCtl(memory=True)) write no file
    if start_offset % chunk or start_offset >= file_size:
        raise ValueError("start_offset must be chunk aligned and inside the file")

    dc_id = file_id.dc_id
    is_bot, is_premium = _flags(client)
    env = os.environ.get
    kind = "BOT" if is_bot else "USER"

    if is_premium and not is_bot:
        pool, workers = int(env("CBGRAM_DL_POOL_PREMIUM", 6)), int(env("CBGRAM_DL_WORKERS_PREMIUM", 4))
        rate, burst = int(env("CBGRAM_DL_RATE_PREMIUM", 150)), int(env("CBGRAM_DL_BURST_PREMIUM", 35))
        ceiling, floor = float(max(rate, 150)), 1.0
        cap = float(env("CBGRAM_DL_SLOWSTART_CAP", 40))
        burst_credit = sustain = 0.0
    else:
        pool, workers = int(env(f"CBGRAM_DL_POOL_{kind}", DOWNLOAD_POOL)), int(env(f"CBGRAM_DL_WORKERS_{kind}", DOWNLOAD_WORKERS_DEFAULT))
        _, _h, _p = await _endpoint(client, dc_id)
        rtt = await _measure_rtt(_h, _p)
        pool, workers = _adapt_to_distance(pool, workers, rtt)
        pool = min(pool, _conn_share() * (2 if rtt and rtt > float(env("CBGRAM_DL_FAR_RTT", 0.09)) else 1))
        start, top = _band(chunk, download=True)
        rate = int(env(f"CBGRAM_DL_RATE_{kind}", round(start)))
        burst = int(env(f"CBGRAM_DL_BURST_{kind}", 8))
        cap = float(env("CBGRAM_DL_SLOWSTART_CAP", top))
        ceiling = float(max(rate, cap))
        floor = _floor(chunk, download=True)
        burst_credit = float(env("CBGRAM_DL_BURST_MIB", DL_BURST_MIB)) * (1024 * 1024) / chunk
        sustain = float(env("CBGRAM_DL_SUSTAIN_MIBS", DL_SUSTAIN_MIBS)) * (1024 * 1024) / chunk

    location = client._file_location(file_id)
    loop = asyncio.get_running_loop()
    pending = threading.Event()

    def wake() -> None:
        pending.clear()
        notify()

    def tick(_done: int) -> None:      # engine thread, only called when new bytes landed
        if not pending.is_set():
            pending.set()
            loop.call_soon_threadsafe(wake)

    if env("CBGRAM_STREAM_POOL"):                       # many small streams: few sockets each
        pool = max(1, min(pool, int(env("CBGRAM_STREAM_POOL"))))

    _dl_state["active"] += 1
    try:
        session, host, port = await _endpoint(client, dc_id)
        prefix = raw.functions.upload.GetFile(location=location, offset=0, limit=0).write()[:-12]
        init_prefix = await _init_prefix(client)
        max_reconnects = int(env("CBGRAM_DL_MAX_RECONNECTS", DOWNLOAD_MAX_RECONNECTS))
        retries = int(env("CBGRAM_DL_RETRIES", DOWNLOAD_RETRIES))

        resume_at = start_offset
        for attempt in range(retries + 1):
            gentle = 2 ** attempt if env("CBGRAM_DL_RETRY_GENTLE", "1") == "1" else 1
            a_pool = max(2, pool // gentle)
            a_rate = max(floor, float(rate) / gentle)
            call = functools.partial(
                cbcrypto.download,
                host=host,
                port=port,
                auth_key=bytes(session.auth_key),
                auth_key_id=bytes(session.auth_key_id),
                salt=session.salt,
                init_prefix=init_prefix,
                req_prefix=prefix,
                path=path,
                file_size=file_size,
                start_offset=resume_at,
                chunk_size=chunk,
                connections=max(1, min(a_pool, math.ceil((file_size - resume_at) / chunk))),
                inflight=max(1, workers),
                paced=is_bot or not is_premium,
                rate=a_rate,
                burst=float(max(burst, round(a_rate * 0.15))),
                ceiling=max(a_rate, ceiling / gentle),
                step=1.0,
                growth=float(env("CBGRAM_DL_GROWTH", 1.5 if is_premium and not is_bot else BAND_GROWTH)) if _slow_start() else 1.0,
                slow_start_cap=max(a_rate, cap / gentle),
                time_offset=float(MsgId.time_offset),
                recv_buffer=_socket_buffer(),
                read_timeout=float(env("CBGRAM_DL_READ_TIMEOUT", DL_READ_TIMEOUT)),
                max_reconnects=max_reconnects,
                floor=floor,
                burst_credit=burst_credit / gentle,
                sustain=max(floor, sustain / gentle) if sustain else 0.0,
                on_flood=_flood_log("upload.GetFile"),
                progress=tick,
                stream=ctl,
            )
            fut = loop.run_in_executor(None, call)
            try:
                await asyncio.shield(fut)
                return True
            except asyncio.CancelledError:
                ctl.cancel()
                try:
                    await fut
                except BaseException:
                    pass
                raise
            except cbcrypto.CoreError as e:
                kind_, _code, _msg = e.args[:3]
                if kind_ == "cancelled":
                    return True
                if kind_ == "cdn":
                    return False
                if kind_ in ("io", "timeout", "protocol") and attempt < retries:
                    # Telegram dropped us: wait, then continue from the contiguous watermark, gentler
                    resume_at = min(file_size - 1, (ctl.watermark() // chunk) * chunk)
                    wait = min(2.0 * (attempt + 1), 10.0)
                    log.warning("Stream interrupted (%s: %s); resuming at %.1f MiB (retry %d/%d in %.0fs)",
                                kind_, _msg, resume_at / 1048576, attempt + 1, retries, wait)
                    await asyncio.sleep(wait)
                    if ctl.watermark() >= file_size:
                        return True
                    continue
                _raise_core_error(e, raw.functions.upload.GetFile)
        return True
    finally:
        _dl_state["active"] -= 1
