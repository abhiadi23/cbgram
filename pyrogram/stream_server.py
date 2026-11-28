#  cbgram - stream links for Telegram files
#
#  A tiny HTTP/1.1 server (standard library only) that serves a Telegram file *while it is being
#  downloaded*: players and browsers get the first bytes after one round trip, can seek anywhere
#  (HTTP Range), and a paused or closed player stops costing bandwidth.
#
#  The fast part is not here. cbcrypto's Rust engine downloads over several MTProto connections,
#  decrypts with AES-NI/VAES and writes each chunk straight into a sparse cache file; it publishes a
#  "contiguous watermark" (everything below it is on disk) and refuses to run more than a window
#  ahead of the reader. This module only parses HTTP, maps byte ranges to engine jobs and hands the
#  bytes to the socket with ``loop.sendfile`` (zero copy from the page cache).
#
#  Without cbcrypto >= 0.6.0 (or with a proxy, or CBGRAM_RUST_CORE=0) it falls back to the slower
#  pure-Python ``Client.get_file`` generator, so links always work.
#
#  Put it behind nginx/Caddy/Cloudflare for TLS; a link is a random unguessable token.

import asyncio
import logging
import mimetypes
import os
import re
import secrets
import shutil
import tempfile
import time
import urllib.parse
from typing import Optional, Union

import pyrogram
from pyrogram import core_transfer, types
from pyrogram.errors import FileReferenceExpired
from pyrogram.file_id import FileId

log = logging.getLogger(__name__)

MIB = 1024 * 1024
_RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")
_MEDIA_KINDS = ("audio", "document", "photo", "sticker", "animation", "video", "voice", "video_note", "new_chat_photo")
_REASONS = {200: "OK", 204: "No Content", 206: "Partial Content", 400: "Bad Request", 404: "Not Found",
            405: "Method Not Allowed", 416: "Range Not Satisfiable", 431: "Request Header Fields Too Large",
            502: "Bad Gateway", 503: "Service Unavailable"}


class _Job:
    """One engine run: downloads [start, file end) into the cache file, gated by the reader."""

    __slots__ = ("start", "ctl", "task", "readers", "idle_since", "event", "error", "finished", "consumed")

    def __init__(self, start: int, ctl):
        self.start = start
        self.ctl = ctl
        self.task: Optional[asyncio.Task] = None
        self.readers = 0
        self.idle_since = time.monotonic()
        self.event = asyncio.Event()
        self.error: Optional[BaseException] = None
        self.finished = False
        self.consumed = start

    def watermark(self) -> int:
        return self.ctl.watermark()

    def wake(self) -> None:
        ev, self.event = self.event, asyncio.Event()
        ev.set()


class _Source:
    def __init__(self, key: str, file_id: FileId, size: int, name: str, mime: str):
        self.key = key
        self.file_id = file_id
        self.size = size
        self.name = name
        self.mime = mime
        self.path: Optional[str] = None
        self.fd: Optional[int] = None
        self.jobs: list = []
        self.readers = 0
        self.last_used = time.monotonic()
        self.python_mode = False
        self.ref: Optional[tuple] = None                 # (chat_id, message_id): where to get a fresh file reference
        self.heal_lock = asyncio.Lock()


class StreamServer:
    """Serve Telegram files over HTTP with Range support while they download.

    Example:
        .. code-block:: python

            server = StreamServer(app, port=8080, public_url="https://stream.example.com")
            await server.start()
            url = server.link(message)        # a Message, a media object or a file id
            ...
            await server.stop()

    Parameters:
        client: a started :obj:`~pyrogram.Client`.
        host / port: where to listen (default ``0.0.0.0:8080``).
        public_url: base of the links you hand out (default ``http://localhost:<port>``).
        cache_dir: where the sparse cache files live. A RAM disk (``/dev/shm``) is the fastest.
        window_mib: how far (MiB) the engine may run ahead of the player. More = smoother on a
            bursty client, less = less wasted data when the player stops. Default 64.
        link_ttl: seconds a link stays valid (default 6 hours).
    """

    def __init__(
        self,
        client: "pyrogram.Client",
        host: str = "0.0.0.0",
        port: int = 8080,
        public_url: Optional[str] = None,
        cache_dir: Optional[str] = None,
        window_mib: Optional[int] = None,
        link_ttl: Optional[int] = None,
        max_connections: int = 256,
        mode: Optional[str] = None,
        mem_window_mib: Optional[float] = None,
    ):
        env = os.environ.get
        self.client = client
        self.host = host
        self.port = port
        self.public_url = (public_url or env("CBGRAM_STREAM_URL") or f"http://localhost:{port}").rstrip("/")
        self.cache_dir = cache_dir or env("CBGRAM_STREAM_DIR") or os.path.join(tempfile.gettempdir(), f"cbgram-stream-{os.getpid()}")
        # the engine needs room for connections x requests in flight (about 24 MiB), so never less than 32
        self.window = max(32, int(window_mib or env("CBGRAM_STREAM_WINDOW_MIB", 64))) * MIB
        self.link_ttl = int(link_ttl or env("CBGRAM_STREAM_TTL", 6 * 3600))
        # "disk": sparse cache file, seeks and repeat viewers are served from it.
        # "memory": no file at all; each request streams through a bounded RAM buffer (the engine never
        # holds more than `mem_window` + what is in flight), which is what you want for MANY concurrent streams.
        self.mode = (mode or env("CBGRAM_STREAM_MODE", "disk")).lower()
        if self.mode not in ("disk", "memory"):
            raise ValueError("mode must be 'disk' or 'memory'")
        self.mem_window = max(2.0, float(mem_window_mib or env("CBGRAM_STREAM_MEM_WINDOW_MIB", 8))) * MIB
        self.jump = int(env("CBGRAM_STREAM_JUMP_MIB", 16)) * MIB        # a seek further than this starts a new job
        self.job_idle = float(env("CBGRAM_STREAM_JOB_IDLE", 10))        # seconds a reader-less job lives on
        self.source_idle = float(env("CBGRAM_STREAM_SOURCE_IDLE", 1800))
        self.max_jobs = int(env("CBGRAM_STREAM_MAX_JOBS", 4))           # per file
        self.cache_cap = int(float(env("CBGRAM_STREAM_CACHE_GB", 20)) * 1024 * MIB)
        self.cors = env("CBGRAM_STREAM_CORS", "*")
        self.send_max = 8 * MIB
        self._tokens: dict = {}      # token -> (source, expires)
        self._sources: dict = {}     # key -> source
        self._server: Optional[asyncio.base_events.Server] = None
        self._janitor: Optional[asyncio.Task] = None
        self._conn_sem = asyncio.Semaphore(max_connections)
        self._sendfile_ok = True
        self._writers: set = set()
        self._tasks: set = set()
        self._mem_ctls: set = set()

    # ------------------------------------------------------------------ public API

    @property
    def fast(self) -> bool:
        """True if the Rust streaming engine is in use (False = slower pure-Python fallback)."""
        return core_transfer.stream_supported(self.client)

    def stats(self) -> dict:
        """Live numbers for monitoring: open players, files, and the RAM held by memory-mode streams."""
        return {
            "mode": self.mode,
            "engine": "rust" if self.fast else "python",
            "connections": len(self._writers),
            "files": len(self._sources),
            "links": len(self._tokens),
            "memory_streams": len(self._mem_ctls),
            "buffered_bytes": sum(c.buffered() for c in list(self._mem_ctls)),
        }

    async def start(self) -> None:
        if self._server:
            return
        os.makedirs(self.cache_dir, exist_ok=True)
        self._server = await asyncio.start_server(self._handle, self.host, self.port, limit=32 * 1024, backlog=256)
        self._janitor = asyncio.create_task(self._janitor_loop())
        log.info("Stream server on %s:%s (%s engine)", self.host, self.port, "Rust" if self.fast else "Python")

    async def stop(self) -> None:
        if self._janitor:
            self._janitor.cancel()
            self._janitor = None
        if self._server:
            self._server.close()
            tasks = list(self._tasks)                    # open players must not keep stop() waiting
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for w in list(self._writers):
                try:
                    w.transport.abort()
                except Exception:
                    pass
            await self._server.wait_closed()
            self._server = None
        for src in list(self._sources.values()):
            await self._drop_source(src)
        self._tokens.clear()
        shutil.rmtree(self.cache_dir, ignore_errors=True)

    def link(
        self,
        media: Union["types.Message", str, object],
        name: Optional[str] = None,
        ttl: Optional[int] = None,
        download: bool = False,
    ) -> str:
        """Create a stream link for a message, a media object or a file id string."""
        ref = None
        if isinstance(media, types.Message):
            ref = (media.chat.id if media.chat else None, media.id)
            for kind in _MEDIA_KINDS:
                obj = getattr(media, kind, None)
                if obj is not None:
                    media = obj
                    break
            else:
                raise ValueError("This message doesn't contain any downloadable media")

        if isinstance(media, str):
            file_id_str, size, fname, mime = media, 0, None, None
        else:
            file_id_str = media.file_id
            size = getattr(media, "file_size", 0) or 0
            fname = getattr(media, "file_name", None)
            mime = getattr(media, "mime_type", None)

        if size <= 0:
            raise ValueError("Unknown file size: pass a Message or a media object, not a bare file id")

        file_id = FileId.decode(file_id_str)
        name = name or fname or ("photo.jpg" if file_id.file_type.name in ("PHOTO", "THUMBNAIL", "CHAT_PHOTO") else "file.bin")
        mime = mime or mimetypes.guess_type(name)[0] or ("image/jpeg" if name.endswith(".jpg") else "application/octet-stream")

        key = f"{file_id_str}:{size}"
        src = self._sources.get(key)
        if src is None:
            self._evict_for(size)
            src = _Source(key, file_id, size, name, mime)
            self._sources[key] = src
        if ref and ref[0] is not None:
            src.ref = ref
        src.last_used = time.monotonic()

        token = secrets.token_urlsafe(16)
        self._tokens[token] = (src, time.monotonic() + (ttl or self.link_ttl), download)
        return f"{self.public_url}/s/{token}/{urllib.parse.quote(name)}" + ("?dl=1" if download else "")

    def revoke(self, link_or_token: str) -> bool:
        m = re.search(r"/s/([A-Za-z0-9_\-]+)", link_or_token)
        token = m.group(1) if m else link_or_token
        return self._tokens.pop(token, None) is not None

    # ------------------------------------------------------------------ housekeeping

    def _evict_for(self, size: int) -> None:
        """Make room in the cache: drop the least recently used sources nobody is reading."""
        total = sum(s.size for s in self._sources.values() if s.path)
        if total + size <= self.cache_cap:
            return
        for src in sorted(self._sources.values(), key=lambda s: s.last_used):
            if total + size <= self.cache_cap:
                break
            if src.readers == 0 and src.path:
                total -= src.size
                asyncio.ensure_future(self._drop_source(src))

    async def _drop_source(self, src: _Source) -> None:
        self._sources.pop(src.key, None)
        for tok in [t for t, v in self._tokens.items() if v[0] is src]:
            self._tokens.pop(tok, None)
        for job in src.jobs:
            self._cancel_job(job)
        tasks = [j.task for j in src.jobs if j.task]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        src.jobs.clear()
        if src.fd is not None:
            try:
                os.close(src.fd)
            except OSError:
                pass
            src.fd = None
        if src.path:
            try:
                os.unlink(src.path)
            except OSError:
                pass
            src.path = None

    @staticmethod
    def _cancel_job(job: _Job) -> None:
        if not job.finished:
            try:
                job.ctl.cancel()
            except Exception:
                pass

    async def _janitor_loop(self) -> None:
        while True:
            await asyncio.sleep(2)
            now = time.monotonic()
            for tok in [t for t, v in self._tokens.items() if v[1] < now]:
                self._tokens.pop(tok, None)
            for src in list(self._sources.values()):
                for job in src.jobs:
                    if job.readers == 0 and not job.finished and now - job.idle_since > self.job_idle:
                        self._cancel_job(job)   # a parked job holds connections; its bytes stay valid on disk
                has_token = any(v[0] is src for v in self._tokens.values())
                if src.readers == 0 and (not has_token or now - src.last_used > self.source_idle):
                    await self._drop_source(src)

    # ------------------------------------------------------------------ engine jobs

    def _ensure_file(self, src: _Source) -> None:
        if src.path:
            return
        src.path = os.path.join(self.cache_dir, secrets.token_hex(8))
        fd = os.open(src.path, os.O_RDWR | os.O_CREAT | os.O_EXCL, 0o600)
        os.ftruncate(fd, src.size)                      # sparse: only what the engine writes uses disk
        src.fd = fd

    def _start_job(self, src: _Source, pos: int) -> _Job:
        import cbcrypto

        self._ensure_file(src)
        chunk = core_transfer.stream_chunk()
        start = (pos // chunk) * chunk
        # keep the number of parked jobs per file small
        live = [j for j in src.jobs if not j.finished]
        while len(live) >= self.max_jobs:
            idle = sorted((j for j in live if j.readers == 0), key=lambda j: j.idle_since)
            victim = idle[0] if idle else live[0]
            self._cancel_job(victim)
            live.remove(victim)
        ctl = cbcrypto.StreamCtl(self.window)
        job = _Job(start, ctl)
        src.jobs.append(job)
        job.task = asyncio.create_task(self._run_job(src, job))
        return job

    async def _heal(self, src: _Source, stale: FileId) -> bool:
        """FILE_REFERENCE_EXPIRED: fetch the message again and swap in the fresh file id. One refresh per
        file no matter how many streams hit the error at once; the others just see the new id."""
        async with src.heal_lock:
            if src.file_id is not stale:
                return True                                # somebody else already healed it
            if not src.ref:
                return False
            chat_id, message_id = src.ref
            try:
                msg = await self.client.get_messages(chat_id, message_id)
                for kind in _MEDIA_KINDS:
                    obj = getattr(msg, kind, None)
                    if obj is not None:
                        src.file_id = FileId.decode(obj.file_id)
                        log.info("Refreshed the file reference of %s", src.name)
                        return True
            except Exception as e:                         # noqa: BLE001
                log.warning("Could not refresh the file reference of %s: %s", src.name, e)
            return False

    async def _download(self, src: _Source, start: int, ctl, notify, path: Optional[str], end_offset: Optional[int] = None) -> bool:
        """One stream job, healing an expired file reference (up to twice) by resuming where it stopped."""
        chunk = core_transfer.stream_chunk()
        for attempt in range(3):
            fid = src.file_id
            at = max(start, (ctl.watermark() // chunk) * chunk)
            if at >= src.size:
                return True
            try:
                return await core_transfer.stream_download(self.client, fid, src.size, path, at, ctl, notify, end_offset=end_offset)
            except FileReferenceExpired:
                if attempt == 2 or not await self._heal(src, fid):
                    raise
        return True

    async def _run_job(self, src: _Source, job: _Job) -> None:
        try:
            ok = await self._download(src, job.start, job.ctl, job.wake, src.path)
            if not ok:                                   # CDN file: the Rust core does not do those
                src.python_mode = True
        except asyncio.CancelledError:
            raise
        except BaseException as e:                       # noqa: BLE001 - reported to the readers
            job.error = e
            log.warning("Stream job for %s failed: %s", src.name, e)
        finally:
            job.finished = True
            job.idle_since = time.monotonic()
            job.wake()

    def _find_job(self, src: _Source, pos: int):
        """(job, ready): a job that already has ``pos`` on disk, or one that will reach it soon."""
        for j in src.jobs:
            if j.start <= pos < j.watermark():
                return j, True
        for j in src.jobs:
            if not j.finished and j.start <= pos < j.watermark() + self.jump:
                return j, False
        return None, False

    # ------------------------------------------------------------------ HTTP

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        async with self._conn_sem:
            self._writers.add(writer)
            self._tasks.add(asyncio.current_task())
            try:
                sock = writer.get_extra_info("socket")
                if sock is not None:
                    import socket
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass
            try:
                while True:
                    try:
                        head = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"), timeout=75)
                    except asyncio.LimitOverrunError:
                        await self._simple(writer, 431, "headers too large")
                        break
                    except (asyncio.IncompleteReadError, asyncio.TimeoutError, ConnectionError):
                        break
                    if not await self._request(reader, writer, head):
                        break
            except (ConnectionError, asyncio.CancelledError):
                pass
            except Exception:
                log.exception("stream connection error")
            finally:
                self._writers.discard(writer)
                self._tasks.discard(asyncio.current_task())
                try:
                    writer.close()
                except Exception:
                    pass

    async def _simple(self, writer, status: int, text: str, extra: Optional[dict] = None, head_only: bool = False) -> None:
        body = text.encode()
        hdr = {"Content-Type": "text/plain; charset=utf-8", "Content-Length": str(len(body)), "Connection": "keep-alive"}
        hdr.update(extra or {})
        writer.write(self._head(status, hdr) + (b"" if head_only else body))
        await writer.drain()

    def _head(self, status: int, headers: dict) -> bytes:
        lines = [f"HTTP/1.1 {status} {_REASONS.get(status, 'OK')}", "Server: cbgram-stream"]
        if self.cors:
            lines.append(f"Access-Control-Allow-Origin: {self.cors}")
            lines.append("Access-Control-Expose-Headers: Content-Length, Content-Range, Accept-Ranges")
        lines += [f"{k}: {v}" for k, v in headers.items()]
        return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1", "replace")

    async def _request(self, reader, writer, head: bytes) -> bool:
        """Handle one request. Returns True to keep the connection open."""
        try:
            lines = head.decode("latin-1").split("\r\n")
            method, target, version = lines[0].split(" ", 2)
            headers = {}
            for ln in lines[1:]:
                if ":" in ln:
                    k, v = ln.split(":", 1)
                    headers[k.strip().lower()] = v.strip()
        except Exception:
            await self._simple(writer, 400, "bad request")
            return False

        keep = version.upper() == "HTTP/1.1" and headers.get("connection", "").lower() != "close"

        if method == "OPTIONS":
            writer.write(self._head(204, {"Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
                                           "Access-Control-Allow-Headers": "Range, Content-Type",
                                           "Content-Length": "0"}))
            await writer.drain()
            return keep
        if method not in ("GET", "HEAD"):
            await self._simple(writer, 405, "method not allowed", {"Allow": "GET, HEAD, OPTIONS"})
            return keep

        url = urllib.parse.urlsplit(target)
        m = re.fullmatch(r"/s/([A-Za-z0-9_\-]+)(?:/.*)?", url.path)
        entry = self._tokens.get(m.group(1)) if m else None
        if entry is None or entry[1] < time.monotonic():
            await self._simple(writer, 404, "not found", head_only=method == "HEAD")
            return keep
        src, _exp, as_download = entry
        as_download = as_download or "dl=1" in url.query
        src.last_used = time.monotonic()
        size = src.size

        # ---- Range
        start, end, status = 0, size - 1, 200
        rng = headers.get("range")
        if rng:
            rm = _RANGE_RE.match(rng.strip().lower())
            if rm and (rm.group(1) or rm.group(2)):
                a, b = rm.group(1), rm.group(2)
                if a == "":                                   # suffix: the last N bytes
                    start, end = max(0, size - int(b)), size - 1
                else:
                    start = int(a)
                    end = min(int(b), size - 1) if b else size - 1
                if start >= size or start > end:
                    await self._simple(writer, 416, "range not satisfiable", {"Content-Range": f"bytes */{size}"},
                                       head_only=method == "HEAD")
                    return keep
                status = 206
            # anything else (multi-range, junk): ignore it and send the whole file, as the RFC allows

        length = end - start + 1
        disp = "attachment" if as_download else "inline"
        ascii_name = re.sub(r'[^\x20-\x7e]|["\\]', "_", src.name)
        hdr = {
            "Content-Type": src.mime,
            "Content-Length": str(length),
            "Accept-Ranges": "bytes",
            "Content-Disposition": f"{disp}; filename=\"{ascii_name}\"; filename*=UTF-8''{urllib.parse.quote(src.name)}",
            "Cache-Control": "private, max-age=3600",
            "Connection": "keep-alive" if keep else "close",
        }
        if status == 206:
            hdr["Content-Range"] = f"bytes {start}-{end}/{size}"

        if method == "HEAD":
            writer.write(self._head(status, hdr))
            await writer.drain()
            return keep

        src.readers += 1
        try:
            if src.python_mode or not self.fast:
                return await self._body_python(reader, writer, src, start, end, status, hdr) and keep
            if self.mode == "memory":
                return await self._body_memory(reader, writer, src, start, end, status, hdr) and keep
            return await self._body_fast(reader, writer, src, start, end, status, hdr) and keep
        finally:
            src.readers -= 1
            src.last_used = time.monotonic()

    # ---- the fast path: Rust engine + sendfile

    async def _body_fast(self, reader, writer, src: _Source, start: int, end: int, status: int, hdr: dict) -> bool:
        loop = asyncio.get_running_loop()
        pos = start
        job: Optional[_Job] = None
        headers_sent = False
        last_progress = time.monotonic()
        last_wm = -1
        f = None
        try:
            while pos <= end:
                if reader.at_eof() or writer.transport.is_closing():
                    return False
                found, ready = self._find_job(src, pos)
                if found is None:
                    if src.python_mode:                          # a CDN file turned up: finish in Python
                        if headers_sent:
                            return await self._body_python(reader, writer, src, pos, end, None, hdr)
                        return await self._body_python(reader, writer, src, pos, end, status, hdr)
                    found, ready = self._start_job(src, pos), False
                if found is not job:
                    if job is not None:
                        job.readers -= 1
                        job.idle_since = time.monotonic()
                    job = found
                    job.readers += 1
                    last_progress = time.monotonic()
                    last_wm = -1

                ev = job.event                                    # grab before looking: no lost wake-ups
                wm = job.watermark()
                if wm <= pos:
                    if job.finished:
                        if job.error is not None and not headers_sent:
                            await self._simple(writer, 502, "upstream error")
                            return True
                        return False                              # failed mid-body: cut the connection so the client retries
                    job.ctl.consumed(pos)                         # lift the gate for a reader that jumped ahead
                    if wm != last_wm:
                        last_wm, last_progress = wm, time.monotonic()
                    elif time.monotonic() - last_progress > 90:
                        log.warning("Stream stalled at %d of %d", pos, src.size)
                        return False
                    try:
                        await asyncio.wait_for(ev.wait(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass
                    continue

                if not headers_sent:
                    writer.write(self._head(status, hdr))         # first byte is ready: now it is safe to answer
                    headers_sent = True
                if f is None:
                    f = open(src.path, "rb", buffering=0)
                n = min(wm - pos, end + 1 - pos, self.send_max)
                await self._send(loop, writer, f, src.fd, pos, n)
                pos += n
                job.consumed = max(job.consumed, pos)
                job.ctl.consumed(pos)
                last_progress = time.monotonic()
            return True
        except (ConnectionError, asyncio.IncompleteReadError):
            return False
        finally:
            if f is not None:
                f.close()
            if job is not None:
                job.readers -= 1
                job.idle_since = time.monotonic()

    async def _send(self, loop, writer, f, fd: int, pos: int, n: int) -> None:
        if self._sendfile_ok:
            try:
                await writer.drain()
                await loop.sendfile(writer.transport, f, pos, n, fallback=True)
                return
            except (NotImplementedError, AttributeError, TypeError):
                self._sendfile_ok = False                         # e.g. uvloop: use pread + write below
        sent = 0
        while sent < n:
            part = await loop.run_in_executor(None, os.pread, fd, min(n - sent, MIB), pos + sent)
            if not part:
                raise ConnectionError("cache file shorter than expected")
            writer.write(part)
            await writer.drain()
            sent += len(part)

    # ---- the memory path: Rust engine -> bounded RAM buffer -> socket, no file

    def _effective_mem_window(self) -> int:
        """Scale per-stream RAM buffer down during rush hour so 20+ streams stay safe in 1 GB RAM."""
        active = len(self._mem_ctls)
        if active >= 16:
            scale = 0.25
        elif active >= 8:
            scale = 0.5
        else:
            scale = 1.0
        return int(max(2 * MIB, self.mem_window * scale))

    async def _body_memory(self, reader, writer, src: _Source, start: int, end: int, status: int, hdr: dict) -> bool:
        import cbcrypto

        chunk = core_transfer.stream_chunk()
        first = (start // chunk) * chunk
        ctl = cbcrypto.StreamCtl(self._effective_mem_window(), True)
        self._mem_ctls.add(ctl)
        box = {"ev": asyncio.Event()}

        def wake() -> None:
            ev, box["ev"] = box["ev"], asyncio.Event()
            ev.set()

        task = asyncio.create_task(self._download(src, first, ctl, wake, None, end_offset=end))
        skip = start - first
        remaining = end - start + 1
        headers_sent = False
        last_progress = time.monotonic()
        try:
            while remaining > 0:
                if reader.at_eof() or writer.transport.is_closing():
                    return False
                ev = box["ev"]                                   # grab before looking: no lost wake-ups
                finished = task.done()
                parts = ctl.pop_ready(8)                         # taking chunks is what lets the engine fetch more
                if not parts:
                    if finished:
                        err = None if task.cancelled() else task.exception()
                        if err is not None and not headers_sent:
                            log.warning("Stream of %s failed: %s", src.name, err)
                            await self._simple(writer, 502, "upstream error")
                            return True
                        return False                             # cut: the client retries with a Range request
                    if time.monotonic() - last_progress > 90:
                        log.warning("Stream stalled at %d of %d", start + (end - start + 1 - remaining), src.size)
                        return False
                    try:
                        await asyncio.wait_for(ev.wait(), timeout=0.5)
                    except asyncio.TimeoutError:
                        pass
                    continue
                last_progress = time.monotonic()
                if not headers_sent:
                    writer.write(self._head(status, hdr))
                    headers_sent = True
                for part in parts:
                    if skip:
                        part, skip = part[skip:], 0
                    if len(part) > remaining:
                        part = part[:remaining]
                    if part:
                        writer.write(part)
                        remaining -= len(part)
                await writer.drain()                             # a slow player holds the engine back right here
            return True
        except (ConnectionError, asyncio.IncompleteReadError):
            return False
        finally:
            ctl.cancel()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._mem_ctls.discard(ctl)

    # ---- the slow path: pure Python get_file (no cbcrypto 0.6, proxy, CDN files)

    async def _body_python(self, reader, writer, src: _Source, start: int, end: int, status: Optional[int], hdr: dict) -> bool:
        chunk = MIB
        first = start // chunk
        skip = start - first * chunk
        remaining = end - start + 1
        if status is not None:
            writer.write(self._head(status, hdr))
        gen = self.client.get_file(src.file_id, src.size, 0, first)
        try:
            async for data in gen:
                if skip:
                    data, skip = data[skip:], 0
                data = data[:remaining]
                writer.write(data)
                await writer.drain()
                remaining -= len(data)
                if remaining <= 0:
                    break
            return remaining <= 0
        except (ConnectionError, asyncio.IncompleteReadError):
            return False
        finally:
            await gen.aclose()
