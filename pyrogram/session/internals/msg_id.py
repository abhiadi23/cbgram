#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

import asyncio
import logging
import os
import sys
import threading
import time
import traceback

log = logging.getLogger(__name__)


class LoopWatchdog:
    """Notices when the asyncio event loop is blocked, from outside the loop.

    A blocking call inside a coroutine (time.sleep, a synchronous HTTP request, ...)
    freezes every session in the process at once: pings stop, Telegram drops the
    connections, and packets that arrived during the freeze are only read once it
    ends. Their msg_ids are then minutes older than "now", which looks exactly like
    a wrong system clock. Two things follow from knowing about the stall:

      * clock samples taken in (and just after) a stall are not trusted;
      * the log says where the loop is stuck, instead of leaving a silent gap.

    A daemon thread keeps running while the loop is blocked, so it can see it.
    """

    def __init__(self, warn_after: float = None, grace: float = 15.0, beat: float = 0.5):
        self.warn_after = float(os.environ.get("CBGRAM_STALL_WARN", 5.0)) if warn_after is None else warn_after
        self.grace = grace
        self.beat = beat
        self.loop = None
        self.loop_thread = None
        self._last_beat = time.monotonic()
        self._stalled_since = None
        self._reported = False
        self._guard_until = 0.0
        self._thread = None
        self._lock = threading.Lock()

    def ensure_started(self, loop: asyncio.AbstractEventLoop = None):
        """Start watching ``loop`` (the running one by default). Safe to call repeatedly."""
        loop = loop or asyncio.get_running_loop()

        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self.loop is loop:
                return

            self.loop = loop
            self.loop_thread = threading.get_ident()
            self._last_beat = time.monotonic()
            self._stalled_since = None
            self._reported = False
            self._thread = threading.Thread(target=self._run, args=(loop,), name="cbgram-loop-watchdog", daemon=True)
            self._thread.start()

    def _beat_in_loop(self):
        now = time.monotonic()

        if self._stalled_since is not None:
            blocked = now - self._stalled_since

            if blocked >= self.warn_after or self._reported:
                log.warning("Event loop was blocked for %.1fs and is responsive again", blocked)

            self._stalled_since = None
            self._reported = False
            # packets that queued up during the stall are processed from here on:
            # keep distrusting clock samples for a while
            self._guard_until = now + self.grace

        self._last_beat = now

    def _release(self):
        """The watched loop is gone: nothing is stalled and no clock sample should be distrusted."""
        self._guard_until = 0.0
        self._stalled_since = None
        self._reported = False

    def _run(self, loop):
        try:
            self._watch(loop)
        finally:
            self._release()

    def _watch(self, loop):
        while not loop.is_closed():
            try:
                loop.call_soon_threadsafe(self._beat_in_loop)
            except RuntimeError:  # loop closed
                return

            time.sleep(self.beat)

            if not loop.is_running():
                # a loop that is not running (stopped, or between run_until_complete calls) is idle, not blocked
                self._last_beat = time.monotonic()
                self._release()
                continue

            idle = time.monotonic() - self._last_beat

            if idle > self.warn_after:
                if self._stalled_since is None:
                    self._stalled_since = self._last_beat

                self._guard_until = float("inf")  # until the loop answers again

                if not self._reported:
                    self._reported = True
                    log.warning(
                        "Event loop has been blocked for %.1fs - every Telegram session in this process is "
                        "frozen. Something is running synchronously (time.sleep, a blocking HTTP call, heavy "
                        "CPU work) inside a coroutine. It is currently here:\n%s",
                        idle, self.stack(),
                    )

    def stack(self, limit: int = 14) -> str:
        frame = sys._current_frames().get(self.loop_thread)

        if frame is None:
            return "  (stack unavailable)"

        return "".join(traceback.format_stack(frame)[-limit:]).rstrip()

    def stalled_recently(self) -> bool:
        """True while the loop is blocked and for ``grace`` seconds after it recovers."""
        return time.monotonic() < self._guard_until


watchdog = LoopWatchdog()


class _MsgIdGenerator:
    _lock = threading.Lock()
    _last_msg_id = 0
    _base_wall = time.time()
    _base_mono = time.monotonic()
    time_offset = 0.0

    @classmethod
    def now(cls) -> float:
        return cls._base_wall + (time.monotonic() - cls._base_mono) + cls.time_offset

    @classmethod
    def sync(cls, server_msg_id: int, rejected: bool = False) -> bool:
        """Adopt the server's clock from one of its msg_ids. Returns False if the sample was ignored.

        ``rejected`` means the server itself refused one of our msg_ids as too old/new (bad_msg_notification
        16/17), which is authoritative. Any other sample is only trusted if it was read promptly: a packet
        that sat in the socket while the event loop was blocked is minutes old and says nothing about the clock.
        """
        wall = time.time()
        offset = (server_msg_id >> 32) - wall

        if not rejected and watchdog.stalled_recently() and abs(offset - cls.time_offset) > 5:
            log.debug(
                "Ignoring a %.1fs clock sample taken around an event-loop stall (offset stays %.1fs)",
                offset, cls.time_offset,
            )
            return False

        with cls._lock:
            cls._base_wall = wall
            cls._base_mono = time.monotonic()
            cls.time_offset = offset

            if rejected:
                cls._last_msg_id = 0

        if abs(cls.time_offset) > 30:
            log.info("System clock is off, time offset set to %.1fs", cls.time_offset)

        return True

    def __call__(self) -> int:
        now = self.now()

        with _MsgIdGenerator._lock:
            msg_id = int(now * 2 ** 32) & ~3

            if msg_id <= _MsgIdGenerator._last_msg_id:
                msg_id = _MsgIdGenerator._last_msg_id + 4

            _MsgIdGenerator._last_msg_id = msg_id
            return msg_id


MsgId = _MsgIdGenerator()
