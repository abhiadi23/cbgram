#  cbgram - event-loop stall watchdog
#
#  Everything in a Telegram client shares one asyncio loop: session reads, pings, acks, update
#  handlers, progress callbacks. One blocking call on it (time.sleep, a synchronous HTTP
#  request, a big sync file/database operation) freezes ALL of them. Telegram then closes the
#  connections that missed their pings, every session reconnects at the same instant, and the
#  clock bookkeeping gets thrown off. From the outside it looks like random slow transfers and
#  "Connection closed by the server" bursts; in the log, many lines carry the same timestamp
#  right after a long silent gap.
#
#  The guard runs a tiny heartbeat ON the loop and a daemon thread OFF it. When the heartbeat is
#  late it logs which line of which file the loop thread is stuck in, so the culprit is named
#  instead of guessed.
#
#  CBGRAM_LOOP_GUARD=0 disables it; CBGRAM_LOOP_STALL=<seconds> sets the threshold (default 1.0).

import asyncio
import logging
import os
import sys
import threading
import time
import traceback
import weakref

log = logging.getLogger(__name__)

_guards: "weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, LoopGuard]" = weakref.WeakKeyDictionary()


class LoopGuard:
    def __init__(self, loop: asyncio.AbstractEventLoop, threshold: float = 1.0, interval: float = 0.25):
        self.loop = loop
        self.threshold = threshold
        self.interval = interval
        self.worst = 0.0
        self.stalls = 0
        self._beat = time.monotonic()
        self._loop_thread = threading.get_ident()
        self._stalled_since = None  # monotonic time the current stall was first seen
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._watch, name="LoopGuard", daemon=True)

    def start(self) -> None:
        self._beat = time.monotonic()
        self._tick()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    # runs on the loop
    def _tick(self) -> None:
        if self._stop.is_set():
            return
        self._beat = time.monotonic()
        self.loop.call_later(self.interval, self._tick)

    # runs in the watchdog thread
    def _watch(self) -> None:
        while not self._stop.wait(self.interval):
            late = time.monotonic() - self._beat - self.interval

            if late >= self.threshold:
                if self._stalled_since is None:
                    self._stalled_since = time.monotonic() - late
                    self.stalls += 1
                    log.warning(
                        "Event loop blocked for %.1fs and still blocked - it is stuck in:\n%s",
                        late, self._where(),
                    )
            elif self._stalled_since is not None:
                lasted = time.monotonic() - self._stalled_since
                self.worst = max(self.worst, lasted)
                self._stalled_since = None
                log.warning("Event loop is responding again after a stall of about %.1fs", lasted)

    def _where(self) -> str:
        frame = sys._current_frames().get(self._loop_thread)
        if frame is None:
            return "  (loop thread not found)"
        return "".join(traceback.format_stack(frame, limit=8)).rstrip()


def ensure(loop: asyncio.AbstractEventLoop) -> "LoopGuard | None":
    """Start the guard for ``loop`` once; call from the loop's own thread."""
    if os.environ.get("CBGRAM_LOOP_GUARD", "1").strip().lower() in ("0", "false", "no", "off"):
        return None

    guard = _guards.get(loop)

    if guard is not None:
        return guard

    try:
        threshold = float(os.environ.get("CBGRAM_LOOP_STALL", 1.0))
    except ValueError:
        threshold = 1.0

    guard = LoopGuard(loop, threshold=threshold)
    guard.start()
    _guards[loop] = guard
    return guard
