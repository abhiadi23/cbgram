#  cbgram - adaptive flood controller
#
#  Pyrogram's answer to FLOOD_WAIT is "sleep, then retry, per call". Under load that
#  means N concurrent callers each hit the limit, each sleep the full penalty, and
#  then all wake in the same instant and hit it again - and every request sent during
#  a penalty can make Telegram lengthen it.
#
#  FloodController sits in front of Client.invoke and does four things instead:
#
#   1. Shared cooldown (single-flight). One FLOOD_WAIT per method is paid once, by
#      everybody. Calls that arrive during the penalty do not touch the network:
#      they wait for it (or fail immediately when it is longer than sleep_threshold,
#      exactly as the real error would have).
#   2. Learned rate. When a flood hits, the rate the method was actually being
#      driven at is measured and a token bucket is installed just below it; it then
#      probes upward again while things stay calm and removes itself when the
#      limit is no longer binding. Nothing is throttled until Telegram objects.
#   3. Proactive pacing for sends: a per-chat bucket and a global bucket, sized to
#      Telegram's documented limits (~1 msg/s per chat, ~30 msg/s overall), because
#      the cheapest flood is the one that is never triggered.
#   4. AIMD concurrency. How many requests of one kind may be in flight starts at
#      8, creeps up while calls succeed and is halved on a flood. Waiting callers
#      are served by priority, so interactive queries are not stuck behind a
#      bulk send loop.
#
#  Disable with Client(flood_control=False) or CBGRAM_FLOOD=0.

import asyncio
import contextvars
import heapq
import itertools
import logging
import os
import random
import time
from collections import OrderedDict, deque
from typing import Any, Callable, Deque, Dict, Optional, Tuple

from pyrogram.errors import FloodWait, FloodPremiumWait

log = logging.getLogger(__name__)

_INF = float("inf")


def TokenBucket(rate: float, burst: Optional[float] = None):  # noqa: N802
    # imported lazily: pyrogram.methods pulls in the whole client, and this module
    # is imported by the session layer long before that exists
    from pyrogram.methods.rate_limiter import TokenBucket as _TokenBucket

    return _TokenBucket(rate, burst)

# the ticket of the call running in this task, so a cooldown can lend its
# concurrency slot back while it sleeps
_current_ticket: "contextvars.ContextVar[Optional[Ticket]]" = contextvars.ContextVar(
    "cbgram_flood_ticket", default=None
)

SEND_CATEGORIES = frozenset({"message", "media"})
LOW_PRIORITY = frozenset({"query", "admin"})  # served first: cheap and usually interactive


class AdaptiveLimit:
    """Concurrency limit with additive increase / multiplicative decrease.

    Waiters are admitted lowest priority-number first, FIFO within a priority.
    """

    def __init__(self, initial: int = 8, minimum: int = 1, maximum: int = 32, grow_after: int = 16):
        self.limit = float(initial)
        self.minimum = minimum
        self.maximum = maximum
        self._grow_after = grow_after
        self._ok = 0
        self._last_cut = 0.0
        self.inflight = 0
        self._waiters: list = []
        self._seq = itertools.count()

    async def acquire(self, priority: int = 1):
        if self.inflight < int(self.limit) and not self._waiters:
            self.inflight += 1
            return

        fut = asyncio.get_running_loop().create_future()
        heapq.heappush(self._waiters, (priority, next(self._seq), fut))

        try:
            await fut
        except asyncio.CancelledError:
            if fut.done() and not fut.cancelled():
                self.release()  # we were granted a slot we will never use
            raise

    def release(self):
        self.inflight -= 1
        self._wake()

    def _wake(self):
        while self._waiters and self.inflight < int(self.limit):
            _, _, fut = heapq.heappop(self._waiters)

            if fut.cancelled():
                continue

            self.inflight += 1
            fut.set_result(None)

    def on_success(self):
        self._ok += 1

        if self._ok >= self._grow_after and self.limit < self.maximum:
            self._ok = 0
            self.limit = min(self.maximum, self.limit + 1)
            self._wake()

    def on_flood(self, now: float):
        if now - self._last_cut < 1.0:  # one cut per burst of floods, not one per caller
            return

        self._last_cut = now
        self._ok = 0
        self.limit = max(float(self.minimum), self.limit * 0.5)

    @property
    def waiting(self) -> int:
        return len(self._waiters)


class _Method:
    __slots__ = ("name", "category", "cooldown_until", "bucket", "rate", "recent",
                 "floods", "last_adjust", "waited", "failed_fast")

    def __init__(self, name: str, category: str):
        self.name = name
        self.category = category
        self.cooldown_until = 0.0
        self.bucket = None
        self.rate = _INF  # inf: not throttled
        self.recent: Deque[float] = deque(maxlen=512)
        self.floods = 0
        self.last_adjust = 0.0
        self.waited = 0.0
        self.failed_fast = 0


class Ticket:
    """Admission for one call. Release it with ``ok()`` or ``fail(exc)``."""

    __slots__ = ("ctl", "method", "limiter", "held", "done")

    def __init__(self, ctl: "FloodController", method: _Method, limiter: AdaptiveLimit):
        self.ctl = ctl
        self.method = method
        self.limiter = limiter
        self.held = True
        self.done = False

    def ok(self):
        self._finish(None)

    def fail(self, exc: BaseException):
        self._finish(exc)

    def _finish(self, exc: Optional[BaseException]):
        if self.done:
            return

        self.done = True

        if self.held:
            self.held = False
            self.limiter.release()

        if exc is None:
            self.limiter.on_success()
            self.ctl._on_success(self.method)
        elif isinstance(exc, (FloodWait, FloodPremiumWait)):
            self.ctl.note_flood(self.method.name, exc.value)


class FloodController:
    def __init__(
        self,
        *,
        enabled: bool = True,
        concurrency: int = 8,
        max_concurrency: int = 32,
        per_peer_rate: Optional[float] = 1.0,
        per_peer_burst: float = 3.0,
        send_rate: float = 30.0,
        send_burst: float = 40.0,
        backoff: float = 0.6,
        floor_rate: float = 0.5,
        ceiling_rate: float = 25.0,
        jitter: float = 0.25,
        max_tracked_peers: int = 4096,
        clock: Callable[[], float] = time.monotonic,
        sleep: Optional[Callable[[float], Any]] = None,
    ):
        self.enabled = enabled
        self._concurrency = concurrency
        self._max_concurrency = max_concurrency
        self._per_peer = (per_peer_rate, per_peer_burst) if per_peer_rate else None
        self._backoff = backoff
        self._floor = floor_rate
        self._ceiling = ceiling_rate
        self._jitter = jitter
        self._clock = clock
        self._sleep = sleep  # None: use asyncio.sleep, looked up at call time (so patching it works)
        self._max_peers = max_tracked_peers

        self._methods: Dict[str, _Method] = {}
        self._limits: Dict[str, AdaptiveLimit] = {}
        self._peers: "OrderedDict[Any, Any]" = OrderedDict()
        self._send = TokenBucket(send_rate, send_burst)

        self.stats_floods = 0
        self.stats_shared_waits = 0
        self.stats_failed_fast = 0

    # ----------------------------------------------------------------- lookup

    @staticmethod
    def method_name(query) -> str:
        qual = getattr(query, "QUALNAME", None) or type(query).__name__
        return ".".join(qual.split(".")[1:]) or qual

    @staticmethod
    def peer_key(query) -> Optional[Tuple[str, Any]]:
        peer = getattr(query, "peer", None)

        if peer is None:
            peer = getattr(query, "to_peer", None)

        if peer is None:
            return None

        for attr in ("user_id", "chat_id", "channel_id"):
            value = getattr(peer, attr, None)

            if value is not None:
                return attr, value

        return type(peer).__name__, 0

    def _method(self, name: str, category: str) -> _Method:
        m = self._methods.get(name)

        if m is None:
            m = self._methods[name] = _Method(name, category)

        return m

    def _limit(self, category: str) -> AdaptiveLimit:
        lim = self._limits.get(category)

        if lim is None:
            lim = self._limits[category] = AdaptiveLimit(
                self._concurrency, 1, self._max_concurrency
            )

        return lim

    def _peer_bucket(self, key):
        b = self._peers.get(key)

        if b is None:
            b = self._peers[key] = TokenBucket(*self._per_peer)

            if len(self._peers) > self._max_peers:
                self._peers.popitem(last=False)
        else:
            self._peers.move_to_end(key)

        return b

    # ------------------------------------------------------------- admission

    async def acquire(
        self,
        query,
        category: str,
        sleep_threshold: float = -1,
        priority: Optional[int] = None,
    ) -> Optional[Ticket]:
        """Wait until ``query`` may be sent; returns the Ticket to release afterwards."""
        if not self.enabled:
            return None

        name = self.method_name(query)
        m = self._method(name, category)
        limiter = self._limit(m.category)
        prio = priority if priority is not None else (0 if category in LOW_PRIORITY else 1)

        while True:
            await self._wait_cooldown(m, sleep_threshold)

            # buckets first, slot last: waiting on a rate must not hog a slot
            if m.bucket is not None:
                await m.bucket.acquire()

            if category in SEND_CATEGORIES:
                key = self.peer_key(query)

                if key is not None and self._per_peer is not None:
                    await self._peer_bucket(key).acquire()

                await self._send.acquire()

            await limiter.acquire(prio)

            # a flood may have started while this call was queued; do not spend the
            # slot on a request that is certain to be refused
            if m.cooldown_until - self._clock() > 0:
                limiter.release()
                continue

            m.recent.append(self._clock())
            return Ticket(self, m, limiter)

    async def _nap(self, seconds: float):
        await (self._sleep or asyncio.sleep)(seconds)

    async def _wait_cooldown(self, m: _Method, sleep_threshold: float, nap=None):
        # bounded: one pass normally, a few if the cooldown keeps being extended by new floods
        for _ in range(64):
            left = m.cooldown_until - self._clock()

            if left <= 0:
                return

            if 0 <= sleep_threshold < left:
                # same outcome the server's answer would have had, minus the request
                m.failed_fast += 1
                self.stats_failed_fast += 1
                raise FloodWait(value=int(left + 0.999), rpc_name=m.name)

            self.stats_shared_waits += 1
            m.waited += left
            await (nap or self._nap)(left + random.uniform(0, self._jitter))

    # -------------------------------------------------------------- feedback

    def note_flood(self, name: str, seconds: float) -> bool:
        """Record a FLOOD_WAIT. Returns True if this opened a new cooldown (as opposed
        to joining one already running)."""
        m = self._methods.get(name)

        if m is None:
            m = self._method(name, "query")

        now = self._clock()
        until = now + float(seconds)
        fresh = m.cooldown_until <= now

        m.cooldown_until = max(m.cooldown_until, until)

        if not fresh:
            return False

        m.floods += 1
        self.stats_floods += 1
        self._learn_rate(m, now)
        self._limit(m.category).on_flood(now)
        log.debug("flood on %s: %ss; rate now %s/s", name, seconds, m.rate if m.rate != _INF else "unlimited")
        return True

    def _learn_rate(self, m: _Method, now: float):
        window = [t for t in m.recent if now - t <= 10.0]

        if len(window) >= 2:
            span = max(now - window[0], 1.0)
            observed = len(window) / span
            target = max(self._floor, observed * self._backoff)
            m.rate = min(m.rate, target) if m.rate != _INF else target
        elif m.rate != _INF:
            m.rate = max(self._floor, m.rate * self._backoff)

        if m.rate != _INF:
            if m.bucket is None:
                m.bucket = TokenBucket(m.rate, max(1.0, m.rate))
            else:
                m.bucket.rate = m.rate
                m.bucket._burst = max(1.0, m.rate)

            # nobody may spend a burst the moment the penalty ends
            m.bucket._tokens = 0.0

        m.last_adjust = now

    def _on_success(self, m: _Method):
        if m.rate == _INF:
            return

        now = self._clock()

        if now - m.last_adjust >= 2.0 and now >= m.cooldown_until:
            m.last_adjust = now
            m.rate *= 1.15

            if m.rate >= self._ceiling:
                m.rate = _INF
                m.bucket = None
            elif m.bucket is not None:
                m.bucket.rate = m.rate
                m.bucket._burst = max(1.0, m.rate)

    async def wait(self, query, sleep_threshold: float = -1, sleep=None):
        """Wait out a running cooldown for ``query``'s method, if there is one. For callers that
        send without going through :meth:`acquire` (e.g. upload parts)."""
        if not self.enabled:
            return

        m = self._methods.get(self.method_name(query))

        if m is not None:
            await self._wait_cooldown(m, sleep_threshold, sleep)

    async def flood(self, name: str, seconds: float, sleep=None):
        """Called by the session in place of ``asyncio.sleep`` when a call is told to
        wait. Every concurrent caller shares one deadline; the one that opened it
        also lowers the rate, and each hands its concurrency slot back meanwhile."""
        opened = self.note_flood(name, seconds)
        m = self._methods[name]

        ticket = _current_ticket.get()
        lent = ticket is not None and ticket.held and not ticket.done

        if lent:
            ticket.held = False
            ticket.limiter.release()

        try:
            nap = sleep or self._nap

            if opened:
                # the call that was told to wait sleeps exactly what the server asked ...
                await nap(seconds)

            # ... everyone else (and anything the cooldown was extended by) joins the shared deadline
            await self._wait_cooldown(m, -1, nap)
        finally:
            if lent and not ticket.done:
                await ticket.limiter.acquire(1)
                ticket.held = True

    # ------------------------------------------------------------ inspection

    def stats(self) -> Dict[str, Any]:
        return {
            "floods": self.stats_floods,
            "shared_waits": self.stats_shared_waits,
            "failed_fast": self.stats_failed_fast,
            "methods": {
                n: {
                    "floods": m.floods,
                    "rate": None if m.rate == _INF else round(m.rate, 2),
                    "waited": round(m.waited, 2),
                    "failed_fast": m.failed_fast,
                }
                for n, m in self._methods.items() if m.floods or m.failed_fast
            },
            "concurrency": {
                c: {"limit": int(l.limit), "inflight": l.inflight, "waiting": l.waiting}
                for c, l in self._limits.items()
            },
        }

    @classmethod
    def from_config(cls, value: Any) -> Optional["FloodController"]:
        """Client(flood_control=...): None -> environment default, bool, dict of
        options, or a ready-made controller."""
        if isinstance(value, FloodController):
            return value

        if value is None:
            value = os.environ.get("CBGRAM_FLOOD", "1") != "0"

        if value is False:
            return None

        return cls(**(value if isinstance(value, dict) else {}))


def bind_ticket(ticket: Optional[Ticket]):
    """Mark ``ticket`` as the running call's (see FloodController.flood)."""
    return _current_ticket.set(ticket)


def unbind_ticket(token):
    _current_ticket.reset(token)
