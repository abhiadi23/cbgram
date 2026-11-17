import asyncio
import time
from collections import deque
from types import SimpleNamespace

import pytest

from pyrogram.errors import FloodWait
from pyrogram.flood import AdaptiveLimit, FloodController, bind_ticket, unbind_ticket


def q(name="messages.SendMessage", **kw):
    return SimpleNamespace(QUALNAME="functions." + name, **kw)


def run(coro):
    return asyncio.run(coro)


# ---------------------------------------------------------------- AdaptiveLimit

def test_limit_grows_slowly_and_halves_on_flood():
    lim = AdaptiveLimit(initial=8, maximum=12, grow_after=4)
    for _ in range(4 * 10):
        lim.on_success()
    assert lim.limit == 12  # capped
    lim.on_flood(100.0)
    assert lim.limit == 6
    lim.on_flood(100.5)  # same burst of floods: one cut only
    assert lim.limit == 6
    lim.on_flood(102.0)
    assert lim.limit == 3


def test_limit_serves_by_priority_then_fifo():
    async def main():
        lim = AdaptiveLimit(initial=1)
        await lim.acquire()
        order = []

        async def waiter(tag, prio):
            await lim.acquire(prio)
            order.append(tag)
            lim.release()

        tasks = [asyncio.create_task(waiter(t, p)) for t, p in
                 (("bulk1", 1), ("bulk2", 1), ("query", 0), ("bulk3", 1))]
        await asyncio.sleep(0)
        lim.release()
        await asyncio.gather(*tasks)
        return order

    assert run(main()) == ["query", "bulk1", "bulk2", "bulk3"]


def test_cancelled_waiter_does_not_leak_a_slot():
    async def main():
        lim = AdaptiveLimit(initial=1)
        await lim.acquire()
        t = asyncio.create_task(lim.acquire())
        await asyncio.sleep(0)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        lim.release()
        await asyncio.wait_for(lim.acquire(), 0.5)  # slot is reusable
        return lim.inflight

    assert run(main()) == 1


# ------------------------------------------------------------ shared cooldown

def test_concurrent_floods_open_exactly_one_cooldown():
    async def main():
        ctl = FloodController(jitter=0.0)
        await asyncio.gather(*[ctl.acquire(q(), "query") for _ in range(5)])
        t0 = time.monotonic()
        await asyncio.gather(*[ctl.flood("messages.SendMessage", 0.2) for _ in range(30)])
        return ctl, time.monotonic() - t0

    ctl, dt = run(main())
    assert ctl.stats()["floods"] == 1
    assert 0.18 < dt < 0.5  # all 30 waited ONE penalty, not 30 sequential ones


def test_calls_during_cooldown_fail_fast_without_network():
    async def main():
        ctl = FloodController()
        ctl.note_flood("messages.SendMessage", 30)
        with pytest.raises(FloodWait) as e:
            await ctl.acquire(q(), "message", sleep_threshold=10)
        return ctl, e.value

    ctl, err = run(main())
    assert 29 <= err.value <= 30
    assert ctl.stats()["failed_fast"] == 1


def test_short_cooldown_is_waited_out_not_raised():
    async def main():
        ctl = FloodController(jitter=0.0)
        ctl.note_flood("users.GetUsers", 0.15)
        t0 = time.monotonic()
        t = await ctl.acquire(q("users.GetUsers"), "query", sleep_threshold=10)
        t.ok()
        return time.monotonic() - t0

    assert 0.12 < run(main()) < 0.4


def test_slot_is_lent_back_while_a_call_sleeps_through_a_flood():
    async def main():
        ctl = FloodController(concurrency=1, jitter=0.0)
        t1 = await ctl.acquire(q("messages.SendMessage"), "query")
        tok = bind_ticket(t1)
        sleeper = asyncio.create_task(ctl.flood("messages.SendMessage", 0.3))
        await asyncio.sleep(0.05)
        # another method of the same category is not stuck behind the sleeper
        other = await asyncio.wait_for(ctl.acquire(q("messages.GetHistory"), "query"), 0.2)
        other.ok()
        await sleeper
        unbind_ticket(tok)
        t1.ok()
        return ctl.stats()["concurrency"]["query"]["inflight"]

    assert run(main()) == 0


# ----------------------------------------------------------------- learned rate

def test_flood_installs_a_rate_just_below_what_was_driven_and_it_recovers():
    async def main():
        ctl = FloodController(concurrency=64, max_concurrency=64)
        for _ in range(40):  # ~40 calls inside one second
            (await ctl.acquire(q(), "query")).ok()
            await asyncio.sleep(0.005)
        ctl.note_flood("messages.SendMessage", 0.01)
        await asyncio.sleep(0.05)  # let the penalty end
        m = ctl._methods["messages.SendMessage"]
        learned = m.rate
        for _ in range(40):  # calm period: rate probes upward
            m.last_adjust -= 3
            ctl._on_success(m)
        return learned, m.rate

    learned, later = run(main())
    assert 0.5 <= learned < 40
    assert later > learned or later == float("inf")


def test_sends_are_paced_per_chat():
    async def main():
        ctl = FloodController(per_peer_rate=20.0, per_peer_burst=2.0, send_rate=1000, send_burst=1000)
        peer = SimpleNamespace(user_id=7)
        t0 = time.monotonic()
        for _ in range(8):
            (await ctl.acquire(q(peer=peer), "message")).ok()
        one_chat = time.monotonic() - t0
        t0 = time.monotonic()
        for i in range(8):
            (await ctl.acquire(q(peer=SimpleNamespace(user_id=100 + i)), "message")).ok()
        many_chats = time.monotonic() - t0
        return one_chat, many_chats

    one, many = run(main())
    assert one > 0.25 and many < 0.1  # same chat is paced, different chats are not


def test_disabled_controller_is_a_noop():
    assert FloodController.from_config(False) is None
    ctl = FloodController(enabled=False)
    assert run(ctl.acquire(q(), "message")) is None


# ------------------------------------------------- naive vs controller, simulated

class FakeServer:
    """limit requests per second per method; a request that arrives while a penalty
    is running is refused AND extends the penalty (what escalation looks like)."""

    def __init__(self, per_sec=40, penalty=0.25, escalate=0.01):
        self.per_sec, self.penalty, self.escalate = per_sec, penalty, escalate
        self.stamps, self.until = deque(), 0.0
        self.requests = self.floods = 0

    def request(self):
        now = time.monotonic()
        self.requests += 1
        if now < self.until:
            self.until = min(self.until + self.escalate, now + 3.0)
            self.floods += 1
            return self.until - now
        while self.stamps and now - self.stamps[0] > 1.0:
            self.stamps.popleft()
        if len(self.stamps) >= self.per_sec:
            self.until = now + self.penalty
            self.floods += 1
            return self.penalty
        self.stamps.append(now)
        return None


async def _naive(server, n):
    async def call():
        while True:
            await asyncio.sleep(0.002)
            wait = server.request()
            if wait is None:
                return
            await asyncio.sleep(wait)  # what Session._invoke does today
    t0 = time.monotonic()
    try:  # naive retry can run away under escalation; bound it, report a lower bound
        await asyncio.wait_for(asyncio.gather(*[call() for _ in range(n)]), 6)
    except asyncio.TimeoutError:
        pass
    return time.monotonic() - t0


async def _controlled(server, n):
    ctl = FloodController(jitter=0.01)

    async def call():
        ticket = await ctl.acquire(q("channels.GetParticipants"), "query", 60)
        tok = bind_ticket(ticket)
        try:
            while True:
                await asyncio.sleep(0.002)
                wait = server.request()
                if wait is None:
                    ticket.ok()
                    return
                await ctl.flood("channels.GetParticipants", wait)
        finally:
            unbind_ticket(tok)

    t0 = time.monotonic()
    await asyncio.gather(*[call() for _ in range(n)])
    return time.monotonic() - t0, ctl


def test_controller_sends_far_fewer_refused_requests_than_naive_retry():
    n = 120
    naive_srv, ctl_srv = FakeServer(), FakeServer()
    naive_t = run(_naive(naive_srv, n))
    ctl_t, ctl = run(_controlled(ctl_srv, n))
    print(f"\nnaive:      {naive_t:5.2f}s  requests={naive_srv.requests:4d}  floods={naive_srv.floods}"
          f"\ncontroller: {ctl_t:5.2f}s  requests={ctl_srv.requests:4d}  floods={ctl_srv.floods}")
    assert ctl_srv.floods * 4 <= naive_srv.floods
    assert ctl_srv.requests < naive_srv.requests
