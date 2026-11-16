import pyrogram.methods.rate_limiter as rate_limiter
from pyrogram.methods.rate_limiter import AdaptiveBucket


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


def make(monkeypatch, **kwargs):
    clock = Clock()
    monkeypatch.setattr(rate_limiter.time, "monotonic", clock)
    return AdaptiveBucket(rate=16, ceiling=100, burst=8, **kwargs), clock


def test_a_flood_lowers_the_rate_once_per_second(monkeypatch):
    bucket, clock = make(monkeypatch)

    for _ in range(8):
        bucket.on_flood()

    assert bucket.rate == 16 * 0.85

    clock.now += 1.0
    bucket.on_flood()

    assert bucket.rate == 16 * 0.85 * 0.85


def test_the_rate_never_drops_below_the_floor(monkeypatch):
    bucket, clock = make(monkeypatch, floor=2.0)

    for _ in range(100):
        clock.now += 1.0
        bucket.on_flood()

    assert bucket.rate == 2.0


def test_clean_seconds_raise_the_rate_up_to_the_ceiling(monkeypatch):
    bucket, clock = make(monkeypatch, step=0.5)

    for _ in range(8):
        bucket.on_success()

    assert bucket.rate == 16

    clock.now += 1.0
    bucket.on_success()

    assert bucket.rate == 16.5

    for _ in range(1000):
        clock.now += 1.0
        bucket.on_success()

    assert bucket.rate == 100


def test_no_raise_right_after_a_flood(monkeypatch):
    bucket, clock = make(monkeypatch)

    clock.now += 1.0
    bucket.on_flood()
    clock.now += 1.5
    bucket.on_success()

    assert bucket.rate == 16 * 0.85

    clock.now += 0.5
    bucket.on_success()

    assert bucket.rate == 16 * 0.85 + 0.5


# ---- slow start (growth > 1) -------------------------------------------------

def test_slow_start_is_off_by_default(monkeypatch):
    bucket, clock = make(monkeypatch)

    assert not bucket.in_slow_start
    clock.now += 1.0
    bucket.on_success()

    assert bucket.rate == 16.5  # old additive behaviour, unchanged


def test_slow_start_multiplies_until_the_cap_then_goes_additive(monkeypatch):
    bucket, clock = make(monkeypatch, growth=2.0, slow_start_cap=64, step=0.5)

    assert bucket.in_slow_start
    rates = []
    for _ in range(4):
        clock.now += AdaptiveBucket.SLOW_START_INTERVAL
        bucket.on_success()
        rates.append(bucket.rate)

    # 0.25 s apart is too soon for the 1 s additive step, so it holds at the cap
    assert rates == [32, 64, 64, 64]
    assert not bucket.in_slow_start

    clock.now += 1.0
    bucket.on_success()
    assert bucket.rate == 64.5


def test_slow_start_is_rate_limited_to_one_step_per_interval(monkeypatch):
    bucket, clock = make(monkeypatch, growth=2.0, slow_start_cap=100)

    for _ in range(50):
        bucket.on_success()  # no time passes

    assert bucket.rate == 16


def test_a_flood_during_slow_start_halves_the_rate_and_ends_it(monkeypatch):
    bucket, clock = make(monkeypatch, growth=2.0, slow_start_cap=100)
    clock.now += AdaptiveBucket.SLOW_START_INTERVAL
    bucket.on_success()  # 32
    clock.now += 2.0

    bucket.on_flood()

    assert bucket.rate == 16
    assert not bucket.in_slow_start


def test_slow_start_never_exceeds_the_ceiling(monkeypatch):
    bucket, clock = make(monkeypatch, growth=10.0, slow_start_cap=10_000)

    for _ in range(20):
        clock.now += AdaptiveBucket.SLOW_START_INTERVAL
        bucket.on_success()

    assert bucket.rate <= 100
