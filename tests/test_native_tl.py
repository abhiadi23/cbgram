"""Differential tests: cbcrypto's native TL codec must be indistinguishable from
the generated pure-Python classes (bytes out, objects in)."""

import gzip
import random
import struct
from io import BytesIO

import pytest

cbcrypto = pytest.importorskip("cbcrypto")
if not hasattr(cbcrypto, "tl_parse"):
    pytest.skip("cbcrypto too old (no native TL codec)", allow_module_level=True)

from pyrogram.raw.core import tl_native
from pyrogram.raw.core.tl_object import TLObject, objects
from pyrogram.raw.core import Message, MsgContainer, FutureSalts, FutureSalt, List
from pyrogram.raw.tl_spec import SPEC
from pyrogram import raw

assert tl_native.init()

SPECS = {}
for line in SPEC.strip().splitlines():
    cid, path, fields = line.split("|", 2)
    fs = [tuple(f.split(",")) for f in fields.split(";")] if fields else []
    SPECS[int(cid, 16)] = (path, fs)

TYPE_IDS = [c for c, (p, _) in SPECS.items() if p.startswith("pyrogram.raw.types.")]
LEAF_IDS = [c for c in TYPE_IDS if all(f[1] not in ("O", "W") for f in SPECS[c][1])]


def rnd_core(rng, code):
    if code == "i":
        return rng.randint(-2**31, 2**31 - 1)
    if code == "q":
        return rng.randint(-2**63, 2**63 - 1)
    if code == "I":
        return rng.randint(-2**127, 2**127 - 1)
    if code == "J":
        return rng.randint(-2**255, 2**255 - 1)
    if code == "d":
        return rng.uniform(-1e12, 1e12)
    if code == "b":
        return rng.randbytes(rng.choice([0, 1, 2, 3, 4, 100, 253, 254, 255, 256, 1000]))
    if code == "s":
        return "".join(rng.choice("aé中😀 z\n") for _ in range(rng.choice([0, 1, 5, 80, 300])))
    if code == "B":
        return rng.random() < 0.5
    raise AssertionError(code)


def rnd_obj(rng, depth):
    pool = LEAF_IDS if depth <= 0 else TYPE_IDS
    cid = rng.choice(pool)
    return build(rng, cid, depth - 1)


def build(rng, cid, depth):
    path, fields = SPECS[cid]
    cls = objects[cid]
    kw = {}
    on = {}  # layers let several fields share one flag bit: they are present together
    for name, code, slot, bit in fields:
        if code == "#":
            continue
        flagged = slot != "-"
        if flagged and (slot, bit) not in on:
            on[(slot, bit)] = rng.random() < 0.6
        present = (not flagged) or on[(slot, bit)]
        if code == "t":
            kw[name] = present
            continue
        if not present:
            kw[name] = [] if code[0] in "VW" else None
            continue
        if code == "O":
            kw[name] = rnd_obj(rng, depth)
        elif code == "W":
            kw[name] = [rnd_obj(rng, depth) for _ in range(rng.randint(1, 3))]
        elif code[0] == "V":
            kw[name] = [rnd_core(rng, code[1]) for _ in range(rng.randint(1, 4))]
        else:
            kw[name] = rnd_core(rng, code)
    return cls(**kw)


@pytest.mark.parametrize("seed", range(3))
def test_all_constructors_roundtrip(seed):
    rng = random.Random(seed)
    for cid in SPECS:
        obj = build(rng, cid, 2)
        expected = obj.write()
        assert cbcrypto.tl_write(obj) == expected, hex(cid)
        py = TLObject.read(BytesIO(expected))
        nat = cbcrypto.tl_parse(expected)
        assert type(py) is type(nat)
        assert repr(py) == repr(nat), hex(cid)
        assert py == nat


def test_bool_vector_and_bare_results():
    assert cbcrypto.tl_parse(struct.pack("<I", 0x997275B5)) is True
    assert cbcrypto.tl_parse(struct.pack("<I", 0xBC799737)) is False
    # bare Vector<int> / Vector<long> as RpcResult payload
    for fmt, n in (("<i", 4), ("<q", 8)):
        raw_vec = struct.pack("<II", 0x1CB5C415, 3) + b"".join(struct.pack(fmt, v) for v in (7, -2, 99))
        assert cbcrypto.tl_parse(raw_vec) == TLObject.read(BytesIO(raw_vec)) == [7, -2, 99]


def test_container_gzip_and_unknown():
    ping = raw.types.Pong(msg_id=5, ping_id=9)
    inner = ping.write()
    good = struct.pack("<qii", 101, 1, len(inner)) + inner
    junk_body = struct.pack("<I", 0xDEADBEEF) + b"\0" * 8
    junk = struct.pack("<qii", 103, 1, len(junk_body)) + junk_body
    cont = struct.pack("<III", 0x73F1F8DC, 3, 0)[:8] + struct.pack("<i", 3) + good + junk + good
    cont = struct.pack("<Ii", 0x73F1F8DC, 3) + good + junk + good

    py = TLObject.read(BytesIO(cont))
    nat = cbcrypto.tl_parse(cont)
    assert isinstance(nat, MsgContainer)
    assert [m.msg_id for m in nat.messages] == [m.msg_id for m in py.messages] == [101, 101]
    assert repr(nat) == repr(py)

    packed = struct.pack("<I", 0x3072CFA1) + raw.core.primitives.Bytes(gzip.compress(inner))
    assert repr(cbcrypto.tl_parse(packed)) == repr(TLObject.read(BytesIO(packed))) == repr(ping)


def test_unknown_constructor_raises_keyerror():
    with pytest.raises(KeyError) as e:
        cbcrypto.tl_parse(struct.pack("<I", 0xDEADBEEF))
    assert e.value.args[0] == 0xDEADBEEF


def test_future_salts():
    salts = FutureSalts(req_msg_id=11, now=22, salts=[FutureSalt(1, 2, 3), FutureSalt(4, 5, 6)])
    data = salts.write()
    assert repr(cbcrypto.tl_parse(data)) == repr(TLObject.read(BytesIO(data)))


def test_write_subclass_override_is_respected():
    class Custom(raw.types.InputPeerSelf):
        def write(self, *a):
            return b"CUSTOM!!"

    q = raw.functions.messages.SendMessage(peer=Custom(), message="x", random_id=1)
    assert b"CUSTOM!!" in cbcrypto.tl_write(q)
    assert cbcrypto.tl_write(q) == q.write()


def test_pq_factorization():
    rng = random.Random(1)
    primes = [2147483647, 2147483629, 2147483587, 1000000007, 4294967291, 3037000493, 3000000019]
    for _ in range(300):
        p, q = sorted(rng.sample(primes, 2))
        assert cbcrypto.factorize_pq(p * q) == (p, q)
