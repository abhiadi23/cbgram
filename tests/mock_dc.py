"""A small fake Telegram data center for testing the Rust transfer core.

It speaks the real wire protocol (obfuscated abridged transport + MTProto 2.0 encryption)
and parses the client's requests with cbgram's own generated TL classes, so it checks the
Rust code's request encoding independently of the Rust code itself. It can also misbehave
on demand: wrong salt, flood waits, dropped connections, containers, gzip, CDN redirects,
clock skew, short chunks.
"""

import asyncio
import gzip
import hashlib
import os
import random
import struct
import threading
import time
from io import BytesIO

import cbcrypto as cb
from pyrogram import raw
from pyrogram.raw.core import TLObject

GZIP_ID = 0x3072CFA1
CONTAINER_ID = 0x73F1F8DC


def tl_bytes(b: bytes) -> bytes:
    n = len(b)
    head = bytes([n]) if n < 254 else b"\xfe" + n.to_bytes(3, "little")
    out = head + b
    return out + b"\x00" * (-len(out) % 4)


def gzip_packed(inner: bytes) -> bytes:
    return struct.pack("<I", GZIP_ID) + tl_bytes(gzip.compress(inner))


def container(msgs) -> bytes:
    """msgs: list of (msg_id, seqno, body)"""
    out = struct.pack("<Ii", CONTAINER_ID, len(msgs))
    for mid, sq, body in msgs:
        out += struct.pack("<qii", mid, sq, len(body)) + body
    return out


class MockDC:
    def __init__(self, auth_key: bytes, files=None, salt: int = 0x1122334455667788, **opts):
        self.ak = auth_key
        self.akid = hashlib.sha1(auth_key).digest()[-8:]
        self.files = files or {}          # location id -> bytes
        self.salt = salt
        self.parts = {}                   # file_id -> {part: bytes}
        self.part_meta = {}               # file_id -> (is_big, total_parts)
        self.opts = dict(
            variant="plain",              # plain | gzip_result | container | gzip_msg | mixed
            flood_offsets=set(),          # offsets whose FIRST request gets FLOOD_WAIT
            flood_secs=1,
            drop_after=None,              # drop the first connection after N responses
            cdn=False, migrate=False, short=False,
            skew=0,                       # server clock minus real clock (seconds)
            require_init=True,
            error=None,                   # (code, message) for every file request
            server_error_offsets=set(),   # offsets/parts whose FIRST request gets 500 INTERNAL
        )
        self.opts.update(opts)
        self.log = []
        self.conns = 0
        self.acks = 0
        self.requests = 0
        self._flooded = set()
        self._dropped = False
        self._inited = set()
        self._lock = threading.Lock()
        self.loop = None
        self.port = None
        self._ready = threading.Event()

    # ---------------------------------------------------------------- lifecycle
    def start(self):
        t = threading.Thread(target=self._run, daemon=True)
        t.start()
        assert self._ready.wait(10)
        return "127.0.0.1", self.port

    def _run(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        async def main():
            srv = await asyncio.start_server(self._handle, "127.0.0.1", 0, limit=1 << 22)
            self.port = srv.sockets[0].getsockname()[1]
            self._ready.set()
            await srv.serve_forever()

        try:
            self.loop.run_until_complete(main())
        except Exception:
            pass

    def stop(self):
        if self.loop:
            self.loop.call_soon_threadsafe(self.loop.stop)

    # ---------------------------------------------------------------- one connection
    async def _handle(self, r, w):
        with self._lock:
            self.conns += 1
            conn_no = self.conns
        try:
            init = await r.readexactly(64)
        except Exception:
            return
        key, iv, st = init[8:40], bytearray(init[40:56]), bytearray(1)
        dec = lambda b: cb.ctr256_decrypt(bytes(b), key, iv, st)
        assert dec(init)[56:60] == b"\xef" * 4, "bad obfuscated init"
        rev = init[8:56][::-1]
        ekey, eiv, est = rev[:32], bytearray(rev[32:48]), bytearray(1)
        enc = lambda b: cb.ctr256_encrypt(bytes(b), ekey, eiv, est)

        state = {"ctr": 0, "seq": 0, "sent": 0}

        def pack(body: bytes, sid: bytes, content=True) -> bytes:
            server_time = int(time.time()) + self.opts["skew"]
            state["ctr"] += 1
            msg_id = (server_time << 32) | (state["ctr"] * 4 + 1)
            seq = state["seq"] * 2 + (1 if content else 0)
            if content:
                state["seq"] += 1
            plain = struct.pack("<q8sqIi", self.salt, sid, msg_id, seq, len(body)) + body
            plain += os.urandom(12 + (-(len(plain) + 12)) % 16)
            mk = hashlib.sha256(self.ak[96:128] + plain).digest()[8:24]
            k, v = cb.kdf(self.ak, mk, False)
            pkt = self.akid + mk + cb.ige256_encrypt(plain, k, v)
            words = len(pkt) // 4
            framed = (bytes([words]) if words <= 126 else b"\x7f" + words.to_bytes(3, "little")) + pkt
            return msg_id, seq, enc(framed)

        async def send(body, sid):
            _, _, wire = pack(body, sid)
            w.write(wire)
            await w.drain()
            state["sent"] += 1

        try:
            while True:
                first = dec(await r.readexactly(1))[0]
                n = first if first != 0x7F else int.from_bytes(dec(await r.readexactly(3)), "little")
                pkt = dec(await r.readexactly(n * 4))
                assert pkt[:8] == self.akid
                k, v = cb.kdf(self.ak, pkt[8:24], True)
                pt = cb.ige256_decrypt(pkt[24:], k, v)
                salt, sid, msg_id, seqno, length = struct.unpack("<q8sqIi", pt[:32])
                body = pt[32 : 32 + length]
                self.requests += 1

                if struct.unpack("<I", body[:4])[0] == raw.types.MsgsAck.ID:
                    self.acks += 1
                    continue

                # clock check, like the real server
                srv_t = int(time.time()) + self.opts["skew"]
                if abs((msg_id >> 32) - srv_t) > 30:
                    code = 16 if (msg_id >> 32) < srv_t else 17
                    self.log.append(("bad_msg_time", code))
                    n_ = raw.types.BadMsgNotification(bad_msg_id=msg_id, bad_msg_seqno=seqno, error_code=code)
                    await send(n_.write(), sid)
                    continue

                if salt != self.salt:
                    self.log.append(("bad_salt",))
                    n_ = raw.types.BadServerSalt(
                        bad_msg_id=msg_id, bad_msg_seqno=seqno, error_code=48, new_server_salt=self.salt
                    )
                    await send(n_.write(), sid)
                    continue

                obj = TLObject.read(BytesIO(body))
                if isinstance(obj, raw.functions.InvokeWithLayer):
                    self._inited.add(sid)
                    self.log.append(("init",))
                    obj = obj.query.query
                elif self.opts["require_init"] and sid not in self._inited:
                    self.log.append(("not_inited",))
                    res = raw.types.RpcError(error_code=400, error_message="CONNECTION_NOT_INITED")
                    await self._reply(send, sid, msg_id, res, state)
                    continue

                res = self._answer(obj)
                await self._reply(send, sid, msg_id, res, state)

                da = self.opts["drop_after"]
                if da is not None and not self._dropped and state["sent"] >= da:
                    self._dropped = True
                    self.log.append(("dropped",))
                    w.transport.abort()
                    return
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            try:
                w.close()
            except Exception:
                pass

    # ---------------------------------------------------------------- request logic
    def _answer(self, obj):
        o = self.opts
        if isinstance(obj, raw.functions.upload.GetFile):
            assert obj.offset % 4096 == 0 and 0 < obj.limit <= (1 << 20) and obj.limit % 4096 == 0, (obj.offset, obj.limit)
            if o["error"]:
                return raw.types.RpcError(error_code=o["error"][0], error_message=o["error"][1])
            if o["migrate"]:
                return raw.types.RpcError(error_code=303, error_message="FILE_MIGRATE_4")
            if o["cdn"]:
                return raw.types.upload.FileCdnRedirect(
                    dc_id=203, file_token=b"t", encryption_key=b"k" * 32, encryption_iv=b"i" * 16, file_hashes=[]
                )
            if obj.offset in o["flood_offsets"] and obj.offset not in self._flooded:
                self._flooded.add(obj.offset)
                self.log.append(("flood", obj.offset))
                return raw.types.RpcError(error_code=420, error_message=f"FLOOD_WAIT_{o['flood_secs']}")
            if obj.offset in o["server_error_offsets"] and ("s", obj.offset) not in self._flooded:
                self._flooded.add(("s", obj.offset))
                self.log.append(("500", obj.offset))
                return raw.types.RpcError(error_code=500, error_message="INTERNAL")
            data = self.files[obj.location.id][obj.offset : obj.offset + obj.limit]
            if o["short"] and len(data) == obj.limit:
                data = data[:-4096]
            return raw.types.upload.File(type=raw.types.storage.FileUnknown(), mtime=0, bytes=data)

        if isinstance(obj, (raw.functions.upload.SaveBigFilePart, raw.functions.upload.SaveFilePart)):
            big = isinstance(obj, raw.functions.upload.SaveBigFilePart)
            self.parts.setdefault(obj.file_id, {})[obj.file_part] = bytes(obj.bytes)
            self.part_meta[obj.file_id] = (big, obj.file_total_parts if big else None)
            if o["error"]:
                return raw.types.RpcError(error_code=o["error"][0], error_message=o["error"][1])
            if obj.file_part in o["server_error_offsets"] and ("s", obj.file_part) not in self._flooded:
                self._flooded.add(("s", obj.file_part))
                self.log.append(("500", obj.file_part))
                return raw.types.RpcError(error_code=500, error_message="INTERNAL")
            if obj.file_part in o["flood_offsets"] and ("p", obj.file_part) not in self._flooded:
                self._flooded.add(("p", obj.file_part))
                self.log.append(("flood", obj.file_part))
                return raw.types.RpcError(error_code=420, error_message=f"FLOOD_WAIT_{o['flood_secs']}")
            return raw.core.BoolTrue()

        if isinstance(obj, raw.functions.Ping):
            return raw.types.Pong(msg_id=0, ping_id=obj.ping_id)
        raise AssertionError(f"unexpected request {obj!r}")

    async def _reply(self, send, sid, req_msg_id, res, state):
        variant = self.opts["variant"]
        if variant == "mixed":
            variant = random.choice(["plain", "gzip_result", "container", "gzip_msg"])
        # pyrogram's BoolTrue()/BoolFalse() are already-serialized bytes, every other object has .write()
        result_bytes = res if isinstance(res, bytes) else res.write()
        head = struct.pack("<Iq", raw.types.RpcResult.ID, req_msg_id)
        if variant == "gzip_result":
            await send(head + gzip_packed(result_bytes), sid)
        elif variant == "container":
            ack = raw.types.MsgsAck(msg_ids=[req_msg_id]).write()
            base = (int(time.time()) + self.opts["skew"]) << 32
            await send(container([(base | 5, 0, ack), (base | 9, 1, head + result_bytes)]), sid)
        elif variant == "gzip_msg":
            await send(gzip_packed(head + result_bytes), sid)
        else:
            await send(head + result_bytes, sid)
