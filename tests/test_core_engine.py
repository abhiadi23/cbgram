"""Tests for the Rust transfer core (cbcrypto.download / cbcrypto.upload) against a mock DC."""

import hashlib
import os
import time

import pytest

import cbcrypto as cb
from pyrogram import raw
from pyrogram.raw.all import layer

from tests.mock_dc import MockDC

AK = os.urandom(256)
AKID = hashlib.sha1(AK).digest()[-8:]
SALT = 0x1122334455667788
MB = 1 << 20


def init_prefix() -> bytes:
    q = raw.functions.InvokeWithLayer(
        layer=layer,
        query=raw.functions.InitConnection(
            api_id=1, app_version="1.0", device_model="test", system_version="x", system_lang_code="en",
            lang_code="en", lang_pack="", query=raw.functions.Ping(ping_id=0),
        ),
    )
    return q.write()[:-12]  # everything up to the (12 byte) placeholder query


def req_prefix(loc_id: int) -> bytes:
    loc = raw.types.InputDocumentFileLocation(id=loc_id, access_hash=7, file_reference=b"ref", thumb_size="")
    return raw.functions.upload.GetFile(location=loc, offset=0, limit=0).write()[:-12]  # minus offset+limit


@pytest.fixture
def dcs():
    made = []

    def make(**kw):
        files = kw.pop("files", None)
        dc = MockDC(AK, files=files, **kw)
        dc.start()
        made.append(dc)
        return dc

    yield make
    for d in made:
        d.stop()


def run_download(dc, data, tmp_path, loc=42, salt=SALT, **kw):
    path = str(tmp_path / "out.bin")
    with open(path, "wb") as f:
        f.truncate(len(data))
    args = dict(
        host="127.0.0.1", port=dc.port, auth_key=AK, auth_key_id=AKID, salt=salt,
        init_prefix=init_prefix(), req_prefix=req_prefix(loc), path=path, file_size=len(data),
        connections=2, inflight=3, paced=False, read_timeout=5.0,
    )
    args.update(kw)
    n = cb.download(**args)
    with open(path, "rb") as f:
        got = f.read()
    return n, got


def blob(n):
    return os.urandom(n)


# ---------------------------------------------------------------- download

@pytest.mark.parametrize("size", [100, MB, MB + 1, 5 * MB + 77, 12 * MB])
def test_download_matches_the_source(dcs, tmp_path, size):
    data = blob(size)
    dc = dcs(files={42: data})
    n, got = run_download(dc, data, tmp_path)
    assert n == size and got == data


@pytest.mark.parametrize("variant", ["plain", "gzip_result", "container", "gzip_msg", "mixed"])
def test_every_response_shape_is_understood(dcs, tmp_path, variant):
    data = blob(4 * MB + 5)
    dc = dcs(files={42: data}, variant=variant)
    n, got = run_download(dc, data, tmp_path)
    assert got == data


def test_first_request_carries_initConnection(dcs, tmp_path):
    data = blob(2 * MB)
    dc = dcs(files={42: data})  # require_init=True: no init -> CONNECTION_NOT_INITED
    n, got = run_download(dc, data, tmp_path, connections=1)
    assert got == data and ("not_inited",) not in dc.log and ("init",) in dc.log


def test_wrong_initial_salt_is_corrected(dcs, tmp_path):
    data = blob(3 * MB)
    dc = dcs(files={42: data})
    n, got = run_download(dc, data, tmp_path, salt=0xDEAD)
    assert got == data and ("bad_salt",) in dc.log


def test_clock_skew_is_synced(dcs, tmp_path):
    data = blob(3 * MB)
    dc = dcs(files={42: data}, skew=120)  # server clock 2 min ahead of ours
    n, got = run_download(dc, data, tmp_path)
    assert got == data and any(e[0] == "bad_msg_time" for e in dc.log)


def test_time_offset_argument_avoids_the_skew_round_trip(dcs, tmp_path):
    data = blob(2 * MB)
    dc = dcs(files={42: data}, skew=120)
    n, got = run_download(dc, data, tmp_path, time_offset=120.0)
    assert got == data and not any(e[0] == "bad_msg_time" for e in dc.log)


def test_flood_wait_is_waited_out_and_retried(dcs, tmp_path):
    data = blob(4 * MB)
    dc = dcs(files={42: data}, flood_offsets={2 * MB}, flood_secs=1)
    t = time.time()
    n, got = run_download(dc, data, tmp_path)
    assert got == data and ("flood", 2 * MB) in dc.log and time.time() - t >= 1.0


def test_transient_500_is_retried_for_download_and_upload(dcs, tmp_path):
    data = blob(4 * MB)
    dc = dcs(files={42: data}, server_error_offsets={MB})
    n, got = run_download(dc, data, tmp_path)
    assert got == data and ("500", MB) in dc.log
    dc2 = dcs(server_error_offsets={2})
    run_upload(dc2, data, tmp_path)
    assert reassemble(dc2, 555, 8) == data and ("500", 2) in dc2.log


def test_too_long_flood_wait_is_raised(dcs, tmp_path):
    data = blob(2 * MB)
    dc = dcs(files={42: data}, flood_offsets={0}, flood_secs=500)
    with pytest.raises(cb.CoreError) as e:
        run_download(dc, data, tmp_path)
    assert e.value.args[0] == "rpc" and e.value.args[1] == 420 and e.value.args[2] == "FLOOD_WAIT_500"


def test_dropped_connection_is_resumed_on_a_new_session(dcs, tmp_path):
    data = blob(10 * MB)
    dc = dcs(files={42: data}, drop_after=3)
    n, got = run_download(dc, data, tmp_path, connections=1)
    assert got == data and ("dropped",) in dc.log and dc.conns >= 2


def test_cdn_redirect_is_reported_not_followed(dcs, tmp_path):
    data = blob(2 * MB)
    dc = dcs(files={42: data}, cdn=True)
    with pytest.raises(cb.CoreError) as e:
        run_download(dc, data, tmp_path)
    assert e.value.args[0] == "cdn"


def test_rpc_errors_are_reported_with_code_and_message(dcs, tmp_path):
    data = blob(2 * MB)
    dc = dcs(files={42: data}, migrate=True)
    with pytest.raises(cb.CoreError) as e:
        run_download(dc, data, tmp_path)
    assert e.value.args[:3] == ("rpc", 303, "FILE_MIGRATE_4")

    dc2 = dcs(files={42: data}, error=(400, "FILE_REFERENCE_EXPIRED"))
    with pytest.raises(cb.CoreError) as e:
        run_download(dc2, data, tmp_path)
    assert e.value.args[2] == "FILE_REFERENCE_EXPIRED"


def test_a_short_chunk_never_passes_silently(dcs, tmp_path):
    data = blob(3 * MB)
    dc = dcs(files={42: data}, short=True)
    with pytest.raises(cb.CoreError) as e:
        run_download(dc, data, tmp_path)
    assert e.value.args[0] == "protocol" and "short chunk" in e.value.args[2]


def test_unreachable_server_fails_cleanly(tmp_path):
    path = str(tmp_path / "x")
    open(path, "wb").write(b"\0" * MB)
    t = time.time()
    with pytest.raises(cb.CoreError) as e:
        cb.download("127.0.0.1", 1, AK, AKID, SALT, init_prefix(), req_prefix(1), path, MB,
                    connections=1, paced=False, max_reconnects=1)
    assert e.value.args[0] in ("io", "timeout") and time.time() - t < 20


def test_the_pacer_slows_a_download_down(dcs, tmp_path):
    data = blob(6 * MB)
    dc = dcs(files={42: data})
    t = time.time()
    # 2 requests/s, burst 1, no slow start: 6 chunks need ~2.5 s
    n, got = run_download(dc, data, tmp_path, paced=True, rate=2.0, burst=1.0, ceiling=2.0, growth=1.0)
    assert got == data and time.time() - t >= 2.0


def test_progress_is_monotonic_and_ends_at_the_size(dcs, tmp_path):
    data = blob(8 * MB + 3)
    dc = dcs(files={42: data})
    seen = []
    n, got = run_download(dc, data, tmp_path, progress=seen.append)
    assert got == data and seen == sorted(seen) and seen[-1] == len(data)


def test_an_exception_in_the_progress_callback_cancels_the_transfer(dcs, tmp_path):
    data = blob(30 * MB)
    dc = dcs(files={42: data})

    class Stop(Exception):
        pass

    def cb_(done):
        if done > 2 * MB:
            raise Stop()

    t = time.time()
    with pytest.raises(Stop):
        run_download(dc, data, tmp_path, progress=cb_)
    assert time.time() - t < 10


def test_bad_arguments_are_rejected(tmp_path):
    p = str(tmp_path / "f")
    open(p, "wb").write(b"\0" * 10)
    with pytest.raises(ValueError):
        cb.download("h", 1, b"x" * 10, AKID, 0, b"", b"", p, 10)
    with pytest.raises(ValueError):
        cb.download("h", 1, AK, AKID, 0, b"", b"", p, 10, chunk_size=1000)


# ---------------------------------------------------------------- upload

def run_upload(dc, data, tmp_path, file_id=555, **kw):
    path = str(tmp_path / "up.bin")
    with open(path, "wb") as f:
        f.write(data)
    args = dict(
        host="127.0.0.1", port=dc.port, auth_key=AK, auth_key_id=AKID, salt=SALT, init_prefix=init_prefix(),
        path=path, file_size=len(data), file_id=file_id, part_size=512 * 1024, is_big=True,
        connections=2, inflight=3, paced=False, read_timeout=5.0,
    )
    args.update(kw)
    return cb.upload(**args)


def reassemble(dc, file_id, total):
    parts = dc.parts[file_id]
    assert sorted(parts) == list(range(total)), "missing or extra parts"
    return b"".join(parts[i] for i in range(total))


@pytest.mark.parametrize("size", [1, 512 * 1024, 512 * 1024 + 1, 5 * MB + 123, 11 * MB])
def test_upload_big_file_arrives_intact(dcs, tmp_path, size):
    data = blob(size)
    dc = dcs()
    n = run_upload(dc, data, tmp_path)
    total = -(-size // (512 * 1024))
    assert n == size and reassemble(dc, 555, total) == data
    assert dc.part_meta[555] == (True, total)


def test_upload_small_file_uses_saveFilePart(dcs, tmp_path):
    data = blob(3 * MB + 1)
    dc = dcs()
    run_upload(dc, data, tmp_path, is_big=False)
    total = -(-len(data) // (512 * 1024))
    assert reassemble(dc, 555, total) == data and dc.part_meta[555][0] is False


def test_upload_can_resume_from_a_part(dcs, tmp_path):
    data = blob(4 * MB)
    dc = dcs()
    n = run_upload(dc, data, tmp_path, start_part=3)
    total = len(data) // (512 * 1024)
    assert sorted(dc.parts[555]) == list(range(3, total))
    assert n == len(data) - 3 * 512 * 1024


@pytest.mark.parametrize("variant", ["plain", "gzip_result", "container", "gzip_msg"])
def test_upload_with_every_response_shape(dcs, tmp_path, variant):
    data = blob(3 * MB)
    dc = dcs(variant=variant)
    run_upload(dc, data, tmp_path)
    assert reassemble(dc, 555, 6) == data


def test_upload_survives_flood_drop_and_wrong_salt(dcs, tmp_path):
    data = blob(8 * MB)
    dc = dcs(flood_offsets={4}, flood_secs=1, drop_after=5)
    run_upload(dc, data, tmp_path, connections=1, salt=0xBEEF)
    assert reassemble(dc, 555, 16) == data
    assert ("dropped",) in dc.log and ("flood", 4) in dc.log and ("bad_salt",) in dc.log


def test_upload_rpc_error_is_reported(dcs, tmp_path):
    dc = dcs(error=(400, "FILE_PARTS_INVALID"))
    with pytest.raises(cb.CoreError) as e:
        run_upload(dc, blob(2 * MB), tmp_path)
    assert e.value.args[:3] == ("rpc", 400, "FILE_PARTS_INVALID")


# ---------------------------------------------------------------- wire constants

def test_constructor_ids_match_the_generated_tl_classes():
    c = cb._constants()
    expect = {
        "RPC_RESULT": raw.types.RpcResult.ID,
        "RPC_ERROR": raw.types.RpcError.ID,
        "UPLOAD_FILE": raw.types.upload.File.ID,
        "CDN_REDIRECT": raw.types.upload.FileCdnRedirect.ID,
        "BOOL_TRUE": raw.core.BoolTrue.ID,
        "BOOL_FALSE": raw.core.BoolFalse.ID,
        "BAD_SERVER_SALT": raw.types.BadServerSalt.ID,
        "BAD_MSG_NOTIFICATION": raw.types.BadMsgNotification.ID,
        "NEW_SESSION": raw.types.NewSessionCreated.ID,
        "MSGS_ACK": raw.types.MsgsAck.ID,
        "SAVE_FILE_PART": raw.functions.upload.SaveFilePart.ID,
        "SAVE_BIG_FILE_PART": raw.functions.upload.SaveBigFilePart.ID,
        "CONTAINER": 0x73F1F8DC,
        "GZIP_PACKED": 0x3072CFA1,
        "VECTOR": 0x1CB5C415,
    }
    for k, v in expect.items():
        assert c[k] == v, k


@pytest.mark.parametrize("i", range(8))
def test_clock_skew_with_many_requests_in_flight_never_loses_a_chunk(dcs, tmp_path, i):
    """Regression: back-to-back bad_msg_notifications used to hand two chunks the same msg_id."""
    data = blob(6 * MB)
    dc = dcs(files={42: data}, skew=120)
    n, got = run_download(dc, data, tmp_path, connections=3, inflight=6)
    assert got == data
