# cbgram's Rust core

cbgram = a Pyrogram-based library whose **crypto and file transfers run in Rust** (the separate **cbcrypto** repository) and
everything else (API, handlers, sessions, TL types, auth) stays Python.

## What runs where

| | Python | Rust (`cbcrypto`) |
|---|---|---|
| auth-key handshake, sessions, DC lookup, TL types, handlers, API methods | ✔ | |
| MTProto crypto (AES-IGE/CTR, KDF, fused pack/unpack) | | ✔ (the only backend, no fallback) |
| **file download** (`download_media`) | request template, progress, errors | sockets, transport, MTProto, acks/salts/flood/reconnect, pacing, file writes |
| **file upload** (`save_file` by path) | request template, md5, progress, errors | same |
| proxies, CDN files, in-memory/stream targets, resumed uploads | ✔ (Python path) | |

`pyrogram/core_transfer.py` is the whole bridge. It decides per call whether the core can handle the
transfer and otherwise lets the original Python path run, so nothing that worked before stops working.

## Installing cbcrypto
cbcrypto is its own repository and a normal dependency of cbgram (`cbcrypto>=0.3.0`):

```bash
pip install git+https://github.com/abhiadi23/cbcrypto                 # builds it, needs Rust
pip install cbcrypto-<version>-cp38-abi3-manylinux_2_34_x86_64.whl            # prebuilt wheel, Linux x86-64
git clone https://github.com/abhiadi23/cbcrypto && cd cbcrypto && ./build.sh   # Termux / anything else
```
Check it with `python -c "import cbcrypto; print(cbcrypto.__version__, cbcrypto.backend())"`: the line should
say `aes_ni=true` (and `vaes=true` on newer CPUs). cbgram refuses to start without it.

## Switches (environment variables)

| variable | default | meaning |
|---|---|---|
| `CBGRAM_RUST_CORE` | `1` | `0` = always use the pure-Python transfer path (debugging) |
| `CBGRAM_SLOW_START` | `1` | `0` = old gentle pacer ramp-up |
| `CBGRAM_DL_GROWTH` / `CBGRAM_UPLOAD_GROWTH` | `1.5` | slow-start multiplier per 0.25 s |
| `CBGRAM_DL_SLOWSTART_CAP` / `CBGRAM_UPLOAD_SLOWSTART_CAP` | `40` / `80` | requests/s where slow start stops |
| `CBGRAM_DL_POOL_*`, `CBGRAM_DL_WORKERS_*`, `CBGRAM_DL_RATE_*`, `CBGRAM_DL_BURST_*` | see docs | connections, in-flight requests per connection, pacer (USER / BOT / PREMIUM) |
| `CBGRAM_UPLOAD_POOL_*`, `CBGRAM_UPLOAD_RATE_*` | see docs | same for uploads |
| `CBGRAM_SOCKET_BUFFER` | `0` | receive buffer in bytes |
| `CBGRAM_UVLOOP` | off | `1` = use uvloop (Linux/macOS) |
| `CBCRYPTO_DEBUG` | off | per-request trace from the engine on stderr |

## What was measured (and what was not)
* Engine against a mock Telegram DC that speaks the real wire protocol: **all transfer paths and fault
  cases pass** (wrong salt, clock skew, flood waits, 5xx, dropped connections, containers, gzip, CDN redirect,
  short chunks, cancellation, 2 transfers at once) — `tests/test_core_engine.py`, `tests/test_core_transfer.py`.
* CPU cost of the engine: ~3 ms per MiB on one core (≈ 340 MiB/s per core) on the x86-64 test VM; at 40 MB/s that
  is ~12 % of a core. The pure-Python path measured ~300 MiB/s per core, so **per core the two are in the same
  ballpark: crypto and the kernel copy dominate**. The core's gains are no GIL contention, real multi-core use,
  and zero-copy writes; it does not make Telegram or your network faster.
* **Not tested against real Telegram servers** and **not run on ARM**. Run `cb_bench.py` on your own setup first.
