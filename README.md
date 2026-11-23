<div align="center">

<img src="assets/logo.png" alt="cbgram" width="420">

<br><br>

### ***Elegant, modern and asynchronous Telegram MTProto framework.***
**For users and bots · Python on top · Rust core underneath**

<br>

![Rust core](https://img.shields.io/badge/CORE-Rust-2563EB?style=for-the-badge&logo=rust&logoColor=white)
![Python](https://img.shields.io/badge/PYTHON-3.10%2B-1D4ED8?style=for-the-badge&logo=python&logoColor=white)
![MTProto](https://img.shields.io/badge/PROTOCOL-MTProto-0EA5E9?style=for-the-badge)
![License](https://img.shields.io/badge/LICENSE-LGPL--3.0-0369A1?style=for-the-badge)

<br>

**[Features](#-features)** &nbsp;·&nbsp; **[Install](#-install)** &nbsp;·&nbsp; **[Quick start](#-quick-start)** &nbsp;·&nbsp; **[Rust core](#-the-rust-core)** &nbsp;·&nbsp; **[Development](#-development)**

</div>

<br>

---

## ✦ &nbsp;**About**

**cbgram** is a *modern*, *async-first* framework for the **Telegram MTProto API**. Log in as a **user account** or run as a **bot**, and get full access to everything Telegram lets an official client do.

It is a ***drop-in replacement for Pyrogram***: write `from cbgram import ...` in new code, and existing `from pyrogram import ...` codebases keep working unchanged, because both names resolve to the *same module*.

Under the hood, **crypto and file transfers run in Rust** through **[cbcrypto](https://github.com/abhiadi23/cbcrypto)**, while the API, handlers, sessions and types stay in friendly Python.

<br>

## ✦ &nbsp;**Features**

| | **Feature** | |
|:---:|:---|:---|
| 🔁 | **Drop-in replacement** | *Migrate from Pyrogram with zero code changes.* |
| 🆕 | **Up to date** | *Gifts, Stories, Topics, Business Accounts, Giveaways and the latest Telegram layer.* |
| ⚡ | **Async natively** | *Fully `async`/`await`, and still usable synchronously via `app.run()`.* |
| 🦀 | **Rust core** | *MTProto crypto plus the file download and upload engine run in compiled Rust.* |
| 🧩 | **Type-hinted** | *Every type and method is annotated for great editor support.* |
| 👤 | **Users and bots** | *One framework for custom clients and bot identities.* |
| 🔌 | **Proxy support** | *Built on `python-socks` with asyncio.* |
| 🗄️ | **Flexible storage** | *SQLite sessions by default, with optional Mongo and Redis extras.* |
| 🚀 | **Optional speed-ups** | *Add `uvloop` on Linux and macOS with a single extra.* |

<br>

## ✦ &nbsp;**Install**

**Latest from GitHub**

```bash
pip install git+https://github.com/abhiadi23/cbgram.git@main
```

**With uvloop** &nbsp;*(Linux · macOS)*

```bash
pip install "cbgram[fast] @ git+https://github.com/abhiadi23/cbgram.git@main"
```

**In `requirements.txt`**

```text
cbgram @ git+https://github.com/abhiadi23/cbgram.git@main
```

> **Requires Python 3.10+** and **[cbcrypto](https://github.com/abhiadi23/cbcrypto)**. It is installed automatically as a dependency, and cbgram *will not start without it*.

**Optional extras**

| Extra | **Adds** |
|:---|:---|
| `fast` | *uvloop for a faster event loop* |
| `mongo` | *MongoDB storage via `motor`* |
| `redis` | *Redis storage* |

<br>

## ✦ &nbsp;**Quick start**

**A tiny greeter**

```python
from cbgram import Client, filters

app = Client("my_account")


@app.on_message(filters.private)
async def hello(client, message):
    await message.reply("Hello from cbgram!")


app.run()
```

**A bot with inline buttons**

```python
from cbgram import Client, filters
from cbgram.types import InlineKeyboardMarkup, InlineKeyboardButton

app = Client("my_bot")


@app.on_message(filters.command("start"))
async def start(client, message):
    await message.reply(
        "Choose an option:",
        reply_markup=InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Website", url="https://example.com"),
                InlineKeyboardButton("Help", callback_data="help"),
            ],
            [InlineKeyboardButton("About", callback_data="about")],
        ]),
    )


app.run()
```

<br>

## ✦ &nbsp;**The Rust core**

***Python where it is friendly, Rust where it counts.***

| | **Python** | **Rust** (`cbcrypto`) |
|:---|:---:|:---:|
| **Auth handshake, sessions, handlers, TL types, API methods** | ✔ | |
| **MTProto crypto** *(AES-IGE / CTR, KDF)* | | ✔ |
| **File downloads** *(`download_media`)* | *request logic, progress* | *sockets, transport, pacing, file writes* |
| **File uploads** *(`save_file` by path)* | *request logic, progress* | *same* |

Anything the core cannot handle, such as proxies or in-memory targets, ***falls back to the original Python path***, so nothing that worked before stops working.

**What you actually gain:** *no GIL contention, real multi-core use and zero-copy file writes.* It does not make Telegram or your network faster.

**Handy switches**

| Variable | **Default** | **Meaning** |
|:---|:---:|:---|
| `CBGRAM_RUST_CORE` | `1` | *Set to `0` to force the pure-Python transfer path (debugging).* |
| `CBGRAM_UVLOOP` | off | *Set to `1` to use uvloop on Linux and macOS.* |
| `CBCRYPTO_DEBUG` | off | *Per-request engine trace on stderr.* |

<br>

## ✦ &nbsp;**Development**

```bash
git clone https://github.com/abhiadi23/cbgram.git
cd cbgram

pip install uv
uv sync --frozen --extra dev

uv run poe api     # generate TL API types
uv run poe test    # run the tests
```

<br>

## ✦ &nbsp;**Resources**

- 💬 &nbsp;*[Source code](https://github.com/abhiadi23/cbgram)*
- 🐞 &nbsp;*[Issue tracker](https://github.com/abhiadi23/cbgram/issues)*
- 🤝 &nbsp;*[Contributing guide](https://github.com/abhiadi23/cbgram/blob/main/CONTRIBUTING.md)*
- 🔒 &nbsp;*[cbcrypto](https://github.com/abhiadi23/cbcrypto)*, the *Rust crypto engine*

<br>

## ✦ &nbsp;**License**

***cbgram*** is released under the **GNU LGPL v3 or later**. See [`COPYING`](https://github.com/abhiadi23/cbgram/blob/main/COPYING) and [`COPYING.lesser`](https://github.com/abhiadi23/cbgram/blob/main/COPYING.lesser).

<br>

---

<div align="center">

***Made with ♥ by [abhiadi23](https://github.com/abhiadi23)***

</div>
