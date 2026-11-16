<div align="center">

<img src="https://raw.githubusercontent.com/abhiadi23/cbgram/main/assets/png/cbgram-icon-256.png" alt="cbgram" width="160">

# cbgram

[![PyPI](https://img.shields.io/pypi/v/cbgram)](https://pypi.org/project/cbgram/)
[![Python](https://img.shields.io/pypi/pyversions/cbgram)](https://pypi.org/project/cbgram/)
[![Downloads](https://img.shields.io/pypi/dm/cbgram)](https://pypi.org/project/cbgram/)
[![License](https://img.shields.io/github/license/abhiadi23/cbgram)](https://github.com/abhiadi23/cbgram/blob/main/COPYING.lesser)
[![Documentation](https://img.shields.io/badge/docs-cbgram.example/blue)](https://cbgram.example)

**Elegant, modern and asynchronous Telegram MTProto API framework in Python for users and bots**

</div>

cbgram is a **drop-in replacement** for Pyrogram, with access to the latest Telegram features including **Gifts, Stories, Topics, Business Accounts**, and more. Import `cbgram`; `from pyrogram import ...` still resolves to the same module rather than a second copy of it, so an existing Pyrogram codebase runs unchanged.

```python
from cbgram import Client, filters

app = Client("my_account")


@app.on_message(filters.private)
async def hello(client, message):
    await message.reply("Hello from cbgram!")


app.run()
```

**cbgram** is a modern, elegant and asynchronous [MTProto API](https://cbgram.example/topics/mtproto-vs-botapi/) framework. It enables you to easily interact with the main Telegram API through a user account (custom client) or a bot identity (bot API alternative) using Python.

### Key Features

- **Drop-in Replacement** — New code says `from cbgram import ...`; existing `from pyrogram import ...` codebases migrate with zero changes, since both names are one module.
- **Up-to-Date** — Supports Gifts, Stories, Topics, Business Accounts, Giveaways, and the latest Telegram layer.
- **Async Natively** — Fully `async`/`await` throughout. Also usable synchronously via `app.run()` for convenience.
- **Type-hinted** — Every type and method is annotated for excellent editor support.
- **Fast** — Rust core — crypto and the file download/upload engine run in **cbcrypto**, a compiled Rust extension; everything else is Python.
- **Powerful** — Full access to Telegram's API for any official client action and more.

### Example with Inline Keyboard

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
        ])
    )


app.run()
```

### Installing

```bash
pip install cbgram
```

Requires Python 3.10+. On Linux and macOS, `cbgram[fast]` also pulls in
[uvloop](https://github.com/MagicStack/uvloop); call `uvloop.install()` yourself to use it
(see the Speedups guide):

```bash
pip install cbgram[fast]
```

### Development

```bash
# Clone the repo
git clone https://github.com/abhiadi23/cbgram.git
cd cbgram

# Install uv (if not already)
pip install uv

# Create virtual environment with dev dependencies
uv sync --frozen --extra dev

# Generate TL API types
uv run poe api

# Run tests
uv run poe test
```

### Documentation

Full documentation at **[https://cbgram.example](https://cbgram.example)**

### Resources

- [Source code](https://github.com/abhiadi23/cbgram)
- [Documentation](https://cbgram.example)
- [Issue tracker](https://github.com/abhiadi23/cbgram/issues)
- [Contributing guide](https://github.com/abhiadi23/cbgram/blob/main/CONTRIBUTING.md)
