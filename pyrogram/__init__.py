#  Pyrogram - Telegram MTProto API Client Library for Python
#  Copyright (C) 2017-present Dan <https://github.com/delivrance>
#
#  This file is part of Pyrogram.
#
#  Pyrogram is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Lesser General Public License as published
#  by the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Pyrogram is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Lesser General Public License for more details.
#
#  You should have received a copy of the GNU Lesser General Public License
#  along with Pyrogram.  If not, see <http://www.gnu.org/licenses/>.

"""cbgram - Elegant, modern and asynchronous Telegram MTProto API framework.

cbgram is a fork of Pyrogram providing support for the latest Telegram features
including Gifts, Stories, Topics, Business Accounts, and more.
"""

__version__ = "3.1.7"
__license__ = "GNU Lesser General Public License v3.0 (LGPL-3.0)"
__copyright__ = "Copyright (C) 2017-present Dan <https://github.com/delivrance>"
__fork__ = "cbgram by abhiadi23 <https://github.com/abhiadi23/cbgram>"
__contributors__ = [
    "SilentDemonSD <https://github.com/SilentDemonSD>",
]


# Faster event loop: uvloop is used whenever it is installed (Linux/macOS, CPython), because the
# whole client - every session read, ping and handler - runs on one loop and uvloop's is
# roughly twice as fast as asyncio's default one. Set CBGRAM_UVLOOP=0 to keep the default loop.
# Falls back silently to asyncio's default loop if uvloop is missing or unsupported (Termux).
import os as _os

if _os.environ.get("CBGRAM_UVLOOP", "1").strip().lower() not in ("0", "false", "no", "off"):
    try:
        import asyncio as _asyncio
        import sys as _sys
        import warnings as _warnings

        if _sys.platform != "win32":
            import uvloop as _uvloop

            # leave a policy the application already chose alone
            if type(_asyncio.get_event_loop_policy()).__module__.startswith("asyncio"):
                with _warnings.catch_warnings():
                    _warnings.simplefilter("ignore", DeprecationWarning)
                    _asyncio.set_event_loop_policy(_uvloop.EventLoopPolicy())
    except Exception:  # not installed / unsupported platform (e.g. Termux)
        pass


class StopTransmission(Exception):
    pass


class StopPropagation(StopAsyncIteration):
    pass


class ContinuePropagation(StopAsyncIteration):
    pass


from . import raw, types, filters, handlers, enums
from .client import Client
from .methods.utilities.idle import idle
from .methods.utilities.compose import compose
from .methods.rate_limiter import RateLimiter, TokenBucket
