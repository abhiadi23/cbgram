Speedups
========

cbgram is fast out of the box: the cryptography runs in Rust, small packets skip the thread
pool entirely, and peers are cached in front of the session database. This page is about the
one thing left for you to add — a faster event loop — and where to look when you want to tune
the rest.


-----

cbcrypto
--------

cbgram depends on cbcrypto, a **Rust** extension that implements what Telegram requires:
AES-256-IGE and AES-256-CTR with hardware AES, the fused MTProto pack/unpack, and the
file download/upload engine. It is a hard dependency, not an optional speedup, and there
is no fallback implementation.

Payloads at or below ``CBGRAM_INLINE_CRYPTO_MAX`` (32 KiB) are encrypted on the event loop
rather than handed to a thread, because the hand-off costs more than the work: a 64-byte
control packet packs in 1.3 µs against roughly 110 µs for a thread round trip. Larger
transfer parts still go to the crypto pool. See :doc:`/features/performance` for the numbers
and the knobs.

uvloop
------

uvloop_ is a fast, drop-in replacement of the built-in asyncio event loop. uvloop is implemented in Cython and uses
libuv under the hood. It makes asyncio 2-4x faster.

Installation
^^^^^^^^^^^^

.. code-block:: bash

    $ pip install -U cbgram[fast]

That extra pulls in uvloop on Linux and macOS. Installing it directly works too:

.. code-block:: bash

    $ pip install -U uvloop

uvloop does not support Windows.

Usage
^^^^^

Call ``uvloop.install()`` before calling ``asyncio.run()`` or ``app.run()``.

.. code-block:: python

    import asyncio
    import uvloop

    from cbgram import Client


    async def main():
        app = Client("my_account")

        async with app:
            print(await app.get_me())


    uvloop.install()
    asyncio.run(main())

The ``uvloop.install()`` call also needs to be placed before creating a Client instance.

.. code-block:: python

    import uvloop
    from cbgram import Client

    uvloop.install()

    app = Client("my_account")


    @app.on_message()
    async def hello(client, message):
        print(await client.get_me())


    app.run()

.. _uvloop: https://github.com/MagicStack/uvloop

Tuning the rest
---------------

Transfer concurrency, read-ahead memory, the peer cache and the crypto threshold are all
environment knobs, and on a small host they matter more than the event loop does.
:doc:`/features/performance` lists every one of them with its default and what it bounds.
