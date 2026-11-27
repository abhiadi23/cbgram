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

from typing import Optional, Union

import pyrogram
from pyrogram import types


class StreamLink:
    async def start_stream_server(
        self: "pyrogram.Client",
        host: str = "0.0.0.0",
        port: int = 8080,
        public_url: Optional[str] = None,
        cache_dir: Optional[str] = None,
        window_mib: Optional[int] = None,
        link_ttl: Optional[int] = None,
    ) -> "pyrogram.stream_server.StreamServer":
        """Start the built-in HTTP stream server (once) and return it.

        Files are served with Range support while they download, using the Rust engine of cbcrypto.
        Put it behind a reverse proxy for HTTPS; set ``public_url`` to the address people will use.

        Example:
            .. code-block:: python

                await app.start_stream_server(port=8080, public_url="https://stream.example.com")
                url = app.get_stream_link(message)
        """
        from pyrogram.stream_server import StreamServer

        server = getattr(self, "_stream_server", None)
        if server is None:
            server = StreamServer(self, host, port, public_url, cache_dir, window_mib, link_ttl)
            self._stream_server = server
        await server.start()
        return server

    def get_stream_link(
        self: "pyrogram.Client",
        media: Union["types.Message", str, object],
        name: Optional[str] = None,
        ttl: Optional[int] = None,
        download: bool = False,
    ) -> str:
        """Get a stream link for a message, a media object or a file id.

        The stream server must be running (see :meth:`~pyrogram.Client.start_stream_server`).
        Open the link in a browser or a player (VLC, mpv, ``<video src=...>``): playback starts right
        away and seeking works. ``download=True`` makes browsers save the file instead of playing it.
        """
        server = getattr(self, "_stream_server", None)
        if server is None or server._server is None:
            raise RuntimeError("The stream server is not running: call `await app.start_stream_server()` first")
        return server.link(media, name=name, ttl=ttl, download=download)

    async def stop_stream_server(self: "pyrogram.Client") -> None:
        """Stop the stream server and delete its cache files."""
        server = getattr(self, "_stream_server", None)
        if server is not None:
            await server.stop()
            self._stream_server = None
