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

from typing import Optional
import logging

from pyrogram.crypto._backend import impl as cbcrypto, name as _backend_name

log = logging.getLogger(__name__)
log.info("Crypto backend: %s", _backend_name)


def ige256_encrypt(data: bytes, key: bytes, iv: bytes) -> bytes:
    return cbcrypto.ige256_encrypt(data, key, iv)


def ige256_decrypt(data: bytes, key: bytes, iv: bytes) -> bytes:
    return cbcrypto.ige256_decrypt(data, key, iv)


def ctr256_encrypt(data: bytes, key: bytes, iv: bytearray, state: Optional[bytearray] = None) -> bytes:
    return cbcrypto.ctr256_encrypt(data, key, iv, state or bytearray(1))


def ctr256_decrypt(data: bytes, key: bytes, iv: bytearray, state: Optional[bytearray] = None) -> bytes:
    return cbcrypto.ctr256_decrypt(data, key, iv, state or bytearray(1))


def ctr256_encrypt_inplace(data: bytearray, key: bytes, iv: bytearray, state: Optional[bytearray] = None):
    return cbcrypto.ctr256_encrypt_inplace(data, key, iv, state or bytearray(1))


def ctr256_decrypt_inplace(data: bytearray, key: bytes, iv: bytearray, state: Optional[bytearray] = None):
    return cbcrypto.ctr256_decrypt_inplace(data, key, iv, state or bytearray(1))


def ctr256_encrypt_batch(
    data_flat: bytes,
    sizes: bytearray,
    key: bytes,
    ivs: bytearray,
    states: bytearray,
) -> bytes:
    return cbcrypto.ctr256_encrypt_batch(data_flat, sizes, key, ivs, states)


def ctr256_decrypt_batch(
    data_flat: bytes,
    sizes: bytearray,
    key: bytes,
    ivs: bytearray,
    states: bytearray,
) -> bytes:
    return cbcrypto.ctr256_decrypt_batch(data_flat, sizes, key, ivs, states)


def xor(a: bytes, b: bytes) -> bytes:
    return int.to_bytes(
        int.from_bytes(a, "big") ^ int.from_bytes(b, "big"),
        len(a),
        "big",
    )
