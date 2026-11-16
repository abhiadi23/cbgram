#  cbgram - crypto backend
#
#  cbgram uses exactly one native crypto implementation: cbcrypto (Rust, AES-NI / ARMv8).
#  There is no fallback. If it is missing or broken, cbgram refuses to start instead of
#  silently running on something else.

import logging

log = logging.getLogger(__name__)

name = "cbcrypto"

try:
    import cbcrypto as impl
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "cbgram needs the 'cbcrypto' extension (the Rust crypto + transfer core) and it is not installed.\n"
        "cbcrypto is its own repository. Install it from there, e.g.\n"
        "  pip install git+https://github.com/abhiadi23/cbcrypto      (builds it, needs Rust)\n"
        "  pip install cbcrypto-<version>-cp38-abi3-manylinux_2_34_x86_64.whl   (prebuilt, Linux x86-64)\n"
        "  Termux / other:  clone the repo and run ./build.sh"
    ) from e


if not all(hasattr(impl, n) for n in ("download", "upload", "_after_fork")):
    raise ImportError(
        f"cbgram needs cbcrypto >= 0.3 (transfer core + fork safety); found {getattr(impl, '__version__', '?')}. "
        "Update it from the cbcrypto repository."
    )


def _self_test() -> None:
    """Known-answer sanity check, so a broken build can never touch a real session."""
    key, iv = bytes(range(32)), bytes(range(32, 64))
    data = bytes(range(256)) * 4
    if impl.ige256_decrypt(impl.ige256_encrypt(data, key, iv), key, iv) != data:
        raise RuntimeError("cbcrypto self-test failed (IGE roundtrip)")
    a, b, sa, sb = bytearray(16), bytearray(16), bytearray(1), bytearray(1)
    ct = impl.ctr256_encrypt(data, key, a, sa)
    if impl.ctr256_decrypt(ct, key, b, sb) != data or a != b or sa != sb:
        raise RuntimeError("cbcrypto self-test failed (CTR roundtrip)")


_self_test()

# Fork safety (a child must not replay its parent's buffered padding randomness) is handled
# inside cbcrypto itself: it registers its own os.register_at_fork hook when it is imported.

log.info("Crypto backend: cbcrypto (%s)", impl.backend())
