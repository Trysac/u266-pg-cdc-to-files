from __future__ import annotations

import re

from ._errors import CdcError

_LSN_RE = re.compile(r"^([0-9A-Fa-f]{1,8})/([0-9A-Fa-f]{1,8})$")


def parse_lsn(text: str) -> int:
    """Parse a PostgreSQL LSN string 'X/Y' into an integer.

    Each part is hexadecimal and at most 8 digits.

    Raises:
        CdcError: with code ``invalid_lsn`` if the format is invalid.
    """
    if not isinstance(text, str):
        raise CdcError("invalid_lsn", f"expected str, got {type(text).__name__}")
    match = _LSN_RE.fullmatch(text)
    if not match:
        raise CdcError("invalid_lsn", f"invalid LSN format: {text!r}")
    x = int(match.group(1), 16)
    y = int(match.group(2), 16)
    if x > 0xFFFFFFFF or y > 0xFFFFFFFF:
        raise CdcError("invalid_lsn", f"LSN parts exceed 8 hex digits: {text!r}")
    return (x << 32) | y


def format_lsn(value: int) -> str:
    """Format an integer LSN value as an uppercase 'X/Y' string."""
    if not isinstance(value, int) or value < 0:
        raise CdcError("invalid_lsn", f"expected non-negative int, got {value!r}")
    x = value >> 32
    y = value & 0xFFFFFFFF
    return f"{x:X}/{y:X}"
