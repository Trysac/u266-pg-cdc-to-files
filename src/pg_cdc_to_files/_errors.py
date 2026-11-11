from __future__ import annotations

from typing import Literal

ErrorCode = Literal[
    "invalid_lsn",
    "invalid_message",
    "input_too_large",
    "transaction_too_large",
    "invalid_checkpoint",
]


class CdcError(Exception):
    """Base error for the library with a stable machine-readable code."""

    def __init__(self, code: ErrorCode, message: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def __str__(self) -> str:
        return f"{self.code}: {self.message}" if self.message else self.code
