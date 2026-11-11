from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Protocol


class ChangeSource(Protocol):
    """Protocol for a source of wal2json messages."""

    def read(self, start_lsn: str | None) -> Iterator[tuple[str, str]]:
        """Yield (lsn, json_line) pairs.

        The returned iterator should produce all messages from the requested
        position; ``start_lsn`` is advisory and implementations may ignore it.
        """
        ...

    def ack(self, lsn: str) -> None:
        """Acknowledge that all changes up to ``lsn`` have been processed."""
        ...


@dataclass(frozen=True)
class RunStats:
    """Statistics returned by :func:`run`."""

    committed: int
    skipped: int
    pending_changes: int
    messages: int = 0
