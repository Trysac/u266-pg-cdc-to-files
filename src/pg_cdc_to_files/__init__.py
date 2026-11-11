"""PostgreSQL CDC to files using wal2json format-version 2.

Public API:
- parse_lsn / format_lsn for LSN conversion
- CdcError with stable error codes
- ChangeSource protocol
- CdcWriter for per-table rotating JSONL files and atomic checkpoint
- run() to process the stream
"""

from __future__ import annotations

from ._errors import CdcError
from ._lsn import format_lsn, parse_lsn
from ._model import ChangeSource, RunStats
from ._runner import run
from ._writer import CdcWriter

__all__ = [
    "CdcError",
    "CdcWriter",
    "ChangeSource",
    "RunStats",
    "__version__",
    "format_lsn",
    "parse_lsn",
    "run",
]

__version__ = "0.1.0"
