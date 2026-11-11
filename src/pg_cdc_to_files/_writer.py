from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import BinaryIO

from ._errors import CdcError
from ._lsn import parse_lsn

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

_TABLE_RE = re.compile(r"^[A-Za-z0-9_$.-]+$")


def _fsync_directory(path: Path) -> None:
    """Best-effort fsync of a directory.

    Opening and fsyncing a directory is only supported on POSIX; elsewhere
    (e.g. Windows) there is no directory file descriptor, so this is a no-op.
    Failures are suppressed: durability of a directory entry is a best-effort
    optimisation, never a reason to fail an otherwise committed transaction.
    """
    if os.name != "posix":
        return
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        with contextlib.suppress(OSError):
            os.close(fd)


def _fsync_handle(handle: BinaryIO) -> None:
    handle.flush()
    os.fsync(handle.fileno())


class FileState:
    """State for a single open JSONL file per table."""

    def __init__(self, table: str, seq: int, path: Path, *, resume: bool = False) -> None:
        self.table = table
        self.seq = seq
        self.path = path
        self.records = 0
        self.bytes = 0
        self.handle: BinaryIO | None = None
        self._open(resume)

    def _open(self, resume: bool) -> None:
        table_dir = self.path.parent
        existed = table_dir.exists()
        table_dir.mkdir(parents=True, exist_ok=True)
        if not existed:
            _fsync_directory(table_dir.parent)
        if resume:
            # Only a file referenced by a valid checkpoint may be resumed.
            try:
                self.handle = self.path.open("r+b")
            except OSError as e:
                raise CdcError(
                    "invalid_checkpoint", f"cannot open checkpointed file {self.path}: {e}"
                ) from e
            self.handle.seek(0, os.SEEK_END)
            self.bytes = self.handle.tell()
        else:
            # Trust only the checkpoint: truncate any orphan bytes left behind
            # by a crash before the checkpoint was written.
            self.handle = self.path.open("wb")
            self.bytes = 0
            _fsync_directory(table_dir)

    def truncate_to(self, bytes_: int) -> None:
        if self.handle is None:
            raise CdcError("invalid_checkpoint", "file not open")
        self.handle.seek(0, os.SEEK_END)
        size = self.handle.tell()
        if size < bytes_:
            raise CdcError(
                "invalid_checkpoint",
                f"file {self.path} is shorter ({size} bytes) than the checkpoint ({bytes_} bytes)",
            )
        self.handle.truncate(bytes_)
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.handle.seek(bytes_)
        self.bytes = bytes_

    def close(self) -> None:
        if self.handle is not None:
            self.handle.close()
            self.handle = None


class CdcWriter:
    """Writes committed transactions to rotating per-table JSONL files.

    Maintains an atomic JSON checkpoint for exactly-once resume.
    """

    def __init__(
        self,
        directory: Path,
        *,
        max_file_records: int = 10_000,
        max_file_bytes: int = 67_108_864,
        on_event: Callable[[dict[str, object]], None] | None = None,
    ) -> None:
        self.directory = Path(directory)
        self.max_file_records = max_file_records
        self.max_file_bytes = max_file_bytes
        self.on_event = on_event
        self._file_states: dict[str, FileState] = {}
        self._checkpoint_tables: set[str] = set()
        self._checkpoint_lsn_int: int = 0
        self._current_commit_lsn_str: str | None = None

    @property
    def checkpoint_lsn(self) -> int:
        return self._checkpoint_lsn_int

    def prepare(self) -> None:
        """Load checkpoint, truncate files to recorded offsets."""
        self.directory.mkdir(parents=True, exist_ok=True)
        checkpoint_path = self.directory / "_checkpoint.json"
        if checkpoint_path.exists():
            try:
                with checkpoint_path.open("r", encoding="utf-8") as f:
                    data = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                raise CdcError("invalid_checkpoint", f"cannot read checkpoint: {e}") from e
            if not isinstance(data, dict) or data.get("version") != 1:
                raise CdcError("invalid_checkpoint", "unsupported checkpoint version")
            lsn_str = data.get("lsn")
            if not isinstance(lsn_str, str):
                raise CdcError("invalid_checkpoint", "missing lsn in checkpoint")
            try:
                self._checkpoint_lsn_int = parse_lsn(lsn_str)
            except CdcError as e:
                raise CdcError("invalid_checkpoint", f"invalid lsn in checkpoint: {e}") from e
            files = data.get("files")
            if not isinstance(files, dict):
                raise CdcError("invalid_checkpoint", "missing files in checkpoint")
            for table, info in files.items():
                if not isinstance(info, dict):
                    raise CdcError("invalid_checkpoint", f"invalid file info for {table}")
                file_name = info.get("file")
                records = info.get("records")
                bytes_ = info.get("bytes")
                if (
                    not isinstance(file_name, str)
                    or not isinstance(records, int)
                    or not isinstance(bytes_, int)
                ):
                    raise CdcError("invalid_checkpoint", f"invalid file entry for {table}")
                if not _TABLE_RE.fullmatch(table):
                    raise CdcError(
                        "invalid_checkpoint", f"invalid table name in checkpoint: {table}"
                    )
                try:
                    seq_str = file_name.split(".")[0]
                    seq = int(seq_str)
                except (ValueError, IndexError) as e:
                    raise CdcError(
                        "invalid_checkpoint", f"invalid file name in checkpoint: {file_name}"
                    ) from e
                file_path = self.directory / table / file_name
                state = FileState(table, seq, file_path, resume=True)
                state.truncate_to(bytes_)
                state.records = records
                self._file_states[table] = state
                self._checkpoint_tables.add(table)
                self._remove_stray_files(table, seq)
        # else: no checkpoint, start fresh; every file is opened in truncate mode

    def _remove_stray_files(self, table: str, keep_seq: int) -> None:
        """Drop files beyond the checkpointed sequence for a table."""
        table_dir = self.directory / table
        if not table_dir.is_dir():
            return
        for child in table_dir.iterdir():
            if child.suffix != ".jsonl" or not child.is_file():
                continue
            try:
                child_seq = int(child.stem)
            except ValueError:
                continue
            if child_seq > keep_seq:
                with contextlib.suppress(OSError):
                    child.unlink()

    def emit_event(
        self, event: str, lsn: str, table: str | None = None, records: int | None = None
    ) -> None:
        if self.on_event:
            payload: dict[str, object] = {"event": event, "lsn": lsn}
            if table is not None:
                payload["table"] = table
            if records is not None:
                payload["records"] = records
            self.on_event(payload)

    def emit_skip(self, lsn: str) -> None:
        self.emit_event("skip", lsn)

    def commit_transaction(
        self, changes: list[dict[str, object]], commit_lsn_str: str, commit_lsn_int: int
    ) -> None:
        """Write all changes for a committed transaction and update checkpoint."""
        self._current_commit_lsn_str = commit_lsn_str
        touched_tables: set[str] = set()
        for change in changes:
            table = str(change["table"])
            record_json = (
                json.dumps(change, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
            )
            record_bytes = record_json.encode("utf-8")
            state = self._get_or_create_file_state(table)
            if (
                state.records >= self.max_file_records
                or state.bytes + len(record_bytes) > self.max_file_bytes
            ):
                self._rotate(state)
                state = self._file_states[table]
            if state.handle is None:
                raise CdcError("invalid_checkpoint", "file handle is None")
            state.handle.write(record_bytes)
            state.records += 1
            state.bytes += len(record_bytes)
            touched_tables.add(table)

        for table in touched_tables:
            current = self._file_states.get(table)
            if current and current.handle:
                _fsync_handle(current.handle)

        self._checkpoint_lsn_int = commit_lsn_int
        file_entries: dict[str, object] = {}
        for table, file_state in self._file_states.items():
            file_entries[table] = {
                "file": f"{file_state.seq:06d}.jsonl",
                "records": file_state.records,
                "bytes": file_state.bytes,
            }
        checkpoint_data: dict[str, object] = {
            "version": 1,
            "lsn": commit_lsn_str,
            "files": file_entries,
        }

        checkpoint_path = self.directory / "_checkpoint.json"
        temp_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.directory, delete=False, prefix=".checkpoint-"
            ) as temp:
                temp_name = temp.name
                json.dump(checkpoint_data, temp, indent=2)
                temp.flush()
                os.fsync(temp.fileno())
            os.replace(temp_name, checkpoint_path)
            _fsync_directory(self.directory)
        except Exception:
            if temp_name is not None:
                with contextlib.suppress(OSError):
                    os.unlink(temp_name)
            raise

        self.emit_event("commit", commit_lsn_str)

    def _get_or_create_file_state(self, table: str) -> FileState:
        if table not in self._file_states:
            seq = 1  # initial sequence for new tables
            file_path = self.directory / table / f"{seq:06d}.jsonl"
            # A table not referenced by the checkpoint is never resumed: any
            # existing bytes are orphans from a crash before the checkpoint.
            resume = table in self._checkpoint_tables
            state = FileState(table, seq, file_path, resume=resume)
            self._file_states[table] = state
        return self._file_states[table]

    def _rotate(self, state: FileState) -> None:
        # Make the rotated file durable before the checkpoint starts referring
        # to the next file, otherwise a crash could lose its tail.
        if state.handle is not None:
            _fsync_handle(state.handle)
        state.close()
        new_seq = state.seq + 1
        new_path = self.directory / state.table / f"{new_seq:06d}.jsonl"
        new_state = FileState(state.table, new_seq, new_path, resume=False)
        self._file_states[state.table] = new_state
        # use current commit lsn for rotate event; may be None if called without transaction
        lsn_for_event = self._current_commit_lsn_str or ""
        self.emit_event("rotate", lsn_for_event, table=state.table, records=state.records)
