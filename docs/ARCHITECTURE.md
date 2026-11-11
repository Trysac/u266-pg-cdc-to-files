# Architecture

This library consumes wal2json format-version 2 messages and writes committed
transactions to rotating per-table JSONL files with an atomic checkpoint.

## Components

- `lsn.py`: parse/format PostgreSQL LSN values (`X/Y`).
- `writer.py`: `CdcWriter` manages per-table files, rotation, and checkpoint
  persistence. On `prepare()`, it reads the checkpoint and truncates files to
  recorded byte offsets.
- `runner.py`: `run()` orchestrates reading messages from a `ChangeSource`,
  buffering per transaction, writing on commit, and acknowledging sources.
- `errors.py`: `CdcError` with machine-readable codes.

## Design Decisions

- **Only committed transactions are written.** Changes are buffered until a
  COMMIT message; the commit LSN is used for the record and checkpoint.
- **Exactly-once resume.** The checkpoint stores each table's current file,
  record count, and byte offset. On startup, files are truncated to those
  offsets, eliminating duplicate lines written after the last checkpoint.
- **Per-table rotating files.** Files are named `<6-digit sequence>.jsonl` inside
  a directory named `<schema>.<table>`; rotation occurs before exceeding
  `max_file_records` or `max_file_bytes`.
- **Atomic checkpoint.** The checkpoint is written via a temporary file, fsynced,
  and `os.replace`d, then the directory is fsynced. Source acknowledgment happens
  only after this succeeds.
- **No runtime dependencies.** Uses only the Python standard library; `decimal`
  preserves numeric precision.

See `docs/SECURITY.md` for the threat model.
