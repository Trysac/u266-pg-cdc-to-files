# Use buffered commit-only writes with atomic checkpoint for exactly-once file output

Status: accepted (generated, pending human review)

## Context

PostgreSQL CDC consumers often duplicate or lose rows when crashing between writing data files and recording the processed position. The utility must write per-table JSONL files and provide exactly-once semantics.

## Decision

Buffer all changes of a transaction in memory until a COMMIT message. On commit, write all records, flush and fsync the data files, then atomically persist a JSON checkpoint containing the commit LSN and file offsets. On startup, truncate existing files to the recorded offsets before processing further messages.

## Consequences

Uncommitted changes are never written; a crash after file write but before checkpoint leaves extra bytes that are truncated on restart. The checkpoint write and fsync provide durability; source acknowledgment occurs only after a successful checkpoint. This design avoids per-record writes and simplifies resume logic.
