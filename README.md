# trysac-pg-cdc-to-files

`pg_cdc_to_files` turns a stream of PostgreSQL logical decoding messages in
wal2json format-version 2 into rotating per-table JSONL files. Only committed
transactions are written, and an atomic LSN checkpoint allows exactly-once
resume after a crash.

## Usage

```python
from pathlib import Path
from pg_cdc_to_files import CdcWriter, ChangeSource, run


class MySource(ChangeSource):
    def read(self, start_lsn=None):
        # yield (lsn, json_line) from your replication slot adapter
        yield from []

    def ack(self, lsn):
        pass


source = MySource()
writer = CdcWriter(Path("/data/lake"))
stats = run(source, writer, max_tx_changes=100_000, max_line_bytes=1_048_576)
```

See `docs/` for architecture and security details.
