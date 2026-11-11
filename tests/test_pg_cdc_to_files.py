from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

import pg_cdc_to_files._writer as writer_module
from pg_cdc_to_files import CdcError, CdcWriter, ChangeSource, RunStats, format_lsn, parse_lsn, run


class FakeSource(ChangeSource):
    """Simple source that yields a predefined list and records acks."""

    def __init__(self, messages: list[tuple[str, str]]) -> None:
        self.messages = messages
        self.acks: list[str] = []

    def read(self, start_lsn: str | None):
        yield from self.messages

    def ack(self, lsn: str) -> None:
        self.acks.append(lsn)


def test_parse_and_format_lsn_acceptance() -> None:
    assert parse_lsn("0/16B3748") == 23803720
    assert parse_lsn("1/0") == 4294967296
    assert format_lsn(23803720) == "0/16B3748"
    with pytest.raises(CdcError) as exc:
        parse_lsn("zz")
    assert exc.value.code == "invalid_lsn"


def test_parse_lsn_invalid_inputs() -> None:
    for bad in ["", "0/", "/0", "0/123456789", "123456789/0", "0/xyz", "0/1/2", 123]:
        with pytest.raises(CdcError) as exc:
            parse_lsn(bad)
        assert exc.value.code == "invalid_lsn"


def test_format_lsn_invalid_value() -> None:
    with pytest.raises(CdcError) as exc:
        format_lsn(-1)
    assert exc.value.code == "invalid_lsn"


def test_run_simple_transaction(tmp_path: Path) -> None:
    messages = [
        ("0/8", '{"action":"B"}'),
        (
            "0/9",
            '{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"ana"}]}',
        ),
        ("0/10", '{"action":"C"}'),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    stats = run(source, writer)
    assert stats == RunStats(committed=1, skipped=0, pending_changes=0, messages=0)
    file_path = tmp_path / "public.users" / "000001.jsonl"
    assert file_path.exists()
    content = file_path.read_text()
    expected = '{"commit_lsn":"0/10","old":null,"op":"insert","row":{"id":1,"name":"ana"},"seq":0,"table":"public.users"}\n'
    assert content == expected
    checkpoint_path = tmp_path / "_checkpoint.json"
    assert checkpoint_path.exists()
    with checkpoint_path.open() as f:
        checkpoint = json.load(f)
    assert checkpoint["lsn"] == "0/10"
    assert source.acks == ["0/10"]


def test_run_pending_transaction_no_commit(tmp_path: Path) -> None:
    messages = [
        ("0/8", '{"action":"B"}'),
        (
            "0/9",
            '{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"ana"}]}',
        ),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    stats = run(source, writer)
    assert stats == RunStats(committed=0, skipped=0, pending_changes=1, messages=0)
    assert not (tmp_path / "public.users").exists()
    assert not (tmp_path / "_checkpoint.json").exists()
    assert source.acks == []


def test_resume_skips_committed_transaction_and_truncates(tmp_path: Path) -> None:
    messages = [
        ("0/8", '{"action":"B"}'),
        (
            "0/9",
            '{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"ana"}]}',
        ),
        ("0/10", '{"action":"C"}'),
    ]
    source1 = FakeSource(messages)
    writer1 = CdcWriter(tmp_path)
    run(source1, writer1)
    # Simulate crash: append extra bytes to the file
    file_path = tmp_path / "public.users" / "000001.jsonl"
    original_size = file_path.stat().st_size
    with file_path.open("a") as f:
        f.write("extra garbage\n")
    assert file_path.stat().st_size > original_size
    # Second run with same source but fresh writer (reads checkpoint and truncates)
    source2 = FakeSource(messages)
    writer2 = CdcWriter(tmp_path)
    stats = run(source2, writer2)
    assert stats == RunStats(committed=0, skipped=1, pending_changes=0, messages=0)
    assert file_path.stat().st_size == original_size
    content = file_path.read_text()
    assert content.count("\n") == 1
    assert source2.acks == []  # skipped transaction is not acked


def test_update_delete_truncate_and_messages(tmp_path: Path) -> None:
    messages = [
        ("1/0", '{"action":"B"}'),
        (
            "1/1",
            '{"action":"U","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"bob"}],"identity":[{"name":"id","type":"integer","value":1}]}',
        ),
        (
            "1/2",
            '{"action":"D","schema":"public","table":"users","identity":[{"name":"id","type":"integer","value":1}]}',
        ),
        ("1/3", '{"action":"T","schema":"public","table":"users"}'),
        ("1/4", '{"action":"M","prefix":"test","content":"hello"}'),
        ("1/5", '{"action":"C"}'),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    stats = run(source, writer)
    assert stats == RunStats(committed=1, skipped=0, pending_changes=0, messages=1)
    file_path = tmp_path / "public.users" / "000001.jsonl"
    lines = file_path.read_text().strip().split("\n")
    assert len(lines) == 3
    # Verify each record
    records = [json.loads(line) for line in lines]
    assert records[0]["op"] == "update"
    assert records[0]["old"] == {"id": 1}
    assert records[0]["row"] == {"id": 1, "name": "bob"}
    assert records[1]["op"] == "delete"
    assert records[1]["row"] is None
    assert records[1]["old"] == {"id": 1}
    assert records[2]["op"] == "truncate"
    assert records[2]["row"] is None
    assert records[2]["old"] is None
    assert source.acks == ["1/5"]


def test_numeric_precision(tmp_path: Path) -> None:
    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"public","table":"nums","columns":[{"name":"decimal_col","type":"numeric","value":12.30},{"name":"big_col","type":"bigint","value":9007199254740993}]}',
        ),
        ("0/2", '{"action":"C"}'),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    run(source, writer)
    file_path = tmp_path / "public.nums" / "000001.jsonl"
    line = file_path.read_text().strip()
    record = json.loads(line)
    assert record["row"]["decimal_col"] == "12.30"
    assert record["row"]["big_col"] == 9007199254740993
    assert isinstance(record["row"]["big_col"], int)


def test_max_file_records_rotation(tmp_path: Path) -> None:
    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"public","table":"t","columns":[{"name":"id","type":"integer","value":1}]}',
        ),
        (
            "0/2",
            '{"action":"I","schema":"public","table":"t","columns":[{"name":"id","type":"integer","value":2}]}',
        ),
        (
            "0/3",
            '{"action":"I","schema":"public","table":"t","columns":[{"name":"id","type":"integer","value":3}]}',
        ),
        ("0/4", '{"action":"C"}'),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path, max_file_records=2)
    run(source, writer)
    path1 = tmp_path / "public.t" / "000001.jsonl"
    path2 = tmp_path / "public.t" / "000002.jsonl"
    assert path1.exists() and path2.exists()
    assert len(path1.read_text().strip().split("\n")) == 2
    assert len(path2.read_text().strip().split("\n")) == 1


def test_input_too_large_line(tmp_path: Path) -> None:
    # Construct a line > max_line_bytes (1_048_576). Use a large column value.
    big_value = "x" * 1_048_600
    line = f'{{"action":"I","schema":"s","table":"t","columns":[{{"name":"c","type":"text","value":"{big_value}"}}]}}'
    source = FakeSource([("0/0", '{"action":"B"}'), ("0/1", line)])
    writer = CdcWriter(tmp_path)
    with pytest.raises(CdcError) as exc:
        run(source, writer)
    assert exc.value.code == "input_too_large"


def test_invalid_json_message(tmp_path: Path) -> None:
    source = FakeSource([("0/0", "not json")])
    writer = CdcWriter(tmp_path)
    with pytest.raises(CdcError) as exc:
        run(source, writer)
    assert exc.value.code == "invalid_message"
    assert "0/0" in str(exc.value)


def test_transaction_too_large(tmp_path: Path) -> None:
    # Use a small max_tx_changes for test.
    max_tx_changes = 2
    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"s","table":"t","columns":[{"name":"c","type":"integer","value":1}]}',
        ),
        (
            "0/2",
            '{"action":"I","schema":"s","table":"t","columns":[{"name":"c","type":"integer","value":2}]}',
        ),
        (
            "0/3",
            '{"action":"I","schema":"s","table":"t","columns":[{"name":"c","type":"integer","value":3}]}',
        ),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    with pytest.raises(CdcError) as exc:
        run(source, writer, max_tx_changes=max_tx_changes)
    assert exc.value.code == "transaction_too_large"


def test_two_tables_in_one_transaction(tmp_path: Path) -> None:
    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"s1","table":"t1","columns":[{"name":"id","type":"int","value":1}]}',
        ),
        (
            "0/2",
            '{"action":"I","schema":"s2","table":"t2","columns":[{"name":"id","type":"int","value":2}]}',
        ),
        ("0/3", '{"action":"C"}'),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    run(source, writer)
    assert (tmp_path / "s1.t1" / "000001.jsonl").exists()
    assert (tmp_path / "s2.t2" / "000001.jsonl").exists()


def test_empty_transaction_advances_checkpoint(tmp_path: Path) -> None:
    messages = [
        ("0/0", '{"action":"B"}'),
        ("0/1", '{"action":"C"}'),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    run(source, writer)
    checkpoint_path = tmp_path / "_checkpoint.json"
    assert checkpoint_path.exists()
    with checkpoint_path.open() as f:
        checkpoint = json.load(f)
    assert checkpoint["lsn"] == "0/1"
    assert source.acks == ["0/1"]


def test_invalid_checkpoint(tmp_path: Path) -> None:
    # Create bad checkpoint
    checkpoint_path = tmp_path / "_checkpoint.json"
    checkpoint_path.write_text('{"version":2,"lsn":"0/0","files":{}}')
    source = FakeSource([("0/0", '{"action":"C"}')])
    writer = CdcWriter(tmp_path)
    with pytest.raises(CdcError) as exc:
        run(source, writer)
    assert exc.value.code == "invalid_checkpoint"


def test_invalid_table_name(tmp_path: Path) -> None:
    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"bad/schema","table":"t","columns":[{"name":"id","type":"int","value":1}]}',
        ),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    with pytest.raises(CdcError) as exc:
        run(source, writer)
    assert exc.value.code == "invalid_message"


def test_logical_message_not_written_and_counted(tmp_path: Path) -> None:
    # Separate test because we already covered messages in combined test.
    messages = [
        ("0/0", '{"action":"M","prefix":"test","content":"hello"}'),
    ]
    source = FakeSource(messages)
    writer = CdcWriter(tmp_path)
    stats = run(source, writer)
    assert stats.messages == 1
    assert not (tmp_path / "public.users").exists()


_TX1 = [
    ("0/8", '{"action":"B"}'),
    (
        "0/9",
        '{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1}]}',
    ),
    ("0/10", '{"action":"C"}'),
]

_TX2 = [
    ("0/11", '{"action":"B"}'),
    (
        "0/12",
        '{"action":"I","schema":"public","table":"audit","columns":[{"name":"id","type":"integer","value":2}]}',
    ),
    (
        "0/13",
        '{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":3}]}',
    ),
    ("0/14", '{"action":"C"}'),
]


def test_crash_after_file_write_before_checkpoint(tmp_path: Path, monkeypatch) -> None:
    # Commit a first transaction so a checkpoint exists.
    run(FakeSource(_TX1), CdcWriter(tmp_path))
    users_path = tmp_path / "public.users" / "000001.jsonl"
    assert len(users_path.read_text().splitlines()) == 1

    # Second run replays the skipped transaction and a new one that touches a
    # table seen for the first time, then crashes before the checkpoint.
    def boom(src: object, dst: object) -> None:
        raise OSError("simulated crash before checkpoint")

    with monkeypatch.context() as m:
        m.setattr(writer_module.os, "replace", boom)
        with pytest.raises(OSError):
            run(FakeSource(_TX1 + _TX2), CdcWriter(tmp_path))

    assert (tmp_path / "public.audit" / "000001.jsonl").exists()
    assert len(users_path.read_text().splitlines()) == 2

    # Resume: orphan lines must be truncated, exactly one line per change.
    source = FakeSource(_TX1 + _TX2)
    stats = run(source, CdcWriter(tmp_path))
    assert stats == RunStats(committed=1, skipped=1, pending_changes=0, messages=0)
    assert users_path.read_text().splitlines() == [
        '{"commit_lsn":"0/10","old":null,"op":"insert","row":{"id":1},"seq":0,"table":"public.users"}',
        '{"commit_lsn":"0/14","old":null,"op":"insert","row":{"id":3},"seq":1,"table":"public.users"}',
    ]
    audit_lines = (tmp_path / "public.audit" / "000001.jsonl").read_text().splitlines()
    assert len(audit_lines) == 1
    assert json.loads(audit_lines[0])["row"] == {"id": 2}


def test_checkpoint_lsn_higher_than_every_message(tmp_path: Path) -> None:
    checkpoint = {"version": 1, "lsn": "1/0", "files": {}}
    (tmp_path / "_checkpoint.json").write_text(json.dumps(checkpoint))
    source = FakeSource(_TX1)
    stats = run(source, CdcWriter(tmp_path))
    assert stats == RunStats(committed=0, skipped=1, pending_changes=0, messages=0)
    assert not (tmp_path / "public.users").exists()
    assert source.acks == []


def test_max_file_bytes_rotation(tmp_path: Path) -> None:
    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"public","table":"t","columns":[{"name":"id","type":"integer","value":1}]}',
        ),
        (
            "0/2",
            '{"action":"I","schema":"public","table":"t","columns":[{"name":"id","type":"integer","value":2}]}',
        ),
        ("0/3", '{"action":"C"}'),
    ]
    run(FakeSource(messages), CdcWriter(tmp_path, max_file_bytes=100))
    path1 = tmp_path / "public.t" / "000001.jsonl"
    path2 = tmp_path / "public.t" / "000002.jsonl"
    assert path1.exists() and path2.exists()
    assert len(path1.read_text().splitlines()) == 1
    assert len(path2.read_text().splitlines()) == 1


def test_on_event_payloads_do_not_leak_row_values(tmp_path: Path) -> None:
    events: list[dict[str, object]] = []
    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":1},{"name":"name","type":"text","value":"ana"}]}',
        ),
        (
            "0/2",
            '{"action":"I","schema":"public","table":"users","columns":[{"name":"id","type":"integer","value":2}]}',
        ),
        ("0/3", '{"action":"C"}'),
    ]
    run(FakeSource(messages), CdcWriter(tmp_path, max_file_records=1, on_event=events.append))
    kinds = [event["event"] for event in events]
    assert "commit" in kinds
    assert "rotate" in kinds
    for event in events:
        assert set(event) <= {"event", "lsn", "table", "records"}
        assert "ana" not in json.dumps(event)
    rotate = next(event for event in events if event["event"] == "rotate")
    assert rotate["table"] == "public.users"
    assert rotate["records"] == 1


def test_rotated_file_is_fsynced_before_checkpoint(tmp_path: Path, monkeypatch) -> None:
    events: list[tuple[str, object]] = []
    real_replace = os.replace

    def record_fsync(handle) -> None:
        events.append(("fsync", handle.name))

    orig_close = writer_module.FileState.close

    def record_close(self) -> None:
        events.append(("close", self.seq))
        orig_close(self)

    def record_replace(src: str, dst: str) -> None:
        events.append(("replace", dst))
        real_replace(src, dst)

    monkeypatch.setattr(writer_module, "_fsync_handle", record_fsync)
    monkeypatch.setattr(writer_module.FileState, "close", record_close)
    monkeypatch.setattr(writer_module.os, "replace", record_replace)

    messages = [
        ("0/0", '{"action":"B"}'),
        (
            "0/1",
            '{"action":"I","schema":"public","table":"t","columns":[{"name":"id","type":"integer","value":1}]}',
        ),
        (
            "0/2",
            '{"action":"I","schema":"public","table":"t","columns":[{"name":"id","type":"integer","value":2}]}',
        ),
        ("0/3", '{"action":"C"}'),
    ]
    run(FakeSource(messages), CdcWriter(tmp_path, max_file_records=1))

    close_index = events.index(("close", 1))
    assert events[close_index - 1][0] == "fsync"
    assert str(events[close_index - 1][1]).endswith("000001.jsonl")
    replace_index = next(i for i, e in enumerate(events) if e[0] == "replace")
    assert close_index < replace_index
    assert all(e[0] != "fsync" or i < replace_index for i, e in enumerate(events))


def test_checkpoint_referencing_missing_file_raises_invalid_checkpoint(tmp_path: Path) -> None:
    checkpoint = {
        "version": 1,
        "lsn": "0/10",
        "files": {"public.users": {"file": "000001.jsonl", "records": 1, "bytes": 90}},
    }
    (tmp_path / "_checkpoint.json").write_text(json.dumps(checkpoint))
    writer = CdcWriter(tmp_path)
    with pytest.raises(CdcError) as exc:
        writer.prepare()
    assert exc.value.code == "invalid_checkpoint"


def test_checkpoint_referencing_shorter_file_raises_invalid_checkpoint(tmp_path: Path) -> None:
    table_dir = tmp_path / "public.users"
    table_dir.mkdir()
    (table_dir / "000001.jsonl").write_text("")
    checkpoint = {
        "version": 1,
        "lsn": "0/10",
        "files": {"public.users": {"file": "000001.jsonl", "records": 1, "bytes": 90}},
    }
    (tmp_path / "_checkpoint.json").write_text(json.dumps(checkpoint))
    writer = CdcWriter(tmp_path)
    with pytest.raises(CdcError) as exc:
        writer.prepare()
    assert exc.value.code == "invalid_checkpoint"
    assert (table_dir / "000001.jsonl").read_text() == ""


def test_stray_higher_sequence_files_are_removed_on_prepare(tmp_path: Path) -> None:
    run(FakeSource(_TX1), CdcWriter(tmp_path))
    table_dir = tmp_path / "public.users"
    stray = table_dir / "000002.jsonl"
    stray.write_text("orphan\n")
    CdcWriter(tmp_path).prepare()
    assert not stray.exists()
    assert (table_dir / "000001.jsonl").read_text().splitlines() == [
        '{"commit_lsn":"0/10","old":null,"op":"insert","row":{"id":1},"seq":0,"table":"public.users"}'
    ]


def test_directory_fsync_failure_does_not_fail_commit(tmp_path: Path, monkeypatch) -> None:
    real_open = os.open

    def fake_open(path, flags, *args, **kwargs):
        if Path(path) == tmp_path:
            raise PermissionError("no directory file descriptors")
        return real_open(path, flags, *args, **kwargs)

    monkeypatch.setattr(writer_module.os, "open", fake_open)
    source = FakeSource(_TX1)
    stats = run(source, CdcWriter(tmp_path))
    assert stats.committed == 1
    assert source.acks == ["0/10"]
    assert (tmp_path / "_checkpoint.json").exists()
