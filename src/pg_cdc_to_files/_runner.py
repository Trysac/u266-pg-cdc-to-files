from __future__ import annotations

import json
import logging
import re
from decimal import Decimal

from ._errors import CdcError
from ._lsn import parse_lsn
from ._model import ChangeSource, RunStats
from ._writer import CdcWriter

logger = logging.getLogger(__name__)

_TABLE_RE = re.compile(r"^[A-Za-z0-9_$.-]+$")


def run(
    source: ChangeSource,
    writer: CdcWriter,
    *,
    max_tx_changes: int = 100_000,
    max_line_bytes: int = 1_048_576,
) -> RunStats:
    """Process messages from ``source`` and write committed transactions.

    Returns statistics about the run.
    """
    writer.prepare()
    committed = 0
    skipped = 0
    messages = 0
    current_tx: list[dict[str, object]] | None = None

    for lsn, line in source.read(None):
        if len(line.encode("utf-8")) > max_line_bytes:
            raise CdcError("input_too_large", f"line for LSN {lsn} exceeds {max_line_bytes} bytes")
        try:
            msg = json.loads(line, parse_float=Decimal)
        except json.JSONDecodeError as e:
            raise CdcError("invalid_message", f"invalid JSON at LSN {lsn}: {e}") from e
        if not isinstance(msg, dict) or "action" not in msg or not isinstance(msg["action"], str):
            raise CdcError("invalid_message", f"missing or invalid action at LSN {lsn}")

        action = msg["action"]

        if action == "B":
            if current_tx is not None:
                raise CdcError(
                    "invalid_message", f"unexpected BEGIN at LSN {lsn}, transaction already open"
                )
            current_tx = []
        elif action == "C":
            if current_tx is None:
                raise CdcError("invalid_message", f"COMMIT without BEGIN at LSN {lsn}")
            try:
                commit_lsn_int = parse_lsn(lsn)
            except CdcError as e:
                raise CdcError("invalid_message", f"invalid LSN at commit {lsn}: {e}") from e
            if commit_lsn_int <= writer.checkpoint_lsn:
                skipped += 1
                writer.emit_skip(lsn)
                current_tx = None
                continue

            records: list[dict[str, object]] = []
            for idx, change in enumerate(current_tx):
                record = {
                    "commit_lsn": lsn,
                    "seq": idx,
                    "op": change["op"],
                    "table": change["table"],
                    "row": change.get("row"),
                    "old": change.get("old"),
                }
                records.append(record)

            writer.commit_transaction(records, lsn, commit_lsn_int)
            source.ack(lsn)
            committed += 1
            current_tx = None
        elif action == "M":
            messages += 1
            # logical message; no transaction required
        elif action in ("I", "U", "D", "T"):
            if current_tx is None:
                raise CdcError("invalid_message", f"{action} without BEGIN at LSN {lsn}")
            if len(current_tx) >= max_tx_changes:
                raise CdcError(
                    "transaction_too_large",
                    f"transaction at LSN {lsn} exceeds {max_tx_changes} changes",
                )
            change = _parse_change(msg, action, lsn)
            current_tx.append(change)
        else:
            raise CdcError("invalid_message", f"unknown action {action!r} at LSN {lsn}")

    pending = len(current_tx) if current_tx is not None else 0
    return RunStats(
        committed=committed, skipped=skipped, pending_changes=pending, messages=messages
    )


def _parse_change(msg: dict[str, object], action: str, lsn: str) -> dict[str, object]:
    table = msg.get("table")
    schema = msg.get("schema")
    if not isinstance(table, str) or not isinstance(schema, str):
        raise CdcError("invalid_message", f"missing schema/table at LSN {lsn}")
    if not _TABLE_RE.fullmatch(schema) or not _TABLE_RE.fullmatch(table):
        raise CdcError("invalid_message", f"invalid schema/table name at LSN {lsn}")
    full_table = f"{schema}.{table}"

    op_map = {"I": "insert", "U": "update", "D": "delete", "T": "truncate"}
    op = op_map[action]
    row: dict[str, object] | None = None
    old: dict[str, object] | None = None

    if action in ("I", "U"):
        columns = msg.get("columns")
        if not isinstance(columns, list):
            raise CdcError("invalid_message", f"missing columns at LSN {lsn}")
        row = {}
        for col in columns:
            if not isinstance(col, dict) or "name" not in col or "value" not in col:
                raise CdcError("invalid_message", f"invalid column at LSN {lsn}")
            name = col["name"]
            if not isinstance(name, str):
                raise CdcError("invalid_message", f"invalid column name at LSN {lsn}")
            row[name] = _convert_value(col["value"])

    if action in ("U", "D"):
        identity = msg.get("identity")
        if not isinstance(identity, list):
            raise CdcError("invalid_message", f"missing identity at LSN {lsn}")
        old = {}
        for ident in identity:
            if not isinstance(ident, dict) or "name" not in ident or "value" not in ident:
                raise CdcError("invalid_message", f"invalid identity column at LSN {lsn}")
            name = ident["name"]
            if not isinstance(name, str):
                raise CdcError("invalid_message", f"invalid identity column name at LSN {lsn}")
            old[name] = _convert_value(ident["value"])

    return {"table": full_table, "op": op, "row": row, "old": old}


def _convert_value(value: object) -> object:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {k: _convert_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_convert_value(v) for v in value]
    return value
