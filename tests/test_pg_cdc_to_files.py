"""Behavior tests for the public API of pg_cdc_to_files."""

from __future__ import annotations

import pytest

import pg_cdc_to_files


def test_normalize_trims_and_lowercases() -> None:
    assert pg_cdc_to_files.normalize("  Hello World ") == "hello world"


@pytest.mark.parametrize("value", ["", "   ", "\t\n"])
def test_normalize_rejects_blank_input(value: str) -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        pg_cdc_to_files.normalize(value)


def test_version_is_semver() -> None:
    parts = pg_cdc_to_files.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
