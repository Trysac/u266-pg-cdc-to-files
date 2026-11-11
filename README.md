# pg-cdc-to-files

[![PyPI version](https://img.shields.io/pypi/v/trysac-pg-cdc-to-files)](https://pypi.org/project/trysac-pg-cdc-to-files/)
[![License: MIT](https://img.shields.io/pypi/l/trysac-pg-cdc-to-files)](LICENSE)
[![CI](https://github.com/Trysac/u266-pg-cdc-to-files/actions/workflows/ci.yml/badge.svg)](https://github.com/Trysac/u266-pg-cdc-to-files/actions/workflows/ci.yml)

Write PostgreSQL logical decoding changes in wal2json format-version 2 to rotating JSONL files per table, committed transactions only, with an atomic LSN checkpoint and exactly-once resume

## Problem

Describe in two or three sentences the concrete problem this utility solves, who hits it, and what
they do today without it. Replace this paragraph when the first real feature lands.

## Install

```sh
pip install trysac-pg-cdc-to-files
# or
uv add trysac-pg-cdc-to-files
```

Requires Python 3.10 or newer. No runtime dependencies.

## Usage

```python
import pg_cdc_to_files

# normalize() trims surrounding whitespace and lower-cases the text.
print(pg_cdc_to_files.normalize("  Hello World "))  # -> "hello world"

# Blank input is rejected explicitly instead of returning an empty string.
try:
    pg_cdc_to_files.normalize("   ")
except ValueError as exc:
    print(f"rejected: {exc}")
```

## Documentation

- [Architecture](docs/ARCHITECTURE.md) and [decisions](docs/decisions/)
- [Testing](docs/TESTING.md) and [security](docs/SECURITY.md)
- [Project record](docs/PROJECT.md) and [stack](docs/STACK.md)
- [Changelog](CHANGELOG.md) and [contributing guide](CONTRIBUTING.md)

## License

[MIT](LICENSE) (c) 2026 Miguel Gomez
