# Project record

## Objective

Write PostgreSQL logical decoding changes in wal2json format-version 2 to rotating JSONL files per table, committed transactions only, with an atomic LSN checkpoint and exactly-once resume

## Scope

- In scope: the public API documented in [README.md](../README.md), typed and tested.
- Out of scope: anything that needs runtime dependencies or I/O without an accepted ADR.

## Timeline

| Date | Event |
|---|---|
| 2026 | Repository created from the factory utility template. |

## Effort metrics

Measured values only; a value that has not been measured stays "pending measurement".

| Metric | Value |
|---|---|
| Tokens (input / output) | 1034596 / 87778 |
| Hours (human) | pending measurement |
| Cost (USD) | 0.1125 |

## Factory run u266 (measured from API usage)

| Date (UTC) | Phase | Model | Batch | Input | Output | Cache write | Cache read | Cost (USD) |
|---|---|---|---|---|---|---|---|---|
| 2026-11-11 | generation | deepseek-v4-pro | yes | 1834 | 23258 | 0 | 3328 | 0.0473 |
| 2026-11-11 | fix-1 | deepseek-flash | no | 13182 | 4662 | 0 | 122752 | 0.0051 |
| 2026-11-11 | audit | deepseek-flash | no | 29024 | 23437 | 0 | 432128 | 0.0197 |
| 2026-11-11 | fix-2 | deepseek-flash | no | 27157 | 20421 | 0 | 315392 | 0.0173 |
| 2026-11-11 | docs | deepseek-flash | no | 89543 | 16000 | 0 | 256 | 0.0230 |

Total: $0.1125. Prices: deepseek line, as measured by its adapter. Providers serve the current version of each model id, so the date tells which version answered; versions, launch stages and price sources per line are in the factory's model register (docs/MODELS.md).
