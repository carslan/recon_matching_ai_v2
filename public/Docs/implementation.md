# Implementation Guide

## Why This App Exists
Financial reconciliation matching — given a dataset with two sides (A and B), determine which records match, which should be excluded, and which are unmatched breaks. Every decision is auditable: SQL trail, reasoning JSON, and a sealed run manifest.

## Architecture

```
                         ┌─────────────┐
                         │  FastAPI     │  port 4017
                         │  server.py   │
                         └──────┬───────┘
                                │
                    POST /api/recon-match/execute
                                │
                         ┌──────▼───────┐
                         │  service.py   │  ReconMatchService
                         └──────┬───────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                  │
       ┌──────▼──────┐  ┌──────▼──────┐  ┌───────▼───────┐
       │   boot.py    │  │orchestrator │  │  finalize.py  │
       │  Phase A/B/C │  │   .py       │  │  CSV export   │
       │  + get-ready │  │  waterfall  │  │  reasoning    │
       └──────────────┘  │  loop       │  │  manifest     │
                         └──────┬──────┘  └───────────────┘
                                │
                    ExecutorRequest per step
                                │
                         ┌──────▼──────┐
                         │ executor.py  │
                         │ DuckDB init │
                         └──────┬──────┘
                                │
                         ┌──────▼──────┐
                         │duckdb_skill │
                         │   .py       │
                         │ 4 operators │
                         │ SQL engine  │
                         └─────────────┘
```

## Data Flow

1. **Request arrives**: `ReconMatchRequest(workspace, ruleset, dataset_path, run_id)`
2. **Boot pipeline** (`boot.py`):
   - Phase A: derive KB_DIR + RUN_DIR paths, validate KB, create RUN_DIR
   - Phase B: load `kb_index.json`, fetch critical markdown sections by byte offset, load 4 JSON configs, validate
   - Phase C: compile `RulesetRuntimeConfig` — merge active core + agent rules, exclusions sorted first, persist `compiled_waterfall.json`
   - Get-ready: copy dataset → `dataset_file.csv`, write `run_manifest.json` skeleton
3. **Orchestrator** (`orchestrator.py`):
   - Loads `compiled_waterfall.json`
   - For each step: builds `ExecutorRequest` → sends to Executor → receives `ExecutorResponse`
   - Tracks zero-progress counter for stop condition (3 consecutive steps with 0 removed)
   - Logs every decision to `InteractionLog`
4. **Executor** (`executor.py`):
   - On first call: opens DuckDB, loads CSV into `base_dataset`, creates `matched_records` table + `pool_unmatched` view, profiles dataset
   - Converts `OperatorSpec` → operator dict → calls `duckdb_execute_operator()`
   - Stores reasoning groups incrementally
5. **DuckDB Skill** (`duckdb_skill.py`):
   - Compiles operator spec → SQL template → executes against `pool_unmatched`
   - Inserts matched records into `matched_records` (append-only)
   - `pool_unmatched` is a view: `base_dataset` minus `matched_records`
   - Appends SQL to `matching_queries.sql` audit trail
6. **Finalization** (`finalize.py`):
   - Export `exclusions.csv`, `matches.csv`, `breaks.csv` (disjoint)
   - Write `breaks_reasoning.json`
   - Seal `run_manifest.json` with summary
   - Validate all 9 required artifacts present

## Operator Details

### exclude_by_predicates
- Side-specific SQL filters applied to `pool_unmatched`
- Records matching the filter are inserted into `matched_records` with `operator='exclude_by_predicates'`
- Always runs first in waterfall (enforced at compile time)

### one_to_one_ranked
- Blocking: JOIN on `keys_a`/`keys_b` (same cusip, same fund, etc.)
- Amount tolerance: `|A.col - B.col| < tolerance`
- Mutual best-match: two ROW_NUMBER window functions ensure each A picks its best B and vice versa — only pairs where both rank #1 are selected
- Deterministic tie-breaker by `record_id`

### many_to_many_balance_k
- Groups A and B records within a blocking key
- Aggregates comparison columns per side (e.g., SUM of amounts)
- Selects blocks where aggregated A ≈ aggregated B within tolerance
- All records in the block (N from A, M from B) form one match group

### one_sided
- Self-join within Side A (no Side B involved)
- Groups by blocking key where `SUM(amount) ≈ 0`
- Used for netting: +5000 and -5000 in the same cusip → matched

## Column Qualification
Side filters like `(sec_group == 'FUND')` become ambiguous in JOINs where both tables have `sec_group`. The `_qualify_filter()` function auto-prefixes unqualified column names with the appropriate table alias (`a.sec_group`, `b.sec_group`).

## Key Files

| File | Purpose |
|------|---------|
| `server.py` | FastAPI app, all API endpoints |
| `service.py` | ReconMatchService — boot → orchestrate → finalize |
| `boot.py` | Phase A/B/C + get-ready |
| `orchestrator.py` | Waterfall loop, stop conditions |
| `executor.py` | DuckDB init + operator dispatch |
| `duckdb_skill.py` | 4 operators, profiling, SQL audit |
| `finalize.py` | CSV export, reasoning, manifest seal |
| `interaction_log.py` | Agent interaction capture |
| `schemas.py` | All Pydantic models |
| `validators.py` | KB + artifact validation |
| `paths.py` | Path derivation |
| `scaffold.py` | RUN_DIR creation |
| `lro_client.py` | LRO mock |
| `migration.py` | ruleset.json → KB files |
| `synthetic_data.py` | Test dataset generator |
| `kb_indexer.py` | Offline KB → kb_index.json |
| `guardrail.py` | Write-path enforcement |

## Output Artifacts (per run)

| File | Content |
|------|---------|
| `dataset_file.csv` | Copy of input dataset |
| `matches.csv` | match_group_id, side, statement_id, record_id |
| `breaks.csv` | side, statement_id, record_id |
| `exclusions.csv` | Full record columns for excluded records |
| `matches_reasoning.json` | Per-group reasoning (incremental) |
| `breaks_reasoning.json` | Per-break reasoning (at finalization) |
| `exclusions_reasoning.json` | Per-exclusion reasoning (incremental) |
| `matching_queries.sql` | Append-only SQL audit trail |
| `run_manifest.json` | Canonical audit record (waterfall steps, summary, timing) |
| `interaction_log.json` | Agent interaction events (66 per run) |
| `dataset_profile.json` | Side counts, amount columns, blocking quality |
| `compiled_waterfall.json` | Ordered operator steps |
| `compiled_ruleset_runtime_config.json` | Runtime config |
| `run.duckdb` | DuckDB database file |

## Dependencies
- Python 3.11+, FastAPI, uvicorn, Pydantic v2, DuckDB, loguru, pytest

## Known Limitations
- Phase 7 (RuleCreator) not implemented
- No Google ADK integration — uses plain Python classes
- `_qualify_filter()` is regex-based — may fail on complex nested SQL
- LRO is in-memory only (lost on restart)
- No authentication
- Start script hardcodes anaconda Python path
