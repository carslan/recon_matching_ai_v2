# Dev Log — Phase 0: Migration Utility

**Date:** 2026-03-26
**Status:** Complete

## What Was Built
- `migration.py` — `ReconAIPReprocessing` class with full migration pipeline
- `synthetic_data.py` — Deterministic dataset generator
- `cli_migrate.py` — CLI entry point
- `test_migration.py` — 16 tests

## Key Decisions
1. **No LLM in migration**: The plan references `SurfaceLlm` for SQL generation in `rule_2_sql()`. Since we're building from scratch without the Google ADK infrastructure, the migration utility does direct rule translation (JSON → JSON) rather than LLM-mediated SQL generation. LLM-based SQL generation can be added later when Phase 5 DuckDB operators need it.
2. **Synthetic data**: No real datasets available. Generated synthetic data matching the `asset/account` schema described in master doc §13 (402+ rows, Side A ~160, Side B ~242).
3. **Null-safe filter rewriting**: Migration automatically replaces raw column references (`ext_asset_info`, `sec_type`) with their null-safe computed variants in exclusion filters.

## Deviations from Plan
- `rule_2_sql()` is not implemented as a separate method — rule normalization happens inline in `migrate_rules()`. The LLM-based SQL generation described in the plan is deferred to Phase 5 when DuckDB operators need compiled SQL templates.
- `create_fat_schema()` signature takes no arguments (reads from `self.ruleset`) rather than `(computed_columns, schema_columns, target_path)`.

## How to Verify
```bash
cd versions/v1/backend
python -m pytest tests/test_migration.py -v  # 16 tests
```

Migration output at: `workspace/knowledgebase/rulesets/asset/account/`
Dataset at: `workspace/asset/account/datasets/active_experiment/dataset.csv`
