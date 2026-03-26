# Features

## Phase 0 — Migration Utility
**Status:** active
`migration.py` — Converts legacy `ruleset.json` into KB-ready files: `schema_structure.json`, `core_matching_rules.json`, `features.json`, `agent_matching_rules.json`, plus markdown files (identity.md, soul.md, how_to_work.md). Strips `VALUE_TYPE_` prefix from types, enforces `EXCL_` prefix on exclusion rules, applies null-safe column references in filters. CLI: `python -m recon_matching.cli_migrate`.

## Synthetic Dataset Generator
**Status:** active
`synthetic_data.py` — Generates deterministic (seeded) CSV datasets with isolated cusip ranges per scenario. Creates verifiable scenarios: 15 A+B exclusions (EXCL_CUST_DERIV), 5 A+B exclusions (EXCL_COLLATERAL), 5 A exclusions (EXCL_FX_HEDGE), 10 1:1 FUND matches, 50 1:1 BLKCUSIP matches, 10 many-to-many balanced groups (1A+2B), 6 one-sided netting pairs, remainder breaks.

## Phase 1 — Pydantic Wire Schemas + Validators
**Status:** active
`schemas.py` — 9 Pydantic models: ReconMatchRequest, RulesetRuntimeConfig, DatasetProfile, OperatorSpec, OperatorResult, ExecutorRequest, ExecutorResponse, MatchGroup, BreakItem. Plus enums (OperatorType, StepStatus, DatasetType).
`validators.py` — KB file validation (JSON parse + structure), operator allowlist, EXCL_ prefix enforcement, output artifact completeness check.
`paths.py` — Deterministic KB_DIR and RUN_DIR derivation.
`scaffold.py` — RUN_DIR creation + KB_DIR validation.
`lro_client.py` — LROClient mock (create/complete/get).

## Phase 2 — KB Indexer
**Status:** active
`kb_indexer.py` — Offline indexer: scans KB directory, splits markdown by headings with byte offsets and tags, extracts operator inventory from rules JSON, writes `kb_index.json`. Runtime uses offsets for minimal reads. CLI: `python -m recon_matching.kb_indexer`.

## Phase 3 — Boot Pipeline
**Status:** active
`boot.py` — Deterministic cold-start in 3 phases:
- Phase A: resolve KB_DIR + RUN_DIR, validate KB, create RUN_DIR
- Phase B: load kb_index.json, fetch critical markdown sections by byte offset, load 4 JSON configs
- Phase C: compile RulesetRuntimeConfig (merge active core + agent rules, exclusions first), persist compiled_waterfall.json
- Get-ready: copy dataset to RUN_DIR, write run_manifest.json skeleton

## Phase 4 — Orchestrator + Executor + Guardrail
**Status:** active
`orchestrator.py` — Iterates compiled_waterfall.json step by step. Dispatches ExecutorRequest per step. Collects ExecutorResponse. Enforces stop conditions (3 consecutive zero-progress steps, pool empty). Seals run manifest. All interactions logged to InteractionLog.
`executor.py` — DuckDB-backed. Initializes DB + pool on first call. Executes operator via duckdb_skill. Stores reasoning incrementally.
`guardrail.py` — Write-path enforcement: only allows writes to RUN_DIR and agent_rule_proposals/.
`service.py` — ReconMatchService: boot → orchestrate → finalize → return LRO.

## Phase 5 — DuckDB Skill + 4 Operators
**Status:** active
`duckdb_skill.py` — Narrow API, no freestyle SQL. All SQL is template-driven from operator specs.
- `exclude_by_predicates`: side-specific SQL predicate filters. Inserts excluded records into matched_records.
- `one_to_one_ranked`: blocking join + amount tolerance + mutual best-match via ROW_NUMBER window functions. Deterministic tie-breakers.
- `many_to_many_balance_k`: aggregated sums per blocking group. SUM(A) ≈ SUM(B) within tolerance. Disjoint selection.
- `one_sided`: A-vs-A self-matching. Groups by blocking key where SUM(amount) nets to ~0.
- `_qualify_filter()`: auto-prefixes unqualified column names with table alias to prevent ambiguous references.
- Dataset profiling: side counts, amount column detection, blocking quality coverage rates.
- `matching_queries.sql`: append-only SQL audit trail per step.

## Phase 6 — Reasoning + Finalization
**Status:** active
`finalize.py` — Run finalization:
- Export exclusions.csv, matches.csv, breaks.csv (disjoint, verified)
- Write breaks_reasoning.json with business-meaningful reasons
- Seal run_manifest.json with summary (total_input = excluded + matched + breaks)
- Validate all 9 required artifacts present
- matches_reasoning.json and exclusions_reasoning.json written incrementally during waterfall by executor

## Agent Interaction Logging
**Status:** active
`interaction_log.py` — Captures every message between agents. 66 events per run covering boot, orchestrator decisions, executor requests/results, DuckDB SQL, pool state changes, errors, finalization. Saved as `interaction_log.json` in RUN_DIR.

## FastAPI Server
**Status:** active
`server.py` — Port 4017. Endpoints:
- POST `/api/recon-match` — create run (validate + scaffold only)
- POST `/api/recon-match/execute` — full run (boot + waterfall + finalize)
- GET `/api/runs`, `/api/runs/{run_id}` — run listing/status
- GET `/api/kb/{ws}/{rs}`, `/api/kb/{ws}/{rs}/files`, `/api/kb/{ws}/{rs}/file/{name}` — KB browsing + file reading
- GET `/api/outputs`, `/api/outputs/{date}/{ws}/{rs}`, `/api/outputs/.../files`, `/api/outputs/.../file/{name}` — output browsing + file reading with CSV pagination
- GET `/api/health`, `/api/lro/{id}` — health + LRO status
- Path traversal protection on all file read endpoints.

## Frontend Dashboard
**Status:** active
`index.html` — 4-tab UI:
- **Run**: execute matching runs, see summary stats + waterfall progress bars
- **Knowledge Base**: browse and read KB files (JSON highlighted, MD rendered)
- **Results**: browse run outputs, read files (CSV as paginated tables, JSON highlighted, SQL highlighted)
- **Agent Logs**: filterable interaction timeline color-coded by actor + level, expandable SQL/JSON details
