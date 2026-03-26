# Dev Log — Phase 1: Project Skeleton & Contracts

**Date:** 2026-03-26
**Status:** Complete

## What Was Built
- `schemas.py` — All 9 Pydantic wire schemas + enums
- `paths.py` — Deterministic path derivation for KB_DIR and RUN_DIR
- `scaffold.py` — RUN_DIR creation + KB_DIR validation
- `validators.py` — KB file validators + output artifact validators
- `lro_client.py` — LROClient mock (create/complete/get)
- `server.py` — FastAPI REST API (port 4017)
- `index.html` — Frontend dashboard
- `test_phase1.py` — 25 tests

## Key Decisions
1. **FastAPI over gRPC**: The plan describes `ReconMatch` as a gRPC-like RPC. For this implementation, REST via FastAPI is simpler and matches the LocalAppStore pattern. The `LROClient` mock preserves the async operation semantics.
2. **Pydantic v2**: Using `model_dump()` / `model_validate()` — modern Pydantic.
3. **Union types for blocking**: `OperatorParams.blocking_strategy` accepts both `TwoSidedBlockingSpec` (keys_a/keys_b) and `OneSidedBlockingSpec` (keys) via Python union type.

## Deviations from Plan
- No `agraph-ai_platform` or `agraph-platform` dependencies — not available and not needed until production integration.
- `google-adk` not installed yet — needed in Phase 4.

## How to Verify
```bash
cd versions/v1/backend
python -m pytest tests/test_phase1.py -v  # 25 tests
bash ../../../start.sh  # Server on port 4017
curl http://localhost:4017/api/health
curl http://localhost:4017/api/kb/asset/account
```
