"""
Recon Matching Service — FastAPI server.

Exposes the ReconMatch RPC as a REST endpoint + a minimal frontend.
"""

import csv
import io
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from loguru import logger

# Resolve paths
APP_ROOT = Path(__file__).resolve().parents[3]
BACKEND_SRC = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(BACKEND_SRC))

from recon_matching.schemas import ReconMatchRequest
from recon_matching.paths import derive_kb_dir, derive_run_dir, validate_kb_dir
from recon_matching.scaffold import validate_and_prepare, ScaffoldError
from recon_matching.validators import validate_kb, validate_run_artifacts
from recon_matching.lro_client import LROClient
from recon_matching.migration import migrate_ruleset
from recon_matching.service import ReconMatchService

WORKSPACE_ROOT = APP_ROOT / "workspace"
DATASETS_ROOT = APP_ROOT / "datasets"

# Configure logging
log_dir = APP_ROOT / "logs" / date.today().isoformat()
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / f"{datetime.now().strftime('%H_%M_%S')}.log"
logger.add(str(log_file), rotation="1 MB", level="INFO")

app = FastAPI(title="Recon Matching Service", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# State
lro_client = LROClient()
recon_service = ReconMatchService(str(WORKSPACE_ROOT), lro_client)
_runs: dict[str, dict] = {}  # run_id -> run metadata


# ─── API Endpoints ──────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.post("/api/recon-match")
def recon_match(request: ReconMatchRequest):
    """
    Start a reconciliation matching run.
    Phase 0-1: validates KB, creates RUN_DIR, returns LRO ID.
    """
    logger.info("ReconMatch request: workspace={}, ruleset={}, run_id={}",
                request.workspace, request.ruleset, request.run_id)

    kb_dir = derive_kb_dir(str(WORKSPACE_ROOT), request.workspace, request.ruleset)
    run_dir = derive_run_dir(str(WORKSPACE_ROOT), request.workspace, request.ruleset, request.run_id)

    # Validate KB
    kb_issues = validate_kb(kb_dir)
    total_issues = sum(len(v) for v in kb_issues.values())
    if total_issues > 0:
        return JSONResponse(status_code=400, content={
            "error": "KB validation failed",
            "details": {k: v for k, v in kb_issues.items() if v},
        })

    # Scaffold
    try:
        validate_and_prepare(kb_dir, run_dir)
    except ScaffoldError as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    # Create LRO
    op_id = lro_client.create()

    # Store run metadata
    _runs[request.run_id] = {
        "run_id": request.run_id,
        "workspace": request.workspace,
        "ruleset": request.ruleset,
        "dataset_path": request.dataset_absolute_path,
        "kb_dir": str(kb_dir),
        "run_dir": str(run_dir),
        "lro_id": op_id,
        "status": "CREATED",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info("Run {} created, LRO={}, RUN_DIR={}", request.run_id, op_id, run_dir)

    return {
        "run_id": request.run_id,
        "lro_id": op_id,
        "status": "CREATED",
        "run_dir": str(run_dir),
        "kb_dir": str(kb_dir),
    }


@app.post("/api/recon-match/execute")
def recon_match_execute(request: ReconMatchRequest):
    """
    Execute a full reconciliation matching run (boot + waterfall + finalize).
    Returns completed result with summary.
    """
    logger.info("ReconMatch EXECUTE: workspace={}, ruleset={}, run_id={}",
                request.workspace, request.ruleset, request.run_id)

    result = recon_service.execute(request)
    _runs[request.run_id] = result
    return result


@app.get("/api/runs")
def list_runs():
    """List all tracked runs."""
    return {"runs": list(_runs.values())}


@app.get("/api/runs/{run_id}")
def get_run(run_id: str):
    """Get status of a specific run."""
    if run_id not in _runs:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")
    return _runs[run_id]


@app.get("/api/lro/{op_id}")
def get_lro(op_id: str):
    """Get LRO status."""
    try:
        return lro_client.get(op_id)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"LRO not found: {op_id}")


@app.get("/api/kb/{workspace}/{ruleset}")
def get_kb_status(workspace: str, ruleset: str):
    """Validate and return KB status for a workspace/ruleset."""
    kb_dir = derive_kb_dir(str(WORKSPACE_ROOT), workspace, ruleset)
    if not kb_dir.exists():
        raise HTTPException(status_code=404, detail=f"KB not found: {kb_dir}")

    issues = validate_kb(kb_dir)
    files = {f.name: f.stat().st_size for f in kb_dir.iterdir() if f.is_file()}

    return {
        "workspace": workspace,
        "ruleset": ruleset,
        "kb_dir": str(kb_dir),
        "files": files,
        "validation": {k: v for k, v in issues.items()},
        "valid": all(len(v) == 0 for v in issues.values()),
    }


@app.post("/api/migrate")
def run_migration(ruleset_path: str, target_workspace: str, target_ruleset: str):
    """Run Phase 0 migration on a ruleset.json file."""
    full_path = DATASETS_ROOT / ruleset_path if not Path(ruleset_path).is_absolute() else Path(ruleset_path)
    if not full_path.exists():
        raise HTTPException(status_code=404, detail=f"Ruleset not found: {full_path}")

    target_dir = derive_kb_dir(str(WORKSPACE_ROOT), target_workspace, target_ruleset)
    summary = migrate_ruleset(str(full_path), str(target_dir))
    return {"status": "ok", "summary": summary}


# ─── File Browser APIs ──────────────────────────────────────────────────────

def _safe_resolve(base: Path, relative: str) -> Path:
    """Resolve a relative path safely within base. Raises on traversal."""
    resolved = (base / relative).resolve()
    if not str(resolved).startswith(str(base.resolve())):
        raise HTTPException(status_code=403, detail="Path traversal blocked")
    return resolved


@app.get("/api/kb/{workspace}/{ruleset}/files")
def list_kb_files(workspace: str, ruleset: str):
    """List all files in KB directory with metadata."""
    kb_dir = derive_kb_dir(str(WORKSPACE_ROOT), workspace, ruleset)
    if not kb_dir.exists():
        raise HTTPException(status_code=404, detail="KB not found")

    files = []
    for f in sorted(kb_dir.iterdir()):
        if f.is_file():
            files.append({
                "name": f.name,
                "size": f.stat().st_size,
                "modified": datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).isoformat(),
                "type": f.suffix.lstrip(".") or "unknown",
            })
        elif f.is_dir():
            files.append({
                "name": f.name + "/",
                "size": 0,
                "modified": "",
                "type": "dir",
            })
    return {"files": files}


@app.get("/api/kb/{workspace}/{ruleset}/file/{filename:path}")
def read_kb_file(workspace: str, ruleset: str, filename: str):
    """Read a KB file's content."""
    kb_dir = derive_kb_dir(str(WORKSPACE_ROOT), workspace, ruleset)
    filepath = _safe_resolve(kb_dir, filename)
    if not filepath.exists() or not filepath.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    ext = filepath.suffix.lower()
    raw = filepath.read_text(errors="replace")

    if ext == ".json":
        try:
            parsed = json.loads(raw)
            return {"filename": filename, "type": "json", "content": parsed, "size": len(raw)}
        except json.JSONDecodeError:
            pass

    if ext == ".csv":
        reader = csv.DictReader(io.StringIO(raw))
        rows = list(reader)
        return {
            "filename": filename,
            "type": "csv",
            "headers": reader.fieldnames or [],
            "rows": rows[:500],
            "total_rows": len(rows),
            "truncated": len(rows) > 500,
            "size": len(raw),
        }

    # md, sql, txt, etc
    return {"filename": filename, "type": ext.lstrip(".") or "text", "content": raw, "size": len(raw)}


@app.get("/api/outputs")
def list_output_dates():
    """List dates that have output runs."""
    outputs_dir = WORKSPACE_ROOT / "outputs"
    if not outputs_dir.exists():
        return {"dates": []}
    dates = sorted([d.name for d in outputs_dir.iterdir() if d.is_dir()], reverse=True)
    return {"dates": dates}


@app.get("/api/outputs/{run_date}/{workspace}/{ruleset}")
def list_runs_for_date(run_date: str, workspace: str, ruleset: str):
    """List all run_ids for a given date/workspace/ruleset."""
    runs_dir = WORKSPACE_ROOT / "outputs" / run_date / workspace / ruleset
    if not runs_dir.exists():
        return {"runs": []}

    runs = []
    for d in sorted(runs_dir.iterdir(), reverse=True):
        if d.is_dir():
            manifest_path = d / "run_manifest.json"
            summary = {}
            if manifest_path.exists():
                try:
                    m = json.loads(manifest_path.read_text())
                    summary = m.get("summary", {})
                except Exception:
                    pass
            runs.append({"run_id": d.name, "summary": summary})
    return {"runs": runs}


@app.get("/api/outputs/{run_date}/{workspace}/{ruleset}/{run_id}/files")
def list_run_files(run_date: str, workspace: str, ruleset: str, run_id: str):
    """List all files in a run output directory."""
    run_dir = WORKSPACE_ROOT / "outputs" / run_date / workspace / ruleset / run_id
    if not run_dir.exists():
        raise HTTPException(status_code=404, detail="Run not found")

    files = []
    for f in sorted(run_dir.iterdir()):
        if f.is_file():
            files.append({
                "name": f.name,
                "size": f.stat().st_size,
                "modified": datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).isoformat(),
                "type": f.suffix.lstrip(".") or "unknown",
            })
    return {"run_dir": str(run_dir), "files": files}


@app.get("/api/outputs/{run_date}/{workspace}/{ruleset}/{run_id}/file/{filename:path}")
def read_run_file(run_date: str, workspace: str, ruleset: str, run_id: str, filename: str,
                  offset: int = Query(0, ge=0), limit: int = Query(500, ge=1, le=5000)):
    """Read a run output file. CSV files support pagination via offset/limit."""
    run_dir = WORKSPACE_ROOT / "outputs" / run_date / workspace / ruleset / run_id
    filepath = _safe_resolve(run_dir, filename)
    if not filepath.exists() or not filepath.is_file():
        raise HTTPException(status_code=404, detail=f"File not found: {filename}")

    ext = filepath.suffix.lower()
    raw = filepath.read_text(errors="replace")

    if ext == ".json":
        try:
            parsed = json.loads(raw)
            return {"filename": filename, "type": "json", "content": parsed, "size": len(raw)}
        except json.JSONDecodeError:
            pass

    if ext == ".csv":
        reader = csv.DictReader(io.StringIO(raw))
        all_rows = list(reader)
        page = all_rows[offset:offset + limit]
        return {
            "filename": filename,
            "type": "csv",
            "headers": reader.fieldnames or [],
            "rows": page,
            "total_rows": len(all_rows),
            "offset": offset,
            "limit": limit,
            "size": len(raw),
        }

    # md, sql, duckdb, txt
    if ext == ".duckdb":
        return {"filename": filename, "type": "binary", "size": len(raw), "content": "(binary DuckDB file)"}

    return {"filename": filename, "type": ext.lstrip(".") or "text", "content": raw, "size": len(raw)}


# ─── Frontend ───────────────────────────────────────────────────────────────

FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"

app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


@app.get("/")
def serve_index():
    return FileResponse(str(FRONTEND_DIR / "index.html"))


logger.info("Recon Matching Service initialized. Workspace: {}", WORKSPACE_ROOT)
