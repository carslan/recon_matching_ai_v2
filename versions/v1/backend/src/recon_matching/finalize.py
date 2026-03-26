"""
Phase 6 — Run Finalization

After all waterfall steps complete:
1. Export remaining pool_unmatched → breaks.csv
2. Write breaks_reasoning.json
3. Export exclusions.csv and matches.csv
4. Seal run_manifest.json with summary
5. Validate all required artifacts present
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb
from loguru import logger

from .duckdb_skill import (
    duckdb_export_exclusions_csv, duckdb_export_matches_csv,
    duckdb_export_breaks_csv, duckdb_get_pool_count,
)
from .validators import validate_run_artifacts


def write_breaks_reasoning(db_path: str, run_dir: str, workspace: str, ruleset: str, run_id: str) -> int:
    """
    Write breaks_reasoning.json from pool_unmatched.
    Each break record gets a business-meaningful entry.
    Returns count of break items written.
    """
    conn = duckdb.connect(db_path)
    try:
        breaks = conn.execute("""
            SELECT side, statement_id, record_id, blk_cusip, sec_id, orig_face
            FROM pool_unmatched
            ORDER BY side, record_id
        """).fetchall()
    finally:
        conn.close()

    break_items = []
    for row in breaks:
        side, stmt_id, rec_id, blk_cusip, sec_id, orig_face = row

        # Determine reason
        reasons = []
        if not blk_cusip or blk_cusip == "":
            reasons.append("MISSING_BLOCKING_KEY")
        else:
            reasons.append("NO_COUNTERPART_IN_BLOCK")

        break_items.append({
            "record_ref": {
                "side": str(side),
                "statement_id": str(stmt_id),
                "record_id": str(rec_id),
            },
            "top_reasons": reasons,
            "details": {
                "attempted_blocking_strategies": ["fund+blk_cusip", "fund+sec_id"],
                "notes": f"Record remained unmatched after all waterfall steps. blk_cusip={blk_cusip}, orig_face={orig_face}",
            },
            "closest_candidates": [],
        })

    reasoning = {
        "workspace": workspace,
        "ruleset": ruleset,
        "run_id": run_id,
        "breaks": break_items,
    }

    out_path = Path(run_dir) / "breaks_reasoning.json"
    with open(out_path, "w") as f:
        json.dump(reasoning, f, indent=2)

    logger.info("Wrote {} break reasoning items to {}", len(break_items), out_path)
    return len(break_items)


def finalize_run(
    db_path: str,
    run_dir: str,
    workspace: str,
    ruleset: str,
    run_id: str,
    waterfall_results: list[dict],
    started_at: datetime,
) -> dict:
    """
    Complete run finalization:
    1. Export all CSVs (exclusions, matches, breaks)
    2. Write breaks_reasoning.json
    3. Seal run_manifest.json
    4. Validate artifacts
    Returns the final manifest summary.
    """
    run_path = Path(run_dir)
    finished_at = datetime.now(timezone.utc)

    # Export CSVs
    duckdb_export_exclusions_csv(db_path, run_dir)
    duckdb_export_matches_csv(db_path, run_dir)
    duckdb_export_breaks_csv(db_path, run_dir)

    # Write breaks reasoning
    break_count = write_breaks_reasoning(db_path, run_dir, workspace, ruleset, run_id)

    # Ensure exclusions_reasoning.json exists (may have been written incrementally by executor)
    excl_path = run_path / "exclusions_reasoning.json"
    if not excl_path.exists():
        with open(excl_path, "w") as f:
            json.dump({"workspace": workspace, "ruleset": ruleset, "run_id": run_id, "exclusions": []}, f, indent=2)

    # Ensure matches_reasoning.json exists
    match_reason_path = run_path / "matches_reasoning.json"
    if not match_reason_path.exists():
        with open(match_reason_path, "w") as f:
            json.dump({"workspace": workspace, "ruleset": ruleset, "run_id": run_id, "match_groups": []}, f, indent=2)

    # Ensure matching_queries.sql exists
    sql_path = run_path / "matching_queries.sql"
    if not sql_path.exists():
        sql_path.write_text("-- No queries executed\n")

    # Compute summary
    conn = duckdb.connect(db_path)
    try:
        total_input = conn.execute("SELECT COUNT(*) FROM base_dataset").fetchone()[0]
        total_excluded = conn.execute(
            "SELECT COUNT(*) FROM matched_records WHERE operator = 'exclude_by_predicates'"
        ).fetchone()[0]
        total_matched = conn.execute(
            "SELECT COUNT(*) FROM matched_records WHERE operator != 'exclude_by_predicates'"
        ).fetchone()[0]
        total_breaks = conn.execute("SELECT COUNT(*) FROM pool_unmatched").fetchone()[0]
    finally:
        conn.close()

    summary = {
        "total_input_records": total_input,
        "total_excluded": total_excluded,
        "total_matched": total_matched,
        "total_breaks": total_breaks,
    }

    # Seal manifest
    manifest_path = run_path / "run_manifest.json"
    with open(manifest_path) as f:
        manifest = json.load(f)

    manifest["created_at"] = started_at.isoformat()
    manifest["finished_at"] = finished_at.isoformat()
    manifest["waterfall"] = waterfall_results
    manifest["summary"] = summary
    manifest["outputs"] = {
        "exclusions_csv": "exclusions.csv",
        "matches_csv": "matches.csv",
        "breaks_csv": "breaks.csv",
        "exclusions_reasoning_json": "exclusions_reasoning.json",
        "matches_reasoning_json": "matches_reasoning.json",
        "breaks_reasoning_json": "breaks_reasoning.json",
        "matching_queries_sql": "matching_queries.sql",
    }

    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    # Validate artifacts
    missing = validate_run_artifacts(run_path)
    if missing:
        logger.warning("Missing artifacts after finalization: {}", missing)
    else:
        logger.info("All required artifacts present")

    logger.info(
        "Run finalized: {} input, {} excluded, {} matched, {} breaks",
        total_input, total_excluded, total_matched, total_breaks,
    )

    return summary
