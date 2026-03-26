"""
Phase 1 — Path derivation utilities.

Deterministic path resolution for KB_DIR and RUN_DIR from request parameters.
"""

from datetime import date
from pathlib import Path


def derive_kb_dir(base_dir: str, workspace: str, ruleset: str) -> Path:
    """Derive KB directory path: base/knowledgebase/rulesets/{workspace}/{ruleset}/"""
    return Path(base_dir) / "knowledgebase" / "rulesets" / workspace / ruleset


def derive_run_dir(base_dir: str, workspace: str, ruleset: str, run_id: str, run_date: date | None = None) -> Path:
    """Derive RUN directory path: base/outputs/{date}/{workspace}/{ruleset}/{run_id}/"""
    d = run_date or date.today()
    return Path(base_dir) / "outputs" / d.isoformat() / workspace / ruleset / run_id


def validate_kb_dir(kb_dir: Path) -> list[str]:
    """
    Validate that KB_DIR exists and contains required files.
    Returns list of missing files (empty = valid).
    """
    required_files = [
        "schema_structure.json",
        "features.json",
        "core_matching_rules.json",
        "agent_matching_rules.json",
        "identity.md",
        "soul.md",
        "how_to_work.md",
    ]

    if not kb_dir.exists():
        return [f"KB directory does not exist: {kb_dir}"]

    missing = []
    for f in required_files:
        if not (kb_dir / f).exists():
            missing.append(f)

    return missing
