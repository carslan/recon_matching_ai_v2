"""
Phase 1 — KB and output artifact validators.

Validates JSON structure, operator allowlists, and run artifact completeness.
"""

import json
from pathlib import Path
from typing import Any

from .schemas import OPERATOR_ALLOWLIST, OperatorType


class ValidationError(Exception):
    pass


# ─── KB Validators ──────────────────────────────────────────────────────────

def validate_json_file(path: Path) -> dict:
    """Load and parse a JSON file. Raises ValidationError on failure."""
    if not path.exists():
        raise ValidationError(f"File not found: {path}")
    try:
        with open(path) as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON in {path}: {e}")


def validate_schema_structure(data: dict) -> list[str]:
    """Validate schema_structure.json structure. Returns list of issues."""
    issues = []
    if "columns" not in data:
        issues.append("Missing 'columns' key")
    if "number_columns" not in data:
        issues.append("Missing 'number_columns' key")
    if "dataset_type" not in data:
        issues.append("Missing 'dataset_type' key")

    if "columns" in data:
        for i, col in enumerate(data["columns"]):
            if "name" not in col:
                issues.append(f"Column {i} missing 'name'")
            if "type" not in col:
                issues.append(f"Column {i} missing 'type'")

    return issues


def validate_features(data: dict) -> list[str]:
    """Validate features.json structure."""
    issues = []
    if "blocking_strategies" not in data:
        issues.append("Missing 'blocking_strategies' key")
    if "amount_policy" not in data:
        issues.append("Missing 'amount_policy' key")
    return issues


def validate_rules_file(data: dict) -> list[str]:
    """Validate core_matching_rules.json or agent_matching_rules.json."""
    issues = []
    if "rules" not in data:
        issues.append("Missing 'rules' key")
        return issues

    allowed_ops = {op.value for op in OperatorType}

    for i, rule in enumerate(data["rules"]):
        if "rule_id" not in rule:
            issues.append(f"Rule {i} missing 'rule_id'")
        if "operator" not in rule:
            issues.append(f"Rule {i} missing 'operator'")
        elif rule["operator"] not in allowed_ops:
            issues.append(f"Rule {i} has unknown operator: {rule['operator']}")
        if "status" not in rule:
            issues.append(f"Rule {i} missing 'status'")

        # Exclusion rules must have EXCL_ prefix
        if rule.get("operator") == "exclude_by_predicates":
            rule_id = rule.get("rule_id", "")
            if not rule_id.startswith("EXCL_"):
                issues.append(f"Exclusion rule '{rule_id}' missing EXCL_ prefix")

    return issues


def validate_kb(kb_dir: Path) -> dict[str, list[str]]:
    """
    Full KB validation. Returns dict of {filename: [issues]}.
    Empty issues list = valid.
    """
    results: dict[str, list[str]] = {}

    # schema_structure.json
    try:
        schema = validate_json_file(kb_dir / "schema_structure.json")
        results["schema_structure.json"] = validate_schema_structure(schema)
    except ValidationError as e:
        results["schema_structure.json"] = [str(e)]

    # features.json
    try:
        features = validate_json_file(kb_dir / "features.json")
        results["features.json"] = validate_features(features)
    except ValidationError as e:
        results["features.json"] = [str(e)]

    # core_matching_rules.json
    try:
        core_rules = validate_json_file(kb_dir / "core_matching_rules.json")
        results["core_matching_rules.json"] = validate_rules_file(core_rules)
    except ValidationError as e:
        results["core_matching_rules.json"] = [str(e)]

    # agent_matching_rules.json
    try:
        agent_rules = validate_json_file(kb_dir / "agent_matching_rules.json")
        results["agent_matching_rules.json"] = validate_rules_file(agent_rules)
    except ValidationError as e:
        results["agent_matching_rules.json"] = [str(e)]

    return results


# ─── Output Artifact Validators ────────────────────────────────────────────

REQUIRED_RUN_ARTIFACTS = [
    "dataset_file.csv",
    "matches.csv",
    "breaks.csv",
    "exclusions.csv",
    "matches_reasoning.json",
    "breaks_reasoning.json",
    "exclusions_reasoning.json",
    "matching_queries.sql",
    "run_manifest.json",
]


def validate_run_artifacts(run_dir: Path) -> list[str]:
    """Validate that RUN_DIR contains all required artifacts. Returns missing files."""
    missing = []
    for f in REQUIRED_RUN_ARTIFACTS:
        if not (run_dir / f).exists():
            missing.append(f)
    return missing
