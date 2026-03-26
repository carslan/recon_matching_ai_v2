"""
Phase 3 — Runtime Boot Pipeline

Deterministic cold-start:
  Phase A: resolve paths (KB_DIR, RUN_DIR), create RUN_DIR
  Phase B: minimal reads via kb_index (sections by offset + JSON configs)
  Phase C: compile RulesetRuntimeConfig, persist compiled artifacts
"""

import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

from loguru import logger

from .kb_indexer import read_section_by_offset
from .paths import derive_kb_dir, derive_run_dir
from .scaffold import validate_and_prepare
from .schemas import (
    RulesetRuntimeConfig, KBPaths, AmountPolicy, Guardrails,
    BlockingStrategy, OperatorType,
)
from .validators import validate_kb, ValidationError


class BootError(Exception):
    pass


class BootResult:
    """Result of the boot pipeline. Contains everything needed to start the run loop."""

    def __init__(self):
        self.kb_dir: Path = Path()
        self.run_dir: Path = Path()
        self.kb_index: dict = {}
        self.kb_sections: dict[str, str] = {}  # "file:tag" -> content
        self.schema_structure: dict = {}
        self.features: dict = {}
        self.core_rules: dict = {}
        self.agent_rules: dict = {}
        self.runtime_config: RulesetRuntimeConfig | None = None
        self.compiled_waterfall: list[dict] = []


def phase_a(workspace_root: str, workspace: str, ruleset: str, run_id: str, run_date: date | None = None) -> tuple[Path, Path]:
    """Phase A: Resolve paths, validate KB, create RUN_DIR."""
    kb_dir = derive_kb_dir(workspace_root, workspace, ruleset)
    run_dir = derive_run_dir(workspace_root, workspace, ruleset, run_id, run_date)

    validate_and_prepare(kb_dir, run_dir)
    logger.info("Phase A complete: KB_DIR={}, RUN_DIR={}", kb_dir, run_dir)
    return kb_dir, run_dir


def phase_b(kb_dir: Path) -> dict[str, Any]:
    """
    Phase B: Minimal reads.
    Load kb_index.json, fetch critical markdown sections by offset, load JSON configs.
    Returns dict with all loaded data.
    """
    # Load kb_index
    index_path = kb_dir / "kb_index.json"
    if not index_path.exists():
        raise BootError(f"kb_index.json not found at {kb_dir}. Run kb_indexer first.")

    with open(index_path) as f:
        kb_index = json.load(f)

    # Fetch critical markdown sections by offset
    CRITICAL_SECTIONS = {
        "identity.md": ["mission", "outputs"],
        "soul.md": ["guardrails", "write_paths"],
        "how_to_work.md": ["waterfall", "stop_conditions"],
    }

    kb_sections: dict[str, str] = {}
    for section in kb_index.get("markdown_sections", []):
        file_name = section.get("file", "")
        tags = section.get("tags", [])

        if file_name in CRITICAL_SECTIONS:
            needed_tags = CRITICAL_SECTIONS[file_name]
            if any(tag in needed_tags for tag in tags):
                filepath = kb_dir / file_name
                content = read_section_by_offset(filepath, section["byte_offset"], section["byte_length"])
                key = f"{file_name}:{','.join(tags)}"
                kb_sections[key] = content

    # Load JSON configs in full
    configs = {}
    for config_name in ["schema_structure.json", "features.json", "core_matching_rules.json", "agent_matching_rules.json"]:
        config_path = kb_dir / config_name
        with open(config_path) as f:
            configs[config_name] = json.load(f)

    # Validate
    kb_issues = validate_kb(kb_dir)
    total_issues = sum(len(v) for v in kb_issues.values())
    if total_issues > 0:
        raise BootError(f"KB validation failed: {kb_issues}")

    logger.info(
        "Phase B complete: loaded {} sections, {} JSON configs",
        len(kb_sections), len(configs),
    )

    return {
        "kb_index": kb_index,
        "kb_sections": kb_sections,
        "schema_structure": configs["schema_structure.json"],
        "features": configs["features.json"],
        "core_rules": configs["core_matching_rules.json"],
        "agent_rules": configs["agent_matching_rules.json"],
    }


def phase_c(
    kb_dir: Path,
    run_dir: Path,
    workspace: str,
    ruleset: str,
    features: dict,
    core_rules: dict,
    agent_rules: dict,
) -> tuple[RulesetRuntimeConfig, list[dict]]:
    """
    Phase C: Compile RulesetRuntimeConfig and waterfall.
    Persist compiled_ruleset_runtime_config.json and compiled_waterfall.json.
    """
    # Build blocking strategies
    blocking_strategies = [
        BlockingStrategy(**bs) for bs in features.get("blocking_strategies", [])
    ]

    # Build amount policy
    ap = features.get("amount_policy", {})
    amount_policy = AmountPolicy(tolerance=ap.get("tolerance", 0.005))

    # Build runtime config
    runtime_config = RulesetRuntimeConfig(
        workspace=workspace,
        ruleset=ruleset,
        kb=KBPaths(
            ruleset_dir=str(kb_dir),
            kb_index_path=str(kb_dir / "kb_index.json"),
        ),
        blocking={"strategies": [bs.model_dump() for bs in blocking_strategies]},
        amount_policy=amount_policy,
        guardrails=Guardrails(
            allowed_output_root=str(run_dir.parent.parent.parent.parent),
            allowed_kb_proposal_root=str(kb_dir.parent.parent.parent),
        ),
    )

    # Compile waterfall: merge active core + active agent rules
    # Exclusion rules ALWAYS first, regardless of source order
    all_rules = []
    for rule in core_rules.get("rules", []):
        if rule.get("status") == "active":
            all_rules.append({**rule, "source": "core"})
    for rule in agent_rules.get("rules", []):
        if rule.get("status") == "active":
            all_rules.append({**rule, "source": "agent"})

    exclusion_rules = [r for r in all_rules if r["operator"] == "exclude_by_predicates"]
    matching_rules = [r for r in all_rules if r["operator"] != "exclude_by_predicates"]

    waterfall = []
    step_id = 1
    for rule in exclusion_rules + matching_rules:
        waterfall.append({
            "step_id": step_id,
            "rule_id": rule["rule_id"],
            "source": rule["source"],
            "operator": rule["operator"],
            "status": rule["status"],
            "params": rule.get("params", {}),
        })
        step_id += 1

    # Persist
    config_path = run_dir / "compiled_ruleset_runtime_config.json"
    with open(config_path, "w") as f:
        json.dump(runtime_config.model_dump(), f, indent=2)

    waterfall_path = run_dir / "compiled_waterfall.json"
    with open(waterfall_path, "w") as f:
        json.dump(waterfall, f, indent=2)

    logger.info(
        "Phase C complete: {} exclusion + {} matching = {} waterfall steps",
        len(exclusion_rules), len(matching_rules), len(waterfall),
    )

    return runtime_config, waterfall


def get_ready(run_dir: Path, dataset_path: str, workspace: str, ruleset: str, run_id: str) -> Path:
    """
    Get-ready: copy dataset to RUN_DIR, write run_manifest.json skeleton.
    Returns the path to the copied dataset.
    """
    # Copy dataset
    src = Path(dataset_path)
    if not src.is_absolute():
        # Try relative to app root
        app_root = run_dir.parents[5] if len(run_dir.parts) > 5 else run_dir.parent
        src = app_root / dataset_path
    if not src.exists():
        raise BootError(f"Dataset not found: {src}")

    dst = run_dir / "dataset_file.csv"
    shutil.copy2(src, dst)

    # Write manifest skeleton
    manifest = {
        "run_id": run_id,
        "created_at": None,  # Set by orchestrator
        "finished_at": None,
        "workspace": workspace,
        "ruleset": ruleset,
        "knowledgebase": {
            "path": str(run_dir.parent.parent.parent.parent / "knowledgebase" / "rulesets" / workspace / ruleset),
        },
        "dataset": {
            "source_absolute_path": str(src),
            "replay_copy_relative_path": "dataset_file.csv",
        },
        "config": {},
        "waterfall": [],
        "outputs": {},
        "summary": {},
    }

    manifest_path = run_dir / "run_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)

    logger.info("Get-ready complete: dataset copied, manifest skeleton written")
    return dst


def boot(
    workspace_root: str,
    workspace: str,
    ruleset: str,
    run_id: str,
    dataset_path: str,
    run_date: date | None = None,
) -> BootResult:
    """
    Full boot pipeline: Phase A + B + C + get-ready.
    Returns a BootResult with everything needed for the run loop.
    """
    result = BootResult()

    # Phase A
    result.kb_dir, result.run_dir = phase_a(workspace_root, workspace, ruleset, run_id, run_date)

    # Phase B
    loaded = phase_b(result.kb_dir)
    result.kb_index = loaded["kb_index"]
    result.kb_sections = loaded["kb_sections"]
    result.schema_structure = loaded["schema_structure"]
    result.features = loaded["features"]
    result.core_rules = loaded["core_rules"]
    result.agent_rules = loaded["agent_rules"]

    # Phase C
    result.runtime_config, result.compiled_waterfall = phase_c(
        result.kb_dir, result.run_dir,
        workspace, ruleset,
        result.features, result.core_rules, result.agent_rules,
    )

    # Get-ready
    get_ready(result.run_dir, dataset_path, workspace, ruleset, run_id)

    logger.info("Boot complete for run_id={}", run_id)
    return result
