"""
Phase 4 — Orchestrator

Drives the run loop: iterates compiled_waterfall.json, invokes Executor per step,
collects results, appends to run_manifest.json, enforces stop conditions.
All interactions are captured in the InteractionLog.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from .executor import Executor
from .finalize import finalize_run
from .guardrail import WriteGuardrail
from .interaction_log import InteractionLog
from .schemas import (
    ExecutorRequest, ExecutorRequestType, RunContext, PoolState, ExecutionMode,
    OperatorSpec, OperatorParams, OperatorType, StepStatus,
    TwoSidedBlockingSpec, OneSidedBlockingSpec,
)


MAX_ZERO_PROGRESS_STEPS = 3


class Orchestrator:
    """
    Top-level run controller.
    Loads compiled waterfall, invokes Executor step by step, enforces stop conditions.
    """

    def __init__(self, run_dir: str, workspace: str, ruleset: str, run_id: str, dataset_replay_path: str,
                 schema_structure: dict | None = None, features: dict | None = None,
                 interaction_log: Optional[InteractionLog] = None):
        self.run_dir = Path(run_dir)
        self.workspace = workspace
        self.ruleset = ruleset
        self.run_id = run_id
        self.dataset_replay_path = dataset_replay_path
        self.ilog = interaction_log or InteractionLog(run_dir, run_id)

        # Load compiled waterfall
        waterfall_path = self.run_dir / "compiled_waterfall.json"
        with open(waterfall_path) as f:
            self.waterfall: list[dict] = json.load(f)

        self.runtime_config_path = str(self.run_dir / "compiled_ruleset_runtime_config.json")

        self.guardrail = WriteGuardrail(
            allowed_output_root=str(self.run_dir),
            allowed_proposal_root=str(self.run_dir.parents[4] / "knowledgebase" / "rulesets"),
        )

        self.executor = Executor(run_dir, schema_structure=schema_structure, features=features, interaction_log=self.ilog)

        self._step_results: list[dict] = []
        self._consecutive_zero_progress = 0
        self._pool_empty = False

        self.ilog.log_decision("orchestrator", "Initialized", {
            "run_id": run_id, "waterfall_steps": len(self.waterfall),
            "rules": [s["rule_id"] for s in self.waterfall],
        })

    def run(self) -> dict:
        started_at = datetime.now(timezone.utc)
        self.ilog.log("orchestrator", "Run started", {"run_id": self.run_id, "started_at": started_at.isoformat()})

        self._update_manifest({"created_at": started_at.isoformat()})

        for step_def in self.waterfall:
            if self._should_stop():
                self.ilog.log_decision("orchestrator", "STOP condition met — halting waterfall", {
                    "at_step": step_def["step_id"],
                    "reason": "pool_empty" if self._pool_empty else f"{self._consecutive_zero_progress} consecutive zero-progress steps",
                })
                break

            self.ilog.log_decision("orchestrator", f"Dispatching step {step_def['step_id']}", {
                "rule_id": step_def["rule_id"],
                "operator": step_def["operator"],
                "params_keys": list(step_def.get("params", {}).keys()),
            })

            response = self._execute_step(step_def)

            removed = response.operator_result.effects.records_removed_from_pool
            groups = response.operator_result.effects.new_match_groups
            matched = response.operator_result.effects.new_matched_records
            duration = response.operator_result.timing.duration_ms

            step_summary = {
                "step_id": step_def["step_id"],
                "rule_id": step_def["rule_id"],
                "operator": step_def["operator"],
                "status": response.status.value,
                "records_removed_from_pool": removed,
                "new_match_groups": groups,
                "new_matched_records": matched,
                "duration_ms": duration,
            }
            self._step_results.append(step_summary)

            self.ilog.log("orchestrator", f"Step {step_def['step_id']} result received", {
                "rule_id": step_def["rule_id"],
                "status": response.status.value,
                "removed": removed, "groups": groups, "matched": matched,
                "duration_ms": duration,
                "pool_after": self.executor.get_pool_count(),
            })

            if removed == 0:
                self._consecutive_zero_progress += 1
                self.ilog.log("orchestrator", f"Zero progress ({self._consecutive_zero_progress}/{MAX_ZERO_PROGRESS_STEPS})", level="warning")
            else:
                self._consecutive_zero_progress = 0

            if response.status == StepStatus.ERROR:
                self.ilog.log_error("orchestrator", f"Step {step_def['step_id']} FAILED", response.errors)

        # Finalize
        finished_at = datetime.now(timezone.utc)
        db_path = self.executor.get_db_path()
        if db_path:
            self.ilog.log("finalize", "Starting run finalization (CSV export + reasoning + manifest seal)")
            summary = finalize_run(
                db_path, str(self.run_dir),
                self.workspace, self.ruleset, self.run_id,
                self._step_results, started_at,
            )
            summary["total_steps_executed"] = len(self._step_results)
        else:
            summary = self._finalize(started_at, finished_at)

        self.ilog.log("orchestrator", "Run complete", {
            "total_steps": len(self._step_results),
            "summary": summary,
            "duration_s": round((finished_at - started_at).total_seconds(), 2),
        })

        # Flush interaction log
        self.ilog.flush()

        self.executor.reset()
        return summary

    def _execute_step(self, step_def: dict) -> object:
        operator_type = OperatorType(step_def["operator"])
        params = step_def.get("params", {})

        blocking = params.get("blocking_strategy")
        blocking_spec = None
        if blocking:
            if "keys" in blocking:
                blocking_spec = OneSidedBlockingSpec(**blocking)
            elif "keys_a" in blocking:
                blocking_spec = TwoSidedBlockingSpec(**blocking)

        op_params = OperatorParams(
            side_a_filter=params.get("side_a_filter"),
            side_b_filter=params.get("side_b_filter"),
            side_filter=params.get("side_filter"),
            blocking_strategy=blocking_spec,
            comparisons=params.get("comparisons"),
        )

        spec = OperatorSpec(
            step_id=step_def["step_id"],
            rule_id=step_def["rule_id"],
            source=step_def.get("source", "core"),
            operator=operator_type,
            status=step_def.get("status", "active"),
            params=op_params,
        )

        request = ExecutorRequest(
            type=ExecutorRequestType.EXECUTE_OPERATOR,
            run=RunContext(
                run_id=self.run_id,
                workspace=self.workspace,
                ruleset=self.ruleset,
                run_dir=str(self.run_dir),
                dataset_replay_path=self.dataset_replay_path,
            ),
            runtime_config_path=self.runtime_config_path,
            operator_spec=spec,
        )

        return self.executor.execute(request)

    def _should_stop(self) -> bool:
        if self._pool_empty:
            return True
        if self._consecutive_zero_progress >= MAX_ZERO_PROGRESS_STEPS:
            return True
        return False

    def _finalize(self, started_at: datetime, finished_at: datetime) -> dict:
        summary = {
            "total_steps_executed": len(self._step_results),
            "total_matched": sum(s["new_matched_records"] for s in self._step_results),
            "total_excluded": sum(
                s["records_removed_from_pool"]
                for s in self._step_results
                if s["operator"] == "exclude_by_predicates"
            ),
            "total_match_groups": sum(s["new_match_groups"] for s in self._step_results),
        }
        manifest_update = {
            "created_at": started_at.isoformat(),
            "finished_at": finished_at.isoformat(),
            "waterfall": self._step_results,
            "summary": summary,
        }
        self._update_manifest(manifest_update)
        return summary

    def _update_manifest(self, updates: dict) -> None:
        manifest_path = self.run_dir / "run_manifest.json"
        with open(manifest_path) as f:
            manifest = json.load(f)
        manifest.update(updates)
        with open(manifest_path, "w") as f:
            json.dump(manifest, f, indent=2)
