"""
Phase 5 — Executor (DuckDB-backed)

Receives a single OperatorSpec, executes it via DuckDB Skill, returns ExecutorResponse.
All interactions logged to InteractionLog.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from .interaction_log import InteractionLog
from .schemas import (
    OperatorSpec, OperatorResult, OperatorEffects, OperatorTiming,
    ArtifactFlags, OperatorEvidence, ReasoningDelta,
    ExecutorRequest, ExecutorResponse, ExecutorResponseType,
    StepStatus, RulesetRuntimeConfig, DatasetProfile,
)
from .duckdb_skill import (
    duckdb_open_or_create, duckdb_load_dataset, duckdb_init_pool,
    duckdb_profile, duckdb_execute_operator, duckdb_get_pool_count,
)


class Executor:

    def __init__(self, run_dir: str, schema_structure: Optional[dict] = None,
                 features: Optional[dict] = None, interaction_log: Optional[InteractionLog] = None):
        self.run_dir = Path(run_dir)
        self.schema_structure = schema_structure or {}
        self.features = features or {}
        self.ilog = interaction_log or InteractionLog(run_dir, "unknown")
        self._db_path: Optional[str] = None
        self._dataset_profile: Optional[DatasetProfile] = None
        self._initialized = False

    def _ensure_initialized(self, run_id: str) -> None:
        if self._initialized:
            return

        dataset_path = self.run_dir / "dataset_file.csv"
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset not found: {dataset_path}")

        self.ilog.log("executor", "Initializing DuckDB", {"dataset": str(dataset_path)})

        self._db_path = duckdb_open_or_create(str(self.run_dir))
        self.ilog.log("duckdb", "Database created", {"path": self._db_path})

        duckdb_load_dataset(self._db_path, str(dataset_path))
        self.ilog.log("duckdb", "Dataset loaded into base_dataset")

        duckdb_init_pool(self._db_path)
        pool_size = duckdb_get_pool_count(self._db_path)
        self.ilog.log("duckdb", "Pool initialized", {"pool_size": pool_size})

        self._dataset_profile = duckdb_profile(
            self._db_path, self.schema_structure, self.features, run_id,
        )

        self.ilog.log_data("executor", "Dataset profiled", {
            "dataset_type": self._dataset_profile.dataset_type.value,
            "row_count": self._dataset_profile.dataset.row_count,
            "side_counts": self._dataset_profile.dataset.side_counts,
            "amount_columns": self._dataset_profile.amount_columns.detected,
            "blocking_quality": self._dataset_profile.blocking_quality,
        })

        profile_path = self.run_dir / "dataset_profile.json"
        with open(profile_path, "w") as f:
            json.dump(self._dataset_profile.model_dump(), f, indent=2)

        self._initialized = True

    def execute(self, request: ExecutorRequest) -> ExecutorResponse:
        spec = request.operator_spec
        run_id = request.run.run_id

        self.ilog.log("executor", f"Received ExecutorRequest for step {spec.step_id}", {
            "rule_id": spec.rule_id,
            "operator": spec.operator.value,
            "params": spec.params.model_dump() if spec.params else {},
        })

        try:
            self._ensure_initialized(run_id)

            pool_before = duckdb_get_pool_count(self._db_path)
            self.ilog.log("executor", f"Pool before step {spec.step_id}: {pool_before} records")

            op_dict = {
                "step_id": spec.step_id,
                "rule_id": spec.rule_id,
                "operator": spec.operator.value,
                "params": {k: v for k, v in (spec.params.model_dump() if spec.params else {}).items() if v is not None},
            }

            result, groups = duckdb_execute_operator(
                self._db_path, op_dict, {}, {},
                str(self.run_dir), run_id, self.ilog,
            )

            pool_after = duckdb_get_pool_count(self._db_path)

            self.ilog.log("executor", f"Step {spec.step_id} execution complete", {
                "status": result.status.value,
                "pool_before": pool_before,
                "pool_after": pool_after,
                "removed": result.effects.records_removed_from_pool,
                "match_groups": result.effects.new_match_groups,
                "matched_records": result.effects.new_matched_records,
            })

            self._store_reasoning_groups(spec.operator.value, groups)

        except Exception as e:
            self.ilog.log_error("executor", f"Step {spec.step_id} FAILED: {e}")
            result = OperatorResult(
                run_id=run_id, step_id=spec.step_id, rule_id=spec.rule_id,
                operator=spec.operator, status=StepStatus.ERROR, errors=[str(e)],
            )

        return ExecutorResponse(
            type=ExecutorResponseType.OPERATOR_RESULT,
            run_id=run_id, step_id=spec.step_id, rule_id=spec.rule_id,
            status=result.status, operator_result=result,
        )

    def _store_reasoning_groups(self, operator: str, groups: list[dict]) -> None:
        if not groups:
            return
        if operator == "exclude_by_predicates":
            reasoning_file = self.run_dir / "exclusions_reasoning.json"
            key = "exclusions"
        else:
            reasoning_file = self.run_dir / "matches_reasoning.json"
            key = "match_groups"

        existing = {"workspace": "", "ruleset": "", "run_id": "", key: []}
        if reasoning_file.exists():
            with open(reasoning_file) as f:
                existing = json.load(f)
        existing[key].extend(groups)
        with open(reasoning_file, "w") as f:
            json.dump(existing, f, indent=2)

    def get_pool_count(self) -> int:
        if not self._db_path:
            return 0
        return duckdb_get_pool_count(self._db_path)

    def get_db_path(self) -> Optional[str]:
        return self._db_path

    def get_dataset_profile(self) -> Optional[DatasetProfile]:
        return self._dataset_profile

    def reset(self) -> None:
        self._db_path = None
        self._dataset_profile = None
        self._initialized = False
