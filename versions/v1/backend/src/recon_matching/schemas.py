"""
Phase 1 — Pydantic Wire Schemas

All contracts between Orchestrator, Executor, and external callers.
Derived from MASTER.md §8.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field, model_validator


# ─── Enums ──────────────────────────────────────────────────────────────────

class OperatorType(str, Enum):
    EXCLUDE_BY_PREDICATES = "exclude_by_predicates"
    ONE_TO_ONE_RANKED = "one_to_one_ranked"
    MANY_TO_MANY_BALANCE_K = "many_to_many_balance_k"
    ONE_SIDED = "one_sided"


OPERATOR_ALLOWLIST = set(OperatorType)


class ToleranceType(str, Enum):
    ABS = "abs"


class ToleranceBehavior(str, Enum):
    EXCLUSIVE = "exclusive"
    INCLUSIVE = "inclusive"


class StepStatus(str, Enum):
    OK = "ok"
    ERROR = "error"
    SKIPPED = "skipped"


class DatasetType(str, Enum):
    TWO_SIDED = "two_sided"
    ONE_SIDED = "one_sided"


class ExecutorRequestType(str, Enum):
    EXECUTE_OPERATOR = "EXECUTE_OPERATOR"


class ExecutorResponseType(str, Enum):
    OPERATOR_RESULT = "OPERATOR_RESULT"


# ─── Request ────────────────────────────────────────────────────────────────

class ReconMatchRequest(BaseModel):
    """Entry point request payload. Minimal fields; paths derived deterministically."""
    workspace: str
    ruleset: str
    dataset_absolute_path: str
    run_id: str


# ─── Blocking & Comparisons ────────────────────────────────────────────────

class BlockingStrategy(BaseModel):
    """Named blocking strategy from features.json."""
    name: str
    keys: list[str]
    required: list[str] = Field(default_factory=list)
    fallback_order: int = 0


class TwoSidedBlockingSpec(BaseModel):
    """Blocking spec for two-sided operators."""
    keys_a: list[str]
    keys_b: list[str]


class OneSidedBlockingSpec(BaseModel):
    """Blocking spec for one_sided operator."""
    keys: list[str]


class Comparison(BaseModel):
    """Two-sided comparison entry."""
    column_a: str
    column_b: str
    tolerance: float = 0.0
    tolerance_type: ToleranceType = ToleranceType.ABS
    tolerance_behavior: ToleranceBehavior = ToleranceBehavior.EXCLUSIVE
    aggregation: Optional[str] = None
    required: bool = False


class OneSidedComparison(BaseModel):
    """One-sided comparison entry (single column reference)."""
    column: str
    tolerance: float = 0.0
    tolerance_type: ToleranceType = ToleranceType.ABS
    aggregation: str = "SumIgnoreNulls"
    required: bool = False


# ─── Operator Spec ──────────────────────────────────────────────────────────

class OperatorParams(BaseModel):
    """Operator parameters — union of all operator param shapes."""
    side_a_filter: Optional[str] = None
    side_b_filter: Optional[str] = None
    side_filter: Optional[str] = None  # one_sided only
    blocking_strategy: Optional[TwoSidedBlockingSpec | OneSidedBlockingSpec] = None
    comparisons: Optional[list[Comparison] | list[OneSidedComparison]] = None
    group_by: Optional[list[str]] = None


class OperatorSpec(BaseModel):
    """Single waterfall step sent from Orchestrator to Executor."""
    step_id: int
    rule_id: str
    source: str = "core"  # "core" or "agent"
    operator: OperatorType
    status: str = "active"
    params: OperatorParams


# ─── Operator Result ────────────────────────────────────────────────────────

class OperatorEffects(BaseModel):
    new_match_groups: int = 0
    new_matched_records: int = 0
    new_break_records: int = 0
    records_removed_from_pool: int = 0


class OperatorTiming(BaseModel):
    started_at: str = ""
    ended_at: str = ""
    duration_ms: int = 0


class ReasoningDelta(BaseModel):
    matches_reasoning_groups_added: int = 0
    breaks_reasoning_items_added: int = 0
    exclusions_reasoning_groups_added: int = 0


class ArtifactFlags(BaseModel):
    sql_appended: bool = False
    sql_notes: list[str] = Field(default_factory=list)
    exclusions_csv_updated: bool = False
    matches_csv_updated: bool = False
    breaks_csv_updated: bool = False
    exclusions_reasoning_updated: bool = False
    matches_reasoning_updated: bool = False
    breaks_reasoning_updated: bool = False
    reasoning_delta: ReasoningDelta = Field(default_factory=ReasoningDelta)


class OperatorEvidence(BaseModel):
    blocking_strategy_used: Optional[str] = None
    comparisons_evaluated: list[str] = Field(default_factory=list)
    tolerance: Optional[float] = None
    reason_code: Optional[str] = None


class OperatorResult(BaseModel):
    """Result of a single operator execution."""
    run_id: str
    step_id: int
    rule_id: str
    operator: OperatorType
    status: StepStatus = StepStatus.OK
    timing: OperatorTiming = Field(default_factory=OperatorTiming)
    effects: OperatorEffects = Field(default_factory=OperatorEffects)
    artifacts: ArtifactFlags = Field(default_factory=ArtifactFlags)
    evidence: OperatorEvidence = Field(default_factory=OperatorEvidence)
    errors: list[str] = Field(default_factory=list)


# ─── Match / Break Items ───────────────────────────────────────────────────

class RecordRef(BaseModel):
    side: str
    statement_id: str
    record_id: str


class MatchGroupComparison(BaseModel):
    a_sum: float = 0.0
    b_sum: float = 0.0
    abs_diff: float = 0.0
    tol: float = 0.0


class MatchGroup(BaseModel):
    """One match group — accumulated into matches_reasoning.json."""
    match_group_id: str
    group_type: str  # "one_sided" | "one_to_one" | "many_to_many_balance_k"
    rule_id: str
    record_refs: list[RecordRef]
    blocking: Optional[dict[str, str]] = None
    comparisons: dict[str, MatchGroupComparison] = Field(default_factory=dict)
    explanation: str = ""


class ClosestCandidate(BaseModel):
    candidate_ref: RecordRef
    block_key: str = ""
    amount_diff: float = 0.0


class BreakItem(BaseModel):
    """One break record — accumulated into breaks_reasoning.json."""
    record_ref: RecordRef
    top_reasons: list[str] = Field(default_factory=list)
    details: dict[str, Any] = Field(default_factory=dict)
    closest_candidates: list[ClosestCandidate] = Field(default_factory=list)


# ─── Executor Request / Response ────────────────────────────────────────────

class RunContext(BaseModel):
    run_id: str
    workspace: str
    ruleset: str
    run_dir: str
    dataset_replay_path: str


class PoolState(BaseModel):
    unmatched_pool_path: str = ""
    already_matched_path: str = ""


class ExecutionMode(BaseModel):
    dry_run: bool = False
    write_outputs: bool = True


class ExecutorRequest(BaseModel):
    """Orchestrator → Executor message."""
    type: ExecutorRequestType = ExecutorRequestType.EXECUTE_OPERATOR
    run: RunContext
    runtime_config_path: str
    operator_spec: OperatorSpec
    pool_state: PoolState = Field(default_factory=PoolState)
    mode: ExecutionMode = Field(default_factory=ExecutionMode)


class ExecutorResponse(BaseModel):
    """Executor → Orchestrator message."""
    type: ExecutorResponseType = ExecutorResponseType.OPERATOR_RESULT
    run_id: str
    step_id: int
    rule_id: str
    status: StepStatus = StepStatus.OK
    operator_result: OperatorResult
    errors: list[str] = Field(default_factory=list)


# ─── Runtime Config ─────────────────────────────────────────────────────────

class AmountPolicy(BaseModel):
    tolerance: float = 0.005


class Guardrails(BaseModel):
    allowed_output_root: str = "workspace/outputs"
    allowed_kb_proposal_root: str = "workspace/knowledgebase/rulesets"
    forbidden_kb_mutations: list[str] = Field(
        default_factory=lambda: ["core_matching_rules.json", "agent_matching_rules.json"]
    )
    max_operator_seconds: int = 600


class KBPaths(BaseModel):
    ruleset_dir: str
    kb_index_path: str


class RulesetRuntimeConfig(BaseModel):
    """Compiled runtime config — Executor's ground truth during a run."""
    workspace: str
    ruleset: str
    kb: KBPaths
    blocking: dict[str, list[BlockingStrategy]] = Field(default_factory=lambda: {"strategies": []})
    amount_policy: AmountPolicy = Field(default_factory=AmountPolicy)
    guardrails: Guardrails = Field(default_factory=Guardrails)
    operator_allowlist: list[str] = Field(
        default_factory=lambda: [
            "exclude_by_predicates",
            "one_to_one_ranked",
            "many_to_many_balance_k",
            "one_sided",
        ]
    )


# ─── Dataset Profile ───────────────────────────────────────────────────────

class DatasetInfo(BaseModel):
    replay_copy_relative_path: str = "dataset_file.csv"
    row_count: int = 0
    side_counts: dict[str, int] = Field(default_factory=dict)


class ColumnInfo(BaseModel):
    present: list[str] = Field(default_factory=list)
    missing_required: list[str] = Field(default_factory=list)


class AmountColumns(BaseModel):
    detected: list[str] = Field(default_factory=list)


class DatasetProfile(BaseModel):
    """Dataset profile — produced early in run, referenced throughout."""
    run_id: str
    dataset_type: DatasetType = DatasetType.TWO_SIDED
    dataset: DatasetInfo = Field(default_factory=DatasetInfo)
    columns: ColumnInfo = Field(default_factory=ColumnInfo)
    amount_columns: AmountColumns = Field(default_factory=AmountColumns)
    blocking_quality: dict[str, dict[str, float]] = Field(default_factory=dict)
