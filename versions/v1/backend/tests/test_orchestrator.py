"""Tests for Phase 4 — Orchestrator, Executor, Guardrail, Service."""

import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recon_matching.boot import boot
from recon_matching.orchestrator import Orchestrator
from recon_matching.executor import Executor
from recon_matching.guardrail import WriteGuardrail, GuardrailViolation
from recon_matching.service import ReconMatchService
from recon_matching.schemas import (
    ReconMatchRequest, ExecutorRequest, ExecutorRequestType, RunContext,
    OperatorSpec, OperatorParams, OperatorType, PoolState, ExecutionMode,
    StepStatus,
)

APP_ROOT = Path(__file__).resolve().parents[4]
WORKSPACE_ROOT = str(APP_ROOT / "workspace")
DATASET_PATH = str(APP_ROOT / "workspace" / "asset" / "account" / "datasets" / "active_experiment" / "dataset.csv")


@pytest.fixture
def booted_run():
    """Boot a run and return the BootResult."""
    return boot(WORKSPACE_ROOT, "asset", "account", "orch_test_001", DATASET_PATH, date(2026, 3, 26))


class TestExecutor:
    def test_executor_returns_ok(self, booted_run):
        """Executor with real DuckDB returns valid results."""
        executor = Executor(str(booted_run.run_dir))
        request = ExecutorRequest(
            run=RunContext(
                run_id="orch_test_001", workspace="asset", ruleset="account",
                run_dir=str(booted_run.run_dir),
                dataset_replay_path=str(booted_run.run_dir / "dataset_file.csv"),
            ),
            runtime_config_path=str(booted_run.run_dir / "compiled_ruleset_runtime_config.json"),
            operator_spec=OperatorSpec(
                step_id=1, rule_id="EXCL_TEST", operator=OperatorType.EXCLUDE_BY_PREDICATES,
                params=OperatorParams(side_a_filter="(1=0)"),  # Matches nothing
            ),
        )
        response = executor.execute(request)
        assert response.status == StepStatus.OK

    def test_executor_reset(self, booted_run):
        executor = Executor(str(booted_run.run_dir))
        executor._db_path = "something"
        executor.reset()
        assert executor._db_path is None


class TestOrchestrator:
    def test_full_waterfall_runs(self, booted_run):
        """Real DuckDB executor runs waterfall steps and produces results."""
        orch = Orchestrator(
            run_dir=str(booted_run.run_dir),
            workspace="asset", ruleset="account", run_id="orch_test_001",
            dataset_replay_path=str(booted_run.run_dir / "dataset_file.csv"),
            schema_structure=booted_run.schema_structure,
            features=booted_run.features,
        )
        summary = orch.run()

        assert summary["total_steps_executed"] > 0
        assert "total_matched" in summary

    def test_manifest_sealed_after_run(self, booted_run):
        orch = Orchestrator(
            run_dir=str(booted_run.run_dir),
            workspace="asset", ruleset="account", run_id="orch_test_001",
            dataset_replay_path=str(booted_run.run_dir / "dataset_file.csv"),
            schema_structure=booted_run.schema_structure,
            features=booted_run.features,
        )
        orch.run()

        with open(booted_run.run_dir / "run_manifest.json") as f:
            manifest = json.load(f)

        assert manifest["created_at"] is not None
        assert manifest["finished_at"] is not None
        assert len(manifest["waterfall"]) > 0
        assert "summary" in manifest

    def test_stop_condition_works(self, booted_run):
        """Orchestrator respects stop conditions."""
        orch = Orchestrator(
            run_dir=str(booted_run.run_dir),
            workspace="asset", ruleset="account", run_id="orch_test_001",
            dataset_replay_path=str(booted_run.run_dir / "dataset_file.csv"),
            schema_structure=booted_run.schema_structure,
            features=booted_run.features,
        )
        summary = orch.run()
        # Should not exceed the waterfall length
        assert summary["total_steps_executed"] <= 7


class TestWriteGuardrail:
    def test_allows_output_writes(self, tmp_path):
        output_root = tmp_path / "outputs"
        output_root.mkdir()
        guardrail = WriteGuardrail(str(output_root), str(tmp_path / "kb"))
        guardrail.check_write(str(output_root / "run1" / "matches.csv"))  # Should not raise

    def test_blocks_kb_mutation(self, tmp_path):
        guardrail = WriteGuardrail(str(tmp_path / "outputs"), str(tmp_path / "kb"))
        with pytest.raises(GuardrailViolation):
            guardrail.check_write(str(tmp_path / "kb" / "core_matching_rules.json"))

    def test_allows_proposal_writes(self, tmp_path):
        kb_root = tmp_path / "kb"
        proposals = kb_root / "asset" / "account" / "agent_rule_proposals"
        proposals.mkdir(parents=True)
        guardrail = WriteGuardrail(str(tmp_path / "outputs"), str(kb_root))
        guardrail.check_write(str(proposals / "new_rule_2026.json"))  # Should not raise

    def test_blocks_arbitrary_writes(self, tmp_path):
        guardrail = WriteGuardrail(str(tmp_path / "outputs"), str(tmp_path / "kb"))
        with pytest.raises(GuardrailViolation):
            guardrail.check_write("/etc/passwd")

    def test_safe_write_creates_file(self, tmp_path):
        output_root = tmp_path / "outputs"
        output_root.mkdir()
        guardrail = WriteGuardrail(str(output_root), str(tmp_path / "kb"))
        target = str(output_root / "test.txt")
        guardrail.safe_write(target, "hello")
        assert Path(target).read_text() == "hello"


class TestReconMatchService:
    def test_full_service_execution(self):
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="service_test_001",
        )
        result = service.execute(request)

        assert result["status"] == "COMPLETED"
        assert result["run_id"] == "service_test_001"
        assert "summary" in result
        assert result["summary"]["total_steps_executed"] > 0

    def test_service_with_bad_dataset(self):
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path="/nonexistent/data.csv",
            run_id="service_fail_001",
        )
        result = service.execute(request)
        assert result["status"] == "FAILED"
        assert "error" in result
