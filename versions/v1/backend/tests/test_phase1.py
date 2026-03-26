"""Tests for Phase 1 — Project skeleton, schemas, validators."""

import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recon_matching.schemas import (
    ReconMatchRequest, RulesetRuntimeConfig, DatasetProfile,
    OperatorSpec, OperatorResult, ExecutorRequest, ExecutorResponse,
    MatchGroup, BreakItem, OperatorType, StepStatus, DatasetType,
    RecordRef, OperatorParams,
)
from recon_matching.paths import derive_kb_dir, derive_run_dir, validate_kb_dir
from recon_matching.scaffold import create_run_dir, validate_and_prepare, ScaffoldError
from recon_matching.validators import (
    validate_json_file, validate_schema_structure, validate_features,
    validate_rules_file, validate_kb, validate_run_artifacts, ValidationError,
)
from recon_matching.lro_client import LROClient


KB_DIR = Path(__file__).resolve().parents[4] / "workspace" / "knowledgebase" / "rulesets" / "asset" / "account"


class TestSchemas:
    def test_recon_match_request(self):
        req = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path="/data/test.csv", run_id="run_001",
        )
        assert req.workspace == "asset"

    def test_operator_spec_exclusion(self):
        spec = OperatorSpec(
            step_id=1, rule_id="EXCL_CUST_DERIV", operator=OperatorType.EXCLUDE_BY_PREDICATES,
            params=OperatorParams(side_a_filter="(ext_asset_info_null_safe IN ('FUT'))"),
        )
        assert spec.operator == OperatorType.EXCLUDE_BY_PREDICATES

    def test_operator_result_serialization(self):
        result = OperatorResult(
            run_id="run_001", step_id=1, rule_id="R1", operator=OperatorType.ONE_TO_ONE_RANKED,
        )
        d = result.model_dump()
        assert d["status"] == "ok"
        assert d["effects"]["new_match_groups"] == 0

    def test_match_group(self):
        mg = MatchGroup(
            match_group_id="MG001", group_type="one_to_one", rule_id="R1",
            record_refs=[
                RecordRef(side="A", statement_id="100", record_id="1000"),
                RecordRef(side="B", statement_id="200", record_id="2000"),
            ],
            explanation="Test match",
        )
        assert len(mg.record_refs) == 2

    def test_break_item(self):
        bi = BreakItem(
            record_ref=RecordRef(side="A", statement_id="100", record_id="1000"),
            top_reasons=["NO_COUNTERPART_IN_BLOCK"],
        )
        assert bi.top_reasons[0] == "NO_COUNTERPART_IN_BLOCK"

    def test_executor_request_response_roundtrip(self):
        from recon_matching.schemas import RunContext, ExecutionMode
        req = ExecutorRequest(
            run=RunContext(run_id="r1", workspace="asset", ruleset="account",
                          run_dir="/tmp/run", dataset_replay_path="/tmp/run/dataset.csv"),
            runtime_config_path="/tmp/run/config.json",
            operator_spec=OperatorSpec(
                step_id=1, rule_id="R1", operator=OperatorType.ONE_TO_ONE_RANKED,
                params=OperatorParams(),
            ),
        )
        assert req.type.value == "EXECUTE_OPERATOR"

    def test_dataset_profile(self):
        dp = DatasetProfile(
            run_id="r1", dataset_type=DatasetType.TWO_SIDED,
        )
        assert dp.dataset_type == DatasetType.TWO_SIDED

    def test_ruleset_runtime_config(self):
        from recon_matching.schemas import KBPaths
        cfg = RulesetRuntimeConfig(
            workspace="asset", ruleset="account",
            kb=KBPaths(ruleset_dir="/kb", kb_index_path="/kb/index.json"),
        )
        assert len(cfg.operator_allowlist) == 4


class TestPaths:
    def test_derive_kb_dir(self):
        p = derive_kb_dir("/base", "asset", "account")
        assert str(p).endswith("knowledgebase/rulesets/asset/account")

    def test_derive_run_dir(self):
        p = derive_run_dir("/base", "asset", "account", "run_001", date(2026, 3, 18))
        assert "2026-03-18" in str(p)
        assert str(p).endswith("run_001")

    def test_validate_kb_dir_valid(self):
        missing = validate_kb_dir(KB_DIR)
        assert missing == [], f"KB validation failed: {missing}"

    def test_validate_kb_dir_missing(self, tmp_path):
        missing = validate_kb_dir(tmp_path / "nonexistent")
        assert len(missing) > 0


class TestScaffold:
    def test_create_run_dir(self, tmp_path):
        rd = create_run_dir(tmp_path / "runs" / "test_run")
        assert rd.exists()

    def test_validate_and_prepare_valid(self, tmp_path):
        run_dir = tmp_path / "run_001"
        validate_and_prepare(KB_DIR, run_dir)
        assert run_dir.exists()

    def test_validate_and_prepare_invalid_kb(self, tmp_path):
        with pytest.raises(ScaffoldError):
            validate_and_prepare(tmp_path / "bad_kb", tmp_path / "run")


class TestValidators:
    def test_validate_kb_full(self):
        results = validate_kb(KB_DIR)
        for filename, issues in results.items():
            assert issues == [], f"{filename}: {issues}"

    def test_validate_schema_structure_valid(self):
        data = json.loads((KB_DIR / "schema_structure.json").read_text())
        issues = validate_schema_structure(data)
        assert issues == []

    def test_validate_schema_structure_invalid(self):
        issues = validate_schema_structure({})
        assert len(issues) == 3  # Missing columns, number_columns, dataset_type

    def test_validate_rules_file_valid(self):
        data = json.loads((KB_DIR / "core_matching_rules.json").read_text())
        issues = validate_rules_file(data)
        assert issues == []

    def test_validate_rules_unknown_operator(self):
        issues = validate_rules_file({
            "rules": [{"rule_id": "X", "operator": "bogus", "status": "active"}]
        })
        assert any("unknown operator" in i for i in issues)

    def test_validate_run_artifacts_missing(self, tmp_path):
        missing = validate_run_artifacts(tmp_path)
        assert len(missing) == 9  # All artifacts missing

    def test_validate_json_file_bad_path(self, tmp_path):
        with pytest.raises(ValidationError):
            validate_json_file(tmp_path / "nope.json")


class TestLROClient:
    def test_create_and_complete(self):
        client = LROClient()
        op_id = client.create()
        assert client.get(op_id)["status"] == "RUNNING"

        client.complete(op_id, response={"result": "ok"})
        op = client.get(op_id)
        assert op["status"] == "COMPLETED"
        assert op["response"]["result"] == "ok"

    def test_complete_with_error(self):
        client = LROClient()
        op_id = client.create()
        client.complete(op_id, error="Something broke")
        assert client.get(op_id)["status"] == "FAILED"

    def test_unknown_operation(self):
        client = LROClient()
        with pytest.raises(ValueError):
            client.get("nonexistent")
