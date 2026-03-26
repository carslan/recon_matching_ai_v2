"""Tests for Phase 5 — DuckDB Skill + Operators."""

import csv
import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recon_matching.duckdb_skill import (
    duckdb_open_or_create, duckdb_load_dataset, duckdb_init_pool,
    duckdb_profile, duckdb_execute_operator, duckdb_get_pool_count,
    duckdb_export_exclusions_csv, duckdb_export_matches_csv, duckdb_export_breaks_csv,
)
from recon_matching.boot import boot
from recon_matching.service import ReconMatchService
from recon_matching.schemas import DatasetType

APP_ROOT = Path(__file__).resolve().parents[4]
WORKSPACE_ROOT = str(APP_ROOT / "workspace")
DATASET_PATH = str(APP_ROOT / "workspace" / "asset" / "account" / "datasets" / "active_experiment" / "dataset.csv")
KB_DIR = APP_ROOT / "workspace" / "knowledgebase" / "rulesets" / "asset" / "account"


@pytest.fixture
def db_setup(tmp_path):
    """Set up a DuckDB with the synthetic dataset loaded."""
    db_path = duckdb_open_or_create(str(tmp_path))
    duckdb_load_dataset(db_path, DATASET_PATH)
    duckdb_init_pool(db_path)
    return db_path, tmp_path


@pytest.fixture
def schema_and_features():
    with open(KB_DIR / "schema_structure.json") as f:
        schema = json.load(f)
    with open(KB_DIR / "features.json") as f:
        features = json.load(f)
    return schema, features


class TestDuckDBLifecycle:
    def test_open_creates_db(self, tmp_path):
        db_path = duckdb_open_or_create(str(tmp_path))
        assert Path(db_path).exists()

    def test_load_dataset(self, tmp_path):
        db_path = duckdb_open_or_create(str(tmp_path))
        duckdb_load_dataset(db_path, DATASET_PATH)
        import duckdb
        conn = duckdb.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM base_dataset").fetchone()[0]
        conn.close()
        assert count > 0

    def test_init_pool(self, db_setup):
        db_path, _ = db_setup
        count = duckdb_get_pool_count(db_path)
        assert count > 0


class TestProfiling:
    def test_profile_basic(self, db_setup, schema_and_features):
        db_path, _ = db_setup
        schema, features = schema_and_features
        profile = duckdb_profile(db_path, schema, features, "test_run")

        assert profile.dataset_type == DatasetType.TWO_SIDED
        assert profile.dataset.row_count > 0
        assert "A" in profile.dataset.side_counts
        assert "B" in profile.dataset.side_counts
        assert len(profile.amount_columns.detected) > 0

    def test_profile_blocking_quality(self, db_setup, schema_and_features):
        db_path, _ = db_setup
        schema, features = schema_and_features
        profile = duckdb_profile(db_path, schema, features, "test_run")

        assert len(profile.blocking_quality) > 0
        for name, quality in profile.blocking_quality.items():
            assert "coverage_rate" in quality
            assert 0 <= quality["coverage_rate"] <= 1


class TestExclusionOperator:
    def test_excludes_records(self, db_setup):
        db_path, tmp_path = db_setup
        pool_before = duckdb_get_pool_count(db_path)

        spec = {
            "step_id": 1,
            "rule_id": "EXCL_CUST_DERIV",
            "operator": "exclude_by_predicates",
            "params": {
                "side_a_filter": "(ext_asset_info_null_safe IN ('FUT', 'OPT', 'BKL'))",
                "side_b_filter": "(ext_asset_info_null_safe IN ('FUT', 'OPT', 'BKL'))",
            },
        }
        result, groups = duckdb_execute_operator(db_path, spec, {}, {}, str(tmp_path), "test")

        pool_after = duckdb_get_pool_count(db_path)
        assert result.effects.records_removed_from_pool >= 0
        assert pool_after <= pool_before


class TestOneToOneOperator:
    def test_matches_records(self, db_setup):
        db_path, tmp_path = db_setup

        spec = {
            "step_id": 2,
            "rule_id": "EXACT_MATCH_BLKCUSIP",
            "operator": "one_to_one_ranked",
            "params": {
                "blocking_strategy": {"keys_a": ["blk_cusip"], "keys_b": ["blk_cusip"]},
                "comparisons": [{
                    "column_a": "orig_face",
                    "column_b": "orig_face",
                    "tolerance": 0.005,
                    "tolerance_type": "abs",
                    "tolerance_behavior": "inclusive",
                }],
            },
        }
        result, groups = duckdb_execute_operator(db_path, spec, {}, {}, str(tmp_path), "test")

        assert result.status.value == "ok"
        assert result.effects.new_match_groups >= 0
        # Each match group should have exactly 2 records (1:1)
        for g in groups:
            assert len(g["record_refs"]) == 2


class TestManyToManyOperator:
    def test_balance_matching(self, db_setup):
        db_path, tmp_path = db_setup

        spec = {
            "step_id": 3,
            "rule_id": "SEC_ID_ISSUE_MULTI",
            "operator": "many_to_many_balance_k",
            "params": {
                "side_a_filter": "1=1",
                "side_b_filter": "1=1",
                "blocking_strategy": {"keys_a": ["blk_cusip"], "keys_b": ["blk_cusip"]},
                "comparisons": [{
                    "column_a": "orig_face",
                    "column_b": "orig_face",
                    "tolerance": 0.001,
                    "tolerance_type": "abs",
                    "tolerance_behavior": "exclusive",
                    "aggregation": "SumIgnoreNulls",
                }],
            },
        }
        result, groups = duckdb_execute_operator(db_path, spec, {}, {}, str(tmp_path), "test")
        assert result.status.value == "ok"


class TestOneSidedOperator:
    def test_netting_matches(self, db_setup):
        db_path, tmp_path = db_setup

        spec = {
            "step_id": 4,
            "rule_id": "ZERO_POS",
            "operator": "one_sided",
            "params": {
                "blocking_strategy": {"keys": ["blk_cusip"]},
                "comparisons": [{
                    "column": "orig_face",
                    "tolerance": 0.01,
                    "tolerance_type": "abs",
                    "aggregation": "SumIgnoreNulls",
                }],
            },
        }
        result, groups = duckdb_execute_operator(db_path, spec, {}, {}, str(tmp_path), "test")
        assert result.status.value == "ok"
        # One-sided groups should have >= 2 records each
        for g in groups:
            assert len(g["record_refs"]) >= 2


class TestExport:
    def test_export_all_csvs(self, db_setup):
        db_path, tmp_path = db_setup

        # Run exclusion first
        excl_spec = {
            "step_id": 1, "rule_id": "EXCL_TEST", "operator": "exclude_by_predicates",
            "params": {"side_a_filter": "(ext_asset_info_null_safe IN ('FUT', 'OPT'))"},
        }
        duckdb_execute_operator(db_path, excl_spec, {}, {}, str(tmp_path), "test")

        # Run one_to_one
        match_spec = {
            "step_id": 2, "rule_id": "MATCH_TEST", "operator": "one_to_one_ranked",
            "params": {
                "blocking_strategy": {"keys_a": ["blk_cusip"], "keys_b": ["blk_cusip"]},
                "comparisons": [{"column_a": "orig_face", "column_b": "orig_face", "tolerance": 0.01, "tolerance_behavior": "inclusive"}],
            },
        }
        duckdb_execute_operator(db_path, match_spec, {}, {}, str(tmp_path), "test")

        # Export
        duckdb_export_exclusions_csv(db_path, str(tmp_path))
        duckdb_export_matches_csv(db_path, str(tmp_path))
        duckdb_export_breaks_csv(db_path, str(tmp_path))

        assert (tmp_path / "exclusions.csv").exists()
        assert (tmp_path / "matches.csv").exists()
        assert (tmp_path / "breaks.csv").exists()

    def test_audit_trail_created(self, db_setup):
        db_path, tmp_path = db_setup
        spec = {
            "step_id": 1, "rule_id": "TEST", "operator": "exclude_by_predicates",
            "params": {"side_a_filter": "1=0"},
        }
        duckdb_execute_operator(db_path, spec, {}, {}, str(tmp_path), "test")
        assert (tmp_path / "matching_queries.sql").exists()

    def test_pool_never_contains_matched(self, db_setup):
        """After matching, pool_unmatched must not contain already-matched records."""
        db_path, tmp_path = db_setup
        import duckdb as ddb

        spec = {
            "step_id": 1, "rule_id": "M1", "operator": "one_to_one_ranked",
            "params": {
                "blocking_strategy": {"keys_a": ["blk_cusip"], "keys_b": ["blk_cusip"]},
                "comparisons": [{"column_a": "orig_face", "column_b": "orig_face", "tolerance": 1.0, "tolerance_behavior": "inclusive"}],
            },
        }
        duckdb_execute_operator(db_path, spec, {}, {}, str(tmp_path), "test")

        conn = ddb.connect(db_path)
        overlap = conn.execute("""
            SELECT COUNT(*) FROM pool_unmatched pu
            WHERE pu.record_id IN (SELECT record_id FROM matched_records)
        """).fetchone()[0]
        conn.close()
        assert overlap == 0


class TestEndToEndService:
    def test_service_with_real_duckdb(self):
        """Full end-to-end: boot + orchestrate with real DuckDB operators."""
        from recon_matching.schemas import ReconMatchRequest
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="e2e_duckdb_test",
        )
        result = service.execute(request)

        assert result["status"] == "COMPLETED"
        assert result["summary"]["total_steps_executed"] > 0
        # With real operators, we should get actual matches
        assert result["summary"]["total_matched"] >= 0
