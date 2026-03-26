"""Tests for Phase 0 — Migration Utility."""

import json
import os
import tempfile
from pathlib import Path

import pytest

# Add src to path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recon_matching.migration import ReconAIPReprocessing
from recon_matching.synthetic_data import generate_dataset


FIXTURES_DIR = Path(__file__).resolve().parents[4] / "datasets" / "cust_of"


@pytest.fixture
def ruleset_path():
    return str(FIXTURES_DIR / "ruleset.json")


@pytest.fixture
def target_dir(tmp_path):
    return str(tmp_path / "kb_output")


class TestMigration:
    """Test suite for ruleset.json -> KB files migration."""

    def test_full_migration_creates_all_files(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        summary = migrator.run_full_migration()

        expected_files = [
            "schema_structure.json",
            "core_matching_rules.json",
            "features.json",
            "agent_matching_rules.json",
            "identity.md",
            "soul.md",
            "how_to_work.md",
        ]
        for f in expected_files:
            assert (Path(target_dir) / f).exists(), f"Missing: {f}"

        assert (Path(target_dir) / "agent_rule_proposals").is_dir()

    def test_schema_structure_columns(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        schema = migrator.create_fat_schema()

        assert schema["dataset_type"] == "account"
        assert len(schema["columns"]) > 0
        assert len(schema["number_columns"]) > 0

        # Base columns present
        col_names = [c["name"] for c in schema["columns"]]
        assert "side" in col_names
        assert "record_id" in col_names
        assert "orig_face" in col_names

        # Computed columns have expression field
        computed = [c for c in schema["columns"] if "expression" in c]
        assert len(computed) > 0
        assert any(c["name"] == "ext_asset_info_null_safe" for c in computed)

        # Number columns include both base and computed
        assert "orig_face" in schema["number_columns"]
        assert "orig_face_abs" in schema["number_columns"]

    def test_schema_type_stripping(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        schema = migrator.create_fat_schema()

        # No VALUE_TYPE_ prefix in output
        for col in schema["columns"]:
            assert not col["type"].startswith("VALUE_TYPE_"), f"Unstripped type: {col['type']}"

    def test_number_columns_only_numeric(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        schema = migrator.create_fat_schema()

        for name, dtype in schema["number_columns"].items():
            assert dtype in ("DOUBLE", "LONG"), f"{name} has non-numeric type: {dtype}"

    def test_rules_migration(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        rules = migrator.migrate_rules()

        assert rules["ruleset"] == "account"
        assert len(rules["rules"]) == 7  # 3 exclusion + 4 matching

        operators = [r["operator"] for r in rules["rules"]]
        assert "exclude_by_predicates" in operators
        assert "one_to_one_ranked" in operators
        assert "many_to_many_balance_k" in operators
        assert "one_sided" in operators

    def test_exclusion_rules_have_excl_prefix(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        rules = migrator.migrate_rules()

        for rule in rules["rules"]:
            if rule["operator"] == "exclude_by_predicates":
                assert rule["rule_id"].startswith("EXCL_"), f"Missing EXCL_ prefix: {rule['rule_id']}"

    def test_exclusion_filters_use_null_safe(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        rules = migrator.migrate_rules()

        cust_deriv = next(r for r in rules["rules"] if r["rule_id"] == "EXCL_CUST_DERIV")
        assert "null_safe" in cust_deriv["params"]["side_a_filter"]

    def test_many_to_many_has_aggregation(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        rules = migrator.migrate_rules()

        mtm = next(r for r in rules["rules"] if r["operator"] == "many_to_many_balance_k")
        for comp in mtm["params"]["comparisons"]:
            assert "aggregation" in comp, "many_to_many comparisons must have aggregation"

    def test_one_sided_has_aggregation(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        rules = migrator.migrate_rules()

        os_rule = next(r for r in rules["rules"] if r["operator"] == "one_sided")
        for comp in os_rule["params"]["comparisons"]:
            assert "aggregation" in comp, "one_sided comparisons must have aggregation"

    def test_operator_allowlist(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        rules = migrator.migrate_rules()

        allowed = {"exclude_by_predicates", "one_to_one_ranked", "many_to_many_balance_k", "one_sided"}
        for rule in rules["rules"]:
            assert rule["operator"] in allowed, f"Unknown operator: {rule['operator']}"

    def test_features_migration(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        features = migrator.migrate_features()

        assert len(features["blocking_strategies"]) == 2
        assert features["amount_policy"]["tolerance"] == 0.005

    def test_agent_matching_rules_empty(self, ruleset_path, target_dir):
        migrator = ReconAIPReprocessing(ruleset_path, target_dir)
        agent_rules = migrator.create_agent_matching_rules()

        assert agent_rules["ruleset"] == "account"
        assert agent_rules["rules"] == []


class TestSyntheticData:
    """Test suite for synthetic dataset generation."""

    def test_generates_correct_row_counts(self, tmp_path):
        path = str(tmp_path / "test_dataset.csv")
        generate_dataset(path, side_a_count=160, side_b_count=242, seed=42)

        import csv
        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        a_count = sum(1 for r in rows if r["side"] == "A")
        b_count = sum(1 for r in rows if r["side"] == "B")

        # A count should be 160 base + 6 netting counterparts = 166
        assert a_count >= 160
        assert b_count > 0
        assert len(rows) > 0

    def test_deterministic_with_same_seed(self, tmp_path):
        path1 = str(tmp_path / "ds1.csv")
        path2 = str(tmp_path / "ds2.csv")

        generate_dataset(path1, seed=42)
        generate_dataset(path2, seed=42)

        with open(path1) as f1, open(path2) as f2:
            assert f1.read() == f2.read()

    def test_has_required_columns(self, tmp_path):
        path = str(tmp_path / "test.csv")
        generate_dataset(path, side_a_count=20, side_b_count=30, seed=1)

        import csv
        with open(path) as f:
            reader = csv.DictReader(f)
            row = next(reader)

        required = ["side", "fund", "statement_id", "record_id", "blk_cusip", "sec_id", "orig_face"]
        for col in required:
            assert col in row, f"Missing required column: {col}"

    def test_computed_columns_enriched(self, tmp_path):
        path = str(tmp_path / "test.csv")
        generate_dataset(path, side_a_count=10, side_b_count=10, seed=1)

        import csv
        with open(path) as f:
            reader = csv.DictReader(f)
            row = next(reader)

        assert "ext_asset_info_null_safe" in row
        assert "orig_face_abs" in row
        assert float(row["orig_face_abs"]) >= 0
