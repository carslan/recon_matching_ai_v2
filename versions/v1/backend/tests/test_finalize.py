"""Tests for Phase 6 — Reasoning Writers + Run Finalization."""

import csv
import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recon_matching.schemas import ReconMatchRequest
from recon_matching.service import ReconMatchService

APP_ROOT = Path(__file__).resolve().parents[4]
WORKSPACE_ROOT = str(APP_ROOT / "workspace")
DATASET_PATH = str(APP_ROOT / "workspace" / "asset" / "account" / "datasets" / "active_experiment" / "dataset.csv")


class TestEndToEndFinalization:
    """Full end-to-end test: boot → orchestrate → finalize → validate all artifacts."""

    def test_all_artifacts_present(self):
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="finalize_e2e_test",
        )
        result = service.execute(request)
        assert result["status"] == "COMPLETED"

        run_dir = Path(result["run_dir"])

        # All 9 required artifacts
        required = [
            "dataset_file.csv", "matches.csv", "breaks.csv", "exclusions.csv",
            "matches_reasoning.json", "breaks_reasoning.json", "exclusions_reasoning.json",
            "matching_queries.sql", "run_manifest.json",
        ]
        for f in required:
            assert (run_dir / f).exists(), f"Missing artifact: {f}"

    def test_disjoint_outputs(self):
        """matches.csv + breaks.csv + exclusions.csv must be disjoint (no record in two files)."""
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="disjoint_test",
        )
        result = service.execute(request)
        run_dir = Path(result["run_dir"])

        # Collect record_ids from each output
        match_ids = set()
        with open(run_dir / "matches.csv") as f:
            for row in csv.DictReader(f):
                match_ids.add(row["record_id"])

        break_ids = set()
        with open(run_dir / "breaks.csv") as f:
            for row in csv.DictReader(f):
                break_ids.add(row["record_id"])

        excl_ids = set()
        with open(run_dir / "exclusions.csv") as f:
            for row in csv.DictReader(f):
                excl_ids.add(row["record_id"])

        # Check disjointness
        assert match_ids.isdisjoint(break_ids), f"Overlap match/break: {match_ids & break_ids}"
        assert match_ids.isdisjoint(excl_ids), f"Overlap match/excl: {match_ids & excl_ids}"
        assert break_ids.isdisjoint(excl_ids), f"Overlap break/excl: {break_ids & excl_ids}"

    def test_breaks_reasoning_count_matches_csv(self):
        """breaks_reasoning.json entry count should equal breaks.csv row count."""
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="reasoning_count_test",
        )
        result = service.execute(request)
        run_dir = Path(result["run_dir"])

        with open(run_dir / "breaks.csv") as f:
            break_rows = sum(1 for _ in csv.DictReader(f))

        with open(run_dir / "breaks_reasoning.json") as f:
            reasoning = json.load(f)
        reasoning_count = len(reasoning.get("breaks", []))

        assert reasoning_count == break_rows

    def test_manifest_summary_consistent(self):
        """Manifest summary totals should be consistent."""
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="manifest_test",
        )
        result = service.execute(request)
        run_dir = Path(result["run_dir"])

        with open(run_dir / "run_manifest.json") as f:
            manifest = json.load(f)

        summary = manifest["summary"]
        total = summary["total_excluded"] + summary["total_matched"] + summary["total_breaks"]
        assert total == summary["total_input_records"], \
            f"Excluded({summary['total_excluded']}) + Matched({summary['total_matched']}) + Breaks({summary['total_breaks']}) != Input({summary['total_input_records']})"

    def test_manifest_has_waterfall_steps(self):
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="waterfall_check_test",
        )
        result = service.execute(request)
        run_dir = Path(result["run_dir"])

        with open(run_dir / "run_manifest.json") as f:
            manifest = json.load(f)

        assert len(manifest["waterfall"]) > 0
        assert manifest["finished_at"] is not None

    def test_matching_queries_sql_accumulated(self):
        """matching_queries.sql should have entries from executed steps."""
        service = ReconMatchService(WORKSPACE_ROOT)
        request = ReconMatchRequest(
            workspace="asset", ruleset="account",
            dataset_absolute_path=DATASET_PATH,
            run_id="sql_audit_test",
        )
        result = service.execute(request)
        run_dir = Path(result["run_dir"])

        sql_content = (run_dir / "matching_queries.sql").read_text()
        assert "step_id:" in sql_content
        assert "rule_id:" in sql_content
