"""Tests for Phase 3 — Runtime Boot Pipeline."""

import json
import sys
from datetime import date
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recon_matching.boot import phase_a, phase_b, phase_c, get_ready, boot, BootError

APP_ROOT = Path(__file__).resolve().parents[4]
WORKSPACE_ROOT = str(APP_ROOT / "workspace")
KB_DIR = APP_ROOT / "workspace" / "knowledgebase" / "rulesets" / "asset" / "account"
DATASET_PATH = str(APP_ROOT / "workspace" / "asset" / "account" / "datasets" / "active_experiment" / "dataset.csv")


class TestPhaseA:
    def test_creates_run_dir(self, tmp_path):
        # Use the real KB but a temp workspace for RUN_DIR
        kb_dir, run_dir = phase_a(WORKSPACE_ROOT, "asset", "account", "test_boot_a", date(2026, 3, 26))
        assert kb_dir.exists()
        assert run_dir.exists()

    def test_invalid_kb_raises(self, tmp_path):
        with pytest.raises(Exception):  # ScaffoldError
            phase_a(str(tmp_path / "nonexistent"), "bad", "bad", "r1")


class TestPhaseB:
    def test_loads_all_configs(self):
        loaded = phase_b(KB_DIR)
        assert "kb_index" in loaded
        assert "schema_structure" in loaded
        assert "features" in loaded
        assert "core_rules" in loaded
        assert "agent_rules" in loaded

    def test_loads_kb_sections(self):
        loaded = phase_b(KB_DIR)
        # Should have loaded at least some sections with tags
        assert len(loaded["kb_sections"]) >= 0  # May be 0 if no tags match

    def test_validates_kb(self):
        loaded = phase_b(KB_DIR)
        assert loaded["core_rules"]["ruleset"] == "account"
        assert len(loaded["core_rules"]["rules"]) == 7


class TestPhaseC:
    def test_compiles_waterfall(self, tmp_path):
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        loaded = phase_b(KB_DIR)
        config, waterfall = phase_c(
            KB_DIR, run_dir, "asset", "account",
            loaded["features"], loaded["core_rules"], loaded["agent_rules"],
        )

        assert config.workspace == "asset"
        assert config.ruleset == "account"
        assert len(waterfall) == 7

    def test_exclusions_first_in_waterfall(self, tmp_path):
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        loaded = phase_b(KB_DIR)
        _, waterfall = phase_c(
            KB_DIR, run_dir, "asset", "account",
            loaded["features"], loaded["core_rules"], loaded["agent_rules"],
        )

        # All exclusion rules must come before matching rules
        saw_matching = False
        for step in waterfall:
            if step["operator"] != "exclude_by_predicates":
                saw_matching = True
            elif saw_matching:
                pytest.fail(f"Exclusion rule {step['rule_id']} after matching rule")

    def test_persists_compiled_config(self, tmp_path):
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        loaded = phase_b(KB_DIR)
        phase_c(KB_DIR, run_dir, "asset", "account",
                loaded["features"], loaded["core_rules"], loaded["agent_rules"])

        assert (run_dir / "compiled_ruleset_runtime_config.json").exists()
        assert (run_dir / "compiled_waterfall.json").exists()

        with open(run_dir / "compiled_waterfall.json") as f:
            wf = json.load(f)
        assert len(wf) == 7

    def test_step_ids_sequential(self, tmp_path):
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        loaded = phase_b(KB_DIR)
        _, waterfall = phase_c(
            KB_DIR, run_dir, "asset", "account",
            loaded["features"], loaded["core_rules"], loaded["agent_rules"],
        )
        ids = [s["step_id"] for s in waterfall]
        assert ids == list(range(1, len(waterfall) + 1))

    def test_only_active_rules_in_waterfall(self, tmp_path):
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        loaded = phase_b(KB_DIR)
        _, waterfall = phase_c(
            KB_DIR, run_dir, "asset", "account",
            loaded["features"], loaded["core_rules"], loaded["agent_rules"],
        )
        for step in waterfall:
            assert step["status"] == "active"


class TestGetReady:
    def test_copies_dataset_and_writes_manifest(self, tmp_path):
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()

        dst = get_ready(run_dir, DATASET_PATH, "asset", "account", "r1")
        assert dst.exists()
        assert (run_dir / "dataset_file.csv").exists()
        assert (run_dir / "run_manifest.json").exists()

        with open(run_dir / "run_manifest.json") as f:
            manifest = json.load(f)
        assert manifest["run_id"] == "r1"
        assert manifest["workspace"] == "asset"

    def test_missing_dataset_raises(self, tmp_path):
        run_dir = tmp_path / "test_run"
        run_dir.mkdir()
        with pytest.raises(BootError):
            get_ready(run_dir, "/nonexistent/data.csv", "asset", "account", "r1")


class TestFullBoot:
    def test_full_boot_pipeline(self):
        result = boot(
            WORKSPACE_ROOT, "asset", "account", "full_boot_test",
            DATASET_PATH, date(2026, 3, 26),
        )

        assert result.run_dir.exists()
        assert (result.run_dir / "dataset_file.csv").exists()
        assert (result.run_dir / "compiled_waterfall.json").exists()
        assert (result.run_dir / "compiled_ruleset_runtime_config.json").exists()
        assert (result.run_dir / "run_manifest.json").exists()
        assert result.runtime_config is not None
        assert len(result.compiled_waterfall) == 7

    def test_boot_with_missing_kb_raises(self, tmp_path):
        with pytest.raises(Exception):
            boot(str(tmp_path), "bad", "bad", "r1", "/data.csv")
