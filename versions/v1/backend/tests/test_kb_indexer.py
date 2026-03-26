"""Tests for Phase 2 — KB Indexer."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recon_matching.kb_indexer import (
    build_kb_index, write_kb_index, read_section_by_offset, _split_markdown_sections,
)

KB_DIR = Path(__file__).resolve().parents[4] / "workspace" / "knowledgebase" / "rulesets" / "asset" / "account"


class TestKBIndexer:
    def test_build_index_has_required_keys(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        assert idx["workspace"] == "asset"
        assert idx["ruleset"] == "account"
        assert "generated_at" in idx
        assert "files" in idx
        assert "markdown_sections" in idx
        assert "operators" in idx
        assert "blocking_strategies" in idx
        assert "amount_policy" in idx

    def test_files_metadata(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        assert "schema_structure.json" in idx["files"]
        assert "core_matching_rules.json" in idx["files"]
        for name, meta in idx["files"].items():
            assert "size_bytes" in meta
            assert "modified_at" in meta

    def test_markdown_sections_extracted(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        sections = idx["markdown_sections"]
        assert len(sections) > 0

        # Check structure
        for sec in sections:
            assert "heading" in sec
            assert "level" in sec
            assert "byte_offset" in sec
            assert "byte_length" in sec
            assert "tags" in sec
            assert "summary" in sec
            assert "file" in sec

    def test_identity_md_sections(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        identity_sections = [s for s in idx["markdown_sections"] if s["file"] == "identity.md"]
        assert len(identity_sections) > 0
        headings = [s["heading"] for s in identity_sections]
        assert "Mission" in headings or "Identity" in headings

    def test_soul_md_sections(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        soul_sections = [s for s in idx["markdown_sections"] if s["file"] == "soul.md"]
        assert len(soul_sections) > 0

    def test_how_to_work_md_sections(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        htw_sections = [s for s in idx["markdown_sections"] if s["file"] == "how_to_work.md"]
        assert len(htw_sections) > 0

    def test_operator_inventory(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        ops = idx["operators"]
        assert len(ops) == 7  # 3 exclusion + 4 matching

        for op in ops:
            assert "rule_id" in op
            assert "operator" in op
            assert "source" in op
            assert op["source"] in ("core", "agent")

        operators_set = {op["operator"] for op in ops}
        assert "exclude_by_predicates" in operators_set
        assert "one_to_one_ranked" in operators_set

    def test_operator_inventory_matches_source(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        with open(KB_DIR / "core_matching_rules.json") as f:
            core = json.load(f)
        core_count = len(core["rules"])
        index_core_count = sum(1 for op in idx["operators"] if op["source"] == "core")
        assert index_core_count == core_count

    def test_blocking_strategies_shortcut(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        assert len(idx["blocking_strategies"]) == 2
        assert idx["blocking_strategies"][0]["name"] == "fund+blk_cusip"

    def test_amount_policy_shortcut(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        assert idx["amount_policy"]["tolerance"] == 0.005

    def test_write_and_read_roundtrip(self):
        path = write_kb_index(KB_DIR, "asset", "account")
        assert path.exists()
        with open(path) as f:
            loaded = json.load(f)
        assert loaded["workspace"] == "asset"
        assert len(loaded["operators"]) == 7

    def test_section_read_by_offset(self):
        idx = build_kb_index(KB_DIR, "asset", "account")
        sections = idx["markdown_sections"]
        assert len(sections) > 0

        sec = sections[0]
        filepath = KB_DIR / sec["file"]
        content = read_section_by_offset(filepath, sec["byte_offset"], sec["byte_length"])
        assert len(content) > 0
        assert sec["heading"] in content

    def test_nonexistent_kb_dir_raises(self):
        with pytest.raises(FileNotFoundError):
            build_kb_index(Path("/nonexistent"), "x", "y")


class TestMarkdownSplitter:
    def test_split_with_tags(self, tmp_path):
        md = tmp_path / "test.md"
        md.write_text("# Title\n<!-- tags: mission, outputs -->\nContent here.\n\n## Sub\nMore content.\n")
        sections = _split_markdown_sections(md)
        assert len(sections) == 2
        assert sections[0]["heading"] == "Title"
        assert "mission" in sections[0]["tags"]
        assert sections[1]["heading"] == "Sub"

    def test_split_preserves_offsets(self, tmp_path):
        md = tmp_path / "test.md"
        text = "# First\nAAA\n\n# Second\nBBB\n"
        md.write_text(text)
        sections = _split_markdown_sections(md)
        assert len(sections) == 2
        # Read back by offset
        content = read_section_by_offset(md, sections[1]["byte_offset"], sections[1]["byte_length"])
        assert "Second" in content
        assert "BBB" in content
