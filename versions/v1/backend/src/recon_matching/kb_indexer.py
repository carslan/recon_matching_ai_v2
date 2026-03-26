"""
Phase 2 — Offline KB Indexer

Scans a ruleset KB directory and produces kb_index.json.
Runtime agents use this index for minimal reads — they never re-scan raw KB files.

Usage:
    python -m recon_matching.kb_indexer --workspace asset --ruleset account \
        --kb-root workspace/knowledgebase/rulesets
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger


def _split_markdown_sections(filepath: Path) -> list[dict]:
    """
    Split a markdown file by headings.
    Returns list of {heading, level, byte_offset, byte_length, tags, summary}.
    Tags are extracted from HTML comments: <!-- tags: tag1, tag2 -->
    """
    content = filepath.read_bytes()
    text = content.decode("utf-8")
    lines = text.split("\n")

    sections: list[dict] = []
    current_heading = None
    current_level = 0
    current_start = 0
    current_tags: list[str] = []
    current_lines: list[str] = []

    byte_pos = 0
    for line in lines:
        line_bytes = len(line.encode("utf-8")) + 1  # +1 for \n

        heading_match = re.match(r"^(#{1,6})\s+(.+)$", line)
        tag_match = re.match(r"^\s*<!--\s*tags?:\s*(.+?)\s*-->", line)

        if tag_match:
            current_tags = [t.strip() for t in tag_match.group(1).split(",")]

        if heading_match:
            # Close previous section
            if current_heading is not None:
                section_text = "\n".join(current_lines).strip()
                summary = _summarize(section_text)
                sections.append({
                    "heading": current_heading,
                    "level": current_level,
                    "byte_offset": current_start,
                    "byte_length": byte_pos - current_start,
                    "tags": current_tags,
                    "summary": summary,
                })

            current_heading = heading_match.group(2).strip()
            current_level = len(heading_match.group(1))
            current_start = byte_pos
            current_tags = []
            current_lines = []
        else:
            current_lines.append(line)

        byte_pos += line_bytes

    # Close final section
    if current_heading is not None:
        section_text = "\n".join(current_lines).strip()
        summary = _summarize(section_text)
        sections.append({
            "heading": current_heading,
            "level": current_level,
            "byte_offset": current_start,
            "byte_length": byte_pos - current_start,
            "tags": current_tags,
            "summary": summary,
        })

    return sections


def _summarize(text: str, max_sentences: int = 3) -> str:
    """Extract first N sentences as summary."""
    # Remove HTML comments and blank lines
    clean = re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL).strip()
    if not clean:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", clean)
    return " ".join(sentences[:max_sentences]).strip()


def _extract_operator_inventory(rules_data: dict, source: str) -> list[dict]:
    """Extract operator inventory from a rules JSON file."""
    operators = []
    for rule in rules_data.get("rules", []):
        operators.append({
            "rule_id": rule.get("rule_id", ""),
            "status": rule.get("status", ""),
            "operator": rule.get("operator", ""),
            "source": source,
            "params_keys": list(rule.get("params", {}).keys()),
            "tags": [],
        })
    return operators


def build_kb_index(kb_dir: Path, workspace: str, ruleset: str) -> dict:
    """
    Build kb_index.json for a given KB directory.

    Returns the index dict with:
    - workspace, ruleset, generated_at
    - files metadata
    - markdown_sections[] with byte offsets
    - operators[] inventory
    - blocking_strategies[], amount_policy shortcuts
    """
    if not kb_dir.exists():
        raise FileNotFoundError(f"KB directory not found: {kb_dir}")

    index: dict[str, Any] = {
        "workspace": workspace,
        "ruleset": ruleset,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": {},
        "markdown_sections": [],
        "operators": [],
        "blocking_strategies": [],
        "amount_policy": {},
    }

    # File metadata
    for f in sorted(kb_dir.iterdir()):
        if f.is_file():
            index["files"][f.name] = {
                "size_bytes": f.stat().st_size,
                "modified_at": datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc).isoformat(),
            }

    # Markdown section splitting
    for md_name in ["identity.md", "soul.md", "how_to_work.md"]:
        md_path = kb_dir / md_name
        if md_path.exists():
            sections = _split_markdown_sections(md_path)
            for sec in sections:
                sec["file"] = md_name
            index["markdown_sections"].extend(sections)

    # Operator inventory from rules
    core_rules_path = kb_dir / "core_matching_rules.json"
    if core_rules_path.exists():
        with open(core_rules_path) as f:
            core_data = json.load(f)
        index["operators"].extend(_extract_operator_inventory(core_data, "core"))

    agent_rules_path = kb_dir / "agent_matching_rules.json"
    if agent_rules_path.exists():
        with open(agent_rules_path) as f:
            agent_data = json.load(f)
        index["operators"].extend(_extract_operator_inventory(agent_data, "agent"))

    # Feature shortcuts
    features_path = kb_dir / "features.json"
    if features_path.exists():
        with open(features_path) as f:
            features = json.load(f)
        index["blocking_strategies"] = features.get("blocking_strategies", [])
        index["amount_policy"] = features.get("amount_policy", {})

    return index


def write_kb_index(kb_dir: Path, workspace: str, ruleset: str) -> Path:
    """Build and write kb_index.json to the KB directory. Returns the path."""
    index = build_kb_index(kb_dir, workspace, ruleset)
    out_path = kb_dir / "kb_index.json"
    with open(out_path, "w") as f:
        json.dump(index, f, indent=2)

    logger.info(
        "Wrote kb_index.json: {} files, {} markdown sections, {} operators",
        len(index["files"]), len(index["markdown_sections"]), len(index["operators"]),
    )
    return out_path


def read_section_by_offset(filepath: Path, byte_offset: int, byte_length: int) -> str:
    """Read a specific markdown section by byte offset. Used at runtime for minimal reads."""
    with open(filepath, "rb") as f:
        f.seek(byte_offset)
        data = f.read(byte_length)
    return data.decode("utf-8")


# ─── CLI ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Phase 2: Build kb_index.json")
    parser.add_argument("--workspace", required=True)
    parser.add_argument("--ruleset", required=True)
    parser.add_argument("--kb-root", required=True, help="Root KB directory (contains workspace/ruleset/)")
    args = parser.parse_args()

    kb_dir = Path(args.kb_root) / args.workspace / args.ruleset
    path = write_kb_index(kb_dir, args.workspace, args.ruleset)
    print(f"Index written: {path}")
