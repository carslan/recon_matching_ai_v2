"""
Phase 0 — Migration Utility

Translates legacy ruleset.json format into KB-ready files:
  - schema_structure.json
  - core_matching_rules.json
  - features.json
  - agent_matching_rules.json (empty scaffold)

This is an offline, one-time migration per ruleset — not part of production runtime.
"""

import json
import os
from pathlib import Path
from typing import Any

from loguru import logger


# Legacy type prefix → canonical type
TYPE_MAP = {
    "VALUE_TYPE_STRING": "STRING",
    "VALUE_TYPE_LONG": "LONG",
    "VALUE_TYPE_DOUBLE": "DOUBLE",
    "VALUE_TYPE_DATE": "DATE",
    "VALUE_TYPE_DATE_TIME": "DATE_TIME",
    # Pass through already-canonical types
    "STRING": "STRING",
    "LONG": "LONG",
    "DOUBLE": "DOUBLE",
    "DATE": "DATE",
    "DATE_TIME": "DATE_TIME",
}

# Legacy rule type → canonical operator
OPERATOR_MAP = {
    "exclusion": "exclude_by_predicates",
    "one_to_one": "one_to_one_ranked",
    "many_to_many": "many_to_many_balance_k",
    "one_sided": "one_sided",
}

NUMERIC_TYPES = {"DOUBLE", "LONG"}


class ReconAIPReprocessing:
    """Migrates legacy ruleset.json into KB-ready artifacts."""

    def __init__(self, ruleset_path: str, target_dir: str):
        self.ruleset_path = Path(ruleset_path)
        self.target_dir = Path(target_dir)
        self.target_dir.mkdir(parents=True, exist_ok=True)

        with open(self.ruleset_path) as f:
            self.ruleset = json.load(f)

        logger.info("Loaded ruleset from {}", self.ruleset_path)

    def create_fat_schema(self) -> dict:
        """
        Merge base columns + computed columns into canonical schema_structure.json.
        Returns the schema dict and writes it to target_dir.
        """
        schema = self.ruleset.get("schema", {})
        base_cols = schema.get("columns", [])
        computed_cols = schema.get("computed_columns", [])

        columns = []
        number_columns = {}

        # Process base columns
        for col in base_cols:
            canonical_type = TYPE_MAP.get(col["type"], col["type"])
            entry = {"name": col["name"], "type": canonical_type}
            columns.append(entry)
            if canonical_type in NUMERIC_TYPES:
                number_columns[col["name"]] = canonical_type

        # Process computed columns
        for col in computed_cols:
            canonical_type = TYPE_MAP.get(col["type"], col["type"])
            entry = {
                "name": col["name"],
                "type": canonical_type,
                "expression": col["expression"],
            }
            columns.append(entry)
            if canonical_type in NUMERIC_TYPES:
                number_columns[col["name"]] = canonical_type

        schema_structure = {
            "dataset_type": self.ruleset.get("dataset_type", self.ruleset.get("ruleset", "unknown")),
            "number_columns": number_columns,
            "columns": columns,
        }

        target_path = self.target_dir / "schema_structure.json"
        with open(target_path, "w") as f:
            json.dump(schema_structure, f, indent=2)

        logger.info(
            "Created schema_structure.json: {} base cols, {} computed cols, {} numeric cols",
            len(base_cols), len(computed_cols), len(number_columns),
        )
        return schema_structure

    def migrate_rules(self) -> dict:
        """
        Convert legacy rules into canonical core_matching_rules.json.
        Exclusion rules get EXCL_ prefix enforced. Returns the rules dict.
        """
        rules = self.ruleset.get("rules", [])
        canonical_rules = []

        for rule in rules:
            legacy_type = rule.get("type", "")
            operator = OPERATOR_MAP.get(legacy_type)
            if not operator:
                logger.warning("Unknown rule type '{}' for rule '{}', skipping", legacy_type, rule.get("rule_id"))
                continue

            rule_id = rule.get("rule_id", rule.get("rule_name", "UNKNOWN").upper().replace(" ", "_"))

            # Enforce EXCL_ prefix for exclusion rules
            if operator == "exclude_by_predicates" and not rule_id.startswith("EXCL_"):
                rule_id = f"EXCL_{rule_id}"

            params = self._build_params(rule, operator)

            canonical_rules.append({
                "rule_id": rule_id,
                "status": "active",
                "operator": operator,
                "params": params,
            })

        core_rules = {
            "ruleset": self.ruleset.get("ruleset", "unknown"),
            "rules": canonical_rules,
        }

        target_path = self.target_dir / "core_matching_rules.json"
        with open(target_path, "w") as f:
            json.dump(core_rules, f, indent=2)

        excl_count = sum(1 for r in canonical_rules if r["operator"] == "exclude_by_predicates")
        match_count = len(canonical_rules) - excl_count
        logger.info(
            "Created core_matching_rules.json: {} exclusion rules, {} matching rules",
            excl_count, match_count,
        )
        return core_rules

    def _build_params(self, rule: dict, operator: str) -> dict:
        """Build canonical params from legacy rule structure."""
        params: dict[str, Any] = {}
        conditions = rule.get("conditions", {})

        if operator == "exclude_by_predicates":
            # Use null-safe column references in filters
            if conditions.get("side_a"):
                params["side_a_filter"] = self._null_safe_filter(conditions["side_a"])
            if conditions.get("side_b"):
                params["side_b_filter"] = self._null_safe_filter(conditions["side_b"])
            return params

        if operator == "one_sided":
            if conditions.get("side_a"):
                params["side_filter"] = conditions["side_a"]
            blocking = rule.get("blocking", {})
            if blocking.get("keys"):
                params["blocking_strategy"] = {"keys": blocking["keys"]}
            comparisons = rule.get("comparisons", [])
            if comparisons:
                params["comparisons"] = self._build_one_sided_comparisons(comparisons)
            return params

        # one_to_one_ranked / many_to_many_balance_k
        if conditions.get("side_a"):
            params["side_a_filter"] = conditions["side_a"]
        if conditions.get("side_b"):
            params["side_b_filter"] = conditions["side_b"]

        blocking = rule.get("blocking", {})
        if blocking.get("keys_a"):
            params["blocking_strategy"] = {
                "keys_a": blocking["keys_a"],
                "keys_b": blocking["keys_b"],
            }

        comparisons = rule.get("comparisons", [])
        if comparisons:
            params["comparisons"] = self._build_two_sided_comparisons(comparisons, operator)

        return params

    def _null_safe_filter(self, filter_str: str) -> str:
        """Replace raw column references with null-safe versions where applicable."""
        replacements = {
            "ext_asset_info IN": "ext_asset_info_null_safe IN",
            "sec_type NOT IN": "sec_type_null_safe NOT IN",
            "sec_type ==": "sec_type_null_safe ==",
        }
        result = filter_str
        for old, new in replacements.items():
            result = result.replace(old, new)
        return result

    def _build_two_sided_comparisons(self, comparisons: list, operator: str) -> list:
        """Build canonical two-sided comparison entries."""
        result = []
        for comp in comparisons:
            entry = {
                "column_a": comp["column_a"],
                "column_b": comp["column_b"],
                "tolerance": comp.get("tolerance", 0),
                "tolerance_type": comp.get("tolerance_type", "abs"),
            }
            if "tolerance_behavior" in comp:
                entry["tolerance_behavior"] = comp["tolerance_behavior"]
            else:
                entry["tolerance_behavior"] = "exclusive"

            if "required" in comp:
                entry["required"] = comp["required"]

            # Aggregation required for many_to_many_balance_k
            if "aggregation" in comp:
                entry["aggregation"] = comp["aggregation"]
            elif operator == "many_to_many_balance_k":
                entry["aggregation"] = "SumIgnoreNulls"

            result.append(entry)
        return result

    def _build_one_sided_comparisons(self, comparisons: list) -> list:
        """Build canonical one-sided comparison entries."""
        result = []
        for comp in comparisons:
            entry = {
                "column": comp["column"],
                "tolerance": comp.get("tolerance", 0),
                "tolerance_type": comp.get("tolerance_type", "abs"),
            }
            if "aggregation" in comp:
                entry["aggregation"] = comp["aggregation"]
            else:
                entry["aggregation"] = "SumIgnoreNulls"

            if "required" in comp:
                entry["required"] = comp["required"]

            result.append(entry)
        return result

    def migrate_features(self) -> dict:
        """Extract features.json from legacy ruleset."""
        features = self.ruleset.get("features", {})

        features_out = {
            "blocking_strategies": features.get("blocking_strategies", []),
            "amount_policy": features.get("amount_policy", {"tolerance": 0.005}),
        }

        target_path = self.target_dir / "features.json"
        with open(target_path, "w") as f:
            json.dump(features_out, f, indent=2)

        logger.info(
            "Created features.json: {} blocking strategies, tolerance={}",
            len(features_out["blocking_strategies"]),
            features_out["amount_policy"].get("tolerance"),
        )
        return features_out

    def create_agent_matching_rules(self) -> dict:
        """Create empty agent_matching_rules.json scaffold."""
        agent_rules = {
            "ruleset": self.ruleset.get("ruleset", "unknown"),
            "rules": [],
        }

        target_path = self.target_dir / "agent_matching_rules.json"
        with open(target_path, "w") as f:
            json.dump(agent_rules, f, indent=2)

        logger.info("Created empty agent_matching_rules.json")
        return agent_rules

    def create_markdown_files(self) -> None:
        """Create KB markdown files: identity.md, soul.md, how_to_work.md."""
        ruleset_name = self.ruleset.get("ruleset", "unknown")

        identity_md = f"""# Identity
<!-- tags: mission, outputs -->

## Mission
You are a reconciliation matching agent for the **{ruleset_name}** ruleset.
Your job is to match records between Side A and Side B of a dataset,
identify exclusions, and report breaks with clear reasoning.

## Outputs
- matches.csv — paired/grouped records
- breaks.csv — unmatched records with reasoning
- exclusions.csv — records removed before matching
- Full reasoning JSON for each category
- Auditable SQL trail of every operation
"""

        soul_md = """# Soul
<!-- tags: guardrails, write_paths -->

## Guardrails
- Never mutate input datasets
- Never modify core_matching_rules.json or agent_matching_rules.json
- Always produce auditable outputs
- Never activate new rules without human approval
- All matching must be deterministic: same input + same KB = same output

## Write Paths
- workspace/outputs/... (run artifacts)
- workspace/knowledgebase/.../agent_rule_proposals/... (proposals only)
"""

        how_to_work_md = """# How to Work
<!-- tags: waterfall, stop_conditions -->

## Waterfall
1. Execute all exclusion rules first (exclude_by_predicates), in declared order
2. Execute matching rules in declared order (one_to_one_ranked, many_to_many_balance_k, one_sided)
3. Never reorder the waterfall
4. Each step operates on pool_unmatched (records not yet matched or excluded)

## Stop Conditions
- Pool is empty
- N consecutive steps with records_removed_from_pool == 0
- Guardrail triggered (e.g., operator timeout)
- All waterfall steps exhausted
"""

        for filename, content in [
            ("identity.md", identity_md),
            ("soul.md", soul_md),
            ("how_to_work.md", how_to_work_md),
        ]:
            target_path = self.target_dir / filename
            with open(target_path, "w") as f:
                f.write(content)

        logger.info("Created KB markdown files: identity.md, soul.md, how_to_work.md")

    def create_agent_rule_proposals_dir(self) -> None:
        """Create the agent_rule_proposals directory."""
        proposals_dir = self.target_dir / "agent_rule_proposals"
        proposals_dir.mkdir(exist_ok=True)
        logger.info("Created agent_rule_proposals/ directory")

    def run_full_migration(self) -> dict:
        """Execute the complete migration pipeline. Returns summary."""
        logger.info("Starting full migration: {} -> {}", self.ruleset_path, self.target_dir)

        schema = self.create_fat_schema()
        rules = self.migrate_rules()
        features = self.migrate_features()
        agent_rules = self.create_agent_matching_rules()
        self.create_markdown_files()
        self.create_agent_rule_proposals_dir()

        summary = {
            "schema_columns": len(schema["columns"]),
            "number_columns": len(schema["number_columns"]),
            "total_rules": len(rules["rules"]),
            "exclusion_rules": sum(1 for r in rules["rules"] if r["operator"] == "exclude_by_predicates"),
            "matching_rules": sum(1 for r in rules["rules"] if r["operator"] != "exclude_by_predicates"),
            "blocking_strategies": len(features["blocking_strategies"]),
            "files_created": [
                "schema_structure.json",
                "core_matching_rules.json",
                "features.json",
                "agent_matching_rules.json",
                "identity.md",
                "soul.md",
                "how_to_work.md",
            ],
        }

        logger.info("Migration complete: {}", json.dumps(summary, indent=2))
        return summary


def migrate_ruleset(ruleset_path: str, target_dir: str) -> dict:
    """Convenience function: migrate a single ruleset.json to KB files."""
    migrator = ReconAIPReprocessing(ruleset_path, target_dir)
    return migrator.run_full_migration()
