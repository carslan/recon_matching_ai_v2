"""
CLI entry point for Phase 0 migration.

Usage:
    python -m recon_matching.cli_migrate \
        --ruleset-path datasets/cust_of/ruleset.json \
        --target-dir workspace/knowledgebase/rulesets/asset/account

    # With synthetic dataset generation:
    python -m recon_matching.cli_migrate \
        --ruleset-path datasets/cust_of/ruleset.json \
        --target-dir workspace/knowledgebase/rulesets/asset/account \
        --generate-dataset \
        --dataset-target workspace/asset/account/datasets/active_experiment/dataset.csv
"""

import argparse
import json
import sys
from pathlib import Path

from loguru import logger

# Resolve project root for imports
PROJECT_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from recon_matching.migration import migrate_ruleset
from recon_matching.synthetic_data import generate_dataset


def main():
    parser = argparse.ArgumentParser(description="Phase 0: Migrate ruleset.json to KB files")
    parser.add_argument("--ruleset-path", required=True, help="Path to legacy ruleset.json")
    parser.add_argument("--target-dir", required=True, help="Target KB directory for output files")
    parser.add_argument("--generate-dataset", action="store_true", help="Also generate synthetic test dataset")
    parser.add_argument("--dataset-target", default=None, help="Path for generated dataset CSV")
    parser.add_argument("--side-a", type=int, default=160, help="Number of Side A records")
    parser.add_argument("--side-b", type=int, default=242, help="Number of Side B records")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for dataset generation")

    args = parser.parse_args()

    # Run migration
    summary = migrate_ruleset(args.ruleset_path, args.target_dir)
    print(json.dumps(summary, indent=2))

    # Optionally generate dataset
    if args.generate_dataset:
        dataset_target = args.dataset_target or str(
            Path(args.target_dir).parent / "datasets" / "active_experiment" / "dataset.csv"
        )
        generate_dataset(dataset_target, args.side_a, args.side_b, args.seed)
        print(f"Dataset generated: {dataset_target}")


if __name__ == "__main__":
    main()
