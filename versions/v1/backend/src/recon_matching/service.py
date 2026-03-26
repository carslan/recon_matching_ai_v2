"""
ReconMatch RPC handler.

Integrates boot pipeline + orchestrator + LROClient into a single callable.
Creates InteractionLog for full observability.
"""

import json
from datetime import date
from pathlib import Path
from typing import Optional

from loguru import logger

from .boot import boot, BootResult, BootError
from .interaction_log import InteractionLog
from .lro_client import LROClient
from .orchestrator import Orchestrator
from .schemas import ReconMatchRequest


class ReconMatchService:

    def __init__(self, workspace_root: str, lro_client: Optional[LROClient] = None):
        self.workspace_root = workspace_root
        self.lro_client = lro_client or LROClient()

    def execute(self, request: ReconMatchRequest) -> dict:
        op_id = self.lro_client.create()
        logger.info("ReconMatch service: run_id={}, lro_id={}", request.run_id, op_id)

        try:
            # Boot
            boot_result = boot(
                workspace_root=self.workspace_root,
                workspace=request.workspace,
                ruleset=request.ruleset,
                run_id=request.run_id,
                dataset_path=request.dataset_absolute_path,
            )

            # Create interaction log
            ilog = InteractionLog(str(boot_result.run_dir), request.run_id)
            ilog.log("boot", "Boot pipeline complete", {
                "kb_dir": str(boot_result.kb_dir),
                "run_dir": str(boot_result.run_dir),
                "waterfall_steps": len(boot_result.compiled_waterfall),
                "schema_columns": len(boot_result.schema_structure.get("columns", [])),
                "number_columns": len(boot_result.schema_structure.get("number_columns", {})),
                "blocking_strategies": [bs["name"] for bs in boot_result.features.get("blocking_strategies", [])],
            })

            # Orchestrate
            orchestrator = Orchestrator(
                run_dir=str(boot_result.run_dir),
                workspace=request.workspace,
                ruleset=request.ruleset,
                run_id=request.run_id,
                dataset_replay_path=str(boot_result.run_dir / "dataset_file.csv"),
                schema_structure=boot_result.schema_structure,
                features=boot_result.features,
                interaction_log=ilog,
            )

            summary = orchestrator.run()

            self.lro_client.complete(op_id, response={
                "run_id": request.run_id,
                "run_dir": str(boot_result.run_dir),
                "summary": summary,
            })

            return {
                "run_id": request.run_id,
                "lro_id": op_id,
                "status": "COMPLETED",
                "run_dir": str(boot_result.run_dir),
                "summary": summary,
            }

        except Exception as e:
            logger.error("ReconMatch failed for run_id={}: {}", request.run_id, e)
            self.lro_client.complete(op_id, error=str(e))
            return {
                "run_id": request.run_id,
                "lro_id": op_id,
                "status": "FAILED",
                "error": str(e),
            }
