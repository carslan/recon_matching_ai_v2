"""
Agent Interaction Logger

Captures every message between Orchestrator and Executor:
- Orchestrator decisions (what to run next, why)
- ExecutorRequest sent
- SQL generated and executed
- OperatorResult received
- Stop condition checks
- Pool state changes

Stored per-run as interaction_log.json in RUN_DIR.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


class InteractionLog:
    """Thread-safe interaction logger for a single run."""

    def __init__(self, run_dir: str, run_id: str):
        self.run_dir = Path(run_dir)
        self.run_id = run_id
        self.entries: list[dict] = []
        self._seq = 0

    def log(self, actor: str, action: str, detail: Any = None, level: str = "info") -> None:
        """
        Log an interaction event.

        actor: "orchestrator", "executor", "duckdb", "guardrail", "boot", "finalize"
        action: short description of what happened
        detail: arbitrary data (dict, string, etc.)
        level: "info", "decision", "data", "sql", "error", "warning"
        """
        self._seq += 1
        entry = {
            "seq": self._seq,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "actor": actor,
            "action": action,
            "level": level,
            "detail": detail,
        }
        self.entries.append(entry)

    def log_decision(self, actor: str, action: str, detail: Any = None) -> None:
        self.log(actor, action, detail, level="decision")

    def log_data(self, actor: str, action: str, detail: Any = None) -> None:
        self.log(actor, action, detail, level="data")

    def log_sql(self, actor: str, sql: str, context: str = "") -> None:
        self.log(actor, f"SQL: {context}", {"sql": sql}, level="sql")

    def log_error(self, actor: str, action: str, detail: Any = None) -> None:
        self.log(actor, action, detail, level="error")

    def flush(self) -> None:
        """Write the full log to interaction_log.json in RUN_DIR."""
        out_path = self.run_dir / "interaction_log.json"
        payload = {
            "run_id": self.run_id,
            "total_events": len(self.entries),
            "events": self.entries,
        }
        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)

    def get_entries(self) -> list[dict]:
        return self.entries
