"""
Phase 1 — LROClient mock.

Simulates the in-house LongrunningOperation tracking system.
In production this would be replaced with the actual gRPC-like service client.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger


class LROClient:
    """Mock LongrunningOperation client."""

    def __init__(self):
        self._operations: dict[str, dict] = {}

    def create(self) -> str:
        """Create a new LRO. Returns the operation ID."""
        op_id = str(uuid.uuid4())
        self._operations[op_id] = {
            "id": op_id,
            "status": "RUNNING",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "completed_at": None,
            "response": None,
            "error": None,
        }
        logger.info("LRO created: {}", op_id)
        return op_id

    def complete(self, op_id: str, response: Optional[dict] = None, error: Optional[str] = None) -> None:
        """Complete an LRO with either a response or an error."""
        if op_id not in self._operations:
            raise ValueError(f"Unknown operation: {op_id}")

        op = self._operations[op_id]
        op["completed_at"] = datetime.now(timezone.utc).isoformat()

        if error:
            op["status"] = "FAILED"
            op["error"] = error
            logger.error("LRO {} failed: {}", op_id, error)
        else:
            op["status"] = "COMPLETED"
            op["response"] = response
            logger.info("LRO {} completed", op_id)

    def get(self, op_id: str) -> dict:
        """Get the current state of an LRO."""
        if op_id not in self._operations:
            raise ValueError(f"Unknown operation: {op_id}")
        return self._operations[op_id]

    def list_operations(self) -> list[dict]:
        """List all tracked operations."""
        return list(self._operations.values())
