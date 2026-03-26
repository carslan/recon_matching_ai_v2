"""
Phase 4 — Write-path guardrail.

Enforces that agents can only write to:
  - workspace/outputs/...
  - workspace/knowledgebase/.../agent_rule_proposals/...

Any write attempt outside these paths raises GuardrailViolation.
"""

from pathlib import Path

from loguru import logger


class GuardrailViolation(Exception):
    pass


class WriteGuardrail:
    """File I/O wrapper that enforces write-path boundaries."""

    def __init__(self, allowed_output_root: str, allowed_proposal_root: str):
        self.allowed_output = Path(allowed_output_root).resolve()
        self.allowed_proposal = Path(allowed_proposal_root).resolve()

    def check_write(self, target_path: str) -> None:
        """Check if writing to target_path is allowed. Raises GuardrailViolation if not."""
        resolved = Path(target_path).resolve()

        if self._is_under(resolved, self.allowed_output):
            return
        if self._is_under(resolved, self.allowed_proposal) and "agent_rule_proposals" in str(resolved):
            return

        raise GuardrailViolation(
            f"Write blocked: {target_path} is outside allowed paths. "
            f"Allowed: {self.allowed_output}, {self.allowed_proposal}/*/agent_rule_proposals/"
        )

    def safe_write(self, target_path: str, content: str) -> None:
        """Write content to file, but only if path passes guardrail check."""
        self.check_write(target_path)
        path = Path(target_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def _is_under(self, child: Path, parent: Path) -> bool:
        try:
            child.relative_to(parent)
            return True
        except ValueError:
            return False
