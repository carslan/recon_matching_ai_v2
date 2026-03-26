# Active TODOs

[2026-03-26] Phase 7 — RuleCreator + Proposal Test Harness
Status: not started
Description: After matching completes, analyze breaks and propose new rules via sandbox execution. Writes to agent_rule_proposals/ only. Blocked on TODO-6 (trigger mode — automatic post-waterfall vs on-demand).

[2026-03-26] Resolve TODO-6
Status: not started
Description: Decide whether RuleCreator runs automatically post-waterfall or on-demand via separate invocation. Blocks Phase 7.

[2026-03-26] Google ADK Integration
Status: not started
Description: Replace plain Python Orchestrator/Executor with ADK LlmAgent/AgentTool wiring per the development plan. Requires google-adk==1.26.0 installation.

[2026-03-26] Real Dataset Integration
Status: not started
Description: When real ruleset.json and dataset CSVs become available, run Phase 0 migration and validate against actual data. Current system only tested with synthetic data.
