# Soul
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
