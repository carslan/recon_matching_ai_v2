# How to Work
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
