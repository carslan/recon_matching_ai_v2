"""
Phase 5 — DuckDB Skill

Narrow API for DuckDB-backed operator execution. No freestyle SQL at runtime.
All SQL is template-driven from operator specs.

Functions:
  duckdb_open_or_create(run_dir) -> db_path
  duckdb_load_dataset(db_path, csv_path) -> None
  duckdb_init_pool(db_path, identity_cols) -> None
  duckdb_profile(db_path, schema_structure, features) -> DatasetProfile
  duckdb_execute_operator(db_path, operator_spec, runtime_config, dataset_profile) -> OperatorResult
  duckdb_export_exclusions_csv(db_path, run_dir, schema_structure) -> None
  duckdb_export_matches_breaks_csv(db_path, run_dir) -> None
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
from loguru import logger

from .schemas import (
    OperatorSpec, OperatorResult, OperatorEffects, OperatorTiming,
    ArtifactFlags, OperatorEvidence, ReasoningDelta,
    DatasetProfile, DatasetInfo, DatasetType, ColumnInfo, AmountColumns,
    StepStatus, OperatorType, MatchGroup, RecordRef, MatchGroupComparison,
)


# ─── DB Lifecycle ───────────────────────────────────────────────────────────

def duckdb_open_or_create(run_dir: str) -> str:
    """Open or create a DuckDB database for this run. Returns db_path."""
    db_path = str(Path(run_dir) / "run.duckdb")
    conn = duckdb.connect(db_path)
    conn.close()
    logger.info("DuckDB opened: {}", db_path)
    return db_path


def duckdb_load_dataset(db_path: str, csv_path: str) -> None:
    """Load enriched dataset CSV into base_dataset table (all columns as VARCHAR initially)."""
    conn = duckdb.connect(db_path)
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS base_dataset AS
            SELECT * FROM read_csv_auto('{csv_path}', all_varchar=true)
        """)
        count = conn.execute("SELECT COUNT(*) FROM base_dataset").fetchone()[0]
        logger.info("Loaded {} rows into base_dataset from {}", count, csv_path)
    finally:
        conn.close()


def duckdb_init_pool(db_path: str, identity_cols: list[str] | None = None) -> None:
    """
    Create matched_records table (append-only) and pool_unmatched view.
    Identity columns: side, statement_id, record_id.
    """
    if identity_cols is None:
        identity_cols = ["side", "statement_id", "record_id"]

    conn = duckdb.connect(db_path)
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS matched_records (
                match_group_id VARCHAR,
                side VARCHAR,
                statement_id VARCHAR,
                record_id VARCHAR,
                rule_id VARCHAR,
                step_id INTEGER,
                operator VARCHAR,
                matched_at TIMESTAMP DEFAULT current_timestamp
            )
        """)

        conn.execute("""
            CREATE OR REPLACE VIEW pool_unmatched AS
            SELECT bd.*
            FROM base_dataset bd
            WHERE bd.record_id NOT IN (SELECT record_id FROM matched_records)
        """)

        total = conn.execute("SELECT COUNT(*) FROM base_dataset").fetchone()[0]
        matched = conn.execute("SELECT COUNT(*) FROM matched_records").fetchone()[0]
        logger.info("Pool initialized: {} total, {} matched, {} unmatched", total, matched, total - matched)
    finally:
        conn.close()


# ─── Profiling ──────────────────────────────────────────────────────────────

def duckdb_profile(db_path: str, schema_structure: dict, features: dict, run_id: str) -> DatasetProfile:
    """Profile the loaded dataset. Persists dataset_profile.json."""
    conn = duckdb.connect(db_path)
    try:
        total = conn.execute("SELECT COUNT(*) FROM base_dataset").fetchone()[0]

        # Side counts
        sides = conn.execute("SELECT side, COUNT(*) FROM base_dataset GROUP BY side").fetchall()
        side_counts = {row[0]: row[1] for row in sides}

        # Dataset type
        ds_type = DatasetType.TWO_SIDED if len(side_counts) > 1 else DatasetType.ONE_SIDED

        # Column presence
        cols = [row[0] for row in conn.execute("DESCRIBE base_dataset").fetchall()]

        # Amount columns from schema_structure.number_columns
        number_cols = list(schema_structure.get("number_columns", {}).keys())
        detected_amounts = [c for c in number_cols if c in cols]

        # Blocking quality
        blocking_quality = {}
        for bs in features.get("blocking_strategies", []):
            keys = bs.get("keys", [])
            if all(k in cols for k in keys):
                # Coverage = fraction of rows with non-null values in all blocking keys
                conditions = " AND ".join(f"{k} IS NOT NULL AND {k} != ''" for k in keys)
                covered = conn.execute(f"SELECT COUNT(*) FROM base_dataset WHERE {conditions}").fetchone()[0]
                blocking_quality[bs["name"]] = {"coverage_rate": round(covered / total, 4) if total > 0 else 0}

        profile = DatasetProfile(
            run_id=run_id,
            dataset_type=ds_type,
            dataset=DatasetInfo(row_count=total, side_counts=side_counts),
            columns=ColumnInfo(present=cols),
            amount_columns=AmountColumns(detected=detected_amounts),
            blocking_quality=blocking_quality,
        )

        logger.info("Profile: {} rows, type={}, {} amount cols, {} blocking strategies",
                     total, ds_type.value, len(detected_amounts), len(blocking_quality))
        return profile
    finally:
        conn.close()


# ─── Operator Execution ────────────────────────────────────────────────────

def duckdb_execute_operator(
    db_path: str,
    operator_spec: dict,
    runtime_config: dict,
    dataset_profile: dict,
    run_dir: str,
    run_id: str,
    ilog=None,
) -> tuple[OperatorResult, list[dict]]:
    """
    Execute a single operator against pool_unmatched.
    Returns (OperatorResult, list of MatchGroup/exclusion dicts for reasoning).
    """
    op = operator_spec["operator"]
    step_id = operator_spec["step_id"]
    rule_id = operator_spec["rule_id"]
    params = operator_spec.get("params", {})

    started = datetime.now(timezone.utc)

    conn = duckdb.connect(db_path)
    try:
        pool_before = conn.execute("SELECT COUNT(*) FROM pool_unmatched").fetchone()[0]

        if ilog:
            ilog.log("duckdb", f"Executing operator: {op}", {"rule_id": rule_id, "step_id": step_id})

        if op == "exclude_by_predicates":
            groups, sql = _execute_exclusion(conn, step_id, rule_id, params)
        elif op == "one_to_one_ranked":
            groups, sql = _execute_one_to_one(conn, step_id, rule_id, params)
        elif op == "many_to_many_balance_k":
            groups, sql = _execute_many_to_many(conn, step_id, rule_id, params)
        elif op == "one_sided":
            groups, sql = _execute_one_sided(conn, step_id, rule_id, params)
        else:
            raise ValueError(f"Unknown operator: {op}")

        if ilog:
            ilog.log_sql("duckdb", sql, context=f"{op} / {rule_id}")
            ilog.log_data("duckdb", f"Operator {op} produced {len(groups)} groups", {
                "groups_count": len(groups),
                "sample_group": groups[0] if groups else None,
            })

        pool_after = conn.execute("SELECT COUNT(*) FROM pool_unmatched").fetchone()[0]
        records_removed = pool_before - pool_after
        new_matched = conn.execute(
            "SELECT COUNT(*) FROM matched_records WHERE step_id = ?", [step_id]
        ).fetchone()[0]
        new_groups = len(groups)

    finally:
        conn.close()

    ended = datetime.now(timezone.utc)

    # Append SQL to audit trail
    _append_sql_audit(run_dir, step_id, rule_id, op, sql)

    result = OperatorResult(
        run_id=run_id,
        step_id=step_id,
        rule_id=rule_id,
        operator=OperatorType(op),
        status=StepStatus.OK,
        timing=OperatorTiming(
            started_at=started.isoformat(),
            ended_at=ended.isoformat(),
            duration_ms=int((ended - started).total_seconds() * 1000),
        ),
        effects=OperatorEffects(
            new_match_groups=new_groups,
            new_matched_records=new_matched,
            records_removed_from_pool=records_removed,
        ),
        artifacts=ArtifactFlags(sql_appended=True),
        evidence=OperatorEvidence(reason_code=rule_id if op == "exclude_by_predicates" else None),
    )

    return result, groups


def _qualify_filter(filt: str, alias: str) -> str:
    """Prefix unqualified column references in a filter with a table alias.
    Handles simple cases like (col == 'X') → (alias.col == 'X')."""
    if filt == "1=1":
        return filt
    import re
    # Match word boundaries that look like column names (not strings, not numbers, not SQL keywords)
    sql_keywords = {'IN', 'AND', 'OR', 'NOT', 'NULL', 'IS', 'LIKE', 'BETWEEN', 'TRUE', 'FALSE', 'AS', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'}
    def replace_col(match):
        word = match.group(0)
        if word.upper() in sql_keywords:
            return word
        if word.startswith("'") or word[0].isdigit():
            return word
        # Already qualified
        if '.' in word:
            return word
        return f"{alias}.{word}"

    # Split by strings first to avoid modifying string literals
    parts = re.split(r"('(?:[^'\\]|\\.)*')", filt)
    result = []
    for i, part in enumerate(parts):
        if i % 2 == 1:  # String literal
            result.append(part)
        else:
            # Replace unqualified identifiers
            result.append(re.sub(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b', replace_col, part))
    return ''.join(result)


# ─── Operator Implementations ──────────────────────────────────────────────

def _execute_exclusion(conn, step_id: int, rule_id: str, params: dict) -> tuple[list[dict], str]:
    """exclude_by_predicates: filter records matching side filters."""
    groups = []
    sqls = []

    for side, filter_key in [("A", "side_a_filter"), ("B", "side_b_filter")]:
        filt = params.get(filter_key)
        if not filt:
            continue

        sql = f"""
            INSERT INTO matched_records (match_group_id, side, statement_id, record_id, rule_id, step_id, operator)
            SELECT
                'EXCL_' || record_id AS match_group_id,
                side, statement_id, record_id,
                '{rule_id}', {step_id}, 'exclude_by_predicates'
            FROM pool_unmatched
            WHERE side = '{side}' AND {filt}
        """
        sqls.append(sql)

        # Get excluded records before inserting
        fetch_sql = f"SELECT side, statement_id, record_id FROM pool_unmatched WHERE side = '{side}' AND {filt}"
        excluded = conn.execute(fetch_sql).fetchall()

        for row in excluded:
            groups.append({
                "rule_id": rule_id,
                "record_refs": [{"side": row[0], "statement_id": str(row[1]), "record_id": str(row[2])}],
            })

        conn.execute(sql)

    return groups, "\n".join(sqls)


def _execute_one_to_one(conn, step_id: int, rule_id: str, params: dict) -> tuple[list[dict], str]:
    """one_to_one_ranked: 1:1 matching with blocking + amount tolerance + mutual best-match."""
    blocking = params.get("blocking_strategy", {})
    comparisons = params.get("comparisons", [])
    side_a_filter = params.get("side_a_filter", "1=1")
    side_b_filter = params.get("side_b_filter", "1=1")

    # Build blocking join condition
    block_join = "1=1"
    if blocking:
        keys_a = blocking.get("keys_a", [])
        keys_b = blocking.get("keys_b", [])
        if keys_a and keys_b:
            block_join = " AND ".join(f"a.{ka} = b.{kb}" for ka, kb in zip(keys_a, keys_b))

    # Build comparison conditions + score expression
    comp_conditions = []
    score_parts = []
    for comp in comparisons:
        col_a = comp["column_a"]
        col_b = comp["column_b"]
        tol = comp.get("tolerance", 0)
        behavior = comp.get("tolerance_behavior", "exclusive")
        op = "<" if behavior == "exclusive" else "<="

        comp_conditions.append(f"ABS(CAST(a.{col_a} AS DOUBLE) - CAST(b.{col_b} AS DOUBLE)) {op} {tol}")
        score_parts.append(f"ABS(CAST(a.{col_a} AS DOUBLE) - CAST(b.{col_b} AS DOUBLE))")

    comp_where = " AND ".join(comp_conditions) if comp_conditions else "1=1"
    score_expr = " + ".join(score_parts) if score_parts else "0"

    sql = f"""
        WITH candidates AS (
            SELECT
                a.record_id AS a_record_id, a.side AS a_side, a.statement_id AS a_statement_id,
                b.record_id AS b_record_id, b.side AS b_side, b.statement_id AS b_statement_id,
                {score_expr} AS match_score,
                ROW_NUMBER() OVER (PARTITION BY a.record_id ORDER BY {score_expr}, b.record_id) AS rank_for_a,
                ROW_NUMBER() OVER (PARTITION BY b.record_id ORDER BY {score_expr}, a.record_id) AS rank_for_b
            FROM pool_unmatched a
            JOIN pool_unmatched b ON a.side = 'A' AND b.side = 'B'
                AND {block_join}
            WHERE ({_qualify_filter(side_a_filter, 'a')}) AND ({_qualify_filter(side_b_filter, 'b')})
                AND {comp_where}
        ),
        mutual_best AS (
            SELECT * FROM candidates WHERE rank_for_a = 1 AND rank_for_b = 1
        )
        SELECT a_record_id, a_side, a_statement_id, b_record_id, b_side, b_statement_id, match_score
        FROM mutual_best
    """

    matches = conn.execute(sql).fetchall()
    groups = []

    for i, row in enumerate(matches):
        mg_id = f"MG_{step_id}_{i+1:06d}"
        # Insert both sides
        conn.execute("""
            INSERT INTO matched_records (match_group_id, side, statement_id, record_id, rule_id, step_id, operator)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [mg_id, row[1], str(row[2]), str(row[0]), rule_id, step_id, "one_to_one_ranked"])
        conn.execute("""
            INSERT INTO matched_records (match_group_id, side, statement_id, record_id, rule_id, step_id, operator)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [mg_id, row[4], str(row[5]), str(row[3]), rule_id, step_id, "one_to_one_ranked"])

        groups.append({
            "match_group_id": mg_id,
            "group_type": "one_to_one",
            "rule_id": rule_id,
            "record_refs": [
                {"side": row[1], "statement_id": str(row[2]), "record_id": str(row[0])},
                {"side": row[4], "statement_id": str(row[5]), "record_id": str(row[3])},
            ],
            "match_score": row[6],
        })

    return groups, sql


def _execute_many_to_many(conn, step_id: int, rule_id: str, params: dict) -> tuple[list[dict], str]:
    """many_to_many_balance_k: N:M matching with aggregated balance within tolerance."""
    blocking = params.get("blocking_strategy", {})
    comparisons = params.get("comparisons", [])
    side_a_filter = params.get("side_a_filter", "1=1")
    side_b_filter = params.get("side_b_filter", "1=1")

    keys_a = blocking.get("keys_a", [])
    keys_b = blocking.get("keys_b", [])

    if not keys_a or not comparisons:
        return [], "-- many_to_many: no blocking keys or comparisons, skipped"

    # Build aggregation per side per block
    block_key_a = ", ".join(f"a.{k}" for k in keys_a)
    block_key_b = ", ".join(f"b.{k}" for k in keys_b)

    agg_cols_a = []
    agg_cols_b = []
    balance_conditions = []

    for comp in comparisons:
        col_a = comp["column_a"]
        col_b = comp["column_b"]
        agg = comp.get("aggregation", "SumIgnoreNulls")
        tol = comp.get("tolerance", 0)
        behavior = comp.get("tolerance_behavior", "exclusive")
        op = "<" if behavior == "exclusive" else "<="

        agg_func = "SUM" if "Sum" in agg else agg.upper()
        if agg_func not in ("SUM", "MIN", "MAX", "AVG", "COUNT"):
            agg_func = "SUM"

        agg_cols_a.append(f"COALESCE({agg_func}(CAST({col_a} AS DOUBLE)), 0) AS agg_{col_a}")
        agg_cols_b.append(f"COALESCE({agg_func}(CAST({col_b} AS DOUBLE)), 0) AS agg_{col_b}")
        balance_conditions.append(f"ABS(sa.agg_{col_a} - sb.agg_{col_b}) {op} {tol}")

    agg_a_str = ", ".join(agg_cols_a)
    agg_b_str = ", ".join(agg_cols_b)
    balance_where = " AND ".join(balance_conditions)

    sql = f"""
        WITH side_a_agg AS (
            SELECT {block_key_a.replace('a.', '')}, {agg_a_str}
            FROM pool_unmatched a WHERE side = 'A' AND ({side_a_filter})
            GROUP BY {block_key_a.replace('a.', '')}
        ),
        side_b_agg AS (
            SELECT {block_key_b.replace('b.', '')}, {agg_b_str}
            FROM pool_unmatched b WHERE side = 'B' AND ({side_b_filter})
            GROUP BY {block_key_b.replace('b.', '')}
        ),
        balanced_blocks AS (
            SELECT sa.{keys_a[0]} AS block_key
            FROM side_a_agg sa
            JOIN side_b_agg sb ON {' AND '.join(f'sa.{ka} = sb.{kb}' for ka, kb in zip(keys_a, keys_b))}
            WHERE {balance_where}
        )
        SELECT block_key FROM balanced_blocks
    """

    blocks = conn.execute(sql).fetchall()
    groups = []

    for i, (block_key,) in enumerate(blocks):
        mg_id = f"MG_{step_id}_{i+1:06d}"

        # Get all A-side records in this block
        a_filter = f"side = 'A' AND ({side_a_filter}) AND {keys_a[0]} = '{block_key}'"
        a_records = conn.execute(f"SELECT side, statement_id, record_id FROM pool_unmatched WHERE {a_filter}").fetchall()

        # Get all B-side records
        b_filter = f"side = 'B' AND ({side_b_filter}) AND {keys_b[0]} = '{block_key}'"
        b_records = conn.execute(f"SELECT side, statement_id, record_id FROM pool_unmatched WHERE {b_filter}").fetchall()

        if not a_records or not b_records:
            continue

        refs = []
        for rec in a_records + b_records:
            conn.execute("""
                INSERT INTO matched_records (match_group_id, side, statement_id, record_id, rule_id, step_id, operator)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [mg_id, rec[0], str(rec[1]), str(rec[2]), rule_id, step_id, "many_to_many_balance_k"])
            refs.append({"side": rec[0], "statement_id": str(rec[1]), "record_id": str(rec[2])})

        groups.append({
            "match_group_id": mg_id,
            "group_type": "many_to_many_balance_k",
            "rule_id": rule_id,
            "record_refs": refs,
            "block_key": str(block_key),
        })

    return groups, sql


def _execute_one_sided(conn, step_id: int, rule_id: str, params: dict) -> tuple[list[dict], str]:
    """one_sided: A-vs-A self-matching where numeric columns net to 0."""
    blocking = params.get("blocking_strategy", {})
    comparisons = params.get("comparisons", [])
    side_filter = params.get("side_filter", "1=1")

    keys = blocking.get("keys", [])
    if not keys or not comparisons:
        return [], "-- one_sided: no blocking keys or comparisons, skipped"

    block_key = keys[0]
    comp = comparisons[0]  # Primary comparison
    col = comp["column"]
    tol = comp.get("tolerance", 0)
    agg = comp.get("aggregation", "SumIgnoreNulls")

    agg_func = "SUM" if "Sum" in agg else "SUM"

    sql = f"""
        WITH block_sums AS (
            SELECT {block_key},
                   COALESCE({agg_func}(CAST({col} AS DOUBLE)), 0) AS total,
                   COUNT(*) AS cnt
            FROM pool_unmatched
            WHERE side = 'A' AND ({side_filter})
            GROUP BY {block_key}
            HAVING cnt >= 2 AND ABS(COALESCE({agg_func}(CAST({col} AS DOUBLE)), 0)) <= {tol}
        )
        SELECT {block_key} FROM block_sums
    """

    blocks = conn.execute(sql).fetchall()
    groups = []

    for i, (bk,) in enumerate(blocks):
        mg_id = f"MG_{step_id}_{i+1:06d}"

        records = conn.execute(f"""
            SELECT side, statement_id, record_id
            FROM pool_unmatched
            WHERE side = 'A' AND ({side_filter}) AND {block_key} = '{bk}'
        """).fetchall()

        if len(records) < 2:
            continue

        refs = []
        for rec in records:
            conn.execute("""
                INSERT INTO matched_records (match_group_id, side, statement_id, record_id, rule_id, step_id, operator)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, [mg_id, rec[0], str(rec[1]), str(rec[2]), rule_id, step_id, "one_sided"])
            refs.append({"side": rec[0], "statement_id": str(rec[1]), "record_id": str(rec[2])})

        groups.append({
            "match_group_id": mg_id,
            "group_type": "one_sided",
            "rule_id": rule_id,
            "record_refs": refs,
            "block_key": str(bk),
        })

    return groups, sql


# ─── Audit & Export ─────────────────────────────────────────────────────────

def _append_sql_audit(run_dir: str, step_id: int, rule_id: str, operator: str, sql: str) -> None:
    """Append SQL block to matching_queries.sql."""
    audit_path = Path(run_dir) / "matching_queries.sql"
    header = f"-- step_id: {step_id} | rule_id: {rule_id} | operator: {operator}\n"
    with open(audit_path, "a") as f:
        f.write(header)
        f.write(sql.strip())
        f.write("\n\n")


def duckdb_export_exclusions_csv(db_path: str, run_dir: str) -> None:
    """Export excluded records to exclusions.csv."""
    conn = duckdb.connect(db_path)
    try:
        out = Path(run_dir) / "exclusions.csv"
        conn.execute(f"""
            COPY (
                SELECT bd.*
                FROM base_dataset bd
                JOIN matched_records mr ON bd.record_id = mr.record_id
                WHERE mr.operator = 'exclude_by_predicates'
            ) TO '{out}' (HEADER, DELIMITER ',')
        """)
        count = conn.execute(
            "SELECT COUNT(*) FROM matched_records WHERE operator = 'exclude_by_predicates'"
        ).fetchone()[0]
        logger.info("Exported {} exclusions to {}", count, out)
    finally:
        conn.close()


def duckdb_export_matches_csv(db_path: str, run_dir: str) -> None:
    """Export matched records to matches.csv."""
    conn = duckdb.connect(db_path)
    try:
        out = Path(run_dir) / "matches.csv"
        conn.execute(f"""
            COPY (
                SELECT match_group_id, side, statement_id, record_id
                FROM matched_records
                WHERE operator != 'exclude_by_predicates'
                ORDER BY match_group_id, side, record_id
            ) TO '{out}' (HEADER, DELIMITER ',')
        """)
        count = conn.execute(
            "SELECT COUNT(*) FROM matched_records WHERE operator != 'exclude_by_predicates'"
        ).fetchone()[0]
        logger.info("Exported {} matched records to {}", count, out)
    finally:
        conn.close()


def duckdb_export_breaks_csv(db_path: str, run_dir: str) -> None:
    """Export unmatched (break) records to breaks.csv."""
    conn = duckdb.connect(db_path)
    try:
        out = Path(run_dir) / "breaks.csv"
        conn.execute(f"""
            COPY (
                SELECT side, statement_id, record_id
                FROM pool_unmatched
                ORDER BY side, record_id
            ) TO '{out}' (HEADER, DELIMITER ',')
        """)
        count = conn.execute("SELECT COUNT(*) FROM pool_unmatched").fetchone()[0]
        logger.info("Exported {} breaks to {}", count, out)
    finally:
        conn.close()


def duckdb_get_pool_count(db_path: str) -> int:
    """Get current unmatched pool size."""
    conn = duckdb.connect(db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM pool_unmatched").fetchone()[0]
    finally:
        conn.close()
