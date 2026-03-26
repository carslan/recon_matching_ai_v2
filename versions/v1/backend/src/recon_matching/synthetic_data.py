"""
Synthetic dataset generator for testing the reconciliation matching system.

Generates a realistic CSV dataset modeled after the asset/account schema.
Each operator scenario is explicitly crafted so the waterfall produces
verifiable, non-trivial results across all 4 operator types.

Key design:
- UNIQUE cusip per scenario group so operators don't contaminate each other
- Exclusion records use cusips 0-14 (won't collide with matching cusips)
- 1:1 matches use cusips 20-69
- Many-to-many uses cusips 70-79
- One-sided netting uses cusips 80-89
- Break records use cusips 90-99
"""

import csv
import random
from pathlib import Path

from loguru import logger

STATEMENT_ID_A = 354199217
STATEMENT_ID_B = 354199212
RECORD_ID_BASE = 35630639000


def _cusip(n: int) -> str:
    """Generate a unique cusip from an index."""
    return f"{n:06d}XX{n % 10}"


def _sec_id(n: int) -> str:
    return f"US{n:09d}"


def _base_record(side: str, fund: int, idx: int, cusip_n: int) -> dict:
    """Minimal record with identity + cusip. Caller fills scenario-specific fields."""
    return {
        "side": side,
        "fund": fund,
        "source": "S",
        "type": "",
        "nav": "",
        "broker_code": f"BR{100 + idx % 900}",
        "statement_id": STATEMENT_ID_A if side == "A" else STATEMENT_ID_B,
        "record_id": RECORD_ID_BASE + idx,
        "blk_cusip": _cusip(cusip_n),
        "sec_id_type": "CUSIP",
        "sec_id": _sec_id(cusip_n),
        "sec_desc": f"SECURITY {cusip_n}",
        "orig_face": 0.0,
        "cur_par": 0.0,
        "fee_amt": 0.0,
        "collateral": 0.0,
        "restricted": 0.0,
        "ind_amt": 0.0,
        "recv_amount": 0.0,
        "deliv_amount": 0.0,
        "avail_amt": 0.0,
        "deal_amount": 0.0,
        "foreign_amount": 0.0,
        "sec_type": "COMMON",
        "sec_group": "EQUITY",
        "ext_asset_info": "STK",
        "ref_entity": "Single",
        "maturity": "",
        "price_date": "2026-03-17",
        "ext_asset_info_null_safe": "STK",
        "sec_type_null_safe": "COMMON",
        "orig_face_plus_fee_amt_factor_plus_1": 0.0,
        "orig_face_minus_collateral_factor_minus_1": 0.0,
        "fail_face_coll": 0.0,
        "orig_face_abs": 0.0,
    }


def _enrich(rec: dict) -> dict:
    """Resolve computed columns."""
    rec["ext_asset_info_null_safe"] = rec["ext_asset_info"] if rec["ext_asset_info"] else "NULL_SAFE_EMPTY"
    rec["sec_type_null_safe"] = rec["sec_type"] if rec["sec_type"] else "NULL_SAFE_EMPTY"
    of = rec["orig_face"]
    fee = rec["fee_amt"]
    coll = rec["collateral"]
    rec["orig_face_plus_fee_amt_factor_plus_1"] = round(of + fee, 2)
    rec["orig_face_minus_collateral_factor_minus_1"] = round(of - coll, 2)
    rec["fail_face_coll"] = round(of + coll - fee, 2)
    rec["orig_face_abs"] = abs(of)
    rec["cur_par"] = rec.get("cur_par") or round(of * 1.01, 2)
    return rec


def generate_dataset(target_path: str, side_a_count: int = 160, side_b_count: int = 242, seed: int = 42) -> str:
    """
    Generate a synthetic dataset with verifiable matching scenarios.

    Approximate composition (for default 160A/242B):
      - 15 A + 15 B excluded by EXCL_CUST_DERIV (ext_asset_info IN FUT/OPT/BKL, sec_type not exempt)
      - 5 A + 5 B excluded by EXCL_COLLATERAL (sec_group=CASH, sec_type=COLLATERAL)
      - 5 A excluded by EXCL_FX_HEDGE (sec_group=FX, type=HEDGE)
      - 10 A + 10 B matched by EXACT_MATCH_FUND (sec_group=FUND, same blk_cusip, orig_face within 0.01)
      - 50 A + 50 B matched by EXACT_MATCH_BLKCUSIP (same blk_cusip, orig_face within 0.005)
      - 10 A + 20 B matched by many_to_many_balance_k (2 B records sum to 1 A amount per block)
      - 12 A matched by one_sided netting (6 pairs, positive + negative orig_face netting to 0)
      - Remainder: natural breaks (no matching counterpart)
    """
    random.seed(seed)
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    records = []
    idx = 0
    fund = 844350

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 1: Exclusions — EXCL_CUST_DERIV (cusips 0-14)
    # ext_asset_info IN (FUT, OPT, BKL) AND sec_type NOT IN (MUNI, GOVT, CD, AGENCY, CORP)
    # ──────────────────────────────────────────────────────────────────────
    for i in range(15):
        for side in ["A", "B"]:
            rec = _base_record(side, fund, idx, i)
            idx += 1
            rec["ext_asset_info"] = ["FUT", "OPT", "BKL"][i % 3]
            rec["sec_type"] = "COMMON"  # NOT in exempt list
            rec["orig_face"] = round(random.uniform(1000, 20000), 2)
            records.append(_enrich(rec))

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 2: Exclusions — EXCL_COLLATERAL (cusips 15-19)
    # sec_group=CASH, sec_type=COLLATERAL
    # ──────────────────────────────────────────────────────────────────────
    for i in range(5):
        for side in ["A", "B"]:
            rec = _base_record(side, fund, idx, 15 + i)
            idx += 1
            rec["sec_group"] = "CASH"
            rec["sec_type"] = "COLLATERAL"
            rec["collateral"] = round(random.uniform(100, 500), 2)
            rec["orig_face"] = round(random.uniform(500, 5000), 2)
            records.append(_enrich(rec))

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 3: Exclusions — EXCL_FX_HEDGE (A-side only, cusips 100-104)
    # sec_group=FX, type=HEDGE
    # ──────────────────────────────────────────────────────────────────────
    for i in range(5):
        rec = _base_record("A", fund, idx, 100 + i)
        idx += 1
        rec["sec_group"] = "FX"
        rec["type"] = "HEDGE"
        rec["orig_face"] = round(random.uniform(1000, 10000), 2)
        records.append(_enrich(rec))

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 4: EXACT_MATCH_FUND — 1:1 (cusips 20-29)
    # sec_group=FUND on both sides, same blk_cusip, orig_face diff < 0.01
    # ──────────────────────────────────────────────────────────────────────
    for i in range(10):
        amount = round(random.uniform(5000, 50000), 2)
        for side in ["A", "B"]:
            rec = _base_record(side, fund, idx, 20 + i)
            idx += 1
            rec["sec_group"] = "FUND"
            rec["orig_face"] = amount if side == "A" else round(amount + random.uniform(-0.009, 0.009), 2)
            records.append(_enrich(rec))

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 5: EXACT_MATCH_BLKCUSIP — 1:1 (cusips 30-79)
    # Same blk_cusip, orig_face diff <= 0.005
    # ──────────────────────────────────────────────────────────────────────
    for i in range(50):
        amount = round(random.uniform(1000, 100000), 2)
        for side in ["A", "B"]:
            rec = _base_record(side, fund, idx, 30 + i)
            idx += 1
            rec["orig_face"] = amount if side == "A" else round(amount + random.uniform(-0.004, 0.004), 2)
            records.append(_enrich(rec))

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 6: MANY_TO_MANY_BALANCE_K (cusips 200-209)
    # 1 A record, 2 B records per block where SUM(B) ≈ A within tolerance 0.001
    # Both sides have source IN (S,B) AND ref_entity=Multiple OR source=P
    # ──────────────────────────────────────────────────────────────────────
    for i in range(10):
        a_amount = round(random.uniform(10000, 50000), 2)
        b1_amount = round(a_amount * 0.6, 2)
        b2_amount = round(a_amount - b1_amount, 2)  # Exact balance

        cusip_n = 200 + i

        # A side
        rec_a = _base_record("A", fund, idx, cusip_n)
        idx += 1
        rec_a["source"] = "S"
        rec_a["ref_entity"] = "Multiple"
        rec_a["orig_face"] = a_amount
        records.append(_enrich(rec_a))

        # B side — record 1
        rec_b1 = _base_record("B", fund, idx, cusip_n)
        idx += 1
        rec_b1["source"] = "B"
        rec_b1["ref_entity"] = "Multiple"
        rec_b1["orig_face"] = b1_amount
        records.append(_enrich(rec_b1))

        # B side — record 2
        rec_b2 = _base_record("B", fund, idx, cusip_n)
        idx += 1
        rec_b2["source"] = "S"
        rec_b2["ref_entity"] = "Multiple"
        rec_b2["orig_face"] = b2_amount
        records.append(_enrich(rec_b2))

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 7: ONE_SIDED netting (cusips 300-305)
    # A-side only: 2 records per cusip with orig_face that sums to 0
    # ──────────────────────────────────────────────────────────────────────
    for i in range(6):
        amount = round(random.uniform(1000, 20000), 2)
        cusip_n = 300 + i

        rec_pos = _base_record("A", fund, idx, cusip_n)
        idx += 1
        rec_pos["orig_face"] = amount
        records.append(_enrich(rec_pos))

        rec_neg = _base_record("A", fund, idx, cusip_n)
        idx += 1
        rec_neg["orig_face"] = -amount  # Exact negative
        records.append(_enrich(rec_neg))

    # ──────────────────────────────────────────────────────────────────────
    # SCENARIO 8: Natural breaks — no matching counterpart
    # Fill remaining A and B to hit target counts
    # ──────────────────────────────────────────────────────────────────────
    a_so_far = sum(1 for r in records if r["side"] == "A")
    b_so_far = sum(1 for r in records if r["side"] == "B")

    for i in range(max(0, side_a_count - a_so_far)):
        rec = _base_record("A", fund, idx, 400 + i)
        idx += 1
        rec["orig_face"] = round(random.uniform(1000, 50000), 2)
        records.append(_enrich(rec))

    for i in range(max(0, side_b_count - b_so_far)):
        rec = _base_record("B", fund, idx, 500 + i)
        idx += 1
        rec["orig_face"] = round(random.uniform(1000, 50000), 2)
        records.append(_enrich(rec))

    # Write CSV
    fieldnames = list(records[0].keys())
    with open(target, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    a_count = sum(1 for r in records if r["side"] == "A")
    b_count = sum(1 for r in records if r["side"] == "B")
    logger.info(
        "Generated synthetic dataset: {} total rows (A={}, B={}) -> {}",
        len(records), a_count, b_count, target,
    )
    return str(target)
