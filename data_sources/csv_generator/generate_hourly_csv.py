"""Source 1: hourly pharmacy claims CSV files (simulates a vendor SFTP drop).

Quirks baked in: ~2% duplicates, ~5% late rows, schema evolution after cutoff.
member_id now drawn from a SHARED pool so gold-layer joins match.
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

from shared_ids import MEMBER_IDS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("csv_generator")

OUTPUT_DIR = Path(__file__).resolve().parents[2] / "data" / "landing" / "pharmacy_claims"
SCHEMA_CHANGE_DATE = datetime(2026, 7, 3)
ROWS_PER_FILE = 500
DUPLICATE_RATE = 0.02
LATE_RATE = 0.05

DRUGS = [
    ("00071-0155", "Lipitor 20mg", 32.50),
    ("00006-0749", "Januvia 100mg", 512.00),
    ("00002-7510", "Trulicity 1.5mg", 886.30),
    ("59762-1740", "Amlodipine 5mg", 4.10),
    ("68180-0512", "Lisinopril 10mg", 3.75),
    ("00093-7146", "Metformin 500mg", 5.20),
]


def make_row(file_hour: datetime, rng: random.Random) -> dict:
    ndc, drug_name, base_cost = rng.choice(DRUGS)
    if rng.random() < LATE_RATE:
        fill_time = file_hour - timedelta(minutes=rng.randint(60, 360))
    else:
        fill_time = file_hour + timedelta(minutes=rng.randint(0, 59))
    return {
        "claim_id": f"RX{rng.randint(10_000_000, 99_999_999)}",
        "member_id": rng.choice(MEMBER_IDS),
        "ndc_code": ndc,
        "drug_name": drug_name,
        "quantity": rng.choice([30, 60, 90]),
        "days_supply": rng.choice([30, 60, 90]),
        "fill_time": fill_time.strftime("%Y-%m-%d %H:%M:%S"),
        "ingredient_cost": round(base_cost * rng.uniform(0.9, 1.1), 2),
        "copay_amount": rng.choice([0.00, 5.00, 10.00, 25.00, 50.00]),
    }


def generate_file(file_hour: datetime, rng: random.Random) -> Path:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    evolved = file_hour >= SCHEMA_CHANGE_DATE
    rows = [make_row(file_hour, rng) for _ in range(ROWS_PER_FILE)]
    dup_count = int(ROWS_PER_FILE * DUPLICATE_RATE)
    rows.extend(rng.sample(rows, dup_count))
    rng.shuffle(rows)
    if evolved:
        for row in rows:
            row["pharmacy_npi"] = str(rng.randint(1_000_000_000, 1_999_999_999))
    path = OUTPUT_DIR / f"pharmacy_claims_{file_hour:%Y%m%d_%H}.csv"
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    log.info("wrote %s (%d rows, schema=%s)", path.name, len(rows), "v2" if evolved else "v1")
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hours", type=int, default=24)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    if args.start:
        start = datetime.fromisoformat(args.start).replace(minute=0, second=0, microsecond=0)
    else:
        start = (datetime.now() - timedelta(hours=args.hours)).replace(minute=0, second=0, microsecond=0)
    for i in range(args.hours):
        generate_file(start + timedelta(hours=i), rng)


if __name__ == "__main__":
    main()
