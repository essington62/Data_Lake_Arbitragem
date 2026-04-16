#!/usr/bin/env python
"""Backfill 30 dias de funding rate history para todas as exchanges habilitadas."""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import get_exchanges, get_instruments, get_collection_config
from src.collectors import COLLECTOR_MAP
from src.normalizers.transforms import validate_records
from src.storage.parquet_writer import write_funding_rates, write_raw


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logger = logging.getLogger("backfill_funding")

    exchanges = get_exchanges()
    instruments = get_instruments()

    all_records: list[dict] = []
    errors: list[dict] = []

    for inst in instruments:
        for name, exc_config in exchanges.items():
            if not exc_config.get("enabled", False):
                continue
            if name not in inst.get("exchange_symbols", {}):
                continue
            if name not in COLLECTOR_MAP:
                continue

            try:
                collector = COLLECTOR_MAP[name](exc_config, inst)
                records = collector.collect_funding_rates()
                all_records.extend(records)
                logger.info(f"OK {name:8s}: {inst['id']} — {len(records)} historical records")
            except Exception as e:
                logger.error(f"FAIL {name}: {inst['id']} — {e}")
                errors.append({"exchange": name, "symbol": inst["id"], "error": str(e)})

    valid_records = validate_records(all_records)

    if valid_records:
        write_funding_rates(valid_records)
        # Group by exchange for raw writes
        by_exchange: dict[str, list] = {}
        for rec in valid_records:
            by_exchange.setdefault(rec["exchange"], []).append(rec)
        for ex, recs in by_exchange.items():
            write_raw(recs, ex)

    logger.info(f"Backfill done: {len(valid_records)} records, {len(errors)} errors")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
