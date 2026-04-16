#!/usr/bin/env python
"""Coleta funding rates atuais de todas as exchanges habilitadas."""

import sys
import logging
import logging.handlers
from pathlib import Path

# Ensure src/ is importable when run from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import get_exchanges, get_instruments, get_collection_config
from src.collectors import COLLECTOR_MAP
from src.normalizers.transforms import validate_records
from src.storage.parquet_writer import write_funding_rates, write_raw


def setup_logging():
    log_cfg = get_collection_config().get("logging", {})
    level = getattr(logging, log_cfg.get("level", "INFO"))
    log_file = log_cfg.get("file", "logs/collection.log")

    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    handler_file = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=log_cfg.get("max_size_mb", 50) * 1024 * 1024,
        backupCount=log_cfg.get("backup_count", 5),
    )
    handler_stdout = logging.StreamHandler(sys.stdout)

    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        handlers=[handler_file, handler_stdout],
    )


def main() -> int:
    setup_logging()
    logger = logging.getLogger("collect_funding")

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
                logger.warning(f"No collector implemented for {name}, skipping")
                continue

            try:
                collector = COLLECTOR_MAP[name](exc_config, inst)
                current = collector.collect_current_funding()
                all_records.append(current)
                rate_pct = current["funding_rate"] * 100
                logger.info(
                    f"OK {name:8s}: {inst['id']} rate={rate_pct:+.4f}% "
                    f"next={current.get('next_funding_time', 'N/A')}"
                )
            except Exception as e:
                logger.error(f"FAIL {name}: {inst['id']} — {e}")
                errors.append({"exchange": name, "symbol": inst["id"], "error": str(e)})

    valid_records = validate_records(all_records)

    if valid_records:
        write_funding_rates(valid_records)
        for rec in valid_records:
            write_raw([rec], rec["exchange"])

    # Spread summary
    if len(valid_records) >= 2:
        rates = {r["exchange"]: r["funding_rate"] for r in valid_records}
        max_ex = max(rates, key=rates.get)
        min_ex = min(rates, key=rates.get)
        spread = rates[max_ex] - rates[min_ex]
        logger.info(
            f"SPREAD: {spread * 100:+.4f}%  "
            f"MAX={max_ex}({rates[max_ex] * 100:+.4f}%)  "
            f"MIN={min_ex}({rates[min_ex] * 100:+.4f}%)"
        )

    logger.info(f"Done: {len(valid_records)} records collected, {len(errors)} errors")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
