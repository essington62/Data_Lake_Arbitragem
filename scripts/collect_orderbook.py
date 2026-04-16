#!/usr/bin/env python
"""Coleta order book snapshots de todas as exchanges habilitadas."""

import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config import get_exchanges, get_instruments
from src.collectors import COLLECTOR_MAP
from src.normalizers.transforms import validate_order_book_records
from src.storage.parquet_writer import write_order_book


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    logger = logging.getLogger("collect_orderbook")

    exchanges = get_exchanges()
    instruments = get_instruments()
    all_records = []
    errors = []

    for inst in instruments:
        for name, exc_config in exchanges.items():
            if not exc_config.get("enabled", False):
                continue
            collector_cls = COLLECTOR_MAP.get(name)
            if collector_cls is None:
                logger.warning(f"No collector registered for {name} — skipping")
                continue
            try:
                collector = collector_cls(exc_config, inst)
                book = collector.collect_order_book()
                all_records.append(book)
                logger.info(
                    f"OK {name:8s}: {inst['id']} "
                    f"bid={book['best_bid']:.1f} ask={book['best_ask']:.1f} "
                    f"spread={book['spread_pct']:.4f}% "
                    f"depth=${book['bid_depth_usd'] / 1000:.0f}k/${book['ask_depth_usd'] / 1000:.0f}k"
                )
            except Exception as e:
                logger.error(f"FAIL {name}: {inst['id']} — {e}")
                errors.append({"exchange": name, "error": str(e)})

    valid = validate_order_book_records(all_records)
    write_order_book(valid)

    # Cross-exchange spread summary
    if len(valid) >= 2:
        best_buy = min(valid, key=lambda x: x["best_ask"])
        best_sell = max(valid, key=lambda x: x["best_bid"])
        cross_spread = (best_sell["best_bid"] - best_buy["best_ask"]) / best_buy["best_ask"] * 100
        logger.info(
            f"CROSS: Buy {best_buy['exchange']} ask=${best_buy['best_ask']:.1f} -> "
            f"Sell {best_sell['exchange']} bid=${best_sell['best_bid']:.1f} -> "
            f"spread={cross_spread:+.4f}%"
        )

    logger.info(f"Done: {len(valid)} records stored, {len(errors)} errors")
    return 0 if not errors else 1


if __name__ == "__main__":
    sys.exit(main())
