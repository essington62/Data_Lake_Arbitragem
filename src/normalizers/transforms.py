import json
import logging
import pandas as pd
from datetime import datetime, timezone
from .schema import FUNDING_RATE_COLUMNS, DEDUP_KEYS, ORDER_BOOK_COLUMNS, ORDER_BOOK_DEDUP_KEYS

logger = logging.getLogger("normalizer.transforms")

REQUIRED_FIELDS = {"timestamp", "exchange", "symbol", "funding_rate"}
FUNDING_RATE_MIN = -1.0
FUNDING_RATE_MAX = 1.0

REQUIRED_OB_FIELDS = {"timestamp", "exchange", "symbol", "best_bid", "best_ask", "spread_pct",
                      "mid_price", "bid_depth_usd", "ask_depth_usd"}
MID_PRICE_MIN = 10_000.0
MID_PRICE_MAX = 200_000.0
SPREAD_PCT_WARN = 1.0


def validate_records(records: list[dict]) -> list[dict]:
    valid = []
    for rec in records:
        # Required fields check
        missing = REQUIRED_FIELDS - rec.keys()
        if missing:
            logger.warning(f"Dropping record missing fields {missing}: {rec}")
            continue

        rate = rec.get("funding_rate")
        if rate is None or not isinstance(rate, (int, float)):
            logger.warning(f"Dropping record with non-numeric funding_rate: {rec}")
            continue

        # Detect % vs decimal — rates like 0.01 are normal; rates like 1.0 could be 100%
        # Values > 1.0 or < -1.0 almost certainly mean the exchange returned percentage
        if abs(rate) > FUNDING_RATE_MAX:
            logger.warning(
                f"funding_rate={rate} out of range [-1.0, 1.0] for {rec.get('exchange')} "
                f"— likely percentage encoding bug. Dropping."
            )
            continue

        ts = rec.get("timestamp")
        if isinstance(ts, datetime) and ts > datetime.now(timezone.utc):
            logger.warning(f"Future timestamp {ts} for {rec.get('exchange')} — keeping but flagging")

        valid.append(rec)

    logger.info(f"Validated {len(valid)}/{len(records)} records")
    return valid


def to_dataframe(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=FUNDING_RATE_COLUMNS)

    df = pd.DataFrame(records, columns=FUNDING_RATE_COLUMNS)

    # Ensure UTC-aware datetimes
    for col in ["timestamp", "next_funding_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
    df["mark_price"] = pd.to_numeric(df["mark_price"], errors="coerce")
    df["index_price"] = pd.to_numeric(df["index_price"], errors="coerce")

    # Deduplicate
    before = len(df)
    df = df.drop_duplicates(subset=DEDUP_KEYS, keep="last")
    if len(df) < before:
        logger.info(f"Removed {before - len(df)} duplicate records")

    return df.reset_index(drop=True)


def validate_order_book_records(records: list[dict]) -> list[dict]:
    valid = []
    for rec in records:
        missing = REQUIRED_OB_FIELDS - rec.keys()
        if missing:
            logger.warning(f"OB: dropping record missing fields {missing}: exchange={rec.get('exchange')}")
            continue

        best_bid = rec.get("best_bid", 0)
        best_ask = rec.get("best_ask", 0)

        if best_bid <= 0 or best_ask <= 0:
            logger.warning(f"OB: non-positive prices bid={best_bid} ask={best_ask} — dropping {rec.get('exchange')}")
            continue

        if best_ask <= best_bid:
            logger.warning(
                f"OB: crossed book bid={best_bid} >= ask={best_ask} — "
                f"corrupted data, dropping {rec.get('exchange')}"
            )
            continue

        mid_price = rec.get("mid_price", 0)
        if not (MID_PRICE_MIN <= mid_price <= MID_PRICE_MAX):
            logger.warning(
                f"OB: mid_price={mid_price:.0f} outside sanity range "
                f"[{MID_PRICE_MIN:.0f}, {MID_PRICE_MAX:.0f}] — dropping {rec.get('exchange')}"
            )
            continue

        if rec.get("bid_depth_usd", 0) <= 0 or rec.get("ask_depth_usd", 0) <= 0:
            logger.warning(f"OB: zero depth for {rec.get('exchange')} — dropping")
            continue

        spread_pct = rec.get("spread_pct", 0)
        if spread_pct > SPREAD_PCT_WARN:
            logger.warning(
                f"OB: spread_pct={spread_pct:.4f}% > {SPREAD_PCT_WARN}% for "
                f"{rec.get('exchange')} — suspicious but keeping"
            )

        valid.append(rec)

    logger.info(f"OB validated {len(valid)}/{len(records)} records")
    return valid


def order_book_to_dataframe(records: list[dict]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(columns=ORDER_BOOK_COLUMNS)

    rows = []
    for rec in records:
        row = {k: rec.get(k) for k in ORDER_BOOK_COLUMNS if k not in ("bids_json", "asks_json")}
        row["bids_json"] = json.dumps(rec.get("bids", []))
        row["asks_json"] = json.dumps(rec.get("asks", []))
        rows.append(row)

    df = pd.DataFrame(rows, columns=ORDER_BOOK_COLUMNS)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")

    for col in ["best_bid", "best_ask", "spread_pct", "mid_price", "bid_depth_usd", "ask_depth_usd"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    before = len(df)
    df = df.drop_duplicates(subset=ORDER_BOOK_DEDUP_KEYS, keep="last")
    if len(df) < before:
        logger.info(f"OB: removed {before - len(df)} duplicate records")

    return df.reset_index(drop=True)
