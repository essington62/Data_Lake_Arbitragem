import logging
import pandas as pd
from datetime import datetime, timezone
from .schema import FUNDING_RATE_COLUMNS, DEDUP_KEYS

logger = logging.getLogger("normalizer.transforms")

REQUIRED_FIELDS = {"timestamp", "exchange", "symbol", "funding_rate"}
FUNDING_RATE_MIN = -1.0
FUNDING_RATE_MAX = 1.0


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
