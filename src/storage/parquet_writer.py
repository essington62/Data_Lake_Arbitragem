import logging
import pandas as pd
from pathlib import Path
from src.normalizers.transforms import to_dataframe
from src.normalizers.schema import DEDUP_KEYS

logger = logging.getLogger("storage.parquet_writer")


def write_funding_rates(records: list[dict], base_path: str = "data/normalized") -> None:
    if not records:
        logger.warning("write_funding_rates called with empty records — skipping")
        return

    df = to_dataframe(records)
    df["_date"] = df["timestamp"].dt.date

    written = 0
    for (exchange, date), group in df.groupby(["exchange", "_date"]):
        group = group.drop(columns=["_date"])
        part_dir = Path(base_path) / "funding_rates" / f"exchange={exchange}" / f"date={date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        outfile = part_dir / "data.parquet"

        if outfile.exists():
            existing = pd.read_parquet(outfile)
            group = pd.concat([existing, group], ignore_index=True).drop_duplicates(
                subset=DEDUP_KEYS, keep="last"
            )

        group.to_parquet(outfile, index=False, compression="snappy")
        written += len(group)
        logger.debug(f"Wrote {len(group)} rows → {outfile}")

    logger.info(f"write_funding_rates: {written} total rows across {df['exchange'].nunique()} exchanges")


def write_raw(records: list[dict], exchange: str, base_path: str = "data/raw") -> None:
    if not records:
        return

    from datetime import date
    today = date.today().isoformat()
    df = to_dataframe(records)

    out_dir = Path(base_path) / exchange
    out_dir.mkdir(parents=True, exist_ok=True)
    outfile = out_dir / f"funding_{today}.parquet"

    if outfile.exists():
        existing = pd.read_parquet(outfile)
        df = pd.concat([existing, df], ignore_index=True).drop_duplicates(
            subset=DEDUP_KEYS, keep="last"
        )

    df.to_parquet(outfile, index=False, compression="snappy")
    logger.debug(f"Raw write: {len(df)} rows → {outfile}")
