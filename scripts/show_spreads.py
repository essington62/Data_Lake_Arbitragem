#!/usr/bin/env python
"""Mostra funding rates mais recentes e spread entre exchanges."""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))


def main():
    base = Path("data/normalized/funding_rates")
    if not base.exists():
        print("Sem dados ainda. Rode: python scripts/collect_funding.py")
        return

    # Load latest record per exchange from all parquet partitions
    frames = []
    for parquet_file in base.rglob("data.parquet"):
        try:
            frames.append(pd.read_parquet(parquet_file))
        except Exception as e:
            print(f"Warning: could not read {parquet_file}: {e}")

    if not frames:
        print("Nenhum arquivo parquet encontrado.")
        return

    df = pd.concat(frames, ignore_index=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

    # Latest record per exchange
    latest = df.sort_values("timestamp").groupby("exchange").last().reset_index()
    latest = latest.sort_values("funding_rate", ascending=False)

    # Print table
    print(f"\n{'Exchange':<12} {'Funding Rate':>14} {'Rate %/yr':>12} {'Next Settlement':<22} {'Mark Price':>12}")
    print("-" * 80)
    for _, row in latest.iterrows():
        rate_pct = row["funding_rate"] * 100
        annualized = row["funding_rate"] * 3 * 365 * 100  # 3x/day settlement
        next_ft = str(row.get("next_funding_time", "N/A"))[:19] if pd.notna(row.get("next_funding_time")) else "N/A"
        mark = f"${row['mark_price']:,.0f}" if pd.notna(row.get("mark_price")) and row.get("mark_price") else "N/A"
        print(f"{row['exchange']:<12} {rate_pct:>+13.4f}% {annualized:>+11.1f}% {next_ft:<22} {mark:>12}")

    if len(latest) >= 2:
        max_row = latest.iloc[0]
        min_row = latest.iloc[-1]
        spread = max_row["funding_rate"] - min_row["funding_rate"]
        spread_annual = spread * 3 * 365 * 100
        print("-" * 80)
        print(f"\nSpread: {spread * 100:+.4f}% ({spread_annual:+.1f}%/yr annualized)")
        print(f"Long: {min_row['exchange']} ({min_row['funding_rate'] * 100:+.4f}%)")
        print(f"Short: {max_row['exchange']} ({max_row['funding_rate'] * 100:+.4f}%)")

    print(f"\nData from: {df['timestamp'].max().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Exchanges: {', '.join(sorted(latest['exchange'].tolist()))}")


if __name__ == "__main__":
    main()
