#!/usr/bin/env python
"""Calcula slippage estimado para diferentes tamanhos de posição em cada exchange."""

import json
import sys
import logging
from pathlib import Path
from datetime import date

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd

POSITION_SIZES_USD = [500, 1_000, 5_000]
DATA_DIR = Path("data/normalized/order_book")


def _load_latest_books() -> list[dict]:
    """Read the most recent parquet partition per exchange and return list of book dicts."""
    books = []
    if not DATA_DIR.exists():
        return books

    for exc_dir in sorted(DATA_DIR.iterdir()):
        if not exc_dir.is_dir() or not exc_dir.name.startswith("exchange="):
            continue
        exchange = exc_dir.name.split("=", 1)[1]
        # Get the most recent date partition
        date_dirs = sorted(exc_dir.iterdir(), reverse=True)
        for date_dir in date_dirs:
            parquet = date_dir / "data.parquet"
            if parquet.exists():
                df = pd.read_parquet(parquet)
                if df.empty:
                    continue
                # Latest snapshot
                row = df.sort_values("timestamp").iloc[-1]
                books.append({
                    "exchange": exchange,
                    "mid_price": float(row["mid_price"]),
                    "best_bid": float(row["best_bid"]),
                    "best_ask": float(row["best_ask"]),
                    "spread_pct": float(row["spread_pct"]),
                    "bid_depth_usd": float(row["bid_depth_usd"]),
                    "ask_depth_usd": float(row["ask_depth_usd"]),
                    "bids": json.loads(row["bids_json"]),
                    "asks": json.loads(row["asks_json"]),
                    "timestamp": str(row["timestamp"]),
                })
                break

    return books


def _calc_slippage(levels: list[list[float]], size_usd: float, side: str) -> float:
    """
    Simulate market order fill against order book levels.
    levels: [[price, qty_in_contracts], ...]
    Returns slippage % vs first level price.
    side: "buy" uses asks (ascending price), "sell" uses bids (descending price).
    """
    if not levels:
        return float("nan")

    ref_price = levels[0][0]
    remaining = size_usd
    total_cost = 0.0
    total_qty = 0.0

    for price, qty in levels:
        level_value = price * qty
        if level_value >= remaining:
            partial_qty = remaining / price
            total_cost += remaining
            total_qty += partial_qty
            remaining = 0
            break
        else:
            total_cost += level_value
            total_qty += qty
            remaining -= level_value

    if total_qty == 0:
        return float("nan")

    avg_price = total_cost / total_qty

    if remaining > 0:
        # Order larger than full book depth — show as nan (insufficient liquidity)
        return float("nan")

    if side == "buy":
        slippage = (avg_price - ref_price) / ref_price * 100
    else:
        slippage = (ref_price - avg_price) / ref_price * 100

    return slippage


def main() -> int:
    logging.basicConfig(level=logging.WARNING)
    books = _load_latest_books()

    if not books:
        print("No order book data found. Run: python scripts/collect_orderbook.py")
        return 1

    # Sort by best_ask for display
    books.sort(key=lambda b: b["exchange"])

    # Header
    col_w = 10
    size_headers = "  ".join(f"{'$' + str(s // 1000) + 'k':>10}" for s in POSITION_SIZES_USD)
    header = f"{'Exchange':<10} {'Mid Price':>10} {'Spread':>8}  {size_headers}  {'As of':>20}"
    print(header)
    print("-" * len(header))

    for b in books:
        spread_str = f"{b['spread_pct']:.4f}%"
        slippages = []
        for size in POSITION_SIZES_USD:
            # Buy slippage (walk asks)
            buy_slip = _calc_slippage(b["asks"], size, "buy")
            sell_slip = _calc_slippage(b["bids"], size, "sell")
            if buy_slip != buy_slip:  # nan
                slippages.append(f"{'n/a':>10}")
            else:
                # Show symmetric average buy/sell as cost estimate
                avg = (buy_slip + sell_slip) / 2 if sell_slip == sell_slip else buy_slip
                slippages.append(f"{avg:.4f}%".rjust(10))

        slippage_str = "  ".join(slippages)
        ts = b["timestamp"][:19] if b["timestamp"] else "n/a"
        print(f"{b['exchange']:<10} ${b['mid_price']:>9,.0f}  {spread_str:>7}  {slippage_str}  {ts:>20}")

    print()
    print("Cross-exchange arbitrage:")
    print("-" * 60)

    if len(books) >= 2:
        FEE_PCT = 0.04  # 0.04% taker per leg (typical)

        best_buy = min(books, key=lambda x: x["best_ask"])
        best_sell = max(books, key=lambda x: x["best_bid"])

        for size in POSITION_SIZES_USD:
            buy_slip = _calc_slippage(best_buy["asks"], size, "buy")
            sell_slip = _calc_slippage(best_sell["bids"], size, "sell")
            if buy_slip != buy_slip or sell_slip != sell_slip:
                print(f"  ${size:>6}: insufficient depth on one leg")
                continue

            gross_spread = (best_sell["best_bid"] - best_buy["best_ask"]) / best_buy["best_ask"] * 100
            total_cost_pct = 2 * FEE_PCT + buy_slip + sell_slip
            net_pct = gross_spread - total_cost_pct
            net_usd = net_pct / 100 * size

            status = "PROFITABLE" if net_usd > 0 else "NOT PROFITABLE"
            print(
                f"  ${size:>6}: Buy {best_buy['exchange']} ask=${best_buy['best_ask']:.1f} -> "
                f"Sell {best_sell['exchange']} bid=${best_sell['best_bid']:.1f} | "
                f"gross={gross_spread:+.4f}% fees+slip={total_cost_pct:.4f}% "
                f"net={net_pct:+.4f}% (${net_usd:+.2f}) [{status}]"
            )

        breakeven = 2 * FEE_PCT
        print(f"\n  Break-even spread (fees only, {FEE_PCT}% x2): {breakeven:.4f}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())
