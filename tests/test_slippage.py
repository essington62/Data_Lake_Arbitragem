import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from scripts.show_slippage import _calc_slippage


def _book(n_levels: int, base_price: float = 74300.0, qty: float = 1.0) -> list[list[float]]:
    """Synthetic order book: n levels at base_price + i*0.1, each with qty."""
    return [[base_price + i * 0.1, qty] for i in range(n_levels)]


class TestCalcSlippage:
    def test_buy_single_level_exact_fill(self):
        # 1 level: price=74300, qty=1 → 1 BTC = $74300 → $74300 order fills exactly at best ask
        asks = [[74300.0, 1.0]]
        slip = _calc_slippage(asks, 74300.0, "buy")
        assert slip == pytest.approx(0.0, abs=1e-6)

    def test_buy_walks_two_levels(self):
        # Level 0: 74300 × 0.5 BTC = $37150
        # Level 1: 74300.1 × 0.5 BTC = $37150.05
        # avg price > 74300 → positive slippage
        asks = [[74300.0, 0.5], [74300.1, 0.5]]
        slip = _calc_slippage(asks, 74300.0 * 0.5 + 74300.1 * 0.5 * 0.5, "buy")
        assert slip > 0

    def test_sell_single_level_exact_fill(self):
        bids = [[74299.0, 1.0]]
        slip = _calc_slippage(bids, 74299.0, "sell")
        assert slip == pytest.approx(0.0, abs=1e-6)

    def test_buy_shallow_book_returns_nan(self):
        # Order size > total depth → nan
        asks = [[74300.0, 0.001]]  # $74.3 available
        slip = _calc_slippage(asks, 1_000_000.0, "buy")
        assert slip != slip  # nan

    def test_sell_shallow_book_returns_nan(self):
        bids = [[74299.0, 0.001]]
        slip = _calc_slippage(bids, 1_000_000.0, "sell")
        assert slip != slip  # nan

    def test_empty_book_returns_nan(self):
        assert _calc_slippage([], 1000.0, "buy") != _calc_slippage([], 1000.0, "buy")  # nan != nan

    def test_10_level_book_small_order(self):
        # $500 order against 10-level book (1 BTC each at $74300+) — should fill first level
        asks = _book(10, base_price=74300.0, qty=1.0)
        slip = _calc_slippage(asks, 500.0, "buy")
        # $500 / $74300 = 0.00673 BTC — fits in first level, slippage ≈ 0
        assert slip == pytest.approx(0.0, abs=1e-6)

    def test_slippage_increases_with_size(self):
        asks = _book(10, base_price=74300.0, qty=0.1)  # each level ~$7430
        small = _calc_slippage(asks, 500.0, "buy")
        large = _calc_slippage(asks, 50_000.0, "buy")
        # large order must cross more levels → higher slippage (or nan if insufficient)
        if large == large:  # not nan
            assert large >= small
