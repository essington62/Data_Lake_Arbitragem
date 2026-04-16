import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

from src.config import get_exchanges, get_instruments
from src.collectors import COLLECTOR_MAP

# Load configs once
EXCHANGES = get_exchanges()
INSTRUMENTS = get_instruments()
BTCUSDT = next(i for i in INSTRUMENTS if i["id"] == "BTCUSDT")


# ── Fixtures: real-world response shapes ──────────────────────────────────────

BINANCE_HISTORY = [
    {"symbol": "BTCUSDT", "fundingTime": 1713139200000, "fundingRate": "0.00010000"},
    {"symbol": "BTCUSDT", "fundingTime": 1713110400000, "fundingRate": "0.00008000"},
]
BINANCE_CURRENT = {
    "symbol": "BTCUSDT",
    "markPrice": "74300.00",
    "indexPrice": "74290.00",
    "lastFundingRate": "0.00010000",
    "nextFundingTime": 1713168000000,
}
BINANCE_ORDER_BOOK = {
    "lastUpdateId": 123456,
    "T": 1713139200000,
    "bids": [["74299.00", "0.500"], ["74298.00", "1.000"], ["74297.00", "2.000"],
             ["74296.00", "1.500"], ["74295.00", "3.000"], ["74294.00", "0.800"],
             ["74293.00", "1.200"], ["74292.00", "2.500"], ["74291.00", "0.300"],
             ["74290.00", "4.000"]],
    "asks": [["74300.00", "0.400"], ["74301.00", "0.900"], ["74302.00", "1.800"],
             ["74303.00", "1.300"], ["74304.00", "2.700"], ["74305.00", "0.600"],
             ["74306.00", "1.100"], ["74307.00", "2.200"], ["74308.00", "0.250"],
             ["74309.00", "3.500"]],
}

OKX_HISTORY = {
    "code": "0", "msg": "",
    "data": [
        {"instId": "BTC-USDT-SWAP", "fundingRate": "0.0001", "fundingTime": "1713139200000"},
        {"instId": "BTC-USDT-SWAP", "fundingRate": "0.00008", "fundingTime": "1713110400000"},
    ]
}
OKX_CURRENT = {
    "code": "0", "msg": "",
    "data": [
        {
            "instId": "BTC-USDT-SWAP",
            "fundingRate": "0.0001",
            "fundingTime": "1713139200000",
            "nextFundingTime": "1713168000000",
            "markPrice": "74300.00",
            "indexPrice": "74290.00",
        }
    ]
}
OKX_ORDER_BOOK = {
    "code": "0", "msg": "",
    "data": [{
        "bids": [["74299", "10", "0", "2"], ["74298", "20", "0", "3"]],
        "asks": [["74300", "8",  "0", "1"], ["74301", "15", "0", "2"]],
        "ts": "1713139200000",
    }]
}

BYBIT_HISTORY = {
    "retCode": 0, "retMsg": "OK",
    "result": {
        "list": [
            {"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingRateTimestamp": "1713139200000"},
            {"symbol": "BTCUSDT", "fundingRate": "0.00008", "fundingRateTimestamp": "1713110400000"},
        ]
    }
}
BYBIT_ORDER_BOOK = {
    "retCode": 0, "retMsg": "OK",
    "result": {
        "s": "BTCUSDT",
        "b": [["74299", "0.5"], ["74298", "1.0"]],
        "a": [["74300", "0.4"], ["74301", "0.9"]],
        "ts": 1713139200000,
    }
}

GATEIO_HISTORY = [
    {"t": 1713139200, "r": "0.0001", "contract": "BTC_USDT"},
    {"t": 1713110400, "r": "0.00008", "contract": "BTC_USDT"},
]
GATEIO_ORDER_BOOK = {
    "id": 123,
    "current": 1713139200000000000,  # nanoseconds
    "update": 1713139200000000000,
    "bids": [{"p": "74299", "s": 10}, {"p": "74298", "s": 20}],
    "asks": [{"p": "74300", "s": 8},  {"p": "74301", "s": 15}],
}

BITGET_HISTORY = {
    "code": "00000", "msg": "success",
    "data": [
        {"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingTime": "1713139200000"},
        {"symbol": "BTCUSDT", "fundingRate": "0.00008", "fundingTime": "1713110400000"},
    ]
}
BITGET_CURRENT = {
    "code": "00000", "msg": "success",
    "data": {"symbol": "BTCUSDT", "fundingRate": "0.0001", "nextSettlementTime": "1713168000000", "markPrice": "74300"}
}
BITGET_ORDER_BOOK = {
    "code": "00000", "msg": "success",
    "data": {
        "bids": [["74299", "0.5"], ["74298", "1.0"]],
        "asks": [["74300", "0.4"], ["74301", "0.9"]],
        "ts": "1713139200000",
    }
}

KUCOIN_CURRENT = {
    "code": "200000",
    "data": {"symbol": "XBTUSDTM", "granularity": 28800000, "timePoint": 1713139200000, "value": 0.0001, "predictedValue": 0.00009}
}
KUCOIN_HISTORY = {
    "code": "200000",
    "data": [
        {"symbol": "XBTUSDTM", "timePoint": 1713139200000, "fundingRate": 0.0001},
        {"symbol": "XBTUSDTM", "timePoint": 1713110400000, "fundingRate": 0.00008},
    ]
}
KUCOIN_ORDER_BOOK = {
    "code": "200000",
    "data": {
        "sequence": "123",
        "bids": [["74299", "0.5"], ["74298", "1.0"]],
        "asks": [["74300", "0.4"], ["74301", "0.9"]],
        "ts": 1713139200000,
    }
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def assert_record_schema(rec: dict, exchange: str):
    assert rec["exchange"] == exchange
    assert rec["symbol"] == "BTCUSDT"
    assert isinstance(rec["timestamp"], datetime)
    assert rec["timestamp"].tzinfo is not None  # UTC-aware
    assert isinstance(rec["funding_rate"], float)
    assert -1.0 <= rec["funding_rate"] <= 1.0


def assert_order_book_schema(book: dict, exchange: str):
    assert book["exchange"] == exchange
    assert book["symbol"] == "BTCUSDT"
    assert isinstance(book["timestamp"], datetime)
    assert book["timestamp"].tzinfo is not None
    assert isinstance(book["best_bid"], float)
    assert isinstance(book["best_ask"], float)
    assert book["best_ask"] > book["best_bid"], "crossed book"
    assert isinstance(book["spread_pct"], float)
    assert book["spread_pct"] > 0
    assert isinstance(book["mid_price"], float)
    assert book["mid_price"] > 0
    assert book["bid_depth_usd"] > 0
    assert book["ask_depth_usd"] > 0
    # Prices must be float, not string
    for lvl in book["bids"]:
        assert isinstance(lvl[0], float)
        assert isinstance(lvl[1], float)
    for lvl in book["asks"]:
        assert isinstance(lvl[0], float)
        assert isinstance(lvl[1], float)


# ── Tests ─────────────────────────────────────────────────────────────────────

class TestBinanceCollector:
    def _collector(self):
        return COLLECTOR_MAP["binance"](EXCHANGES["binance"], BTCUSDT)

    def test_collect_funding_rates(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BINANCE_HISTORY):
            records = c.collect_funding_rates()
        assert len(records) == 2
        for r in records:
            assert_record_schema(r, "binance")

    def test_collect_current_funding(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BINANCE_CURRENT):
            rec = c.collect_current_funding()
        assert_record_schema(rec, "binance")
        assert isinstance(rec["next_funding_time"], datetime)
        assert rec["mark_price"] > 0

    def test_collect_order_book(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BINANCE_ORDER_BOOK):
            book = c.collect_order_book()
        assert_order_book_schema(book, "binance")
        assert len(book["bids"]) == 10
        assert len(book["asks"]) == 10


class TestOKXCollector:
    def _collector(self):
        return COLLECTOR_MAP["okx"](EXCHANGES["okx"], BTCUSDT)

    def test_collect_funding_rates(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=OKX_HISTORY):
            records = c.collect_funding_rates()
        assert len(records) == 2
        for r in records:
            assert_record_schema(r, "okx")

    def test_collect_current_funding(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=OKX_CURRENT):
            rec = c.collect_current_funding()
        assert_record_schema(rec, "okx")

    def test_collect_order_book(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=OKX_ORDER_BOOK):
            book = c.collect_order_book()
        assert_order_book_schema(book, "okx")
        assert len(book["bids"]) == 2
        assert len(book["asks"]) == 2


class TestBybitCollector:
    def _collector(self):
        return COLLECTOR_MAP["bybit"](EXCHANGES["bybit"], BTCUSDT)

    def test_collect_funding_rates(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BYBIT_HISTORY):
            records = c.collect_funding_rates()
        assert len(records) == 2
        for r in records:
            assert_record_schema(r, "bybit")

    def test_collect_order_book(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BYBIT_ORDER_BOOK):
            book = c.collect_order_book()
        assert_order_book_schema(book, "bybit")


class TestGateIOCollector:
    def _collector(self):
        return COLLECTOR_MAP["gateio"](EXCHANGES["gateio"], BTCUSDT)

    def test_collect_funding_rates(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=GATEIO_HISTORY):
            records = c.collect_funding_rates()
        assert len(records) == 2
        for r in records:
            assert_record_schema(r, "gateio")

    def test_collect_order_book(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=GATEIO_ORDER_BOOK):
            book = c.collect_order_book()
        assert_order_book_schema(book, "gateio")


class TestBitgetCollector:
    def _collector(self):
        return COLLECTOR_MAP["bitget"](EXCHANGES["bitget"], BTCUSDT)

    def test_collect_funding_rates(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BITGET_HISTORY):
            records = c.collect_funding_rates()
        assert len(records) == 2
        for r in records:
            assert_record_schema(r, "bitget")

    def test_collect_current_funding(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BITGET_CURRENT):
            rec = c.collect_current_funding()
        assert_record_schema(rec, "bitget")

    def test_collect_order_book(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=BITGET_ORDER_BOOK):
            book = c.collect_order_book()
        assert_order_book_schema(book, "bitget")


class TestKuCoinCollector:
    def _collector(self):
        return COLLECTOR_MAP["kucoin"](EXCHANGES["kucoin"], BTCUSDT)

    def test_collect_current_funding(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=KUCOIN_CURRENT):
            rec = c.collect_current_funding()
        assert_record_schema(rec, "kucoin")

    def test_collect_funding_rates(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=KUCOIN_HISTORY):
            records = c.collect_funding_rates()
        assert len(records) == 2
        for r in records:
            assert_record_schema(r, "kucoin")

    def test_collect_order_book(self):
        c = self._collector()
        with patch.object(c, "fetch", return_value=KUCOIN_ORDER_BOOK):
            book = c.collect_order_book()
        assert_order_book_schema(book, "kucoin")
