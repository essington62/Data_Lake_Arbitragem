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

BYBIT_HISTORY = {
    "retCode": 0, "retMsg": "OK",
    "result": {
        "list": [
            {"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingRateTimestamp": "1713139200000"},
            {"symbol": "BTCUSDT", "fundingRate": "0.00008", "fundingRateTimestamp": "1713110400000"},
        ]
    }
}

GATEIO_HISTORY = [
    {"t": 1713139200, "r": "0.0001", "contract": "BTC_USDT"},
    {"t": 1713110400, "r": "0.00008", "contract": "BTC_USDT"},
]

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


# ── Helpers ───────────────────────────────────────────────────────────────────

def assert_record_schema(rec: dict, exchange: str):
    assert rec["exchange"] == exchange
    assert rec["symbol"] == "BTCUSDT"
    assert isinstance(rec["timestamp"], datetime)
    assert rec["timestamp"].tzinfo is not None  # UTC-aware
    assert isinstance(rec["funding_rate"], float)
    assert -1.0 <= rec["funding_rate"] <= 1.0


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
