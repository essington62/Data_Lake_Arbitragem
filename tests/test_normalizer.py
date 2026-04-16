import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from datetime import datetime, timezone
from src.normalizers.transforms import validate_records, to_dataframe
from src.normalizers.schema import FUNDING_RATE_COLUMNS


def _make_record(**kwargs):
    base = {
        "timestamp": datetime.now(timezone.utc),
        "exchange": "binance",
        "symbol": "BTCUSDT",
        "funding_rate": 0.0001,
        "next_funding_time": None,
        "mark_price": None,
        "index_price": None,
    }
    base.update(kwargs)
    return base


class TestValidateRecords:
    def test_valid_record_passes(self):
        rec = _make_record()
        result = validate_records([rec])
        assert len(result) == 1

    def test_rejects_out_of_range_rate(self):
        rec = _make_record(funding_rate=1.5)  # likely % encoding bug
        result = validate_records([rec])
        assert len(result) == 0

    def test_rejects_negative_out_of_range(self):
        rec = _make_record(funding_rate=-1.5)
        result = validate_records([rec])
        assert len(result) == 0

    def test_rejects_missing_required_field(self):
        rec = _make_record()
        del rec["funding_rate"]
        result = validate_records([rec])
        assert len(result) == 0

    def test_rejects_non_numeric_rate(self):
        rec = _make_record(funding_rate="bad")
        result = validate_records([rec])
        assert len(result) == 0

    def test_boundary_rates_pass(self):
        for rate in [0.0, 0.01, -0.01, 1.0, -1.0]:
            result = validate_records([_make_record(funding_rate=rate)])
            assert len(result) == 1, f"rate={rate} should pass"

    def test_multiple_records(self):
        records = [
            _make_record(exchange="binance"),
            _make_record(exchange="okx", funding_rate=2.0),  # invalid
            _make_record(exchange="bybit"),
        ]
        result = validate_records(records)
        assert len(result) == 2
        exchanges = {r["exchange"] for r in result}
        assert "okx" not in exchanges


class TestToDataframe:
    def test_empty_returns_empty_df(self):
        df = to_dataframe([])
        assert list(df.columns) == FUNDING_RATE_COLUMNS
        assert len(df) == 0

    def test_columns_match_schema(self):
        rec = _make_record()
        df = to_dataframe([rec])
        assert list(df.columns) == FUNDING_RATE_COLUMNS

    def test_deduplication(self):
        ts = datetime.now(timezone.utc)
        rec = _make_record(timestamp=ts, exchange="binance", funding_rate=0.0001)
        dup = _make_record(timestamp=ts, exchange="binance", funding_rate=0.0002)
        df = to_dataframe([rec, dup])
        assert len(df) == 1
        assert df.iloc[0]["funding_rate"] == 0.0002  # keep="last"

    def test_timestamps_are_utc(self):
        rec = _make_record()
        df = to_dataframe([rec])
        assert str(df["timestamp"].dtype) == "datetime64[ns, UTC]"
