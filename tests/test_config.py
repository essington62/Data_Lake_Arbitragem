import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest
from src.config import get_exchanges, get_instruments, get_collection_config

REQUIRED_EXCHANGE_FIELDS = {"name", "base_url", "endpoints", "rate_limit", "symbol_format"}
REQUIRED_RATE_LIMIT_FIELDS = {"max_requests_per_minute", "backoff_seconds"}


def test_exchanges_have_required_fields():
    exchanges = get_exchanges()
    assert exchanges, "exchanges.yml is empty"
    for name, cfg in exchanges.items():
        missing = REQUIRED_EXCHANGE_FIELDS - cfg.keys()
        assert not missing, f"{name} missing fields: {missing}"


def test_exchanges_rate_limit_fields():
    exchanges = get_exchanges()
    for name, cfg in exchanges.items():
        rl = cfg.get("rate_limit", {})
        missing = REQUIRED_RATE_LIMIT_FIELDS - rl.keys()
        assert not missing, f"{name}.rate_limit missing: {missing}"


def test_instruments_have_exchange_symbols():
    instruments = get_instruments()
    exchanges = get_exchanges()
    enabled = {k for k, v in exchanges.items() if v.get("enabled")}

    for inst in instruments:
        syms = inst.get("exchange_symbols", {})
        missing = enabled - syms.keys()
        assert not missing, f"{inst['id']} missing exchange_symbols for: {missing}"


def test_collection_config_has_funding_rate():
    cfg = get_collection_config()
    assert "collection" in cfg
    assert "funding_rate" in cfg["collection"]
    fr = cfg["collection"]["funding_rate"]
    assert "retry_attempts" in fr
    assert "timeout_seconds" in fr


def test_instruments_nonempty():
    instruments = get_instruments()
    assert len(instruments) >= 1
