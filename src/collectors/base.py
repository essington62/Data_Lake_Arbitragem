from abc import ABC, abstractmethod
import requests
import time
import logging
from src.config import get_collection_config


class BaseCollector(ABC):
    def __init__(self, exchange_config: dict, instrument_config: dict):
        self.config = exchange_config
        self.instrument = instrument_config
        self.name = exchange_config["name"]
        self.base_url = exchange_config["base_url"]
        self.rate_limit = exchange_config["rate_limit"]
        self._collection_config = None
        self.logger = logging.getLogger(f"collector.{self.name}")

    @property
    def collection_config(self) -> dict:
        if self._collection_config is None:
            self._collection_config = get_collection_config()["collection"]["funding_rate"]
        return self._collection_config

    def get_symbol(self) -> str:
        exchange_key = self.name.lower()
        symbols = self.instrument.get("exchange_symbols", {})
        return symbols[exchange_key]

    def fetch(self, endpoint_key: str, extra_params: dict = None, section: str = "funding_rate") -> dict:
        endpoint = self.config["endpoints"][endpoint_key]
        symbol = self.get_symbol()

        # Build URL (handle path params like {symbol} in KuCoin)
        path = endpoint["path"]
        if "{symbol}" in path:
            path = path.replace("{symbol}", symbol)
        url = self.base_url + path

        # Build query params — substitute {symbol} in values
        params = {}
        for k, v in endpoint.get("params", {}).items():
            if isinstance(v, str):
                params[k] = v.replace("{symbol}", symbol)
            else:
                params[k] = v

        if extra_params:
            params.update(extra_params)

        cfg = get_collection_config()["collection"].get(section, self.collection_config)
        retry_attempts = cfg.get("retry_attempts", 3)
        retry_delay = cfg.get("retry_delay_seconds", 10)
        timeout = cfg.get("timeout_seconds", 30)

        for attempt in range(retry_attempts):
            try:
                resp = requests.get(url, params=params, timeout=timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1}/{retry_attempts} failed for {endpoint_key}: {e}")
                if attempt < retry_attempts - 1:
                    time.sleep(retry_delay)

        raise RuntimeError(
            f"Failed to fetch {endpoint_key} from {self.name} after {retry_attempts} attempts"
        )

    def _compute_book_metrics(self, bids: list[list[float]], asks: list[list[float]]) -> dict:
        """Compute derived fields from normalized bids/asks ([[price, qty], ...] as float)."""
        best_bid = bids[0][0]
        best_ask = asks[0][0]
        mid_price = (best_bid + best_ask) / 2
        spread_pct = (best_ask - best_bid) / mid_price * 100
        bid_depth_usd = sum(p * q for p, q in bids)
        ask_depth_usd = sum(p * q for p, q in asks)
        return {
            "best_bid": best_bid,
            "best_ask": best_ask,
            "mid_price": mid_price,
            "spread_pct": spread_pct,
            "bid_depth_usd": bid_depth_usd,
            "ask_depth_usd": ask_depth_usd,
        }

    @abstractmethod
    def collect_funding_rates(self) -> list[dict]:
        """Returns list of dicts with normalized schema:
        {
            "timestamp": datetime (UTC),
            "exchange": str,
            "symbol": str,
            "funding_rate": float,
            "next_funding_time": datetime or None,
            "mark_price": float or None,
            "index_price": float or None,
        }
        """

    @abstractmethod
    def collect_current_funding(self) -> dict:
        """Returns dict with current funding rate and next settlement."""

    @abstractmethod
    def collect_order_book(self) -> dict:
        """Returns dict with normalized order book snapshot:
        {
            "timestamp": datetime (UTC),
            "exchange": str,
            "symbol": str,
            "bids": list[list[float]],   # [[price, qty], ...] top N, float
            "asks": list[list[float]],   # [[price, qty], ...] top N, float
            "best_bid": float,
            "best_ask": float,
            "spread_pct": float,         # (best_ask - best_bid) / mid_price * 100
            "mid_price": float,
            "bid_depth_usd": float,
            "ask_depth_usd": float,
        }
        """
