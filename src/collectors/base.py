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

    def fetch(self, endpoint_key: str, extra_params: dict = None) -> dict:
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

        retry_attempts = self.collection_config.get("retry_attempts", 3)
        retry_delay = self.collection_config.get("retry_delay_seconds", 10)
        timeout = self.collection_config.get("timeout_seconds", 30)

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
