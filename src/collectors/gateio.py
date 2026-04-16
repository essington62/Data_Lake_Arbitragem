from datetime import datetime, timezone
from .base import BaseCollector


class GateIOCollector(BaseCollector):
    """
    funding_rate: GET /api/v4/futures/usdt/funding_rate
      Response: list of [{t(unix seconds), r(rate), contract}]
      Note: 'contract' may not be in response — inferred from request param.
    """

    def collect_funding_rates(self) -> list[dict]:
        data = self.fetch("funding_rate")
        symbol = self.get_symbol()
        results = []
        for item in data:
            try:
                results.append({
                    "timestamp": datetime.fromtimestamp(int(item["t"]), tz=timezone.utc),
                    "exchange": "gateio",
                    "symbol": self.instrument["id"],
                    "funding_rate": float(item["r"]),
                    "next_funding_time": None,
                    "mark_price": None,
                    "index_price": None,
                })
            except (KeyError, ValueError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e} — {item}")
        return results

    def collect_current_funding(self) -> dict:
        # Gate.io: fetch with limit=1 to get latest
        data = self.fetch("funding_rate", extra_params={"limit": 1})
        if not data:
            raise RuntimeError("Gate.io returned no funding rate data")
        item = data[0]
        return {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "gateio",
            "symbol": self.instrument["id"],
            "funding_rate": float(item["r"]),
            "next_funding_time": None,
            "mark_price": None,
            "index_price": None,
        }
