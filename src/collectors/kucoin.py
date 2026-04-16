from datetime import datetime, timezone
from .base import BaseCollector


def _ms_to_dt(ms) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


class KuCoinCollector(BaseCollector):
    """
    funding_rate_current: GET /api/v1/funding-rate/{symbol}/current
      Response: {code, data: {symbol, granularity, timePoint(ms), value, predictedValue}}
      Note: symbol in PATH, not query param.

    funding_rate_history: GET /api/v1/contract/funding-rates
      Response: {code, data: [{symbol, timePoint(ms), fundingRate, ...}]}
    """

    def _unwrap(self, response: dict) -> dict | list:
        if str(response.get("code")) != "200000":
            raise RuntimeError(f"KuCoin API error: {response.get('msg', response)}")
        return response.get("data", {})

    def collect_funding_rates(self) -> list[dict]:
        # history endpoint requires from/to timestamps
        import time as _time
        now_ms = int(_time.time() * 1000)
        days_back = 30
        start_ms = now_ms - days_back * 24 * 3600 * 1000

        response = self.fetch(
            "funding_rate_history",
            extra_params={"from": start_ms, "to": now_ms},
        )
        items = self._unwrap(response)
        if isinstance(items, dict):
            items = [items]

        results = []
        for item in items:
            try:
                # KuCoin history uses lowercase 'timepoint'; current uses 'timePoint'
                ts_raw = item.get("timePoint") or item.get("timepoint")
                results.append({
                    "timestamp": _ms_to_dt(ts_raw),
                    "exchange": "kucoin",
                    "symbol": self.instrument["id"],
                    "funding_rate": float(item["fundingRate"]),
                    "next_funding_time": None,
                    "mark_price": None,
                    "index_price": None,
                })
            except (KeyError, ValueError, TypeError) as e:
                self.logger.warning(f"Skipping malformed record: {e} — {item}")
        return results

    def collect_current_funding(self) -> dict:
        response = self.fetch("funding_rate_current")
        item = self._unwrap(response)
        if isinstance(item, list):
            item = item[0]
        return {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "kucoin",
            "symbol": self.instrument["id"],
            "funding_rate": float(item["value"]),
            "next_funding_time": None,
            "mark_price": None,
            "index_price": None,
        }
