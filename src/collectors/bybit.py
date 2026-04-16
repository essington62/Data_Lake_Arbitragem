from datetime import datetime, timezone
from .base import BaseCollector


def _ms_to_dt(ms) -> datetime:
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)


class BybitCollector(BaseCollector):
    """
    funding_rate: GET /v5/market/funding/history
      Response: {retCode, result: {list: [{symbol, fundingRate, fundingRateTimestamp(ms)}]}}

    No dedicated current endpoint — use last record from history as proxy.
    """

    def _unwrap(self, response: dict) -> list:
        if response.get("retCode") != 0:
            raise RuntimeError(f"Bybit API error: {response.get('retMsg', response)}")
        return response.get("result", {}).get("list", [])

    def collect_funding_rates(self) -> list[dict]:
        response = self.fetch("funding_rate")
        items = self._unwrap(response)
        results = []
        for item in items:
            try:
                results.append({
                    "timestamp": _ms_to_dt(item["fundingRateTimestamp"]),
                    "exchange": "bybit",
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
        # Bybit doesn't have a public premium index endpoint without auth for current funding
        # Use the most recent historical record as best proxy
        records = self.collect_funding_rates()
        if not records:
            raise RuntimeError("Bybit returned no funding rate records")
        latest = max(records, key=lambda r: r["timestamp"])
        latest["timestamp"] = datetime.now(timezone.utc)
        return latest

    def collect_order_book(self) -> dict:
        # Response: {retCode:0, result:{s:"BTCUSDT", b:[["price","qty"],...], a:[...], ts:<ms>}}
        response = self.fetch("order_book", section="order_book")
        if response.get("retCode") != 0:
            raise RuntimeError(f"Bybit order book error: {response.get('retMsg', response)}")
        result = response["result"]
        ts = _ms_to_dt(result["ts"])
        bids = [[float(lvl[0]), float(lvl[1])] for lvl in result["b"]]
        asks = [[float(lvl[0]), float(lvl[1])] for lvl in result["a"]]
        metrics = self._compute_book_metrics(bids, asks)
        return {
            "timestamp": ts,
            "exchange": "bybit",
            "symbol": self.instrument["id"],
            "bids": bids,
            "asks": asks,
            **metrics,
        }
